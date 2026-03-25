# NDC Product Matching Pipeline

A system that matches supplier catalog products (identified by MPN) to Amazon listings (identified by ASIN) using product code matching, fuzzy title comparison, attribute validation, and optional GPT AI verification.

Built for medical supplies and B2B products.

---

## Project Structure

Each brand/manufacturer gets its own folder under `data/`:

```
data/
  NDC/
    input/
      catalog.xlsx    ← your catalog (must have mpn + title columns)
      keepa.xlsx      ← Keepa export (.xlsx multi-sheet or .csv)
    output/
      NDC.db          ← auto-generated SQLite database
      results.xlsx    ← auto-generated match results
  Medline/
    input/
      catalog.xlsx
      keepa.xlsx
    output/
      Medline.db
      results.xlsx
```

---

## Quick Start

```bash
pip install -r requirements.txt

# Run the full pipeline for a brand (recommended)
python run_pipeline.py --brand NDC

# Add filters or skip stages
python run_pipeline.py --brand NDC --medium-min-fuzzy 40
python run_pipeline.py --brand NDC --skip-build       # re-run SQL + export only
python run_pipeline.py --brand NDC --skip-sql         # re-export only

# Optional: fill missing quantities using GPT, then re-score
python run_pipeline.py --brand NDC --ai-parse-qty

# Optional: verify uncertain matches with GPT
python scripts/ai_verify.py --db data/NDC/output/NDC.db
python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories High Medium
python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories Certain High Medium
python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories High Medium --parse-qty

# Re-export after AI verification
python run_pipeline.py --brand NDC --skip-build --skip-sql

# Or run steps individually with explicit paths:
python scripts/main.py --catalog data/NDC/input/catalog.xlsx --keepa data/NDC/input/keepa.xlsx --db data/NDC/output/NDC.db
python scripts/run_sql.py --db data/NDC/output/NDC.db --sql scripts/pipeline.sql
python scripts/export_excel.py --db data/NDC/output/NDC.db --out data/NDC/output/results.xlsx
```

---

## How It Works

The pipeline has 4 stages:

### Stage 1: Parse & Normalize (`main.py`)

Reads catalog and Keepa files and normalizes them into a SQLite database.

**Keepa input:** Accepts `.xlsx` (single or multi-sheet — all sheets are combined automatically) or `.csv`.

**Catalog file** columns used: `MPN`, `Manufacturer`, `Title`, `uom_unit`, `uom_quantity`

**Keepa file** columns used: `ASIN`, `Title`, `Brand`, `Model`, `Color`, `Size`, `Number of Items`, `Package: Quantity`, `Product Codes: UPC/EAN/GTIN/PartNumber`, pricing fields (`Buy Box`, `FBA Pick&Pack Fee`, `Referral Fee`, etc.), `Bought in past month`

For each product, the parser extracts:
- **Quantity** — pack/case count (e.g., "Pack of 12" → 12). Supports hierarchical patterns like "2/pk, 6pk/cs" → 12 total eaches. Wipes and bandages are exceptions: "100/pk, 5pk/cs" → 5 (inner sheet count is ignored).
- **Color** — from a lexicon of ~80 colors with alias normalization (e.g., "grey" → "gray")
- **Size** — dimensions ("4x6 inch"), measurements ("20mm"), or categories ("XL"). Handles:
  - Fractions in hyphenated ("4-1/2") and space-separated ("3 3/4") formats, converted to decimal before comparison ("3 3/4" → 3.75)
  - Labeled dimension format: "3\" Width, 8\" Length" → "3 X 8 IN"
  - Compact formats: "4X4" → "4 X 4"
  - Size variants matched longest-first to prevent "L" matching before "XL"
- **Product codes** — normalized MPN, UPC, EAN, GTIN for cross-referencing
- **Manufacturer fallback** — when the `Manufacturer` column is empty, the first meaningful alphabetic word from the catalog title is used (e.g., "CURITY GZE SPG 4X4" → manufacturer "curity"). This enables brand matching for catalogs without a dedicated manufacturer column.

Quantity resolution priority:
1. `uom_quantity` field from the spreadsheet
2. Hierarchical title parsing (e.g., "2/pk, 6pk/cs" → 12)
3. Simple title parsing (e.g., "Pack of 12" → 12)

**Size field cleanup (Keepa):** If the Keepa `Size` column contains packaging text (e.g., "1 Pair (Pack of 1)", "24 count") instead of actual dimensions, it is cleared and the size is re-extracted from the product title instead.

Outputs 4 database tables: `catalog_std`, `catalog_codes`, `keepa_std`, `keepa_codes`.

### Stage 2: Match & Score (`pipeline.sql` via `run_sql.py`)

Runs inside SQLite with custom functions registered from Python (`rapidfuzz` if installed, otherwise `difflib`).

**Step 1 — Code Frequency Analysis:** Assigns a rarity score to each product code. Common codes (appearing in many ASINs) get lower weight rather than being dropped entirely.

**Step 2 — Candidate Generation:** Joins catalog product codes to Keepa product codes using rarity-weighted scoring.

**Step 3 — Validation Flags:** For each candidate pair (MPN ↔ ASIN), computes:

| Flag | What it checks | Weight |
|------|---------------|--------|
| `flag_code_match` | Product code matched (weighted by rarity) | up to +20 |
| `flag_brand_match` | Manufacturer matches brand (exact or substring, min 3 chars) | +15 |
| `flag_code_in_title` | MPN appears in Amazon title | +10 |
| `flag_qty_match` | Quantities are equal | +25 |
| `flag_qty_both_null` | Both quantities unknown | +10 |
| `flag_qty_ratio_match` | One qty is a clean 2x–100x multiple of the other (case/box relationship) | +10 |
| `flag_color_match` | Colors match | +15 |
| `flag_size_match` | Sizes match exactly or within 3% per dimension | +15 |
| `flag_qty_mismatch` | Quantities differ AND not a clean multiple (tiered penalty by fuzzy score) | -15 to -45 |
| `flag_unit_type_mismatch` | Catalog sells singles (uom EA/each) but ASIN qty > 10; suppressed when MPN appears in Amazon title | -20 |
| `flag_color_mismatch` | Colors differ | -15 |
| `flag_size_mismatch` | Sizes differ (exact, approximate, and prefix checks all clear) | -15 |

**Qty ratio match:** If `catalog_qty / keepa_qty` or `keepa_qty / catalog_qty` is a clean integer between 2 and 100, the pair is treated as a case/box relationship (e.g., catalog sells a case of 720, Keepa sells a box of 20). This suppresses the mismatch penalty and awards +10.

**Unit type mismatch suppression:** `flag_unit_type_mismatch` is suppressed when the MPN (≥4 chars) appears literally in the Amazon title. In that case product identity is confirmed and "1 EA" in the catalog most likely means "1 orderable unit of this product" (which may itself be a bulk pack), not a literal single-item mismatch.

**Size comparison logic:**
1. Exact string match → `flag_size_match`
2. Approximate numeric match within 3% per dimension (e.g., "4.5 X 4.1 YD" ≈ "4.5 X 4.125YD") → `flag_size_match`
3. One size is a leading prefix of the other (e.g., catalog "4" vs Keepa "4 X 75") → no mismatch penalty (partial info, not contradiction)
4. Otherwise → `flag_size_mismatch`

Also computes a `fuzzy_score` (0–100) using token-set-ratio on cleaned titles, plus business metrics: `break_even_cost_now`, `break_even_cost_30day`, `estimate_revenue`, `asin_cases`.

**Step 4 — Confidence Score:** Weighted sum of flags + fuzzy bonus (roughly 0–125 scale).

**Step 5 — Categorization:**

Uses a sliding scale: as fuzzy score rises, less confidence is required to reach Certain. The full decision table (evaluated top to bottom, first match wins):

| Category | Criteria |
|----------|----------|
| **Certain** | Fuzzy ≥90 + no negatives |
| **Certain** | Fuzzy ≥90 + confidence ≥50 + only qty mismatch (no size/color/unit-type conflict) — near-identical titles, qty difference is a catalog data issue |
| **Certain** | Fuzzy ≥85 + confidence ≥60 + no negatives |
| **Certain** | Fuzzy ≥85 + code match + no negatives |
| **Certain** | Fuzzy ≥75 + confidence ≥65 + no negatives |
| **Certain** | Fuzzy ≥40 + code + brand + (qty match / ratio / both-null / fuzzy ≥70) + no negatives |
| **Certain** | Fuzzy ≥40 + confidence ≥70 + no negatives |
| **Certain** | Code + brand + qty match + no negatives + confidence ≥55 |
| **Certain** | Code + brand + (size match OR color match) + no qty/unit mismatch + no negatives + fuzzy ≥25 |
| **High** | Fuzzy ≥85 + ≤1 negative + no unit-type mismatch — at this fuzzy level, qty mismatch is likely a catalog data issue |
| **High** | Fuzzy ≥75 + ≤1 negative + no qty mismatch + no unit-type mismatch |
| **High** | Fuzzy ≥60 + code match + no qty mismatch + no unit-type mismatch |
| **High** | Fuzzy ≥25 + code + brand + no qty mismatch + no unit-type mismatch |
| **High** | Fuzzy ≥25 + confidence ≥50 + no qty mismatch + no unit-type mismatch |
| **Medium** | Fuzzy ≥40 + code match + 2+ positive flags |
| **Medium** | Confidence ≥30 + 2+ positive flags |
| **Medium** | Code + brand + fuzzy ≥25 + ≤1 negative |
| **Medium** | Code + no negatives + fuzzy ≥15 + confidence ≥20 |
| **Verify** | Fuzzy <30; OR 2+ negative flags; OR qty/unit mismatch; OR confidence <20; OR <2 positive flags with fuzzy <40 |

**Hard floor:** Matches with fuzzy < 15, or fuzzy < 25 with 2+ negative flags and negative confidence, are excluded entirely from output.

**Step 6 — Deduplication:** Keeps the single best match per ASIN, ranked by category → confidence → fuzzy → code length → positive flags → negative flags.

### Stage 3: Export (`export_excel.py`)

Writes an Excel workbook with sheets: **Summary**, **Certain**, **High**, **Medium**, **Verify**, and **All Matches** (if ≤100k rows). Each sheet is sorted by confidence score descending.

Supports per-category fuzzy filtering so weaker categories can be held to a higher standard before export.

### Stage 4 (Optional): AI Verification (`ai_verify.py`)

Sends matches to GPT for AI-powered verification. GPT evaluates whether two product listings are truly the same product. The prompt provides medical domain context (common abbreviations: SPG=sponge, STR=sterile, NS=non-sterile, NW=non-woven, etc.) and includes each match's quantities and fuzzy score so GPT can make an informed decision.

Results update the match categories:

- AI confidence ≥0.9 AND fuzzy ≥40 → Certain
- AI confidence ≥0.7 → High
- AI confidence ≥0.3 → Medium
- AI confidence <0.3 → floored by SQL signals (see below)

**Downgrade protection:** GPT can downgrade a match but is bounded by SQL signal strength. When GPT confidence < 0.3, the category floors are:

| SQL signal | Floor |
|---|---|
| Fuzzy ≥90 | High |
| Fuzzy ≥75 + confidence ≥40 | High |
| Confidence ≥50 (strong multi-signal) | Medium |
| Fuzzy ≥50 (decent title overlap) | Medium |
| Otherwise | Verify |

This prevents abbreviated catalog titles (e.g., "2634 SPG GAUZE 4X4-12PLY") from being downgraded to Verify just because GPT sees a short title.

**Parse error fallback:** If GPT returns malformed JSON in a batch call, each match is automatically retried individually with a simpler prompt. If that also fails, the match defaults to confidence 0.65 (stays High) rather than an arbitrary 0.5.

**Fuzzy floors before sending to GPT:** High matches require fuzzy ≥30. Medium matches default to fuzzy 50–90.

Default behavior verifies **High** matches (fuzzy 30–85). Use `--categories` to control which tiers are verified. A per-tier fuzzy floor can be set to reduce API cost.

After verification, `--parse-qty` optionally uses GPT to fill missing quantities for confirmed matches.

---

## Typical Workflows

### Standard run (no AI)
```bash
python run_pipeline.py --brand NDC
```

### Run with GPT qty fill (fills missing quantities, re-scores)
```bash
python run_pipeline.py --brand NDC --ai-parse-qty
```

### Run pipeline + AI verification + re-export
```bash
python run_pipeline.py --brand NDC
python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories High Medium
python run_pipeline.py --brand NDC --skip-build --skip-sql
```

### Run pipeline + AI qty fill + AI verification + re-export
```bash
python run_pipeline.py --brand NDC --ai-parse-qty
python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories High Medium --parse-qty
python run_pipeline.py --brand NDC --skip-build --skip-sql
```

### Re-export with tighter filters (no re-processing)
```bash
python run_pipeline.py --brand NDC --skip-build --skip-sql --medium-min-fuzzy 40 --verify-min-fuzzy 60
```

---

## File Reference

| File | Purpose |
|------|---------|
| `run_pipeline.py` | Entry point — orchestrates all stages; use `--brand` for automatic path resolution |
| `requirements.txt` | Python dependencies |
| **`scripts/`** | |
| `scripts/pipeline.sql` | SQL matching logic: candidate generation, validation flags, scoring, categorization, dedup |
| `scripts/main.py` | Parses catalog + Keepa files, normalizes data, extracts attributes, builds SQLite DB |
| `scripts/run_sql.py` | Executes `pipeline.sql` with custom functions (`fuzzy_ratio`, `fuzzy_partial`, `fuzzy_token_sort`, `contains_text`, `size_approx_match`) |
| `scripts/export_excel.py` | Exports final matches to Excel with category sheets and optional filters |
| `scripts/ai_helper.py` | OpenAI API utilities: batch title parsing (`batch_parse_titles`), batch match verification (`batch_verify_matches`), cost estimation |
| `scripts/ai_verify.py` | Reads matches from DB, sends to GPT for verification, updates categories; optionally fills missing qtys |
| `scripts/ai_parse_qty.py` | Standalone GPT qty filler — finds null-qty items in Certain/High matches and parses quantities from titles |

---

## Data Flow

```
catalog.xlsx ──┐
               ├──► main.py ──► NDC.db ──► run_sql.py ──► NDC.db ──► export_excel.py ──► results.xlsx
keepa.xlsx ────┘                                              │
(or .csv,                                               [optional]
 multi-sheet)                                         ai_parse_qty.py  ← fills missing qtys
                                                            + run_sql.py  ← re-scores
                                                              │
                                                        ai_verify.py  ← GPT match verification
                                                              │
                                                          NDC.db (updated categories)
                                                              │
                                                        export_excel.py ──► results.xlsx
```

---

## Database Tables

Created by `main.py`:
- **`catalog_std`** — Normalized catalog products (mpn, manufacturer, title, quantity, color, size, etc.)
- **`catalog_codes`** — MPN → normalized code mappings
- **`keepa_std`** — Normalized Amazon products (asin, title, brand, quantity, color, size, pricing, etc.)
- **`keepa_codes`** — ASIN → normalized code mappings (from UPC, EAN, GTIN, PartNumber, Model)

Created by `pipeline.sql`:
- **`keepa_code_df`** — Code frequency + rarity scores
- **`candidates`** — Initial MPN ↔ ASIN pairs from code matching
- **`validated`** — Candidates with all validation flags and business metrics
- **`scored`** — Validated matches with confidence scores and flag counts
- **`final_matches_all`** — Categorized matches (Certain/High/Medium/Verify)
- **`final_matches_dedup_asin`** — Best single match per ASIN (garbage-filtered)

Created by `ai_verify.py`:
- **`ai_match_scores`** — GPT verification results (confidence, reasoning, original category)

---

## Key Algorithms

### Product Code Normalization (`norm_code`)
Strips non-alphanumeric characters, uppercases, filters out garbage values (NA, NAN, all-zeros, too-short, no-digits).

### Title Cleaning (`clean_title`)
Removes parentheticals, punctuation, stopwords, medical descriptor words, and unit tokens. Expands abbreviations (bx→box, cs→case, etc.). Keeps numbers. Used for fuzzy comparison so that "Acme Bandage 4x6 in, Pack of 12" and "ACME BANDAGE 4 x 6 INCH 12-Pack" score high.

### Quantity Extraction
40+ regex patterns ordered from most specific to least. Distinguishes quantity from dimensions/measurements using negative patterns. Supports compound patterns ("4 Boxes of 60" → 240), hierarchical patterns, compact formats like "BGof10", slash patterns like "50/cs", and hyphenated/suffixed like "12-pack", "24ct".

### Quantity Ratio Matching
When exact quantities differ, checks if one is a clean integer multiple (2x–100x) of the other. Indicates a case/box/bundle relationship — the same product sold at different pack levels. Suppresses the mismatch penalty and awards a weaker positive signal (+10 vs +25 for exact match).

### Size Extraction & Normalization
Extracts dimensions, measurements, and size categories from titles. Handles:
- Fractions: hyphenated ("4-1/2") and space-separated ("3 3/4") mixed numbers are converted to decimal before extraction ("3 3/4" → 3.75)
- Labeled dimensions: "3\" Width, 8\" Length" is converted to "3 X 8 IN" before the main dimension regex runs
- Unit stripping: "INCH"/"IN" suffixes are stripped from imperial dimensions since they are implicit in medical products (so "4 X 4" and "4 INCH X 4 INCH" normalize to the same value)
- Compact formats: "4X4" → "4 X 4"
- Prefix matching: if catalog has only width ("4") and Keepa has full dimensions ("4 X 75"), no mismatch penalty is applied — the catalog is providing partial information, not contradicting

### Size Approximate Matching (`size_approx_match`)
Custom SQLite function that compares two normalized size strings numerically within a 3% tolerance per dimension. Handles measurement rounding differences (e.g., "4.5 X 4.1 YD" vs "4.5 X 4.125YD" are treated as the same size). Requires matching unit suffixes when both sizes have one (e.g., YD vs CM are not compatible).

### Fuzzy Matching
Uses `rapidfuzz.fuzz.token_set_ratio` (or `difflib.SequenceMatcher` fallback). Token-set-ratio handles word reordering well, which is common in product titles. Computed during `run_sql.py` via a Python callback registered as a custom SQLite function — not available in plain SQLite shells.

### Brand Matching
Checks if the catalog manufacturer name (minimum 3 characters) appears in the Keepa brand field or product title. Allows short but legitimate brand names like "BD", "3M", "KCI" while filtering out single-character noise. When the `Manufacturer` column is empty, the pipeline attempts to extract a brand name from the catalog title automatically.

### AI Verification Guardrails
GPT is bounded on both sides — it cannot freely upgrade or downgrade:
- **Upgrade to Certain**: requires fuzzy ≥40 in addition to GPT confidence ≥0.9. GPT alone cannot promote a weak title match to Certain.
- **Downgrade protection**: strong SQL signals set a floor. Fuzzy ≥90 or (fuzzy ≥75 + confidence ≥40) → stays at least High. Confidence ≥50 or fuzzy ≥50 → stays at least Medium. This prevents abbreviated catalog titles from being wrongly sent to Verify.
- **Medical domain context**: GPT is told that catalog titles use common abbreviations (SPG=sponge, STR=sterile, NS=non-sterile, NW=non-woven, etc.), that non-woven sponge and gauze sponge are the same product type, and that a missing sterility spec in the catalog title is not a conflict.
- **Parse error retry**: if GPT returns malformed JSON for a batch, each match is retried individually before falling back to a safe default (0.65 confidence, stays High).

---

## Configuration

### Environment Variables
- `CHATGPT_API_KEY` — Required only for AI verification/parsing steps. Can be set in `.env` file.

### Optional Dependencies
- `rapidfuzz` — Significantly faster fuzzy matching (recommended). `pip install rapidfuzz`

### CLI Options

**`run_pipeline.py`**:
- `--brand` — Brand folder name under `data/` (e.g. `--brand NDC`). Auto-resolves all paths. Recommended.
- `--catalog` / `--keepa` — Input files (required if `--brand` not set); Keepa accepts `.xlsx` (multi-sheet) or `.csv`
- `--out` — Output Excel path (required if `--brand` not set)
- `--db` — SQLite database path (default: `data/<brand>/output/<brand>.db` when using `--brand`, else `match.db`)
- `--catalog-sheet` / `--keepa-sheet` — Target a specific sheet (skips multi-sheet combine)
- `--skip-build` / `--skip-sql` — Skip pipeline stages
- `--ai-parse-qty` — After SQL, use GPT to fill missing quantities for Certain/High matches, then re-run SQL to update scores. Requires `CHATGPT_API_KEY`.
- `--minimal` — Export fewer columns
- `--categories Certain High` — Only export selected categories
- `--min-confidence 40` — Global minimum confidence score filter
- `--certain-min-fuzzy` / `--high-min-fuzzy` / `--medium-min-fuzzy` / `--verify-min-fuzzy` — Per-category fuzzy floor for export
- `--validate-only` — Check input column names without running the pipeline

**`export_excel.py`**:
- `--db` / `--out` — Required
- `--categories Certain High Medium Verify` — Which sheets to include
- `--min-confidence 40` — Drop rows below this confidence score
- `--certain-min-fuzzy` / `--high-min-fuzzy` / `--medium-min-fuzzy` / `--verify-min-fuzzy` — Per-sheet fuzzy floor
- `--minimal` — Fewer columns
- `--include-all` — All columns
- `--summary-only` — Summary sheet only

**`ai_verify.py`**:
- `--db` — SQLite database path (required)
- `--categories Certain High Medium` — Which tiers to verify (default: `High`). E.g. `--categories High Medium`
- `--limit` — Max matches to verify
- `--dry-run` — Preview without calling GPT
- `--batch-size` — Matches per API call (default: 5)
- `--high-min-fuzzy` / `--medium-min-fuzzy` / `--certain-min-fuzzy` — Min fuzzy floor per tier (saves API cost by skipping weak matches)
- `--parse-qty` — After verification, use GPT to fill missing qtys for Certain/High matches

**`ai_parse_qty.py`** (standalone):
- `--db` — SQLite database path (required)
- `--categories` — Which match tiers to fill qty for (default: `Certain High`)
- `--dry-run` — Preview without calling GPT or updating DB

---

## GUDID Utility (`gudid/`)

Standalone tool that queries the FDA's Global Unique Device Identification Database (GUDID) API. Given a list of MPNs in `mpns.txt`, it looks up package type, quantity, status, and brand for each device and saves results to `package_types.csv`. Rate-limited to 0.3s between requests.
