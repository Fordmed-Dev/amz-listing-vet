# NDC Product Matching Pipeline

A system that matches supplier catalog products (identified by MPN) to Amazon listings (identified by ASIN) using product code matching, fuzzy title comparison, attribute validation, and optional GPT-4o-mini AI verification.

Built for medical supplies and B2B products.

Create 2 files - catalog.xlsx and keepa.xlsx. For the keepa file, use the custom template tempVet. The catalog will need to have a column mpn and title, you may also add uom_qty and uom.
---

## Quick Start

```bash
pip install -r requirements.txt

# Full pipeline (build DB, run matching, export results)
python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx

# Or run steps individually:
python main.py --catalog catalog.xlsx --keepa keepa.xlsx --db match.db
python run_sql.py --db match.db --sql pipeline.sql
python export_excel.py --db match.db --out results.xlsx

# Optional: verify uncertain matches with GPT-4o-mini
python ai_verify.py --db match.db
```

---

## How It Works

The pipeline has 4 stages:

### Stage 1: Parse & Normalize (`main.py`)

Reads two Excel files and normalizes them into a SQLite database.

**Catalog file** columns used: `MPN`, `Manufacturer`, `Title`, `uom_unit`, `uom_quantity`

**Keepa file** columns used: `ASIN`, `Title`, `Brand`, `Model`, `Color`, `Size`, `Number of Items`, `Package: Quantity`, `Product Codes: UPC/EAN/GTIN/PartNumber`, pricing fields (`Buy Box`, `FBA Pick&Pack Fee`, `Referral Fee`, etc.), `Bought in past month`

For each product, the parser extracts:
- **Quantity** — pack/case count (e.g., "Pack of 12" → 12). Supports hierarchical patterns like "2/pk, 6pk/cs" → 12 total eaches. Wipes are an exception: "100/pk, 5pk/cs" → 5 (sheets don't count as eaches).
- **Color** — from a lexicon of ~80 colors with alias normalization (e.g., "grey" → "gray")
- **Size** — dimensions ("4x6 inch"), measurements ("20mm"), or categories ("XL")
- **Product codes** — normalized MPN, UPC, EAN, GTIN for cross-referencing

Quantity resolution priority (when `uom_quantity` is empty in the catalog):
1. `uom_quantity` field from the spreadsheet
2. Hierarchical title parsing (e.g., "2/pk, 6pk/cs" → 12)
3. Simple title parsing (e.g., "Pack of 12" → 12)

Outputs 4 database tables: `catalog_std`, `catalog_codes`, `keepa_std`, `keepa_codes`.

### Stage 2: Match & Score (`pipeline.sql` via `run_sql.py`)

Runs inside SQLite with custom fuzzy-matching functions registered from Python (`rapidfuzz` if installed, otherwise `difflib`).

**Step 1 — Candidate Generation:** Joins catalog product codes to Keepa product codes. Filters out codes that appear in more than 50 ASINs to prevent candidate explosion.

**Step 2 — Validation Flags:** For each candidate pair (MPN ↔ ASIN), computes:

| Flag | What it checks | Weight |
|------|---------------|--------|
| `flag_code_match` | Product code matched | +20 |
| `flag_brand_match` | Manufacturer matches brand (exact or substring) | +15 |
| `flag_code_in_title` | MPN appears in Amazon title | +10 |
| `flag_qty_match` | Quantities are equal | +25 |
| `flag_color_match` | Colors match | +15 |
| `flag_size_match` | Sizes match | +15 |
| `flag_qty_mismatch` | Quantities differ | -30 |
| `flag_color_mismatch` | Colors differ | -15 |
| `flag_size_mismatch` | Sizes differ | -15 |

Also computes a `fuzzy_score` (0–100) using token-set-ratio on cleaned titles, plus business metrics like `break_even_cost` and `estimate_revenue`.

**Step 3 — Confidence Score:** Weighted sum of flags + fuzzy bonus (0–100 scale).

**Step 4 — Categorization:**

| Category | Criteria |
|----------|----------|
| **Certain** | Fuzzy ≥85 + code match + no negatives, OR code + brand + qty match, OR confidence ≥80 |
| **High** | Fuzzy ≥60 + code match, OR code + brand match, OR confidence ≥50 (no qty mismatch) |
| **Medium** | Fuzzy ≥40 + code match, OR confidence ≥30 |
| **Verify** | Fuzzy <30, OR 2+ negative flags, OR qty mismatch, OR confidence <20 |

**Step 5 — Deduplication:** Keeps the single best match per ASIN, ranked by category → confidence → fuzzy → code length → positive flags → negative flags.

### Stage 3: Export (`export_excel.py`)

Writes an Excel workbook with sheets: **Summary**, **Certain**, **High**, **Medium**, **Verify**, and **All Matches**. Each sheet is sorted by confidence score descending.

### Stage 4 (Optional): AI Verification (`ai_verify.py`)

Sends Medium matches (fuzzy < 85%) to GPT-4o-mini for AI-powered verification. GPT evaluates whether two product listings are truly the same product (same brand, same SKU, same quantity). Results update the match categories:

- AI confidence ≥0.9 → Certain
- 0.7–0.9 → High
- 0.5–0.7 → Medium
- <0.3 → Verify

---

## File Reference

| File | Purpose |
|------|---------|
| `main.py` | Parses catalog + Keepa Excel files, normalizes data, extracts attributes, builds SQLite DB |
| `pipeline.sql` | SQL matching logic: candidate generation, validation flags, scoring, categorization, dedup |
| `run_sql.py` | Executes `pipeline.sql` with custom fuzzy functions (`fuzzy_ratio`, `fuzzy_partial`, `fuzzy_token_sort`, `contains_text`) |
| `run_pipeline.py` | Orchestrator — runs `main.py` → `run_sql.py` → `export_excel.py` in sequence |
| `export_excel.py` | Exports final matches to Excel with category sheets |
| `ai_helper.py` | OpenAI GPT-4o-mini API utilities: batch title parsing, batch match verification, cost estimation |
| `ai_verify.py` | Reads Medium matches (fuzzy < 85%) from DB, sends to GPT for verification, updates categories |
| `check_columns.py` | Debug utility — inspects database schema and AI verification columns |
| `debug_categories.py` | Debug utility — analyzes categorization accuracy and edge cases |
| `requirements.txt` | Python dependencies |
| `gudid/main.py` | Standalone utility — queries FDA GUDID API for medical device package info by MPN |
| `gudid/mpns.txt` | Input file of MPNs for GUDID lookup |
| `gudid/package_types.csv` | GUDID lookup results |

---

## Data Flow

```
catalog.xlsx ──┐
               ├──► main.py ──► match.db ──► run_sql.py ──► match.db ──► export_excel.py ──► results.xlsx
keepa.xlsx ────┘                                                │
                                                                ▼
                                                        ai_verify.py (optional)
                                                                │
                                                                ▼
                                                          match.db (updated categories)
                                                                │
                                                                ▼
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
- **`keepa_code_df`** — Code frequency counts (for filtering common codes)
- **`candidates`** — Initial MPN ↔ ASIN pairs from code matching
- **`validated`** — Candidates with all validation flags and business metrics
- **`scored`** — Validated matches with confidence scores and flag counts
- **`final_matches_all`** — Categorized matches (Certain/High/Medium/Verify)
- **`final_matches_dedup_asin`** — Best single match per ASIN

Created by `ai_verify.py`:
- **`ai_match_scores`** — GPT verification results (confidence, reasoning)

---

## Key Algorithms

### Product Code Normalization (`norm_code`)
Strips non-alphanumeric characters, uppercases, filters out garbage values (NA, NAN, all-zeros, too-short, no-digits).

### Title Cleaning (`clean_title`)
Removes parentheticals, punctuation, stopwords, and unit tokens. Keeps numbers. Used for fuzzy comparison so that "Acme Bandage 4x6 in, Pack of 12" and "ACME BANDAGE 4 x 6 INCH 12-Pack" score high.

### Quantity Extraction
40+ regex patterns ordered from most specific to least. Distinguishes quantity from dimensions/measurements using negative patterns. Supports compact formats like "BGof10", slash patterns like "50/cs", hyphenated like "12-pack", and suffixed like "24ct".

### Hierarchical Quantity Parsing
Handles multi-level packaging up to 3 levels:
- 2-level: "2/pk, 6pk/cs" → 2 × 6 = 12 total eaches
- 3-level: "4/pk, 10 pk/bg, 1 bg/cs" → 4 × 10 × 1 = 40 total eaches

Exception for wipes where inner count = sheets: "100/pk, 5pk/cs" → 5.

### Fuzzy Matching
Uses `rapidfuzz.fuzz.token_set_ratio` (or `difflib.SequenceMatcher` fallback). Token-set-ratio handles word reordering well, which is common in product titles.

---

## Configuration

### Environment Variables
- `CHATGPT_API_KEY` — Required only for AI verification step. Can be set in `.env` file.

### Optional Dependencies
- `rapidfuzz` — Significantly faster fuzzy matching (recommended). Install with `pip install rapidfuzz`.

### CLI Options

**`run_pipeline.py`**:
- `--catalog` / `--keepa` — Input Excel files (required)
- `--out` — Output Excel path (required)
- `--db` — SQLite database path (default: `match.db`)
- `--catalog-sheet` / `--keepa-sheet` — Specific sheet names
- `--skip-build` / `--skip-sql` — Skip pipeline stages
- `--minimal` — Export fewer columns

**`ai_verify.py`**:
- `--db` — SQLite database path (required)
- `--limit` — Max matches to verify
- `--dry-run` — Preview without calling GPT
- `--batch-size` — Matches per API call (default: 5)

---

## GUDID Utility (`gudid/`)

Standalone tool that queries the FDA's Global Unique Device Identification Database (GUDID) API. Given a list of MPNs in `mpns.txt`, it looks up package type, quantity, status, and brand for each device and saves results to `package_types.csv`. Rate-limited to 0.3s between requests.
