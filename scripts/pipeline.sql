-- pipeline.sql
-- Product matching pipeline: catalog MPNs to Amazon ASINs
-- Run with: python run_sql.py --db match.db --sql pipeline.sql

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=-200000;

-- =============================================================================
-- CLEANUP
-- =============================================================================

DROP TABLE IF EXISTS keepa_code_df;
DROP TABLE IF EXISTS candidates;
DROP TABLE IF EXISTS validated;
DROP TABLE IF EXISTS scored;
DROP TABLE IF EXISTS final_matches_all;
DROP TABLE IF EXISTS final_matches_dedup_asin;

-- =============================================================================
-- 1) CODE FREQUENCY ANALYSIS
-- Assigns a rarity score to each code instead of hard-cutting at 50 ASINs.
-- Common codes (appear in many ASINs) get a lower weight so they contribute
-- less to the confidence score rather than being dropped entirely.
-- =============================================================================

CREATE TABLE keepa_code_df AS
SELECT
    code,
    COUNT(DISTINCT asin) AS asin_count,
    -- Rarity score: 1.0 for codes seen in 1 ASIN, approaches 0 for very common codes.
    -- LOG(1+1)=0.69, so a code in 1 ASIN scores 1.0 / 0.69 = 1.44 -> clamped to 1.0
    -- LOG(50+1)=3.93, so a code in 50 ASINs scores 1.0 / 3.93 = 0.25
    -- LOG(500+1)=6.21, so a code in 500 ASINs scores 1.0 / 6.21 = 0.16
    MIN(1.0, 1.0 / LOG(COUNT(DISTINCT asin) + 1)) AS code_rarity_score
FROM keepa_codes
GROUP BY code;

CREATE INDEX IF NOT EXISTS ix_keepa_code_df_code ON keepa_code_df(code);

-- =============================================================================
-- 2) CANDIDATE GENERATION
-- All code matches are kept; rarity score is carried through for scoring.
-- Removed the hard asin_count <= 50 cutoff — soft weighting handles this now.
-- =============================================================================

CREATE TABLE candidates AS
SELECT DISTINCT
    cc.mpn,
    kc.asin,
    cc.code AS matched_code,
    d.asin_count      AS code_frequency,
    d.code_rarity_score,
    1                 AS flag_code_match
FROM catalog_codes cc
JOIN keepa_code_df d ON d.code = cc.code
JOIN keepa_codes kc  ON kc.code = cc.code;

CREATE INDEX IF NOT EXISTS ix_cand_mpn  ON candidates(mpn);
CREATE INDEX IF NOT EXISTS ix_cand_asin ON candidates(asin);
CREATE INDEX IF NOT EXISTS ix_cand_code ON candidates(matched_code);

-- =============================================================================
-- 3) VALIDATION FLAGS
-- Compute matching signals between catalog and keepa
-- =============================================================================

CREATE TABLE validated AS
SELECT
    c.mpn AS catalog_mpn,
    k.asin,
    c.manufacturer AS catalog_manufacturer,
    c.catalog_title,
    c.catalog_title2,

    -- Quantities
    c.catalog_qty,
    c.uom_unit AS catalog_uom_unit,

    -- Catalog attributes
    c.catalog_color,
    c.catalog_size,
    c.catalog_color_norm,
    c.catalog_size_norm,

    -- Keepa data
    k.keepa_title,
    k.keepa_title2,
    k.keepa_brand,
    k.keepa_color,
    k.keepa_size,
    k.keepa_color_norm,
    k.keepa_size_norm,

    -- Keepa quantity
    k.keepa_qty,
    k.codescombined AS keepa_codescombined,

    -- Fuzzy score on cleaned titles
    fuzzy_ratio(c.catalog_title2, k.keepa_title2) AS fuzzy_score,

    -- Match info
    cand.matched_code,
    cand.code_frequency,
    cand.code_rarity_score,
    cand.flag_code_match,

    -- FLAG: Brand Match
    CASE
        WHEN c.manufacturer IS NULL OR c.manufacturer = '' THEN NULL
        WHEN LENGTH(c.manufacturer_lc) < 3 THEN NULL
        WHEN k.brand_lc = c.manufacturer_lc THEN 1
        WHEN INSTR(k.brand_lc, c.manufacturer_lc) > 0 THEN 1
        WHEN INSTR(k.title_lc, c.manufacturer_lc) > 0 THEN 1
        ELSE 0
    END AS flag_brand_match,

    -- FLAG: Code in Title
    CASE
        WHEN c.mpn IS NULL OR c.mpn = '' THEN NULL
        WHEN LENGTH(c.mpn) < 4 THEN NULL
        WHEN INSTR(k.title_lc, LOWER(c.mpn)) > 0 THEN 1
        ELSE 0
    END AS flag_code_in_title,

    -- FLAG: Quantity Match
    CASE
        WHEN c.catalog_qty IS NULL OR k.keepa_qty IS NULL THEN NULL
        WHEN c.catalog_qty = k.keepa_qty THEN 1
        ELSE 0
    END AS flag_qty_match,

    -- FLAG: Quantity Mismatch (negative signal — severity depends on fuzzy score,
    -- computed in scoring step below)
    -- Does NOT fire when qtys are clean multiples (case/box relationship handled by ratio match).
    CASE
        WHEN c.catalog_qty IS NULL OR k.keepa_qty IS NULL THEN 0
        WHEN c.catalog_qty = k.keepa_qty THEN 0
        WHEN c.catalog_qty > 0 AND k.keepa_qty > 0
             AND c.catalog_qty % k.keepa_qty = 0
             AND (c.catalog_qty / k.keepa_qty) BETWEEN 2 AND 100 THEN 0
        WHEN c.catalog_qty > 0 AND k.keepa_qty > 0
             AND k.keepa_qty % c.catalog_qty = 0
             AND (k.keepa_qty / c.catalog_qty) BETWEEN 2 AND 100 THEN 0
        WHEN c.catalog_qty != k.keepa_qty THEN 1
        ELSE 0
    END AS flag_qty_mismatch,

    -- FLAG: Quantity Ratio Match — one qty is a clean multiple of the other (2x–100x).
    -- Indicates a case/box/bundle relationship (e.g. catalog sells cases of 720, keepa sells boxes of 20).
    -- Positive signal but weaker than exact qty match.
    CASE
        WHEN c.catalog_qty IS NULL OR k.keepa_qty IS NULL THEN 0
        WHEN c.catalog_qty = k.keepa_qty THEN 0
        WHEN c.catalog_qty > 0 AND k.keepa_qty > 0
             AND c.catalog_qty % k.keepa_qty = 0
             AND (c.catalog_qty / k.keepa_qty) BETWEEN 2 AND 100 THEN 1
        WHEN c.catalog_qty > 0 AND k.keepa_qty > 0
             AND k.keepa_qty % c.catalog_qty = 0
             AND (k.keepa_qty / c.catalog_qty) BETWEEN 2 AND 100 THEN 1
        ELSE 0
    END AS flag_qty_ratio_match,

    -- FLAG: Both quantities unknown
    CASE
        WHEN c.catalog_qty IS NULL AND k.keepa_qty IS NULL THEN 1
        ELSE 0
    END AS flag_qty_both_null,

    -- FLAG: Unit type mismatch (added)
    -- Catches cases where the catalog sells singles but the ASIN is clearly a case/bulk pack.
    -- Triggered when: catalog UOM is ea/each/pr/pair AND keepa qty > 10.
    -- Suppressed when the MPN appears in the Amazon title: in that case product identity is
    -- confirmed and the 'EA' most likely means "1 orderable unit of this product" (which itself
    -- may be a bulk pack), not a literal single-item mismatch.
    CASE
        WHEN c.uom_unit IS NULL OR c.uom_unit = '' THEN 0
        WHEN k.keepa_qty IS NULL THEN 0
        WHEN c.mpn IS NOT NULL AND LENGTH(c.mpn) >= 4
             AND INSTR(k.title_lc, LOWER(c.mpn)) > 0 THEN 0
        WHEN LOWER(TRIM(c.uom_unit)) IN ('ea','each','pr','pair')
             AND k.keepa_qty > 10 THEN 1
        ELSE 0
    END AS flag_unit_type_mismatch,

    -- FLAG: Color Match
    CASE
        WHEN c.catalog_color_norm IS NULL OR k.keepa_color_norm IS NULL THEN NULL
        WHEN c.catalog_color_norm = k.keepa_color_norm THEN 1
        ELSE 0
    END AS flag_color_match,

    -- FLAG: Color Mismatch
    CASE
        WHEN c.catalog_color_norm IS NULL OR k.keepa_color_norm IS NULL THEN 0
        WHEN c.catalog_color_norm != k.keepa_color_norm THEN 1
        ELSE 0
    END AS flag_color_mismatch,

    -- FLAG: Size Match
    -- Fires on exact string equality OR numeric approximation within 3%
    -- (e.g. "4.5 X 4.1 YD" ≈ "4.5 X 4.125YD")
    CASE
        WHEN c.catalog_size_norm IS NULL OR k.keepa_size_norm IS NULL THEN NULL
        WHEN c.catalog_size_norm = k.keepa_size_norm THEN 1
        WHEN size_approx_match(c.catalog_size_norm, k.keepa_size_norm) = 1 THEN 1
        ELSE 0
    END AS flag_size_match,

    -- FLAG: Size Mismatch
    -- Does NOT fire when: sizes are equal, approx-equal within 3%, or one is a
    -- leading prefix of the other (e.g. catalog "4" vs keepa "4 X 75" — partial
    -- dimension info from the catalog, not a contradiction).
    CASE
        WHEN c.catalog_size_norm IS NULL OR k.keepa_size_norm IS NULL THEN 0
        WHEN c.catalog_size_norm = k.keepa_size_norm THEN 0
        WHEN size_approx_match(c.catalog_size_norm, k.keepa_size_norm) = 1 THEN 0
        WHEN k.keepa_size_norm LIKE (c.catalog_size_norm || ' %') THEN 0
        WHEN c.catalog_size_norm LIKE (k.keepa_size_norm || ' %') THEN 0
        ELSE 1
    END AS flag_size_mismatch,

    -- ASIN cases ratio
    CASE
        WHEN c.catalog_qty IS NULL OR k.keepa_qty IS NULL OR c.catalog_qty = 0 THEN NULL
        ELSE (k.keepa_qty * 1.0) / c.catalog_qty
    END AS asin_cases,

    -- Keepa pricing data
    k.buy_box_current,
    k.buy_box_30d_avg,
    k.bought_past_month,

    -- Break-even calculations
    CASE
        WHEN k.buy_box_current IS NULL OR k.pick_pack_fee IS NULL OR k.referral_fee IS NULL
            OR c.catalog_qty IS NULL OR k.keepa_qty IS NULL OR c.catalog_qty = 0
        THEN NULL
        ELSE (k.buy_box_current - k.pick_pack_fee - k.referral_fee - 0.22)
            / ((k.keepa_qty * 1.0) / c.catalog_qty)
    END AS break_even_cost_now,

    CASE
        WHEN k.buy_box_30d_avg IS NULL OR k.pick_pack_fee IS NULL OR k.referral_fee_pct IS NULL
            OR c.catalog_qty IS NULL OR k.keepa_qty IS NULL OR c.catalog_qty = 0
        THEN NULL
        ELSE (k.buy_box_30d_avg - k.pick_pack_fee - (k.referral_fee_pct * k.buy_box_30d_avg) - 0.22)
            / ((k.keepa_qty * 1.0) / c.catalog_qty)
    END AS break_even_cost_30day,

    CASE
        WHEN k.bought_past_month IS NULL OR k.buy_box_30d_avg IS NULL THEN NULL
        ELSE k.bought_past_month * k.buy_box_30d_avg
    END AS estimate_revenue

FROM candidates cand
JOIN catalog_std c ON c.mpn = cand.mpn
JOIN keepa_std k   ON k.asin = cand.asin;

CREATE INDEX IF NOT EXISTS ix_valid_asin ON validated(asin);
CREATE INDEX IF NOT EXISTS ix_valid_mpn  ON validated(catalog_mpn);

-- =============================================================================
-- 4) SCORING
-- =============================================================================

CREATE TABLE scored AS
SELECT
    v.*,

    -- Count positive flags
    (
        CASE WHEN v.flag_code_match = 1        THEN 1 ELSE 0 END +
        CASE WHEN v.flag_brand_match = 1       THEN 1 ELSE 0 END +
        CASE WHEN v.flag_code_in_title = 1     THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_match = 1         THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_both_null = 1     THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_ratio_match = 1   THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_match = 1       THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_match = 1        THEN 1 ELSE 0 END
    ) AS positive_flags,

    -- Count negative flags (now includes unit_type_mismatch)
    (
        CASE WHEN v.flag_qty_mismatch       = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_unit_type_mismatch = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_mismatch     = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_mismatch      = 1 THEN 1 ELSE 0 END
    ) AS negative_flags,

    -- Count available flags
    (
        CASE WHEN v.flag_code_match    IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_brand_match   IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_match     IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_match   IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_match    IS NOT NULL THEN 1 ELSE 0 END
    ) AS available_flags,

    -- Score ratio
    CASE
        WHEN (
            CASE WHEN v.flag_code_match    IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match   IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match     IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match   IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match    IS NOT NULL THEN 1 ELSE 0 END
        ) = 0 THEN 0.0
        ELSE (
            CASE WHEN v.flag_code_match    = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match   = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match     = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match   = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match    = 1 THEN 1 ELSE 0 END
        ) * 1.0 / (
            CASE WHEN v.flag_code_match    IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match   IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match     IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match   IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match    IS NOT NULL THEN 1 ELSE 0 END
        )
    END AS score_ratio,

    -- Weighted confidence score (0-100)
    -- Code match weight is now scaled by code_rarity_score (1.2 was added so
    -- a perfectly rare code still contributes its full 20 points).
    (
        -- Positive signals
        CASE WHEN v.flag_code_match = 1
             THEN ROUND(20 * MIN(1.0, v.code_rarity_score * 1.2))
             ELSE 0 END +
        CASE WHEN v.flag_brand_match   = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.flag_code_in_title = 1 THEN 10 ELSE 0 END +
        CASE WHEN v.flag_qty_match       = 1 THEN 25 ELSE 0 END +
        CASE WHEN v.flag_qty_both_null   = 1 THEN 10 ELSE 0 END +
        CASE WHEN v.flag_qty_ratio_match = 1 THEN 10 ELSE 0 END +
        CASE WHEN v.flag_color_match   = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.flag_size_match    = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.fuzzy_score >= 80  THEN 15
             WHEN v.fuzzy_score >= 60  THEN 10
             WHEN v.fuzzy_score >= 40  THEN 5
             ELSE 0 END

        -- Negative signals (tiered qty mismatch penalty based on fuzzy score)
        -- High fuzzy (>=70): titles are very similar, mismatch may be a bundle variant -> softer penalty
        -- Mid fuzzy (40-70): standard penalty
        -- Low fuzzy (<40):   titles differ AND qty differs -> almost certainly wrong -> hard penalty
        - CASE WHEN v.flag_qty_mismatch = 1 THEN
              CASE WHEN v.fuzzy_score >= 70 THEN 15
                   WHEN v.fuzzy_score >= 40 THEN 30
                   ELSE 45
              END
          ELSE 0 END

        -- Unit type mismatch: catalog sells singles, ASIN is a bulk pack
        - CASE WHEN v.flag_unit_type_mismatch = 1 THEN 20 ELSE 0 END

        - CASE WHEN v.flag_color_mismatch = 1 THEN 15 ELSE 0 END
        - CASE WHEN v.flag_size_mismatch  = 1 THEN 15 ELSE 0 END
    ) AS confidence_score

FROM validated v;

CREATE INDEX IF NOT EXISTS ix_scored_asin ON scored(asin);
CREATE INDEX IF NOT EXISTS ix_scored_mpn  ON scored(catalog_mpn);

-- =============================================================================
-- 5) CATEGORIZATION
-- =============================================================================

CREATE TABLE final_matches_all AS
SELECT
    s.*,

    CASE
        -- CERTAIN: Strong signals
        -- ---------------------------------------------------------------
        -- Sliding scale: as fuzzy rises, less confidence is needed.
        -- fuzzy 90+ : no negatives (titles identical enough on their own)
        -- fuzzy 85+ : confidence >= 60 (strong signals + near-identical)
        -- fuzzy 75+ : confidence >= 65 (solid signals + very similar)
        -- fuzzy 40+ : confidence >= 70 (needs more signals when fuzzy lower)
        -- ---------------------------------------------------------------
        WHEN s.fuzzy_score >= 90 AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.fuzzy_score >= 85 AND s.confidence_score >= 60 AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.fuzzy_score >= 85 AND s.flag_code_match = 1 AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.fuzzy_score >= 75 AND s.confidence_score >= 65 AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.fuzzy_score >= 40 AND s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND (s.flag_qty_match = 1 OR s.flag_qty_both_null = 1 OR s.flag_qty_ratio_match = 1 OR s.fuzzy_score >= 70)
             AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.fuzzy_score >= 40 AND s.confidence_score >= 70 AND s.negative_flags = 0 THEN 'Certain'
        -- Near-identical titles + only qty differs: catalog qty data is often wrong or represents
        -- a different pack level (e.g. catalog "1 EA" = Amazon "Box of 50"). At fuzzy >= 90 the
        -- title evidence is stronger than the qty signal, so allow it into Certain if the only
        -- conflict is qty (no size, color, or unit-type contradiction).
        WHEN s.fuzzy_score >= 90 AND s.confidence_score >= 50
             AND s.flag_qty_mismatch = 1
             AND s.flag_size_mismatch = 0 AND s.flag_color_mismatch = 0
             AND s.flag_unit_type_mismatch = 0 THEN 'Certain'

        -- Definitive multi-signal: code + brand + exact qty agreement + no contradictions.
        -- A matched UPC/EAN + correct manufacturer + correct pack count leaves no ambiguity,
        -- even when titles are heavily abbreviated (common in medical catalog data).
        -- confidence >= 55 guards against very-low-rarity codes (code+brand+qty alone = ~60 pts).
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND s.flag_qty_match = 1 AND s.negative_flags = 0
             AND s.confidence_score >= 55 THEN 'Certain'
        -- Code + brand + at least one physical attribute match (size or color) + no contradictions.
        -- Four corroborating signals with zero conflicts is sufficient regardless of fuzzy.
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND (s.flag_size_match = 1 OR s.flag_color_match = 1)
             AND s.flag_qty_mismatch = 0 AND s.flag_unit_type_mismatch = 0
             AND s.negative_flags = 0 AND s.fuzzy_score >= 25 THEN 'Certain'

        -- HIGH: Good signals
        -- Very high fuzzy: at 85+ the title evidence dominates any single negative signal.
        -- Allow qty mismatch (catalog "1 EA" often = Amazon bulk pack); block unit-type mismatch
        -- (singles vs bulk is a stronger structural difference than a pack-count discrepancy).
        WHEN s.fuzzy_score >= 85 AND s.negative_flags <= 1
             AND s.flag_unit_type_mismatch = 0 THEN 'High'
        -- Fuzzy-dominant: very similar titles with at most minor conflicts
        WHEN s.fuzzy_score >= 75 AND s.negative_flags <= 1
             AND s.flag_qty_mismatch = 0 AND s.flag_unit_type_mismatch = 0 THEN 'High'
        WHEN s.fuzzy_score >= 60 AND s.flag_code_match = 1 AND s.flag_qty_mismatch = 0
             AND s.flag_unit_type_mismatch = 0 THEN 'High'
        WHEN s.fuzzy_score >= 25 AND s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND s.flag_qty_mismatch = 0 AND s.flag_unit_type_mismatch = 0 THEN 'High'
        WHEN s.fuzzy_score >= 25 AND s.confidence_score >= 50 AND s.flag_qty_mismatch = 0
             AND s.flag_unit_type_mismatch = 0 THEN 'High'

        -- MEDIUM: Decent signals
        WHEN s.fuzzy_score >= 40 AND s.flag_code_match = 1 AND s.positive_flags >= 2 THEN 'Medium'
        WHEN s.confidence_score >= 30 AND s.positive_flags >= 2 THEN 'Medium'
        -- Safety net: code + brand surfaces for review, but only if titles aren't completely different
        -- and there aren't too many conflicting signals.
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND s.fuzzy_score >= 25 AND s.negative_flags <= 1 THEN 'Medium'
        -- Code match with no negative signals: a clean UPC/EAN hit with no contradictions
        -- is worth surfacing for review even when titles are abbreviated / brand doesn't match.
        WHEN s.flag_code_match = 1 AND s.negative_flags = 0
             AND s.fuzzy_score >= 15 AND s.confidence_score >= 20 THEN 'Medium'

        -- VERIFY: Weak or conflicting signals
        WHEN s.fuzzy_score < 30 THEN 'Verify'
        WHEN s.negative_flags >= 2 THEN 'Verify'
        WHEN s.flag_qty_mismatch = 1 THEN 'Verify'
        WHEN s.flag_unit_type_mismatch = 1 THEN 'Verify'
        WHEN s.confidence_score < 20 THEN 'Verify'
        -- Only penalise low positive_flags if fuzzy is also weak.
        -- Code match + fuzzy 40-59 is enough for Medium; only truly weak
        -- single-signal matches (fuzzy < 40) go to Verify.
        WHEN s.positive_flags < 2 AND s.fuzzy_score < 40 THEN 'Verify'
        WHEN s.flag_brand_match = 0 AND s.fuzzy_score < 40 THEN 'Verify'

        ELSE 'Medium'
    END AS Category,

    -- Match reasoning (updated to include unit type mismatch)
    CASE
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1 AND s.flag_qty_match = 1
            THEN 'Code + Brand + Qty match'
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1 AND s.flag_qty_ratio_match = 1
            THEN 'Code + Brand + Qty ratio match'
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
            THEN 'Code + Brand match'
        WHEN s.flag_code_match = 1 AND s.fuzzy_score >= 70
            THEN 'Code match + High title similarity'
        WHEN s.flag_unit_type_mismatch = 1
            THEN 'Unit type mismatch (single vs bulk)'
        WHEN s.flag_code_match = 1
            THEN 'Code match only'
        WHEN s.fuzzy_score >= 80
            THEN 'High title similarity'
        ELSE 'Weak signals'
    END AS match_reason

FROM scored s;

CREATE INDEX IF NOT EXISTS ix_final_cat ON final_matches_all(Category);
CREATE INDEX IF NOT EXISTS ix_final_asin ON final_matches_all(asin);
CREATE INDEX IF NOT EXISTS ix_final_mpn  ON final_matches_all(catalog_mpn);

-- =============================================================================
-- 6) DEDUPLICATION - Best match per ASIN
-- =============================================================================

CREATE TABLE final_matches_dedup_asin AS
WITH ranked AS (
    SELECT
        f.*,
        CASE f.Category
            WHEN 'Certain' THEN 4
            WHEN 'High'    THEN 3
            WHEN 'Medium'  THEN 2
            ELSE 1
        END AS cat_rank,
        ROW_NUMBER() OVER(
            PARTITION BY f.asin
            ORDER BY
                CASE f.Category
                    WHEN 'Certain' THEN 4
                    WHEN 'High'    THEN 3
                    WHEN 'Medium'  THEN 2
                    ELSE 1
                END DESC,
                f.confidence_score DESC,
                f.fuzzy_score DESC,
                LENGTH(COALESCE(f.matched_code, '')) DESC,
                f.positive_flags DESC,
                f.negative_flags ASC
        ) AS rn
    FROM final_matches_all f
)
SELECT * FROM ranked
WHERE rn = 1
  -- Hard floor: exclude clearly garbage matches
  -- Absolute fuzzy floor: titles share almost no words
  AND fuzzy_score >= 15
  -- Low fuzzy + multiple negatives + negative confidence = almost certainly wrong product
  AND NOT (fuzzy_score < 25 AND negative_flags >= 2 AND confidence_score < -10);

CREATE INDEX IF NOT EXISTS ix_dedup_cat ON final_matches_dedup_asin(Category);
CREATE INDEX IF NOT EXISTS ix_dedup_asin ON final_matches_dedup_asin(asin);
CREATE INDEX IF NOT EXISTS ix_dedup_mpn  ON final_matches_dedup_asin(catalog_mpn);

-- =============================================================================
-- 7) SUMMARY
-- =============================================================================

SELECT '=== MATCH SUMMARY ===' AS info;

SELECT
    Category,
    COUNT(*)                       AS match_count,
    ROUND(AVG(fuzzy_score), 1)     AS avg_fuzzy,
    ROUND(AVG(confidence_score), 1) AS avg_confidence
FROM final_matches_dedup_asin
GROUP BY Category
ORDER BY
    CASE Category
        WHEN 'Certain' THEN 1
        WHEN 'High'    THEN 2
        WHEN 'Medium'  THEN 3
        ELSE 4
    END;