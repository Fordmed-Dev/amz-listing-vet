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
-- Prevents candidate explosion from common codes
-- =============================================================================

CREATE TABLE keepa_code_df AS
SELECT
    code,
    COUNT(DISTINCT asin) AS asin_count
FROM keepa_codes
GROUP BY code;

CREATE INDEX IF NOT EXISTS ix_keepa_code_df_code ON keepa_code_df(code);

-- =============================================================================
-- 2) CANDIDATE GENERATION
-- Match catalog codes to keepa codes (filtered by frequency)
-- =============================================================================

CREATE TABLE candidates AS
SELECT DISTINCT
    cc.mpn,
    kc.asin,
    cc.code AS matched_code,
    d.asin_count AS code_frequency,
    1 AS flag_code_match
FROM catalog_codes cc
JOIN keepa_code_df d
    ON d.code = cc.code
    AND d.asin_count <= 50
JOIN keepa_codes kc
    ON kc.code = cc.code;

CREATE INDEX IF NOT EXISTS ix_cand_mpn ON candidates(mpn);
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
    cand.flag_code_match,

    -- FLAG: Brand Match
    CASE
        WHEN c.manufacturer IS NULL OR c.manufacturer = '' THEN NULL
        WHEN LENGTH(c.manufacturer_lc) < 2 THEN NULL
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

    -- FLAG: Quantity Mismatch (negative signal)
    CASE
        WHEN c.catalog_qty IS NULL OR k.keepa_qty IS NULL THEN 0
        WHEN c.catalog_qty != k.keepa_qty THEN 1
        ELSE 0
    END AS flag_qty_mismatch,

    -- FLAG: Both quantities unknown (partial credit — likely same single-unit product)
    CASE
        WHEN c.catalog_qty IS NULL AND k.keepa_qty IS NULL THEN 1
        ELSE 0
    END AS flag_qty_both_null,

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
    CASE
        WHEN c.catalog_size_norm IS NULL OR k.keepa_size_norm IS NULL THEN NULL
        WHEN c.catalog_size_norm = k.keepa_size_norm THEN 1
        ELSE 0
    END AS flag_size_match,

    -- FLAG: Size Mismatch
    CASE
        WHEN c.catalog_size_norm IS NULL OR k.keepa_size_norm IS NULL THEN 0
        WHEN c.catalog_size_norm != k.keepa_size_norm THEN 1
        ELSE 0
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
JOIN keepa_std k ON k.asin = cand.asin;

CREATE INDEX IF NOT EXISTS ix_valid_asin ON validated(asin);
CREATE INDEX IF NOT EXISTS ix_valid_mpn ON validated(catalog_mpn);

-- =============================================================================
-- 4) SCORING
-- =============================================================================

CREATE TABLE scored AS
SELECT
    v.*,

    -- Count positive flags
    (
        CASE WHEN v.flag_code_match = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_brand_match = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_code_in_title = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_match = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_both_null = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_match = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_match = 1 THEN 1 ELSE 0 END
    ) AS positive_flags,

    -- Count negative flags
    (
        CASE WHEN v.flag_qty_mismatch = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_mismatch = 1 THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_mismatch = 1 THEN 1 ELSE 0 END
    ) AS negative_flags,

    -- Count available flags
    (
        CASE WHEN v.flag_code_match IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_brand_match IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_qty_match IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_color_match IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN v.flag_size_match IS NOT NULL THEN 1 ELSE 0 END
    ) AS available_flags,

    -- Score ratio
    CASE
        WHEN (
            CASE WHEN v.flag_code_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match IS NOT NULL THEN 1 ELSE 0 END
        ) = 0 THEN 0.0
        ELSE (
            CASE WHEN v.flag_code_match = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match = 1 THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match = 1 THEN 1 ELSE 0 END
        ) * 1.0 / (
            CASE WHEN v.flag_code_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_brand_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_code_in_title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_qty_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_color_match IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.flag_size_match IS NOT NULL THEN 1 ELSE 0 END
        )
    END AS score_ratio,

    -- Weighted confidence score (0-100)
    (
        CASE WHEN v.flag_code_match = 1 THEN 20 ELSE 0 END +
        CASE WHEN v.flag_brand_match = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.flag_code_in_title = 1 THEN 10 ELSE 0 END +
        CASE WHEN v.flag_qty_match = 1 THEN 25 ELSE 0 END +
        CASE WHEN v.flag_qty_both_null = 1 THEN 10 ELSE 0 END +
        CASE WHEN v.flag_color_match = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.flag_size_match = 1 THEN 15 ELSE 0 END +
        CASE WHEN v.fuzzy_score >= 80 THEN 15
             WHEN v.fuzzy_score >= 60 THEN 10
             WHEN v.fuzzy_score >= 40 THEN 5
             ELSE 0 END
        - CASE WHEN v.flag_qty_mismatch = 1 THEN 30 ELSE 0 END
        - CASE WHEN v.flag_color_mismatch = 1 THEN 15 ELSE 0 END
        - CASE WHEN v.flag_size_mismatch = 1 THEN 15 ELSE 0 END
    ) AS confidence_score

FROM validated v;

CREATE INDEX IF NOT EXISTS ix_scored_asin ON scored(asin);
CREATE INDEX IF NOT EXISTS ix_scored_mpn ON scored(catalog_mpn);

-- =============================================================================
-- 5) CATEGORIZATION
-- =============================================================================

CREATE TABLE final_matches_all AS
SELECT
    s.*,

    CASE
        -- CERTAIN: Strong signals
        WHEN s.fuzzy_score >= 85 AND s.flag_code_match = 1 AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
             AND (s.flag_qty_match = 1 OR s.flag_qty_both_null = 1 OR s.fuzzy_score >= 70)
             AND s.negative_flags = 0 THEN 'Certain'
        WHEN s.confidence_score >= 80 AND s.negative_flags = 0 THEN 'Certain'

        -- HIGH: Good signals
        WHEN s.fuzzy_score >= 60 AND s.flag_code_match = 1 AND s.flag_qty_mismatch = 0 THEN 'High'
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1 AND s.flag_qty_mismatch = 0 THEN 'High'
        WHEN s.confidence_score >= 50 AND s.flag_qty_mismatch = 0 THEN 'High'

        -- MEDIUM: Decent signals (require at least 2 positive signals)
        WHEN s.fuzzy_score >= 40 AND s.flag_code_match = 1 AND s.positive_flags >= 2 THEN 'Medium'
        WHEN s.confidence_score >= 30 AND s.positive_flags >= 2 THEN 'Medium'

        -- VERIFY: Weak or conflicting signals
        WHEN s.fuzzy_score < 30 THEN 'Verify'
        WHEN s.negative_flags >= 2 THEN 'Verify'
        WHEN s.flag_qty_mismatch = 1 THEN 'Verify'
        WHEN s.confidence_score < 20 THEN 'Verify'
        WHEN s.positive_flags < 2 THEN 'Verify'
        WHEN s.flag_brand_match = 0 AND s.fuzzy_score < 40 THEN 'Verify'

        ELSE 'Medium'
    END AS Category,

    -- Match reasoning
    CASE
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1 AND s.flag_qty_match = 1
            THEN 'Code + Brand + Qty match'
        WHEN s.flag_code_match = 1 AND s.flag_brand_match = 1
            THEN 'Code + Brand match'
        WHEN s.flag_code_match = 1 AND s.fuzzy_score >= 70
            THEN 'Code match + High title similarity'
        WHEN s.flag_code_match = 1
            THEN 'Code match only'
        WHEN s.fuzzy_score >= 80
            THEN 'High title similarity'
        ELSE 'Weak signals'
    END AS match_reason

FROM scored s;

CREATE INDEX IF NOT EXISTS ix_final_cat ON final_matches_all(Category);
CREATE INDEX IF NOT EXISTS ix_final_asin ON final_matches_all(asin);
CREATE INDEX IF NOT EXISTS ix_final_mpn ON final_matches_all(catalog_mpn);

-- =============================================================================
-- 6) DEDUPLICATION - Best match per ASIN
-- =============================================================================

CREATE TABLE final_matches_dedup_asin AS
WITH ranked AS (
    SELECT
        f.*,
        CASE f.Category
            WHEN 'Certain' THEN 4
            WHEN 'High' THEN 3
            WHEN 'Medium' THEN 2
            ELSE 1
        END AS cat_rank,
        ROW_NUMBER() OVER(
            PARTITION BY f.asin
            ORDER BY
                CASE f.Category
                    WHEN 'Certain' THEN 4
                    WHEN 'High' THEN 3
                    WHEN 'Medium' THEN 2
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
SELECT * FROM ranked WHERE rn = 1;

CREATE INDEX IF NOT EXISTS ix_dedup_cat ON final_matches_dedup_asin(Category);
CREATE INDEX IF NOT EXISTS ix_dedup_asin ON final_matches_dedup_asin(asin);
CREATE INDEX IF NOT EXISTS ix_dedup_mpn ON final_matches_dedup_asin(catalog_mpn);

-- =============================================================================
-- 7) SUMMARY
-- =============================================================================

SELECT '=== MATCH SUMMARY ===' AS info;

SELECT
    Category,
    COUNT(*) AS match_count,
    ROUND(AVG(fuzzy_score), 1) AS avg_fuzzy,
    ROUND(AVG(confidence_score), 1) AS avg_confidence
FROM final_matches_dedup_asin
GROUP BY Category
ORDER BY
    CASE Category
        WHEN 'Certain' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        ELSE 4
    END;
