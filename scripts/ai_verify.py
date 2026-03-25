# file: ai_verify.py
"""
GPT verification for product matches.

Default: verifies High matches (fuzzy < 85)
Use --categories to control which tiers are verified.

Usage:
    python scripts/ai_verify.py --db data/NDC/output/NDC.db
    python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories High Medium
    python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories Certain High Medium
    python scripts/ai_verify.py --db data/NDC/output/NDC.db --categories Certain
    python scripts/ai_verify.py --db data/NDC/output/NDC.db --limit 100 --dry-run
    python scripts/ai_verify.py --db data/NDC/output/NDC.db --parse-qty
"""

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from ai_helper import (
    get_openai_client,
    batch_verify_matches,
    batch_parse_titles,
    estimate_cost,
)


# =============================================================================
# DATA LOADING
# =============================================================================

def load_matches(
    db_path: str,
    categories: list[str],
    limit: int = None,
    high_min_fuzzy: float = None,
    medium_min_fuzzy: float = None,
    certain_min_fuzzy: float = None,
) -> pd.DataFrame:
    """
    Load matches for AI verification using per-tier fuzzy thresholds.
    High defaults to fuzzy < 85 (no point re-verifying near-certain ones).
    Verify tier is never sent to GPT — too weak to be worth the cost.
    """
    con = sqlite3.connect(db_path)

    cursor = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='final_matches_dedup_asin'"
    )
    if not cursor.fetchone():
        print("Error: 'final_matches_dedup_asin' table not found.")
        print("  Run pipeline.sql first to generate matches.")
        con.close()
        return pd.DataFrame()

    conditions = []
    if "High" in categories:
        cond = "Category = 'High' AND fuzzy_score < 85 AND fuzzy_score >= 30"
        if high_min_fuzzy is not None:
            cond += f" AND fuzzy_score >= {high_min_fuzzy}"
        conditions.append(f"({cond})")
    if "Medium" in categories:
        cond = "Category = 'Medium' AND fuzzy_score < 90 AND fuzzy_score >= 50"
        if medium_min_fuzzy is not None:
            cond += f" AND fuzzy_score >= {medium_min_fuzzy}"
        conditions.append(f"({cond})")
    if "Certain" in categories:
        cond = "Category = 'Certain'"
        if certain_min_fuzzy is not None:
            cond += f" AND fuzzy_score >= {certain_min_fuzzy}"
        conditions.append(f"({cond})")

    if not conditions:
        print("Warning: No valid categories selected for verification.")
        con.close()
        return pd.DataFrame()

    where_clause = " OR ".join(conditions)

    query = f"""
        SELECT
            catalog_mpn,
            asin,
            catalog_title,
            keepa_title,
            catalog_qty,
            keepa_qty,
            matched_code,
            fuzzy_score,
            confidence_score,
            positive_flags,
            negative_flags,
            match_reason,
            Category
        FROM final_matches_dedup_asin
        WHERE {where_clause}
        ORDER BY
            CASE Category
                WHEN 'Certain' THEN 1
                WHEN 'High'    THEN 2
                WHEN 'Medium'  THEN 3
                ELSE 4
            END,
            confidence_score DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql_query(query, con)
    con.close()
    return df


# =============================================================================
# DATABASE WRITES
# =============================================================================

def save_gpt_scores(db_path: str, scores: List[Dict[str, Any]]) -> int:
    """Save GPT verification scores to database."""
    if not scores:
        return 0

    con = sqlite3.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ai_match_scores (
            mpn               TEXT,
            asin              TEXT,
            ai_match_score    REAL,
            ai_reasoning      TEXT,
            original_category TEXT,
            PRIMARY KEY (mpn, asin)
        )
    """)

    try:
        con.execute("ALTER TABLE ai_match_scores ADD COLUMN original_category TEXT")
    except Exception:
        pass  # Column already exists

    for score in scores:
        con.execute("""
            INSERT OR REPLACE INTO ai_match_scores
            (mpn, asin, ai_match_score, ai_reasoning, original_category)
            VALUES (?, ?, ?, ?, ?)
        """, (
            score["mpn"],
            score["asin"],
            score["confidence"],
            score["reasoning"],
            score.get("original_category"),
        ))

    con.commit()
    con.close()
    return len(scores)


def update_categories(db_path: str) -> int:
    """
    Update match categories based on GPT scores (upgrades AND downgrades):
      GPT >= 0.9 + fuzzy >= 40  -> Certain
      GPT >= 0.7               -> High
      GPT >= 0.3               -> Medium
      GPT <  0.3 (want Verify) -> floored by SQL signals:
          fuzzy >= 90                       -> High  (never dropped below High)
          fuzzy >= 75 + confidence >= 40    -> High
          confidence >= 50                  -> Medium (strong multi-signal match)
          fuzzy >= 50                       -> Medium (decent title overlap)
          otherwise                         -> Verify
    """
    con = sqlite3.connect(db_path)

    result = con.execute("""
        UPDATE final_matches_all
        SET Category = CASE
            -- GPT says match: require minimum fuzzy to reach Certain (GPT alone can't override weak titles)
            WHEN (SELECT ai_match_score FROM ai_match_scores s
                  WHERE s.mpn  = final_matches_all.catalog_mpn
                    AND s.asin = final_matches_all.asin) >= 0.9
                 AND fuzzy_score >= 40 THEN 'Certain'
            WHEN (SELECT ai_match_score FROM ai_match_scores s
                  WHERE s.mpn  = final_matches_all.catalog_mpn
                    AND s.asin = final_matches_all.asin) >= 0.7 THEN 'High'
            WHEN (SELECT ai_match_score FROM ai_match_scores s
                  WHERE s.mpn  = final_matches_all.catalog_mpn
                    AND s.asin = final_matches_all.asin) >= 0.3 THEN 'Medium'
            -- Protect SQL matches from GPT over-downgrade.
            -- GPT sees abbreviated catalog titles and underestimates similarity —
            -- use SQL signals as a floor so strong multi-signal matches can't fall to Verify.
            WHEN fuzzy_score >= 90 THEN 'High'
            WHEN fuzzy_score >= 75 AND confidence_score >= 40 THEN 'High'
            -- Multi-signal SQL confidence: code + brand + qty etc. → floor at Medium
            WHEN confidence_score >= 50 THEN 'Medium'
            -- Decent title overlap → floor at Medium
            WHEN fuzzy_score >= 50 THEN 'Medium'
            ELSE 'Verify'
        END
        WHERE EXISTS (
            SELECT 1 FROM ai_match_scores s
            WHERE s.mpn  = final_matches_all.catalog_mpn
              AND s.asin = final_matches_all.asin
        )
    """)
    updated_all = result.rowcount

    # Rebuild dedup table
    con.execute("DROP TABLE IF EXISTS final_matches_dedup_asin")
    con.execute("""
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
          AND fuzzy_score >= 15
          AND NOT (fuzzy_score < 25 AND negative_flags >= 2 AND confidence_score < -10)
    """)

    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_cat  ON final_matches_dedup_asin(Category)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_asin ON final_matches_dedup_asin(asin)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_mpn  ON final_matches_dedup_asin(catalog_mpn)")

    con.commit()
    con.close()
    return updated_all


# =============================================================================
# QTY PARSING (optional post-verification step)
# =============================================================================

def parse_and_fill_qtys(db_path: str, client, categories: list[str]) -> int:
    """
    After verification, fill missing qtys for matches in the given categories.
    Returns total rows updated.
    """
    con = sqlite3.connect(db_path)
    placeholders = ",".join("?" * len(categories))

    rows = con.execute(f"""
        SELECT DISTINCT c.mpn, c.catalog_title
        FROM final_matches_dedup_asin f
        JOIN catalog_std c ON c.mpn = f.catalog_mpn
        WHERE f.Category IN ({placeholders})
          AND c.catalog_qty IS NULL
    """, categories).fetchall()
    catalog_titles = {mpn: title for mpn, title in rows}

    rows = con.execute(f"""
        SELECT DISTINCT k.asin, k.keepa_title
        FROM final_matches_dedup_asin f
        JOIN keepa_std k ON k.asin = f.asin
        WHERE f.Category IN ({placeholders})
          AND k.keepa_qty IS NULL
    """, categories).fetchall()
    keepa_titles = {asin: title for asin, title in rows}
    con.close()

    print(f"  Catalog titles missing qty : {len(catalog_titles)}")
    print(f"  Keepa titles missing qty   : {len(keepa_titles)}")

    if not catalog_titles and not keepa_titles:
        print("  No missing quantities found.")
        return 0

    total = 0

    if catalog_titles:
        print(f"\n  Parsing {len(catalog_titles)} catalog titles...")
        items = list(catalog_titles.items())
        results = batch_parse_titles(client, items, parse_type="catalog")
        updates = {id_: int(a.quantity) for id_, a in results.items() if a.quantity and a.quantity > 0}
        if updates:
            con = sqlite3.connect(db_path)
            for mpn, qty in updates.items():
                r = con.execute(
                    "UPDATE catalog_std SET catalog_qty = ? WHERE mpn = ? AND catalog_qty IS NULL",
                    (qty, mpn)
                )
                total += r.rowcount
            con.commit()
            con.close()
        print(f"  Filled {len(updates)} catalog qtys")

    if keepa_titles:
        print(f"\n  Parsing {len(keepa_titles)} keepa titles...")
        items = list(keepa_titles.items())
        results = batch_parse_titles(client, items, parse_type="keepa")
        updates = {id_: int(a.quantity) for id_, a in results.items() if a.quantity and a.quantity > 0}
        if updates:
            con = sqlite3.connect(db_path)
            for asin, qty in updates.items():
                r = con.execute(
                    "UPDATE keepa_std SET keepa_qty = ? WHERE asin = ? AND keepa_qty IS NULL",
                    (qty, asin)
                )
                total += r.rowcount
            con.commit()
            con.close()
        print(f"  Filled {len(updates)} keepa qtys")

    return total


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Verify product matches with GPT",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--db",         required=True,          help="SQLite database path")
    ap.add_argument("--limit",      type=int, default=None, help="Max matches to verify")
    ap.add_argument("--dry-run",    action="store_true",    help="Preview without calling GPT")
    ap.add_argument("--batch-size", type=int, default=5,    help="Matches per API call (default: 5)")
    ap.add_argument(
        "--categories", nargs="+",
        choices=["Certain", "High", "Medium"],
        default=["High"],
        help="Which tiers to verify (default: High). E.g. --categories High Medium",
    )
    ap.add_argument("--high-min-fuzzy",    type=float, default=None, help="Min fuzzy for High matches sent to GPT")
    ap.add_argument("--medium-min-fuzzy",  type=float, default=None, help="Min fuzzy for Medium matches sent to GPT")
    ap.add_argument("--certain-min-fuzzy", type=float, default=None, help="Min fuzzy for Certain matches sent to GPT")
    ap.add_argument(
        "--parse-qty", action="store_true",
        help="After verification, use GPT to fill missing qtys for verified matches, then re-run SQL",
    )
    args = ap.parse_args()

    # Initialize OpenAI client
    gpt_client = get_openai_client()
    if not gpt_client and not args.dry_run:
        print("Error: CHATGPT_API_KEY not set. Cannot verify matches.")
        print("  Set the environment variable or add to .env file.")
        return 1

    # Load matches
    print(f"\nLoading matches from {args.db}...")
    print(f"  Categories: {', '.join(args.categories)}")
    df = load_matches(
        args.db,
        categories=args.categories,
        limit=args.limit,
        high_min_fuzzy=args.high_min_fuzzy,
        medium_min_fuzzy=args.medium_min_fuzzy,
        certain_min_fuzzy=args.certain_min_fuzzy,
    )

    if df.empty:
        print("No matches found for the selected tiers.")
        return 0

    print(f"Found {len(df)} matches to verify:")
    for cat, grp in df.groupby("Category"):
        print(f"  {cat}: {len(grp)}")

    cost = estimate_cost(0, len(df))
    print(f"Estimated verification cost: ${cost['estimated_verify_cost_usd']:.4f}")

    if args.dry_run:
        print("\n=== DRY RUN — no API calls will be made ===")
        print(
            df[["catalog_mpn", "asin", "Category", "fuzzy_score", "confidence_score"]]
            .head(20)
            .to_string(index=False)
        )
        if len(df) > 20:
            print(f"  ... and {len(df) - 20} more")
        return 0

    # Verify with GPT
    matches = []
    for _, row in df.iterrows():
        matches.append({
            "id":            f"{row['catalog_mpn']}|{row['asin']}",
            "catalog_title": row["catalog_title"],
            "keepa_title":   row["keepa_title"],
            "catalog_qty":   row["catalog_qty"],
            "keepa_qty":     row["keepa_qty"],
            "matched_code":  row["matched_code"],
            "fuzzy_score":   row["fuzzy_score"],
        })

    print(f"\nVerifying {len(matches)} matches with GPT...")
    results = batch_verify_matches(gpt_client, matches, args.batch_size)

    scores = []
    for i, (match, result) in enumerate(zip(matches, results)):
        mpn, asin = match["id"].split("|", 1)
        scores.append({
            "mpn":               mpn,
            "asin":              asin,
            "confidence":        result.confidence,
            "is_match":          result.is_match,
            "reasoning":         result.reasoning,
            "original_category": df.iloc[i]["Category"],
        })
        if (i + 1) % 10 == 0 or (i + 1) == len(matches):
            print(f"  Processed {i + 1}/{len(matches)}")

    print(f"\nSaving {len(scores)} scores to database...")
    saved = save_gpt_scores(args.db, scores)
    print(f"  Saved {saved} scores")

    print("Updating match categories...")
    updated = update_categories(args.db)
    print(f"  Updated {updated} rows")

    # Summary
    print("\n=== VERIFICATION SUMMARY ===")
    cat_order = {"Certain": 4, "High": 3, "Medium": 2, "Verify": 1}
    upgrades = downgrades = 0
    for s in scores:
        orig_rank = cat_order.get(s["original_category"], 0)
        new_cat = (
            "Certain" if s["confidence"] >= 0.9 else
            "High"    if s["confidence"] >= 0.7 else
            "Medium"  if s["confidence"] >= 0.3 else
            "Verify"
        )
        new_rank = cat_order.get(new_cat, 0)
        if new_rank > orig_rank:
            upgrades += 1
        elif new_rank < orig_rank:
            downgrades += 1

    print(f"  High confidence   (>=0.7) : {sum(1 for s in scores if s['confidence'] >= 0.7)}")
    print(f"  Medium confidence (0.3-0.7): {sum(1 for s in scores if 0.3 <= s['confidence'] < 0.7)}")
    print(f"  Low confidence    (<0.3)  : {sum(1 for s in scores if s['confidence'] < 0.3)}")
    print(f"  Upgraded          : {upgrades}")
    print(f"  Downgraded        : {downgrades}")
    print(f"  Unchanged         : {len(scores) - upgrades - downgrades}")

    # Optional: fill missing qtys for verified matches
    if args.parse_qty:
        print("\n=== QTY PARSING FOR VERIFIED MATCHES ===")
        # Always parse Certain+High since those are the useful ones
        parse_cats = list(set(args.categories) & {"Certain", "High"}) or ["Certain", "High"]
        print(f"  Filling missing qtys for: {', '.join(parse_cats)}")
        filled = parse_and_fill_qtys(args.db, gpt_client, parse_cats)
        if filled > 0:
            print(f"\n  Filled {filled} qty fields.")
            print("  Re-run SQL to update scores: python run_pipeline.py --brand <name> --skip-build")
        else:
            print("  No qty fields needed filling.")

    print("\nDone! Re-export results:")
    print("  python run_pipeline.py --brand <name> --skip-build --skip-sql")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
