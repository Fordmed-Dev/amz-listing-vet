# file: ai_verify.py
"""
GPT-4o-mini verification for uncertain product matches.
Reads Medium matches (fuzzy < 85%) from database and uses GPT to verify them.

Usage:
    python ai_verify.py --db match.db
    python ai_verify.py --db match.db --limit 100
    python ai_verify.py --db match.db --dry-run
"""

import argparse
import sqlite3
from typing import List, Dict, Any

import pandas as pd

from ai_helper import (
    get_openai_client,
    batch_verify_matches,
    estimate_cost,
)


def load_medium_matches(db_path: str, limit: int = None) -> pd.DataFrame:
    """Load Medium matches with fuzzy_score < 85% from database."""
    con = sqlite3.connect(db_path)

    # Check if table exists
    cursor = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='final_matches_dedup_asin'"
    )
    if not cursor.fetchone():
        print("Error: 'final_matches_dedup_asin' table not found.")
        print("  Run pipeline.sql first to generate matches.")
        con.close()
        return pd.DataFrame()

    query = """
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
        WHERE Category = 'Medium'
          AND fuzzy_score < 85
    """
    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql_query(query, con)
    con.close()
    return df


def save_gpt_scores(db_path: str, scores: List[Dict[str, Any]]) -> int:
    """Save GPT verification scores to database."""
    if not scores:
        return 0

    con = sqlite3.connect(db_path)

    # Ensure table exists (reuse same schema for compatibility)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ai_match_scores (
            mpn TEXT,
            asin TEXT,
            ai_match_score REAL,
            ai_reasoning TEXT,
            PRIMARY KEY (mpn, asin)
        )
    """)

    # Insert or replace scores
    for score in scores:
        con.execute("""
            INSERT OR REPLACE INTO ai_match_scores
            (mpn, asin, ai_match_score, ai_reasoning)
            VALUES (?, ?, ?, ?)
        """, (
            score["mpn"],
            score["asin"],
            score["confidence"],
            score["reasoning"]
        ))

    con.commit()
    con.close()
    return len(scores)


def update_categories(db_path: str) -> int:
    """
    Update match categories based on GPT scores.
    Returns number of rows updated.
    """
    con = sqlite3.connect(db_path)

    # Update final_matches_all
    result = con.execute("""
        UPDATE final_matches_all
        SET Category = CASE
            WHEN (SELECT ai_match_score FROM ai_match_scores cms
                  WHERE cms.mpn = final_matches_all.catalog_mpn
                    AND cms.asin = final_matches_all.asin) >= 0.9 THEN 'Certain'
            WHEN (SELECT ai_match_score FROM ai_match_scores cms
                  WHERE cms.mpn = final_matches_all.catalog_mpn
                    AND cms.asin = final_matches_all.asin) >= 0.7 THEN 'High'
            WHEN (SELECT ai_match_score FROM ai_match_scores cms
                  WHERE cms.mpn = final_matches_all.catalog_mpn
                    AND cms.asin = final_matches_all.asin) >= 0.5 THEN 'Medium'
            WHEN (SELECT ai_match_score FROM ai_match_scores cms
                  WHERE cms.mpn = final_matches_all.catalog_mpn
                    AND cms.asin = final_matches_all.asin) < 0.3 THEN 'Verify'
            ELSE Category
        END
        WHERE EXISTS (
            SELECT 1 FROM ai_match_scores cms
            WHERE cms.mpn = final_matches_all.catalog_mpn
              AND cms.asin = final_matches_all.asin
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
        SELECT * FROM ranked WHERE rn = 1
    """)

    # Recreate indexes
    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_cat ON final_matches_dedup_asin(Category)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_asin ON final_matches_dedup_asin(asin)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_dedup_mpn ON final_matches_dedup_asin(catalog_mpn)")

    con.commit()
    con.close()

    return updated_all


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify Medium matches (fuzzy < 85%) with GPT-4o-mini")
    ap.add_argument("--db", required=True, help="SQLite database path")
    ap.add_argument("--limit", type=int, default=None, help="Limit matches to verify")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be done without calling GPT")
    ap.add_argument("--batch-size", type=int, default=5, help="Matches per GPT API call")
    args = ap.parse_args()

    # Initialize OpenAI
    gpt_client = get_openai_client()
    if not gpt_client and not args.dry_run:
        print("Error: CHATGPT_API_KEY not set. Cannot verify matches.")
        print("  Set the environment variable or add to .env file")
        return 1

    # Load Medium matches with fuzzy < 85%
    print(f"Loading Medium matches (fuzzy < 85%) from {args.db}...")
    df = load_medium_matches(args.db, args.limit)

    if df.empty:
        print("No Medium matches with fuzzy < 85% found to verify.")
        return 0

    print(f"Found {len(df)} Medium matches with fuzzy < 85%")

    # Show cost estimate
    cost = estimate_cost(0, len(df))
    print(f"Estimated cost: ${cost['estimated_verify_cost_usd']:.4f}")

    if args.dry_run:
        print("\n=== DRY RUN MODE ===")
        print("Would verify these matches:")
        print(df[["catalog_mpn", "asin", "fuzzy_score", "confidence_score", "Category"]].head(20).to_string())
        print(f"\n... and {len(df) - 20} more" if len(df) > 20 else "")
        return 0

    # Prepare matches for verification
    matches = []
    for _, row in df.iterrows():
        matches.append({
            "id": f"{row['catalog_mpn']}|{row['asin']}",
            "catalog_title": row["catalog_title"],
            "keepa_title": row["keepa_title"],
            "catalog_qty": row["catalog_qty"],
            "keepa_qty": row["keepa_qty"],
            "matched_code": row["matched_code"],
            "fuzzy_score": row["fuzzy_score"],
        })

    # Verify with GPT
    print(f"\nVerifying {len(matches)} matches with GPT-4o-mini...")
    results = batch_verify_matches(gpt_client, matches, args.batch_size)

    # Process results
    scores = []
    for i, (match, result) in enumerate(zip(matches, results)):
        mpn, asin = match["id"].split("|", 1)
        scores.append({
            "mpn": mpn,
            "asin": asin,
            "confidence": result.confidence,
            "is_match": result.is_match,
            "reasoning": result.reasoning,
        })

        # Show progress
        if (i + 1) % 10 == 0 or (i + 1) == len(matches):
            print(f"  Processed {i + 1}/{len(matches)}")

    # Save scores
    print(f"\nSaving {len(scores)} scores to database...")
    saved = save_gpt_scores(args.db, scores)
    print(f"  Saved {saved} scores")

    # Update categories
    print("Updating match categories...")
    updated = update_categories(args.db)
    print(f"  Updated {updated} rows")

    # Summary
    print("\n=== VERIFICATION SUMMARY ===")
    high_confidence = sum(1 for s in scores if s["confidence"] >= 0.7)
    medium_confidence = sum(1 for s in scores if 0.5 <= s["confidence"] < 0.7)
    low_confidence = sum(1 for s in scores if s["confidence"] < 0.5)

    print(f"  High confidence (>=0.7): {high_confidence}")
    print(f"  Medium confidence (0.5-0.7): {medium_confidence}")
    print(f"  Low confidence (<0.5): {low_confidence}")

    print("\nDone! Re-export results with export_excel.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
