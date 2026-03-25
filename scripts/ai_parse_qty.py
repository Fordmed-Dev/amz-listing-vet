# file: ai_parse_qty.py
"""
GPT fallback for missing quantities in matched pairs.

Runs AFTER pipeline.sql so we know which items are Certain/High.
Finds matched pairs where catalog_qty or keepa_qty is NULL,
calls GPT to extract qty from the title, updates the DB,
then the caller re-runs pipeline.sql to re-score.

Usage:
    python scripts/ai_parse_qty.py --db data/NDC/output/NDC.db
    python scripts/ai_parse_qty.py --db data/NDC/output/NDC.db --categories Certain High Medium
    python scripts/ai_parse_qty.py --db data/NDC/output/NDC.db --dry-run
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# Allow importing ai_helper from same directory
sys.path.insert(0, str(Path(__file__).parent))

from ai_helper import get_openai_client, batch_parse_titles


def load_null_qty_titles(db_path: str, categories: list[str]) -> tuple[dict, dict]:
    """
    Return two dicts of titles that need qty parsed:
      catalog_titles: {mpn: catalog_title}
      keepa_titles:   {asin: keepa_title}
    Only includes items appearing in the given categories.
    """
    con = sqlite3.connect(db_path)
    placeholders = ",".join("?" * len(categories))

    # Catalog MPNs with null qty in matched pairs
    rows = con.execute(f"""
        SELECT DISTINCT c.mpn, c.catalog_title
        FROM final_matches_dedup_asin f
        JOIN catalog_std c ON c.mpn = f.catalog_mpn
        WHERE f.Category IN ({placeholders})
          AND c.catalog_qty IS NULL
    """, categories).fetchall()
    catalog_titles = {mpn: title for mpn, title in rows}

    # Keepa ASINs with null qty in matched pairs
    rows = con.execute(f"""
        SELECT DISTINCT k.asin, k.keepa_title
        FROM final_matches_dedup_asin f
        JOIN keepa_std k ON k.asin = f.asin
        WHERE f.Category IN ({placeholders})
          AND k.keepa_qty IS NULL
    """, categories).fetchall()
    keepa_titles = {asin: title for asin, title in rows}

    con.close()
    return catalog_titles, keepa_titles


def update_catalog_qty(db_path: str, updates: dict[str, int]) -> int:
    """Write GPT-extracted qtys back to catalog_std. Returns count updated."""
    if not updates:
        return 0
    con = sqlite3.connect(db_path)
    updated = 0
    for mpn, qty in updates.items():
        result = con.execute(
            "UPDATE catalog_std SET catalog_qty = ? WHERE mpn = ? AND catalog_qty IS NULL",
            (qty, mpn)
        )
        updated += result.rowcount
    con.commit()
    con.close()
    return updated


def update_keepa_qty(db_path: str, updates: dict[str, int]) -> int:
    """Write GPT-extracted qtys back to keepa_std. Returns count updated."""
    if not updates:
        return 0
    con = sqlite3.connect(db_path)
    updated = 0
    for asin, qty in updates.items():
        result = con.execute(
            "UPDATE keepa_std SET keepa_qty = ? WHERE asin = ? AND keepa_qty IS NULL",
            (qty, asin)
        )
        updated += result.rowcount
    con.commit()
    con.close()
    return updated


def parse_qty_from_titles(client, titles: dict[str, str], label: str) -> dict[str, int]:
    """Call GPT to extract quantities. Returns {id: qty} for non-null results."""
    if not titles:
        return {}

    print(f"\nParsing {len(titles)} {label} titles with GPT...")
    items = list(titles.items())  # [(id, title), ...]
    results = batch_parse_titles(client, items, parse_type=label)

    updates = {}
    for id_, attrs in results.items():
        if attrs.quantity is not None and attrs.quantity > 0:
            updates[id_] = int(attrs.quantity)

    print(f"  GPT extracted qty for {len(updates)}/{len(titles)} titles")
    return updates


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Use GPT to fill in missing quantities for matched pairs",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--db", required=True, help="SQLite database path")
    ap.add_argument(
        "--categories", nargs="+",
        choices=["Certain", "High", "Medium", "Verify"],
        default=["Certain", "High"],
        help="Which match tiers to fill qty for (default: Certain High)"
    )
    ap.add_argument("--dry-run", action="store_true", help="Preview without calling GPT or updating DB")
    args = ap.parse_args()

    # Check DB exists
    if not Path(args.db).exists():
        print(f"Error: database not found: {args.db}")
        print("  Run the pipeline first to generate matches.")
        return 1

    # Check final_matches_dedup_asin exists
    con = sqlite3.connect(args.db)
    has_matches = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='final_matches_dedup_asin'"
    ).fetchone()
    con.close()
    if not has_matches:
        print("Error: 'final_matches_dedup_asin' table not found.")
        print("  Run pipeline.sql first.")
        return 1

    # Load titles that need qty
    print(f"\nLoading null-qty titles from {args.db}...")
    print(f"  Categories: {', '.join(args.categories)}")
    catalog_titles, keepa_titles = load_null_qty_titles(args.db, args.categories)

    print(f"  Catalog titles missing qty : {len(catalog_titles)}")
    print(f"  Keepa titles missing qty   : {len(keepa_titles)}")

    if not catalog_titles and not keepa_titles:
        print("\nNo missing quantities found — nothing to do.")
        return 0

    if args.dry_run:
        print("\n=== DRY RUN — no API calls or DB updates ===")
        if catalog_titles:
            print("\nSample catalog titles that would be parsed:")
            for id_, title in list(catalog_titles.items())[:5]:
                print(f"  [{id_}] {title}")
        if keepa_titles:
            print("\nSample keepa titles that would be parsed:")
            for id_, title in list(keepa_titles.items())[:5]:
                print(f"  [{id_}] {title}")
        return 0

    # Init GPT client
    gpt_client = get_openai_client()
    if not gpt_client:
        print("Error: CHATGPT_API_KEY not set.")
        print("  Set the environment variable or add to .env file.")
        return 1

    # Parse and update catalog
    catalog_updates = parse_qty_from_titles(gpt_client, catalog_titles, "catalog")
    catalog_updated = update_catalog_qty(args.db, catalog_updates)
    print(f"  Updated catalog_std qty for {catalog_updated} rows")

    # Parse and update keepa
    keepa_updates = parse_qty_from_titles(gpt_client, keepa_titles, "keepa")
    keepa_updated = update_keepa_qty(args.db, keepa_updates)
    print(f"  Updated keepa_std qty for {keepa_updated} rows")

    total = catalog_updated + keepa_updated
    print(f"\nTotal qty fields filled: {total}")
    if total > 0:
        print("Re-run SQL pipeline to update scores: run_pipeline.py --brand <name> --skip-build")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
