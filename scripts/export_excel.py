# file: export_excel.py
"""
Export match results to Excel with separate sheets by category.

Usage:
    python export_excel.py --db match.db --out results.xlsx
    python export_excel.py --db match.db --out results.xlsx --include-all
"""

import argparse
import sqlite3

import pandas as pd

# Try to import rapidfuzz, but don't fail if not available
try:
    from rapidfuzz import fuzz
    HAVE_RAPIDFUZZ = True
except ImportError:
    HAVE_RAPIDFUZZ = False


# Columns to include in export (in order)
EXPORT_COLUMNS = [
    # Match identifiers
    "catalog_mpn",
    "asin",

    # Category and scoring
    "Category",
    "confidence_score",
    "fuzzy_score",
    "match_reason",

    # Titles
    "catalog_title",
    "keepa_title",

    # Quantities
    "catalog_qty",
    "keepa_qty",

    # Attributes
    "catalog_manufacturer",
    "keepa_brand",
    "catalog_color",
    "keepa_color",
    "catalog_size",
    "keepa_size",

    # Match details
    "matched_code",
    "code_frequency",

    # Flags
    "flag_code_match",
    "flag_brand_match",
    "flag_code_in_title",
    "flag_qty_match",
    "flag_color_match",
    "flag_size_match",
    "flag_qty_mismatch",
    "flag_color_mismatch",
    "flag_size_mismatch",
    "positive_flags",
    "negative_flags",

    # Scoring details
    "score_ratio",
    "ai_match_score",
    "ai_reasoning",

    # Business data
    "asin_cases",
    "buy_box_current",
    "buy_box_30d_avg",
    "bought_past_month",
    "break_even_cost_now",
    "break_even_cost_30day",
    "estimate_revenue",
]

# Minimal columns for quick review
MINIMAL_COLUMNS = [
    "catalog_mpn",
    "asin",
    "Category",
    "confidence_score",
    "fuzzy_score",
    "match_reason",
    "catalog_title",
    "keepa_title",
    "catalog_qty",
    "keepa_qty",
    "matched_code",
    "catalog_manufacturer",
    "keepa_brand",
]


def load_matches(db_path: str, table: str = "final_matches_dedup_asin") -> pd.DataFrame:
    """Load matches from database."""
    con = sqlite3.connect(db_path)

    # Check if table exists
    cursor = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not cursor.fetchone():
        print(f"Error: Table '{table}' not found in database.")
        con.close()
        return pd.DataFrame()

    # Load data
    table_escaped = table.replace('"', '""')
    df = pd.read_sql_query(f'SELECT * FROM "{table_escaped}"', con)
    con.close()

    return df


def select_columns(df: pd.DataFrame, columns: list, include_all: bool = False) -> pd.DataFrame:
    """Select and order columns for export."""
    if include_all:
        return df

    # Select only columns that exist
    available = [c for c in columns if c in df.columns]

    # Add any remaining columns at the end
    remaining = [c for c in df.columns if c not in available]

    return df[available + remaining]


def main() -> int:
    ap = argparse.ArgumentParser(description="Export matches to Excel")
    ap.add_argument("--db", required=True, help="SQLite database path")
    ap.add_argument("--out", required=True, help="Output Excel file")
    ap.add_argument("--table", default="final_matches_dedup_asin", help="Table to export")
    ap.add_argument("--include-all", action="store_true", help="Include all columns")
    ap.add_argument("--minimal", action="store_true", help="Export minimal columns only")
    ap.add_argument("--summary-only", action="store_true", help="Only create summary sheet")
    ap.add_argument(
        "--min-confidence", type=float, default=None,
        help="Minimum confidence score to include (0-100). E.g. --min-confidence 40 drops everything below 40."
    )
    ap.add_argument(
        "--categories", nargs="+",
        choices=["Certain", "High", "Medium", "Verify"],
        default=None,
        help="Which categories to include. E.g. --categories Certain High"
    )
    # Per-category fuzzy score thresholds
    ap.add_argument("--certain-min-fuzzy",  type=float, default=None, help="Min fuzzy score for Certain sheet")
    ap.add_argument("--high-min-fuzzy",     type=float, default=None, help="Min fuzzy score for High sheet")
    ap.add_argument("--medium-min-fuzzy",   type=float, default=None, help="Min fuzzy score for Medium sheet (e.g. 40)")
    ap.add_argument("--verify-min-fuzzy",   type=float, default=None, help="Min fuzzy score for Verify sheet")
    args = ap.parse_args()

    # Per-category fuzzy thresholds map
    fuzzy_thresholds = {
        "Certain": args.certain_min_fuzzy,
        "High":    args.high_min_fuzzy,
        "Medium":  args.medium_min_fuzzy,
        "Verify":  args.verify_min_fuzzy,
    }

    # Load data
    print(f"Loading matches from {args.db}...")
    df = load_matches(args.db, args.table)

    if df.empty:
        print("No data to export.")
        return 1

    print(f"Loaded {len(df)} matches")

    # Apply category filter
    if args.categories is not None and "Category" in df.columns:
        before = len(df)
        df = df[df["Category"].isin(args.categories)].copy()
        print(f"Filtered to categories {args.categories}: {len(df)} matches ({before - len(df)} dropped)")

    # Apply global confidence filter
    if args.min_confidence is not None and "confidence_score" in df.columns:
        df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce")
        before = len(df)
        df = df[df["confidence_score"] >= args.min_confidence].copy()
        print(f"Filtered to confidence >= {args.min_confidence}: {len(df)} matches ({before - len(df)} dropped)")

    # Select columns
    columns = MINIMAL_COLUMNS if args.minimal else EXPORT_COLUMNS
    df = select_columns(df, columns, args.include_all)

    # Convert numeric columns
    numeric_cols = ["confidence_score", "fuzzy_score", "score_ratio", "ai_match_score",
                    "buy_box_current", "buy_box_30d_avg", "break_even_cost_now",
                    "break_even_cost_30day", "estimate_revenue"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Split by category and apply per-category fuzzy thresholds
    def _filter_cat(cat_df: pd.DataFrame, cat: str) -> pd.DataFrame:
        threshold = fuzzy_thresholds.get(cat)
        if threshold is not None and "fuzzy_score" in cat_df.columns and not cat_df.empty:
            before = len(cat_df)
            cat_df = cat_df[cat_df["fuzzy_score"] >= threshold].copy()
            dropped = before - len(cat_df)
            if dropped:
                print(f"  {cat}: dropped {dropped} rows with fuzzy < {threshold}")
        return cat_df

    categories = {
        cat: _filter_cat(
            df[df["Category"] == cat].copy() if "Category" in df.columns else pd.DataFrame(),
            cat
        )
        for cat in ["Certain", "High", "Medium", "Verify"]
    }

    # Create summary
    summary_data = []
    for cat, cat_df in categories.items():
        if not cat_df.empty:
            summary_data.append({
                "Category": cat,
                "Count": len(cat_df),
                "Avg Confidence": round(cat_df["confidence_score"].mean(), 1) if "confidence_score" in cat_df.columns else None,
                "Avg Fuzzy": round(cat_df["fuzzy_score"].mean(), 1) if "fuzzy_score" in cat_df.columns else None,
                "With AI Score": cat_df["ai_match_score"].notna().sum() if "ai_match_score" in cat_df.columns else 0,
            })
    summary_df = pd.DataFrame(summary_data)

    # Write Excel
    print(f"Writing to {args.out}...")
    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        # Summary sheet first
        summary_df.to_excel(writer, index=False, sheet_name="Summary")

        if not args.summary_only:
            # Category sheets
            for cat, cat_df in categories.items():
                if not cat_df.empty:
                    # Sort by confidence score
                    if "confidence_score" in cat_df.columns:
                        cat_df = cat_df.sort_values("confidence_score", ascending=False)
                    cat_df.to_excel(writer, index=False, sheet_name=cat)

            # All matches sheet
            if len(df) <= 100000:  # Don't create All sheet for very large datasets
                all_df = df.copy()
                if "confidence_score" in all_df.columns:
                    all_df = all_df.sort_values("confidence_score", ascending=False)
                all_df.to_excel(writer, index=False, sheet_name="All Matches")

    print(f"\nExported to {args.out}")
    print("\nSummary:")
    for _, row in summary_df.iterrows():
        print(f"  {row['Category']}: {row['Count']} matches")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
