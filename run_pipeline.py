# file: run_pipeline.py
"""
Complete product matching pipeline orchestrator.

Usage:
    # Full run
    python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx

    # Validate input files only (no processing)
    python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx --validate-only

    # Skip stages you've already run
    python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx --skip-build
    python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx --skip-sql
"""

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}\n")

    try:
        result = subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nError: {description} failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\nError: Command not found: {cmd[0]}")
        return False


def print_summary(db_path: str) -> None:
    """Print match category summary from the database."""
    try:
        con = sqlite3.connect(db_path)
        cursor = con.execute("""
            SELECT Category, COUNT(*) as count
            FROM final_matches_dedup_asin
            GROUP BY Category
            ORDER BY
                CASE Category
                    WHEN 'Certain' THEN 1
                    WHEN 'High'    THEN 2
                    WHEN 'Medium'  THEN 3
                    ELSE 4
                END
        """)
        rows = cursor.fetchall()
        con.close()

        print("\nMatch Summary:")
        total = 0
        for cat, count in rows:
            print(f"  {cat}: {count}")
            total += count
        print(f"  ─────────────")
        print(f"  Total: {total}")
    except Exception:
        pass


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run complete product matching pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Brand shortcut — resolves all paths automatically
    ap.add_argument(
        "--brand",
        default=None,
        help=(
            "Brand/manufacturer folder name under data/.\n"
            "  Looks for: data/<brand>/input/catalog.xlsx\n"
            "             data/<brand>/input/keepa.xlsx\n"
            "  Outputs to: data/<brand>/output/results.xlsx\n"
            "  DB placed at: data/<brand>/output/<brand>.db\n"
            "When --brand is set, --catalog/--keepa/--out are optional overrides."
        ),
    )

    # Required arguments (required unless --brand is given)
    ap.add_argument("--catalog", default=None, help="Catalog Excel file path")
    ap.add_argument("--keepa",   default=None, help="Keepa Excel file path")
    ap.add_argument("--out",     default=None, help="Output Excel file path")

    # Optional arguments
    ap.add_argument("--db", default="match.db", help="SQLite database path (default: match.db)")
    ap.add_argument("--catalog-sheet", default=None, help="Catalog sheet name")
    ap.add_argument("--keepa-sheet",   default=None, help="Keepa sheet name")

    # Skip / mode options
    ap.add_argument("--skip-build",    action="store_true", help="Skip database building step")
    ap.add_argument("--skip-sql",      action="store_true", help="Skip SQL matching step")
    ap.add_argument(
        "--ai-parse-qty", action="store_true",
        help=(
            "After SQL, use GPT to fill missing quantities for Certain/High matches,\n"
            "then re-run SQL to update scores before export. Requires CHATGPT_API_KEY."
        ),
    )
    ap.add_argument("--minimal",       action="store_true", help="Export minimal columns only")
    ap.add_argument(
        "--min-confidence", type=float, default=None,
        help="Minimum confidence score to include in export (0-100). E.g. --min-confidence 40"
    )
    ap.add_argument(
        "--categories", nargs="+",
        choices=["Certain", "High", "Medium", "Verify"],
        default=None,
        help="Which match categories to include in export. E.g. --categories Certain High"
    )
    ap.add_argument("--certain-min-fuzzy", type=float, default=None, help="Min fuzzy score for Certain sheet")
    ap.add_argument("--high-min-fuzzy",    type=float, default=None, help="Min fuzzy score for High sheet")
    ap.add_argument("--medium-min-fuzzy",  type=float, default=None, help="Min fuzzy score for Medium sheet")
    ap.add_argument("--verify-min-fuzzy",  type=float, default=None, help="Min fuzzy score for Verify sheet")
    ap.add_argument(
        "--validate-only",
        action="store_true",
        help="Check input file columns and exit without running the pipeline",
    )

    args = ap.parse_args()

    # Resolve paths
    script_dir  = Path(__file__).parent.resolve()
    scripts_dir = script_dir / "scripts"
    python      = sys.executable

    # ------------------------------------------------------------------
    # --brand shortcut: fill in catalog/keepa/out/db from folder layout
    # ------------------------------------------------------------------
    if args.brand:
        brand_dir = script_dir / "data" / args.brand
        input_dir  = brand_dir / "input"
        output_dir = brand_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        if args.catalog is None:
            # Accept either .xlsx or .csv
            for candidate in (input_dir / "catalog.xlsx", input_dir / "catalog.csv"):
                if candidate.exists():
                    args.catalog = str(candidate)
                    break
            if args.catalog is None:
                args.catalog = str(input_dir / "catalog.xlsx")  # will fail with clear error later

        if args.keepa is None:
            for candidate in (input_dir / "keepa.xlsx", input_dir / "keepa.csv"):
                if candidate.exists():
                    args.keepa = str(candidate)
                    break
            if args.keepa is None:
                args.keepa = str(input_dir / "keepa.xlsx")

        if args.out is None:
            args.out = str(output_dir / "results.xlsx")

        if args.db == "match.db":
            args.db = str(output_dir / f"{args.brand}.db")
    elif not all([args.catalog, args.keepa, args.out]):
        ap.error("--catalog, --keepa, and --out are required when --brand is not set.")

    # ------------------------------------------------------------------
    # Input file checks (always run unless --skip-build)
    # ------------------------------------------------------------------
    if not args.skip_build:
        missing = []
        if not Path(args.catalog).exists():
            missing.append(f"  Catalog : {args.catalog}")
        if not Path(args.keepa).exists():
            missing.append(f"  Keepa   : {args.keepa}")
        if missing:
            print("\n[ERROR] Input file(s) not found:")
            for m in missing:
                print(m)
            return 1

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------
    print("\n" + "="*60)
    print("PRODUCT MATCHING PIPELINE")
    print("="*60)
    print(f"  Catalog  : {args.catalog}")
    print(f"  Keepa    : {args.keepa}")
    print(f"  Output   : {args.out}")
    print(f"  Database : {args.db}")
    if args.validate_only:
        print("  Mode     : VALIDATE ONLY")

    # ------------------------------------------------------------------
    # Step 1: Build database (or validate-only)
    # ------------------------------------------------------------------
    if not args.skip_build:
        cmd = [
            python, str(scripts_dir / "main.py"),
            "--catalog", args.catalog,
            "--keepa",   args.keepa,
            "--db",      args.db,
        ]
        if args.catalog_sheet:
            cmd.extend(["--catalog-sheet", args.catalog_sheet])
        if args.keepa_sheet:
            cmd.extend(["--keepa-sheet", args.keepa_sheet])
        if args.validate_only:
            cmd.append("--validate-only")

        if not run_command(cmd, "Validate inputs" if args.validate_only else "Build database"):
            return 1

        # Stop here if we only wanted validation
        if args.validate_only:
            print("\n" + "="*60)
            print("VALIDATION COMPLETE — re-run without --validate-only to process.")
            print("="*60)
            return 0
    else:
        print("\n[Skipping database build]")

    # ------------------------------------------------------------------
    # Step 2: Run SQL pipeline
    # ------------------------------------------------------------------
    if not args.skip_sql:
        cmd = [
            python, str(scripts_dir / "run_sql.py"),
            "--db",  args.db,
            "--sql", str(scripts_dir / "pipeline.sql"),
        ]
        if not run_command(cmd, "Run matching SQL pipeline"):
            return 1
    else:
        print("\n[Skipping SQL pipeline]")

    # ------------------------------------------------------------------
    # Step 2b (optional): GPT qty fill + re-run SQL
    # ------------------------------------------------------------------
    if args.ai_parse_qty:
        cmd = [
            python, str(scripts_dir / "ai_parse_qty.py"),
            "--db", args.db,
            "--categories", "Certain", "High",
        ]
        if not run_command(cmd, "GPT qty fill for Certain/High matches"):
            return 1

        # Re-run SQL so updated qtys affect flag_qty_match scores
        cmd = [
            python, str(scripts_dir / "run_sql.py"),
            "--db",  args.db,
            "--sql", str(scripts_dir / "pipeline.sql"),
        ]
        if not run_command(cmd, "Re-run SQL pipeline with updated quantities"):
            return 1

    # ------------------------------------------------------------------
    # Step 3: Export results
    # ------------------------------------------------------------------
    cmd = [
        python, str(scripts_dir / "export_excel.py"),
        "--db",  args.db,
        "--out", args.out,
    ]
    if args.minimal:
        cmd.append("--minimal")
    if args.min_confidence is not None:
        cmd.extend(["--min-confidence", str(args.min_confidence)])
    if args.categories is not None:
        cmd.extend(["--categories"] + args.categories)
    for flag, val in [
        ("--certain-min-fuzzy", args.certain_min_fuzzy),
        ("--high-min-fuzzy",    args.high_min_fuzzy),
        ("--medium-min-fuzzy",  args.medium_min_fuzzy),
        ("--verify-min-fuzzy",  args.verify_min_fuzzy),
    ]:
        if val is not None:
            cmd.extend([flag, str(val)])

    if not run_command(cmd, "Export results to Excel"):
        return 1

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"\n  Results  : {args.out}")
    print(f"  Database : {args.db}")

    print_summary(args.db)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())