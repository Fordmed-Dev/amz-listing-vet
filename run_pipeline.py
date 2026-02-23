# file: run_pipeline.py
"""
Complete product matching pipeline orchestrator.

Usage:
    python run_pipeline.py --catalog catalog.xlsx --keepa keepa.xlsx --out results.xlsx
"""

import argparse
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Run complete product matching pipeline")

    # Required arguments
    ap.add_argument("--catalog", required=True, help="Catalog Excel file path")
    ap.add_argument("--keepa", required=True, help="Keepa Excel file path")
    ap.add_argument("--out", required=True, help="Output Excel file path")

    # Optional arguments
    ap.add_argument("--db", default="match.db", help="SQLite database path (default: match.db)")
    ap.add_argument("--catalog-sheet", default=None, help="Catalog sheet name")
    ap.add_argument("--keepa-sheet", default=None, help="Keepa sheet name")

    # Skip options
    ap.add_argument("--skip-build", action="store_true", help="Skip database building")
    ap.add_argument("--skip-sql", action="store_true", help="Skip SQL pipeline")

    # Export options
    ap.add_argument("--minimal", action="store_true", help="Export minimal columns only")

    args = ap.parse_args()

    # Validate inputs
    if not args.skip_build:
        if not Path(args.catalog).exists():
            print(f"Error: Catalog file not found: {args.catalog}")
            return 1
        if not Path(args.keepa).exists():
            print(f"Error: Keepa file not found: {args.keepa}")
            return 1

    # Get script directory
    script_dir = Path(__file__).parent.resolve()
    python = sys.executable

    print("\n" + "="*60)
    print("PRODUCT MATCHING PIPELINE")
    print("="*60)
    print(f"Catalog: {args.catalog}")
    print(f"Keepa:   {args.keepa}")
    print(f"Output:  {args.out}")
    print(f"Database:{args.db}")

    # Step 1: Build database
    if not args.skip_build:
        cmd = [
            python, str(script_dir / "main.py"),
            "--catalog", args.catalog,
            "--keepa", args.keepa,
            "--db", args.db,
        ]
        if args.catalog_sheet:
            cmd.extend(["--catalog-sheet", args.catalog_sheet])
        if args.keepa_sheet:
            cmd.extend(["--keepa-sheet", args.keepa_sheet])

        if not run_command(cmd, "Build database and parse titles"):
            return 1
    else:
        print("\n[Skipping database build]")

    # Step 2: Run SQL pipeline
    if not args.skip_sql:
        cmd = [
            python, str(script_dir / "run_sql.py"),
            "--db", args.db,
            "--sql", str(script_dir / "pipeline.sql"),
        ]

        if not run_command(cmd, "Run matching SQL pipeline"):
            return 1
    else:
        print("\n[Skipping SQL pipeline]")

    # Step 3: Export results
    cmd = [
        python, str(script_dir / "export_excel.py"),
        "--db", args.db,
        "--out", args.out,
    ]
    if args.minimal:
        cmd.append("--minimal")

    if not run_command(cmd, "Export results to Excel"):
        return 1

    # Done
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"\nResults exported to: {args.out}")
    print(f"Database saved to: {args.db}")

    # Print summary from database
    try:
        import sqlite3
        con = sqlite3.connect(args.db)
        cursor = con.execute("""
            SELECT Category, COUNT(*) as count
            FROM final_matches_dedup_asin
            GROUP BY Category
            ORDER BY
                CASE Category
                    WHEN 'Certain' THEN 1
                    WHEN 'High' THEN 2
                    WHEN 'Medium' THEN 3
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
        print(f"  Total: {total}")
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
