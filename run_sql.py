# file: run_sql.py
"""
SQL execution helper with custom fuzzy matching functions.

Usage:
    python run_sql.py --db match.db --sql pipeline.sql
"""

import argparse
import sqlite3
from pathlib import Path

# Try to use rapidfuzz (faster, better), fall back to difflib
try:
    from rapidfuzz import fuzz as rapidfuzz_fuzz
    HAVE_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    HAVE_RAPIDFUZZ = False
    print("Note: Install 'rapidfuzz' for faster fuzzy matching: pip install rapidfuzz")


def fuzzy_ratio(a: str | None, b: str | None) -> float:
    """
    Returns a 0..100 similarity score.
    Uses rapidfuzz if available (faster), otherwise difflib.
    """
    if not a or not b:
        return 0.0

    a = str(a).strip().lower()
    b = str(b).strip().lower()

    if not a or not b:
        return 0.0

    # Exact match shortcut
    if a == b:
        return 100.0

    if HAVE_RAPIDFUZZ:
        # Use token_set_ratio - better for comparing product titles
        # where word order may differ
        return round(rapidfuzz_fuzz.token_set_ratio(a, b), 2)
    else:
        return round(100.0 * SequenceMatcher(None, a, b).ratio(), 2)


def fuzzy_partial(a: str | None, b: str | None) -> float:
    """
    Partial ratio - good for finding substrings.
    Returns 0..100.
    """
    if not a or not b:
        return 0.0

    a = str(a).strip().lower()
    b = str(b).strip().lower()

    if not a or not b:
        return 0.0

    if a == b:
        return 100.0

    if HAVE_RAPIDFUZZ:
        return round(rapidfuzz_fuzz.partial_ratio(a, b), 2)
    else:
        # Partial match approximation with difflib
        shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
        if shorter in longer:
            return 100.0
        return round(100.0 * SequenceMatcher(None, a, b).ratio(), 2)


def fuzzy_token_sort(a: str | None, b: str | None) -> float:
    """
    Token sort ratio - sorts tokens before comparing.
    Good for titles with same words in different order.
    Returns 0..100.
    """
    if not a or not b:
        return 0.0

    a = str(a).strip().lower()
    b = str(b).strip().lower()

    if not a or not b:
        return 0.0

    if a == b:
        return 100.0

    if HAVE_RAPIDFUZZ:
        return round(rapidfuzz_fuzz.token_sort_ratio(a, b), 2)
    else:
        # Sort tokens and compare
        a_sorted = " ".join(sorted(a.split()))
        b_sorted = " ".join(sorted(b.split()))
        return round(100.0 * SequenceMatcher(None, a_sorted, b_sorted).ratio(), 2)


def contains_text(haystack: str | None, needle: str | None) -> int:
    """
    Check if needle is contained in haystack (case-insensitive).
    Returns 1 if found, 0 otherwise.
    """
    if not haystack or not needle:
        return 0

    haystack = str(haystack).strip().lower()
    needle = str(needle).strip().lower()

    if not haystack or not needle:
        return 0

    return 1 if needle in haystack else 0


def drop_objects_referencing_angle(con: sqlite3.Connection) -> None:
    """Drop any views/triggers referencing 'Angle' (legacy cleanup)."""
    ANGLE_NAME = "Angle"
    rows = con.execute(
        "SELECT name, type, sql FROM sqlite_master WHERE sql LIKE ?",
        (f"%{ANGLE_NAME}%",),
    ).fetchall()

    for name, obj_type, _sql in rows:
        if obj_type == "view":
            con.execute(f'DROP VIEW IF EXISTS "{name}"')
            print(f"Dropped view referencing {ANGLE_NAME}: {name}")
        elif obj_type == "trigger":
            con.execute(f'DROP TRIGGER IF EXISTS "{name}"')
            print(f"Dropped trigger referencing {ANGLE_NAME}: {name}")

    if rows:
        con.commit()


def register_functions(con: sqlite3.Connection) -> None:
    """Register all custom SQL functions."""
    con.create_function("fuzzy_ratio", 2, fuzzy_ratio)
    con.create_function("fuzzy_partial", 2, fuzzy_partial)
    con.create_function("fuzzy_token_sort", 2, fuzzy_token_sort)
    con.create_function("contains_text", 2, contains_text)


def execute_sql_file(con: sqlite3.Connection, sql_text: str) -> None:
    """Execute SQL file statement by statement."""
    lines = sql_text.splitlines()
    buf: list[str] = []
    start_line = 1

    for i, line in enumerate(lines, start=1):
        buf.append(line)
        stmt = "\n".join(buf).strip()

        if stmt and sqlite3.complete_statement(stmt):
            try:
                cursor = con.execute(stmt)
                # Print SELECT results
                if stmt.strip().upper().startswith("SELECT"):
                    rows = cursor.fetchall()
                    if rows:
                        # Get column names
                        cols = [desc[0] for desc in cursor.description]
                        print("\n" + " | ".join(cols))
                        print("-" * (len(" | ".join(cols)) + 10))
                        for row in rows:
                            print(" | ".join(str(v) if v is not None else "NULL" for v in row))
            except sqlite3.Error as e:
                print("\n=== SQL ERROR ===")
                print("Error:", e)
                print(f"Statement starts at line: {start_line}")
                print("\n--- Failing statement ---")
                print(stmt[:500] + ("..." if len(stmt) > 500 else ""))
                print("\n--- End failing statement ---")
                raise
            buf = []
            start_line = i + 1

    con.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description="Execute SQL file against SQLite database")
    ap.add_argument("--db", required=True, help="SQLite database path")
    ap.add_argument("--sql", required=True, help="SQL file to execute")
    ap.add_argument("--verbose", "-v", action="store_true", help="Show more details")
    args = ap.parse_args()

    sql_path = Path(args.sql).resolve()
    sql_text = sql_path.read_text(encoding="utf-8", errors="replace")

    print("DB :", Path(args.db).resolve())
    print("SQL:", sql_path)
    if HAVE_RAPIDFUZZ:
        print("Fuzzy: rapidfuzz (fast)")
    else:
        print("Fuzzy: difflib (install rapidfuzz for speed)")

    con = sqlite3.connect(args.db)
    try:
        # Register custom functions
        register_functions(con)

        # Legacy cleanup
        drop_objects_referencing_angle(con)

        # Execute SQL
        execute_sql_file(con, sql_text)

        print("\nOK")
    finally:
        con.close()


if __name__ == "__main__":
    main()
