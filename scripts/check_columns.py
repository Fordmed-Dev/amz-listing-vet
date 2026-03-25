import sqlite3
import sys

db_path = sys.argv[1] if len(sys.argv) > 1 else "match.db"

con = sqlite3.connect(db_path)
cursor = con.cursor()

# Check what tables exist
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
print(f"Tables in database: {tables}\n")

# Check if final table exists
if "final_matches_dedup_asin" in tables:
    cursor.execute("PRAGMA table_info(final_matches_dedup_asin)")
    columns = cursor.fetchall()
    print("Columns in final_matches_dedup_asin:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")

    # Check for AI verification column
    ai_cols = [col for col in columns if 'ai_match' in col[1].lower()]
    if ai_cols:
        print(f"\nFound AI column: {[c[1] for c in ai_cols]}")

        # Check sample values
        cursor.execute("SELECT ai_match_score FROM final_matches_dedup_asin LIMIT 5")
        samples = cursor.fetchall()
        print(f"  Sample values: {[s[0] for s in samples]}")
    else:
        print("\nNo AI verification column found")
else:
    print("final_matches_dedup_asin table doesn't exist yet. Run pipeline.sql first.")

# Check ai_match_scores table
if "ai_match_scores" in tables:
    cursor.execute("SELECT COUNT(*) FROM ai_match_scores")
    count = cursor.fetchone()[0]
    print(f"\nai_match_scores table has {count} rows")
else:
    print("\nai_match_scores table doesn't exist")

con.close()
