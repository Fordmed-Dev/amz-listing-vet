import sqlite3
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()
    
    con = sqlite3.connect(args.db)
    
    print("\n=== CHECK 1: ASINs with multiple matches ===")
    query1 = """
    SELECT 
      asin,
      COUNT(*) as match_count,
      MAX(fuzzy_score) as max_fuzzy,
      GROUP_CONCAT(DISTINCT Category) as categories
    FROM final_matches_all
    GROUP BY asin
    HAVING match_count > 1 
      AND max_fuzzy > 85
      AND categories LIKE '%Verify%'
    LIMIT 10;
    """
    
    for row in con.execute(query1):
        print(row)
    
    print("\n=== CHECK 2: Examples of wrong categorization ===")
    query2 = """
    SELECT 
      asin,
      catalog_mpn,
      fuzzy_score,
      score_ratio,
      flag_code_match,
      flag_brand_match,
      Category,
      matched_code
    FROM final_matches_all
    WHERE asin IN (
      SELECT asin 
      FROM final_matches_dedup_asin 
      WHERE fuzzy_score > 85 AND Category != 'Certain'
      LIMIT 3
    )
    ORDER BY asin, fuzzy_score DESC;
    """
    
    for row in con.execute(query2):
        print(row)
    
    print("\n=== CHECK 3: Category distribution ===")
    query3 = """
    SELECT 
      Category,
      COUNT(*) as count,
      AVG(fuzzy_score) as avg_fuzzy,
      MIN(fuzzy_score) as min_fuzzy,
      MAX(fuzzy_score) as max_fuzzy
    FROM final_matches_dedup_asin
    GROUP BY Category;
    """
    
    for row in con.execute(query3):
        print(row)

        # Add this to debug_categories.py

    print("\n=== CHECK 4: Any fuzzy > 85 NOT in Certain? ===")
    query4 = """
    SELECT 
    Category,
    COUNT(*) as count
    FROM final_matches_dedup_asin
    WHERE fuzzy_score > 85
    GROUP BY Category;
    """

    for row in con.execute(query4):
        print(row)

    print("\n=== CHECK 5: Show some 90% records near the threshold ===")
    query5 = """
    SELECT 
    asin,
    catalog_mpn,
    fuzzy_score,
    Category,
    flag_code_match,
    flag_brand_match
    FROM final_matches_dedup_asin
    WHERE Category = '90%'
    ORDER BY fuzzy_score DESC
    LIMIT 10;
    """

    for row in con.execute(query5):
        print(row)
        
    con.close()

if __name__ == "__main__":
    main()