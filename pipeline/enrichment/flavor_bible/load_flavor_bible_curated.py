import sqlite3
import csv
import os
import re
from gastrometric.config.paths import DB_PATH, DATA_DIR

CSV_PATH = os.path.join(DATA_DIR, "flavor_bible_curated.csv")

def clean_target(text):
    text = text.lower().strip()

    # normalize unicode first (fixes gruyère-type issues downstream)
    import unicodedata
    text = unicodedata.normalize("NFKD", text)

    # strip trailing punctuation/symbols
    text = re.sub(r"[^\w\s]+$", "", text)

    return text.strip()

def load_flavor_bible_curated():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)

        for row in reader:
            cur.execute("""
                INSERT INTO flavor_bible_curated (
                        source, 
                        target_cleaned, 
                        score, 
                        key_ingredient, 
                        seasonality, 
                        ingredient, accompaniment
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row["source"].lower(),
                clean_target(row["target_cleaned"]),
                int(row["score"]),
                row["key_ingredient"].lower(),
                row["seasonality"].lower(),
                row["ingredient"].lower(),
                row["accompaniment"].lower()
            ))

    conn.commit()
    conn.close()

def main():
    try:
        load_flavor_bible_curated()

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT count(*) FROM flavor_bible_curated")
            count = c.fetchone()[0]

        print(f"flavor_bible_curated populated with {count} flavor pairings")

    except Exception:
        print("lines failed to parse")
        raise


if __name__ == "__main__":
    main()