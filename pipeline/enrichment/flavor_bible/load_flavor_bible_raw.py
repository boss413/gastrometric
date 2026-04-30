import sqlite3
import csv
import os
from gastrometric.config.paths import DB_PATH, DATA_DIR, BASE_DIR

CSV_PATH = os.path.join(DATA_DIR, "flavor_bible_edges.csv")

def load_flavor_bible_raw():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)

        for row in reader:
            cur.execute("""
                INSERT INTO flavor_bible_raw (source_text, target_text, score)
                VALUES (?, ?, ?)
            """, (
                row["source"].lower(),
                row["target"].lower(),
                int(row["score"])
            ))

    conn.commit()
    conn.close()

def main():
    try:
        load_flavor_bible_raw()

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT count(*) FROM flavor_bible_raw")
            count = c.fetchone()[0]

        print(f"flavor_bible_raw populated with {count} flavor pairings")

    except Exception:
        print("lines failed to parse")
        raise


if __name__ == "__main__":
    main()