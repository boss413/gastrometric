import sqlite3
import csv
import os

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
DB_PATH = os.path.join(DATA_DIR, "gastrometric.db")

CSV_PATH = os.path.join(DATA_DIR, "flavor_bible_edges.csv")


def main():
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

    print("Loaded raw flavor bible data")


if __name__ == "__main__":
    main()