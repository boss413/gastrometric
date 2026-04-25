import json
import os
import csv

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

INPUT = os.path.join(DATA_DIR, "flavor_bible_extract.json")
OUT = os.path.join(DATA_DIR, "flavor_bible_edges.csv")

def dedupe(rows):
    best = {}

    for r in rows:
        key = (r["source"], r["target"])
        if key not in best or r["score"] > best[key]["score"]:
            best[key] = r

    return list(best.values())

def main():
    with open(INPUT) as f:
        data = json.load(f)

    rows = []

    for source, payload in data.items():
        source_norm = source.lower()

        for item in payload["ingredients"]:
            target = item["name"]
            score = item["score"]

            rows.append({
                "source": source_norm,
                "target": target,
                "score": score
            })

    with open(OUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "target", "score"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows → {OUT}")


if __name__ == "__main__":
    main()