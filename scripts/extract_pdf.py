import os
import json
import pdfplumber

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "docs")
OUT_DIR = os.path.join(BASE_DIR, "..", "data")

FLAVOR_BIBLE = os.path.join(DATA_DIR, "flavor_bible.pdf")

START_PAGE = 463
END_PAGE = 951


# ---------- helpers ----------

def chars_to_words(chars):
    words = []
    current = []

    for c in chars:
        if c["text"].isspace():
            if current:
                words.append(current)
                current = []
        else:
            current.append(c)

    if current:
        words.append(current)

    return words


def word_features(word_chars):
    text = "".join(c["text"] for c in word_chars)
    fontnames = {c["fontname"] for c in word_chars}
    sizes = [c["size"] for c in word_chars]

    return {
        "text": text.strip(),
        "is_bold": any("Bold" in f for f in fontnames),
        "avg_size": sum(sizes) / len(sizes),
        "is_caps": text.strip().isupper()
    }


def group_chars_to_lines(chars, y_tolerance=3):
    lines = []
    current_line = []
    current_y = None

    for c in chars:
        if current_y is None:
            current_y = c["top"]

        if abs(c["top"] - current_y) <= y_tolerance:
            current_line.append(c)
        else:
            lines.append(current_line)
            current_line = [c]
            current_y = c["top"]

    if current_line:
        lines.append(current_line)

    return lines


# ---------- classification ----------

def is_heading(feats, avg_size):
    return (
        all(w["is_caps"] for w in feats)
        and any(w["is_bold"] for w in feats)
        and avg_size > 15
    )


def is_flavor_affinity_header(text):
    return text == "Flavor Affinities"


def is_metadata_line(text, has_bold):
    keys = ["Taste:", "Season:", "Function:", "Weight:", "Volume:", "Tips:"]
    return has_bold and any(text.startswith(k) for k in keys)


def is_dishes_line(text):
    return text.startswith("Dishes:")


def is_chef_line(text):
    return text.startswith("—") or (
        "(" in text and ")" in text and "," in text and len(text.split()) > 4
    )


def looks_like_sentence(text):
    return (
        "." in text or
        len(text.split()) > 6 or
        text.lower().startswith(("use ", "add ", "this ", "that "))
    )


def clean_text(text):
    return text.strip().lstrip("*")


def split_variants(text):
    if ":" in text:
        base, rest = text.split(":", 1)
        parts = [p.strip() for p in rest.split(",")]
        return [f"{base.strip()} {p}" for p in parts]
    return [text]


def score_from_feats(feats):
    # take strongest signal on the line
    has_caps = any(w["is_caps"] for w in feats)
    has_bold = any(w["is_bold"] for w in feats)
    text = " ".join(w["text"] for w in feats)

    if text.startswith("*") and has_caps:
        return 4
    elif has_caps:
        return 3
    elif has_bold:
        return 2
    else:
        return 1


# ---------- extraction ----------

def extract():
    results = {}

    current_root = None
    mode = "ingredients"

    with pdfplumber.open(FLAVOR_BIBLE) as pdf:
        for i in range(START_PAGE - 1, END_PAGE):
            page = pdf.pages[i]

            chars = sorted(page.chars, key=lambda c: (c["top"], c["x0"]))
            lines = group_chars_to_lines(chars)

            for line in lines:
                words = chars_to_words(line)
                feats = [word_features(w) for w in words]

                if not feats:
                    continue

                line_text = " ".join(w["text"] for w in feats).strip()
                has_bold = any(w["is_bold"] for w in feats)
                avg_size = sum(w["avg_size"] for w in feats) / len(feats)

                # ---- heading ----
                if is_heading(feats, avg_size):
                    current_root = clean_text(line_text)

                    if current_root not in results:
                        results[current_root] = {
                            "ingredients": [],
                            "affinities": [],
                            "metadata": {}
                        }

                    mode = "ingredients"
                    continue

                if not current_root:
                    continue

                # ---- section switch ----
                if is_flavor_affinity_header(line_text):
                    mode = "affinities"
                    continue

                # ---- metadata ----
                if is_metadata_line(line_text, has_bold):
                    key, val = line_text.split(":", 1)
                    results[current_root]["metadata"][key.strip().lower()] = val.strip()
                    continue

                # ---- skip junk ----
                if is_dishes_line(line_text):
                    continue

                if is_chef_line(line_text):
                    continue

                if looks_like_sentence(line_text):
                    continue

                # ---- ingredient list ----
                if mode == "ingredients":
                    raw_line = clean_text(line_text)
                    variants = split_variants(raw_line)
                    score = score_from_feats(feats)

                    for v in variants:
                        v = v.lower().strip()
                        if not v:
                            continue

                        results[current_root]["ingredients"].append({
                            "name": v,
                            "score": score
                        })

                # ---- affinities ----
                elif mode == "affinities" and "+" in line_text:
                    parts = [clean_text(p).lower() for p in line_text.split("+")]
                    results[current_root]["affinities"].append(parts)

    return results


# ---------- run ----------

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)

    data = extract()

    out_path = os.path.join(OUT_DIR, "flavor_bible_extract.json")

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Extracted {len(data)} entries → {out_path}")