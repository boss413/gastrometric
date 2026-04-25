import os
import pdfplumber

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "docs")

FLAVOR_BIBLE = os.path.join(DATA_DIR, "flavor_bible.pdf")

START_PAGE = 80
END_PAGE = 82


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
        "text": text,
        "is_bold": any("Bold" in f for f in fontnames),
        "avg_size": sum(sizes) / len(sizes),
        "is_caps": text.isupper()
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


with pdfplumber.open(FLAVOR_BIBLE) as pdf:
    for i in range(START_PAGE - 1, END_PAGE):
        page = pdf.pages[i]

        # sort chars into reading order
        chars = sorted(page.chars, key=lambda c: (c["top"], c["x0"]))

        lines = group_chars_to_lines(chars)

        print(f"\n=== Page {page.page_number} ===")

        for line in lines:
            words = chars_to_words(line)
            word_feats = [word_features(w) for w in words]

            if not word_feats:
                continue

            line_text = " ".join(w["text"] for w in word_feats)

            has_bold = any(w["is_bold"] for w in word_feats)
            has_caps = any(w["is_caps"] for w in word_feats)
            avg_size = sum(w["avg_size"] for w in word_feats) / len(word_feats)

            print(line_text)
            print(f"  -> size={avg_size:.1f}, bold={has_bold}, caps={has_caps}")