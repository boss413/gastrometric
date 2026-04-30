import os
import re
import json
import pdfplumber

from gastrometric.config.paths import DOCS_DIR, BASE_DIR

OUT_DIR = os.path.join(BASE_DIR, "data")
FLAVOR_BIBLE = os.path.join(DOCS_DIR, "flavor_bible.pdf")

START_PAGE = 68
END_PAGE = 958


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


def merge_wrapped_lines(lines):
    merged = []
    buffer = ""

    for line in lines:
        text = line.strip()

        if not text:
            continue

        # merge if previous line looks incomplete
        if buffer and (
            buffer.endswith(",") or
            buffer.endswith("(") or
            not re.search(r"[a-zA-Z0-9\)]$", buffer)
        ):
            buffer += " " + text
        else:
            if buffer:
                merged.append(buffer)
            buffer = text

    # FIX 1: flush the final buffered line — was missing, causing the last
    # line of every page to be silently dropped
    if buffer:
        merged.append(buffer)

    return merged


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


# FIX 2: removed the recursive self-call and the misplaced merge block that
# had been copy-pasted into the middle of this function.  The function now
# does exactly one thing: group a flat list of char-dicts into a list of
# lines, where each line is itself a list of char-dicts.
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

def is_heading_line(text, avg_size, body_size):
    """
    Returns True only when the line is both typographically and textually a
    top-level heading.

    avg_size  - mean font size of the chars on this line
    body_size - median font size across the whole page (the "normal" size)

    Headings are set in a noticeably larger font than body text.  ALL-CAPS
    ingredient pairings (score 3/4) share the body font, so the size gate is
    the primary discriminator.
    """
    text = text.strip()

    if len(text) < 2:
        return False

    # ---- font-size gate (the key fix) ----
    # Require the line's average glyph size to be at least 10% larger than
    # the page's body size.  Score-3 ALL-CAPS ingredients are set in the same
    # body font and will fail here; true headings are visibly larger.
    if avg_size < body_size * 1.10:
        return False

    words = text.split()

    # Must have at least one fully-uppercase alphabetic word
    if not any(w.isupper() and w.isalpha() for w in words):
        return False

    # Strong signal: contains the token "CUISINE"
    if "CUISINE" in text:
        return True

    # High caps ratio - at least 80% of alpha words are ALL-CAPS
    alpha_words = [w for w in words if re.sub(r"[^a-zA-Z]", "", w)]
    if not alpha_words:
        return False

    caps_ratio = sum(
        1 for w in alpha_words if re.sub(r"[^a-zA-Z]", "", w).isupper()
    ) / len(alpha_words)

    return caps_ratio >= 0.8


def is_flavor_affinity_header(text):
    return text == "Flavor Affinities"


def is_metadata_line(text, has_bold):
    keys = ["Taste:", "Season:", "Function:", "Weight:", "Volume:", "Tips:"]
    return has_bold and any(text.startswith(k) for k in keys)


def is_dishes_line(text):
    return text.startswith("Dishes:")


def is_chef_line(text):
    # Attribution lines: start with em-dash or contain chef/restaurant format
    if text.startswith("—") or text.startswith("–"):
        return True
    # "Name, Restaurant (City, State)" pattern
    if re.search(r"—\s*[A-Z]", text):
        return True
    return False


def looks_like_sentence(text):
    # Possessives and contractions strongly indicate prose / chef quotes
    if re.search(r"\b(it's|its|i'm|i've|they're|don't|doesn't|it's)\b",
                 text, re.IGNORECASE):
        return True
    # Pronoun-led sentences
    if text.lower().startswith(("i ", "it ", "use ", "add ", "this ", "that ",
                                "in ", "when ", "for ", "you ")):
        return True
    # Ends with a period and has enough words to be a sentence
    if text.endswith(".") and len(text.split()) > 4:
        return True
    # Very long lines are almost always prose, not ingredient names
    if len(text.split()) > 8:
        return True
    return False


def clean_text(text):
    return text.strip().lstrip("*")


def split_variants(text):
    # Only split on "key: val1, val2" patterns where the part before the colon
    # looks like a short label (no spaces), e.g. "oils: olive, sesame".
    # This avoids splitting ingredient qualifiers like "fish, esp. grilled"
    # or "citrus (e.g., sour orange)".
    if ":" in text:
        base, rest = text.split(":", 1)
        # Only treat as variant expansion if base is a single short token
        if " " not in base.strip() and len(base.strip()) <= 20:
            parts = [p.strip() for p in rest.split(",")]
            return [f"{base.strip()} {p}" for p in parts if p]
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


def split_multi_headings(text):
    """
    Splits lines like:
    'TEX-MEX CUISINE (...) THAI CUISINE'
    into:
    ['TEX-MEX CUISINE', 'THAI CUISINE']
    """
    # remove "(See ...)" blocks completely
    text = re.sub(r"\(See[^)]*\)", "", text)

    # find ALL uppercase heading chunks
    matches = re.findall(r"[A-Z][A-Z\s\-\(\)]+CUISINE(?:\s\([A-Z]+\))?", text)

    if matches:
        return [m.strip() for m in matches]

    return [text]


# ---------- extraction ----------

def extract():
    results = {}

    current_root = None
    mode = "ingredients"

    with pdfplumber.open(FLAVOR_BIBLE) as pdf:
        for i in range(START_PAGE - 1, END_PAGE):
            page = pdf.pages[i]

            chars = sorted(page.chars, key=lambda c: (c["top"], c["x0"]))

            # FIX 3: wire the merge pipeline correctly.
            # group_chars_to_lines returns lists-of-char-dicts (one per line).
            # Convert each to a text string, then merge wrapped lines before
            # any classification logic runs.
            char_lines = group_chars_to_lines(chars)

            # Compute the page's body font size as the median of all char sizes.
            # This is the baseline against which heading lines are compared.
            all_sizes = sorted(c["size"] for c in chars if c.get("size"))
            body_size = all_sizes[len(all_sizes) // 2] if all_sizes else 10

            raw_lines = []
            # Keep a parallel list so we can recover font features after merging.
            # Each entry maps the merged line's text to the char-level data for
            # the first (and usually only) physical line that produced it —
            # good enough for bold/caps scoring.
            line_chars_map = {}

            for char_line in char_lines:
                words = chars_to_words(char_line)
                word_feats = [word_features(w) for w in words]
                line_text = " ".join(w["text"] for w in word_feats).strip()

                if line_text:
                    raw_lines.append(line_text)
                    # store feats keyed by text so we can look them up later
                    line_chars_map[line_text] = word_feats

            merged_lines = merge_wrapped_lines(raw_lines)

            # FIX 4: iterate over merged text strings, not raw char-line lists.
            # Reconstruct feats from the map; fall back to an empty list when a
            # merged line has no exact match (two physical lines were joined).
            for line_text in merged_lines:
                feats = line_chars_map.get(line_text, [])

                if not feats:
                    # Merged line: rebuild minimal feats from the text alone so
                    # scoring still works (caps/bold will be conservative).
                    feats = [{"text": w, "is_bold": False, "avg_size": 10,
                              "is_caps": w.isupper()}
                             for w in line_text.split()]

                has_bold = any(w["is_bold"] for w in feats)
                avg_size = sum(w["avg_size"] for w in feats) / len(feats)

                # ---- skip junk first, before any classification ----
                # Chef quotes and prose must be rejected before is_heading_line
                # sees them, because a capitalised sentence fragment can
                # accidentally pass the caps-ratio check.
                if is_chef_line(line_text):
                    continue

                if looks_like_sentence(line_text):
                    continue

                if is_dishes_line(line_text):
                    continue

                # ---- heading ----
                if is_heading_line(line_text, avg_size, body_size):
                    headings = split_multi_headings(line_text)

                    for h in headings:
                        h_clean = clean_text(h)

                        if h_clean not in results:
                            results[h_clean] = {
                                "ingredients": [],
                                "affinities": [],
                                "metadata": {}
                            }

                        current_root = h_clean  # last one becomes active

                    mode = "ingredients"
                    # FIX 5: removed the duplicate dead block that appeared
                    # after this continue — it could never be reached.
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