import re


def is_metadata_line(line):
    return bool(re.match(
        r"^(author|source|url|yield|note|notes|attribution|video):",
        line,
        re.IGNORECASE
    ))


def new_section(name="Main"):
    return {
        "name": name,
        "ingredients": [],
        "instructions": []
    }


def parse_markdown_file(filepath):
    with open(filepath, "r") as f:
        lines = f.readlines()

    recipes = []
    current_recipe = None
    current_section = None
    mode = None

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        # -------------------------
        # RECIPE START
        # -------------------------
        if re.match(r"^#(?!#)\s*", line):
            if current_recipe:
                recipes.append(current_recipe)

            current_recipe = {
                "name": re.sub(r"^#\s*", "", line).strip(),
                "metadata": {},
                "sections": []
            }

            # ALWAYS create a default section
            current_section = new_section()
            current_recipe["sections"].append(current_section)

            mode = None
            continue

        # ignore anything before first recipe
        if current_recipe is None:
            continue

        # -------------------------
        # SECTION START
        # -------------------------
        if re.match(r"^##(?!#)\s*", line):
            current_section = new_section(
                re.sub(r"^##\s*", "", line).strip()
            )
            current_recipe["sections"].append(current_section)
            mode = None
            continue

        # -------------------------
        # METADATA
        # -------------------------
        if is_metadata_line(line):
            key, value = line.split(":", 1)
            current_recipe["metadata"][key.strip().lower()] = value.strip()
            continue

        # -------------------------
        # MODE SWITCH
        # -------------------------
        if line.lower() == "ingredients:":
            mode = "ingredients"
            continue

        if line.lower() == "instructions:":
            mode = "instructions"
            continue

        # -------------------------
        # CRITICAL FIX:
        # Ensure section ALWAYS exists
        # -------------------------
        if current_section is None:
            current_section = new_section()
            current_recipe["sections"].append(current_section)

        # -------------------------
        # CONTENT
        # -------------------------
        if mode == "ingredients":
            cleaned = re.sub(r"^[-*]\s*", "", line)
            current_section["ingredients"].append(cleaned)

        elif mode == "instructions":
            current_section["instructions"].append(line)

        else:
            # fallback heuristic
            if re.match(r"^[-*\d]", line):
                cleaned = re.sub(r"^[-*]\s*", "", line)
                current_section["ingredients"].append(cleaned)
            else:
                current_section["instructions"].append(line)

    if current_recipe:
        recipes.append(current_recipe)

    return recipes