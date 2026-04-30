import asyncio
import csv
import json
import os
import sqlite3
import re
from playwright.async_api import async_playwright
import pandas as pd
import random

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

RECIPES_CSV = os.path.join(DATA_DIR, "recipes.csv")
ROWS_CSV = os.path.join(DATA_DIR, "recipe_rows.csv")

# ---------- ID GENERATION ----------
def gen_id():
    return random.randint(100000, 999999)

# ---------- JSON-LD EXTRACTION ----------
def extract_recipe_json_ld(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    scripts = soup.find_all("script", type="application/ld+json")

    for script in scripts:
        if not script.string:
            continue

        try:
            data = json.loads(script.string)

            # Case: list of objects
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Recipe":
                        return item

            # Case: single object
            if isinstance(data, dict):
                if data.get("@type") == "Recipe":
                    return data

        except json.JSONDecodeError:
            continue

    return None

# ---------- SCRAPER ----------
async def scrape_foodnetwork(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)  # allow JS to render

        html = await page.content()
        await browser.close()

    recipe_json = extract_recipe_json_ld(html)

    if not recipe_json:
        print("FAILED: No JSON-LD recipe found")
        return None

    # ---------- METADATA ----------
    recipe_id = gen_id()

    name = recipe_json.get("name")
    author = None

    if isinstance(recipe_json.get("author"), dict):
        author = recipe_json["author"].get("name")

    ingredients = recipe_json.get("recipeIngredient", [])
    instructions_raw = recipe_json.get("recipeInstructions", [])

    # Normalize instructions
    instructions = []
    if isinstance(instructions_raw, list):
        for step in instructions_raw:
            if isinstance(step, dict):
                instructions.append(step.get("text", ""))
            else:
                instructions.append(step)
    elif isinstance(instructions_raw, str):
        instructions = [instructions_raw]

    # ---------- BUILD CSV ROWS ----------

    recipe_row = {
        "id": recipe_id,
        "recipe_name": name,
        "alt_names": None,
        "recipe_author": author,
        "recipe_attribution": f"From {author} at Food Network" if author else "Food Network",
        "recipe_source": "foodnetwork",
        "recipe_url": url,
        "images": None,
        "videos": None,
        "notes": None,
        "yield": recipe_json.get("recipeYield")
    }

    row_id = gen_id()

    recipe_rows_entry = {
        "id": row_id,
        "recipe_id": recipe_id,
        "recipe_name": name,
        "source_row_ref": 0,
        "section_name": "main",
        "ingredient_block": "\n".join(ingredients),
        "instruction_block": "\n".join(instructions)
    }

    return {
        "recipes": [recipe_row],
        "recipe_rows": [recipe_rows_entry]
    }

# ---------- APPEND TO CSV ----------
def append_to_csv(data):
    recipes_df = pd.DataFrame(data["recipes"])
    rows_df = pd.DataFrame(data["recipe_rows"])

    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(RECIPES_CSV):
        recipes_df.to_csv(
            RECIPES_CSV,
            mode="a",
            header=not os.path.exists(RECIPES_CSV),
            index=False,
            quoting=csv.QUOTE_ALL,
            escapechar="\\"
        )
    else:
        recipes_df.to_csv(RECIPES_CSV, index=False)

    if os.path.exists(ROWS_CSV):
        rows_df.to_csv(
            ROWS_CSV,
            mode="a",
            header=not os.path.exists(ROWS_CSV),
            index=False,
            quoting=csv.QUOTE_ALL,
            escapechar="\\"
        )
    else:
        rows_df.to_csv(ROWS_CSV, index=False)

# ---------- RUNNER ----------
def load_existing_urls():
    if not os.path.exists(RECIPES_CSV):
        return set()

    df = pd.read_csv(RECIPES_CSV)
    return set(df["recipe_url"].dropna().tolist())

async def run():
    links_df = pd.read_csv(os.path.join(DATA_DIR, "recipe_links.csv"))

    existing_urls = load_existing_urls()

    for _, row in links_df.iterrows():
        url = row["recipe_url"]

        if "foodnetwork.com" not in url:
            continue

        if url in existing_urls:
            print(f"SKIP (already scraped): {url}")
            continue

        print(f"Scraping: {url}")

        data = await scrape_foodnetwork(url)

        if data:
            append_to_csv(data)
            print("SUCCESS\n")
        else:
            print("FAILED\n")

if __name__ == "__main__":
    asyncio.run(run())