import sqlite3
import os
import re
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


# ============================================================
# NORMALIZATION
# ============================================================

def normalize(text):
    if not text:
        return ""

    text = text.lower()

    # unify punctuation
    text = re.sub(r'[^a-z0-9\s\-]', ' ', text)

    # collapse whitespace
    text = " ".join(text.split())

    return text


# ============================================================
# STRIP JUNK TOKENS
# ============================================================

JUNK_WORDS = {
    "chunks", "chunk", "pieces", "piece", "finely", "rough",
    "halved", "lengthwise", "crosswise", "bite", "size",
    "sized", "cubed", "cubes", "dice", "diced", "bite-sized",
    "thin", "thick", "extra", "virgin", "pure",
    "fresh", "freshly", "large", "small",
    "boneless", "skinless", "removed", "reserved",
    "plus", "minus", "or", "and", "with", "from",
    "such", "as", "for", "serving", "garnish",
}


def strip_junk_tokens(text):
    tokens = text.split()
    tokens = [t for t in tokens if t not in JUNK_WORDS]
    return " ".join(tokens)


# ============================================================
# HIGH CONFIDENCE CANONICAL RULES
# ============================================================

# These solve your most important real-world errors
CANONICAL_RULES = [
    # --- fats / oils ---
    (r'.*olive oil.*', "olive oil"),
    (r'.*vegetable oil.*', "vegetable oil"),
    (r'.*canola oil.*', "canola oil"),
    (r'.*peanut oil.*', "peanut oil"),
    (r'.*coconut oil.*', "coconut oil"),
    (r'.*grapeseed|grape oil.*', "grapeseed oil"),
    (r'.*neutral oil.*', "neutral oil"),
    (r'.*sesame oil.*', "sesame oil"),
    (r'.*basil pesto|pesto.*', "basil pesto"),
    (r'.*rendered|reserved fat|fat cap|animal fat|chicken fat|schmalz|schmaltz|drippings.*', "lard"),
    (r'.*shortening.*', "shortening"),

    # --- butter ---
    (r'.*cashew butter.*', 'cashew butter'),
    (r'.*cashew.*', 'cashew'),
    (r'.*pine nut.*', 'pine nut'),
    (r'.*peanut butter.*', 'peanut butter'),
    (r'.*buttermilk.*', 'buttermilk'),
    (r'.*cocoa butter.*', 'cocoa butter'),
    (r'.*chocolate chips|chocolate morsels|chocolate pieces|chocolate chunks|chocolate drops.*', 'chocolate chips'),
    (r'.*bittersweet|semisweet|semi-sweet.*', "chocolate"),
    (r'.*butter.*', "butter"),

    # --- mushrooms ---
    (r'.*crimini.*', "crimini"),
    (r'.*shiitake.*', "shiitake"),
    (r'.*oyster mushroom.*', "oyster mushroom"),
    (r'.*portobello.*', "portobello"),
    (r'.*porcini.*', "porcini"),
    (r'.*mushroom.*', "mushroom"),

    (r'.*turkey*(stock|broth).*', "turkey stock"),
    (r'.*turkey breast.*', "turkey breast"),
    (r'.*turkey thigh.*', "turkey thigh"),
    (r'.*turkey.*', "turkey"),

    # --- chicken ---
    (r'.*chicken.*(stock|broth|jus|juice).*', "chicken stock"),
    (r'.*chicken.*(base|bouillon).*', "bouillon"),    
    (r'.*chicken breast.*', "chicken breast"),
    (r'.*chicken thigh.*', "chicken thigh"),
    (r'.*chicken wing.*', "chicken wing"),
    (r'.*chicken leg|leg quarter|drumstick.*', "chicken leg"),
    (r'.*chicken.*', "chicken"),

    # --- pork ---
    (r'.*pork chop|porkchop|loin chop.*', "pork chop"),
    (r'.*pork belly.*', "pork belly"),
    (r'.*italian sausage.*', "italian sausage"),
    (r'.*sausage|sausages.*', "sausage"),
    (r'.*pork.*', "pork"),
    (r'.*bacon|pancetta.*', "bacon"),

    # --- seafood ---
    (r'.*shrimp|prawns.*', "shrimp"),
    (r'.*whitefish|cod|tilapia|haddock|sole.*', "whitefish"),
    (r'.*canned salmon.*', "canned salmon"),
    (r'.*canned tuna.*', "canned tuna"),
    (r'.*salmon.*', "salmon"),
    (r'.*tuna|mahi.*', "tuna"),
    (r'.*crab.*', "crab"),
    (r'.*clam juice.*', "clam juice"),
    (r'.*clam*', "clams"),
    (r'.*lobster.*', "lobster"),
    (r'.*squid|calamari.*', "squid"),

    # --- beef ---
    (r'.*lamb.*', "lamb"),
    (r'.*beef.*(stock|broth).*', "beef stock"),
    (r'.*steak.*', "steak"),
    (r'.*beef|hamburger|chuck roast|rib roast|chuck.*', "beef"),

    # --- junk food ---
    (r'.*corn chips|tortilla chips.*', "corn chips"),
    (r'.*potato chips|kettle chips|kettle-style.*', "potato chips"),
    (r'.*cookie|shortbread|grahams|gingersnap.*', "cookies"),    
    (r'.*crackers.*', "crackers"),
    (r'.*tater tots.*', "tater tots"),
    (r'.*fries.*', "fries"),

    # melting cheeses
    (r'.*cheddar powder.*', 'cheddar powder'),
    (r'.*cheese powder.*', 'cheese powder'),
    (r'.*fresh mozzarella.*', 'fresh mozzarella'),
    (r'.*mozzarella.*', 'mozzarella'),
    (r'.*monterey jack.*', 'monterey jack'),
    (r'.*colby.*', 'colby jack'),
    (r'.*muenster.*', 'muenster'),
    (r'.*swiss cheese.*', 'swiss cheese'),
    (r'.*gruyere.*', 'gruyere'),
    (r'.*american cheese.*', 'american cheese'),
    (r'.*melting cheese.*', 'melting cheese'),

    # --- dairy ---
    (r'.*soy milk.*', 'soy milk'),
    (r'.*oat milk.*', 'oat milk'),
    (r'.*almond milk.*', 'almond milk'),
    (r'.*whipped cream.*', 'whipped cream'),
    (r'.*coconut milk.*', 'coconut milk'),
    (r'.*sweetened condensed milk.*', 'sweetened condensed milk'),
    (r'.*evaporated milk.*', 'evaporated milk'),
    (r'.*ricotta.*', 'ricotta'),
    (r'.*cream cheese.*', 'cream cheese'),
    (r'.*ricotta.*', 'ricotta'),
    (r'.*milk powder|powdered milk.*', "milk powder"),
    
    # soft / fresh
    (r'.*cottage cheese.*', 'cottage cheese'),
    (r'.*sour cream.*', 'sour cream'),
    (r'.*coconut cream.*', 'coconut cream'),
    (r'.*ricotta.*', 'ricotta'),

    # sharp / hard
    (r'.*cheddar.*', 'cheddar'),
    (r'.*sharp cheese.*', 'sharp cheese'),
    (r'.*parmesan|parmigiano|pecorino.*', 'parmesan'),

    # fallback
    (r'.*cheese.*', 'cheese'),

    # --- fruits & vegetables ---
    (r'.*chili garlic.*', "chili garlic sauce"),
    (r'.*garlic powder|granulated garlic|dehydrated garlic.*', "garlic powder"),
    (r'.*garlic salt.*', "garlic salt"),
    (r'.*carrot.*', "carrot"),
    (r'.*celery.*', "celery"),
    (r'.*broccoli.*', 'broccoli'),
    (r'.*tarragon.*', "tarragon"),
    (r'.*chervil.*', "chervil"),
    (r'.*spinach.*', "spinach"),
    (r'.*brussels sprouts|brussels|brussel.*', "brussels sprouts"),
    (r'.*bell pepper.*', "bell pepper"),
    (r'.*apricot.*', "apricot"),
    (r'.*shallot.*', "shallot"),
    (r'.*apple sauce|applesauce.*', "apple sauce"),
    (r'.*apple.*', "apple"),
    (r'.*raisins.*', "raisins"),
    (r'.*sage.*', "sage"),
    (r'.*chive.*', "chive"),
    (r'.*green onion|scallion|scalion|spring onion.*', "green onion"),
    (r'.*green pepper.*', "green pepper"),
    (r'.*zucchini.*', "zucchini"),
    (r'.*yellow squash|summer squash.*', "yellow squash"),
    (r'.*kale.*', "kale"),
    (r'.*collard.*', "collard greens"),
    (r'.*asparagus.*', 'asparagus'),
    (r'.*coconut extract.*', "coconut extract"),
    (r'.*coconut.*', "coconut"),
    (r'.*strawberry|strawberries.*', "strawberry"),
    (r'.*blueberry|blueberries.*', "blueberry"),
    (r'.*banana.*', "banana"),
    (r'.*water chestnut.*', 'water chestnut'),
    (r'.*avocado|guacamole.*', 'avocado'),
    (r'.*eggplant.*', 'eggplant'),
    (r'.*bamboo.*', 'bamboo shoots'),
    (r'.*sweet potato|sweet potatoes|yams.*', "sweet potato"),
    (r'.*potato starch.*', "potato starch"),
    (r'.*onion.*', "onion"),
    (r'.*potato.*', "potato"),
    
    # --- tomatoes ---
    (r'.*tomato juice|v8.*', "tomato juice"),
    (r'.*cherry tomato|grape tomato.*', "cherry tomato"),
    (r'.*tomato paste.*', "tomato paste"),
    (r'.*tomato powder.*', "tomato powder"),  
    (r'.*tomato sauce|pizza sauce.*', "tomato sauce"),
    (r'.*tomato puree.*', "tomato puree"),
    (r'.*diced tomato|diced tomatoes.*', "diced tomato"),
    (r'.*crushed tomato|crushed tomatoes.*', "crushed tomato"),    
    (r'.*tomato|tomatoes.*', "tomato"),

    # --- grains ---
    (r'.*bread flour|pizza flour|00 flour|hard flour.*', "bread flour"),
    (r'.*cake flour|soft flour.*', "cake flour"),
    (r'.*oats|oatmeal.*', "oats"),
    (r'.*all purpose|all-purpose|ap flour.*', 'all purpose flour'),
    (r'.*rice flour.*', "rice flour"),
    (r'.*corn flour.*', "corn flour"),
    (r'.*cornstarch|corn starch.*', "cornstarch"),
    (r'.*potato starch.*', "potato starch"),
    (r'.*bread|baguette|bread crumbs|panko|crouton.*', "bread"),
    (r'.*pie dough|pie shell.*', "pie dough"),
    (r'.*flour.*', "flour"),
    (r'.*rice cereal|rice krispies|rice krispy|rice crispies.*', "rice cereal"),
    (r'.*rice noodles|rice sticks.*', "rice noodles"),
    (r'.*bean thread.*', "bean thread noodles"),
    (r'.*sushi rice.*', "sushi rice"),
    (r'.*brown rice.*', "brown rice"),
    (r'.*rice|white rice.*', "rice"),
    (r'.*pasta.*|.*spaghetti.*|.*noodle.*', "pasta"),

    # --- legumes ---
    (r'.*chili bean.*', "chili bean paste"),
    (r'.*black bean.*', "black beans"),
    (r'.*kidney bean.*', "kidney beans"),
    (r'.*tofu.*', "tofu"),
    (r'.*pinto bean.*', "pinto beans"),
    (r'.*garbanzo bean.*|chickpea.*', "chickpeas"),
    (r'.*red kidney bean.*', "red kidney beans"),
    (r'.*bean.*', "beans"),

    # --- liquids ---
    (r'.*oyster sauce.*', "oyster sauce"),
    (r'.*worcestershire.*|worchestershire|worcester', "worcestershire sauce"),
    (r'.*balsamic.*', "balsamic vinegar"),
    (r'.*rice wine vinegar|rice vinegar.*', "rice vinegar"),
    (r'.*soy sauce.*', "soy sauce"),
    (r'.*fish sauce.*', "fish sauce"),
    (r'.*miso.*', "miso"),
    (r'.*tamarind.*', "tamarind"),
    (r'.*hot sauce|tabasco.*', "hot sauce"),
    (r'.*pepper sauce.*', "pepper sauce"),
    (r'.*red wine vinegar.*', " red wine vinegar"),
    (r'.*red wine.*', " red wine"),
    (r'.*white wine vinegar.*', "white wine vinegar"),
    (r'.*white wine.*', "white wine"),
    (r'.*vinegar powder.*', "vinegar powder"),
    (r'.*vinegar.*', "vinegar"),
    (r'.*shaoxing|xiaoxing .*', "shaoxing wine"),
    (r'.*wine.*', "wine"),
    (r'.*ginger beer.*', "ginger beer"),
    (r'.*beer.*', "beer"),

    # --- sugar ---
    (r'.*powdered sugar.*', "powdered sugar"),
    (r'.*confectioners sugar.*', "confectioners sugar"),
    (r'.*brown sugar.*', "brown sugar"),
    (r'.*molasses.*', "molasses"),
    (r'.*corn syrup.*', "corn syrup"),
    (r'.*maple syrup.*', "maple syrup"),
    (r'.*syrup.*', "syrup"),
    (r'.*sugar.*', "sugar"),
    (r'.*honey.*', "honey"),

    # --- herbs & spices ---
    (r'.*white pepper.*', "white pepper"),
    (r'.*black pepper|ground pepper.*', "black pepper"),
    (r'.*allspice.*', "allspice"),
    (r'.*juniper.*', "juniper"),
    (r'.*coriander seed|coriander.*', "coriander seed"),
    (r'.*sesame seed.*', "sesame seed"),
    (r'.*cardamom.*', "cardamom"),
    (r'.*cinnamon.*', "cinnamon"),
    (r'.*cloves.*', "cloves"),
    (r'.*anise.*', "anise"),
    (r'.*cilantro|coriander leaf|coriander leaves.*', "cilantro"),
    (r'.*rosemary.*', "rosemary"),
    (r'.*oregano.*', "oregano"),
    (r'.*parsley.*', "parsley"),
    (r'.*basil.*', "basil"),
    (r'.*cumin.*', "cumin"),
    (r'.*fennel seed.*', "fennel seed"),
    (r'.*caraway.*', "caraway"),
    (r'.*marjoram.*', "marjoram"),
    (r'.*thyme.*', "thyme"),    
    (r'.*cocoa.*', "cocoa powder"),
    (r'.*espresso.*', "espresso"),
    (r'.*cayenne.*', "cayenne"),
    (r'.*chili flakes|pepper flakes|crushed red pepper.*', 'red pepper flakes'),
    (r'.*chili powder|chilli powder|chile powder|pepper powder.*', 'chili powder'),
    (r'.*curry powder.*', "curry powder"),
    (r'.*paprika.*', "paprika"),    
    (r'.*turmeric.*', "turmeric"),
    (r'.*mustard powder|powdered mustard|mustard seed|dry mustard|dried mustard.*', "mustard seed"),
    (r'.*grey poupon|dijon|mustard.*', "mustard"),
    (r'.*ground ginger|ginger powder|dry ginger|dried ginger.*', "ground ginger"),
    (r'.*orange zest|orange peel.*', "orange zest"),
    (r'.*lime zest|lime peel.*', "lime zest"),
    (r'.*zest|lemon peel.*', "lemon zest"),
    (r'.*lemon juice|lemons|lemon.*', "lemon"),
    (r'.*lime juice|limes|lime.*', "lime"),

    # --- chemicals ---
    (r'.*baking powder.*', "baking powder"),
    (r'.*baking soda.*', "baking soda"),
    (r'.*cream of tartar.*', "cream of tartar"),
    (r'.*xanthan gum.*', "xanthan gum"),
    (r'.*sodium citrate.*', "sodium citrate"),
    (r'.*cloves.*', "cloves"),

    # --- fallback ---
    (r'.*berry|berries.*', "berry"),
    (r'.*ginger.*', "ginger"),
    (r'.*garlic.*', "garlic"),
    (r'.*stock|broth.*', "stock"),
    (r'.*milk.*', "milk"),
    (r'.*cream.*', "cream"),
    (r'.*bay leaf|bay leaves|bay.*', "bay leaf"),
    (r'.*egg white|whites.*', "egg white"),
    (r'.*egg yolk|yolk.*', "egg yolk"),
    (r'.*water.*', "water"),
    (r'.*peanut|peanuts.*', "peanuts"),
    (r'.*pea.*', "peas"),
    (r'.*base.*', "bouillon"),
    (r'.*jalapeno|chile|chilli|chili|peppers.*', "chili pepper"),
    (r'.*black pepper.*', "black pepper"),
    (r'.*salt.*', "salt"),
    (r'\begg(s)?\b', "egg"),
    (r'.*oil.*', "oil"),
]


# ============================================================
# DERIVE CANONICAL
# ============================================================

def derive_canonical(name):
    if not name:
        return None, "empty"

    original = name
    text = normalize(name)

    if not text or len(text) < 2:
        return None, "garbage"

    text = strip_junk_tokens(text)

    # --- apply rules ---
    for pattern, canonical in CANONICAL_RULES:
        if re.search(pattern, text):
            return canonical, "rule"

    # --- fallback: last meaningful token ---
    tokens = text.split()

    if not tokens:
        return None, "garbage"

    base = tokens[-1]

    # crude singularization
    if base.endswith("es"):
        base = base[:-2]
    elif base.endswith("s") and not base.endswith("ss"):
        base = base[:-1]

    return base, "fallback"


# ============================================================
# RUN + METRICS
# ============================================================

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("SELECT id, name FROM ingredients")
rows = c.fetchall()

stats = defaultdict(int)
fail_examples = []

for ing_id, name in rows:
    canonical, method = derive_canonical(name)

    stats["total"] += 1
    stats[method] += 1

    if method in ("garbage", "empty"):
        fail_examples.append((ing_id, name))

    c.execute("""
        UPDATE ingredients
        SET canonical_group = ?
        WHERE id = ?
    """, (canonical, ing_id))


conn.commit()
conn.close()


# ============================================================
# REPORT
# ============================================================

print("\n=== CANONICALIZATION REPORT ===")
print(f"Total: {stats['total']}")
print(f"Rule-based: {stats['rule']}")
print(f"Fallback: {stats['fallback']}")
print(f"Garbage: {stats['garbage']}")
print(f"Empty: {stats['empty']}")

print("\n--- Needs Review (sample) ---")
for row in fail_examples[:20]:
    print(row)

print("\nDone.")