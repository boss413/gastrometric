# data/exclusions.py

BRAND_TERMS = {
    "kraft", "pillsbury", "campbell", "nestle", "kellogg",
    "heinz", "oreo", "hershey", "pepsi", "coca-cola",
    "general mills", "kraft foods", "conagra", "tyson",
    "mccormick", "stouffer", "dole", "unilever", "subway", 
    "quaker", "on the border", "wendy's", "burger king", "domino's", "pizza hut",
    "mcdonald's", "taco bell", "chipotle", "panera", "digiorno", "school lunch",
    "little caesars", "arby's", "carl's jr", "hardee's", 
    "jack in the box", "sonic", "dairy queen", "malt-o-meal",
    "slim-fast", "slimfast", "cracker barrel", "betty crocker", "pillsbury",
    "denny's", "ihop", "olive garden", "red lobster", "cheesecake factory",
    "applebee's", "chili's", "outback steakhouse", "red robin", "buffalo wild wings",
    "t.g.i. friday's", "longhorn steakhouse", "ruby tuesday", "red roof inn",
    "carrabba's", "kfc", "popeye", "chick-fil-a",

}

ULTRA_PROCESSED_KEYWORDS = {
    "ready-to-eat", "tv dinner", "frozen meal", "microwav",
    "restaurant", "fast food", "drive-in",
    "with sauce", "in gravy", "meal kit"
}

EXCLUDE_FOOD_TYPES = {
    "babyfood",  # optional depending on your goals
}

# allowlist override: always keep these even if they look processed
ALLOWLIST = {
    "ketchup",
    "mayonnaise",
    "mustard",
    "soy sauce",
    "hot sauce",
    "vinegar",
}