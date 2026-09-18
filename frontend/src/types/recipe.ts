/**
 * Recipe discovery types, modeled directly on the confirmed backend
 * response shapes for GET /api/recipes/search and GET /api/recipes/matches
 * (FE-04 work order §1/§2). Deliberately excludes fields the matches
 * endpoint doesn't provide (quantities, servings, images) — those belong
 * to the recipe-detail endpoint (FE-05).
 */

export type RecipeSearchType = "name" | "ingredient";

/**
 * `match_reason` is only ever inspected for existence, never rendered or
 * interpreted structurally (work order §7: "do not attempt to interpret
 * arbitrary future match_reason structures"), so it's typed as an opaque
 * unknown-shaped record rather than modeled field-by-field.
 *
 * `alt_title`/`restaurant`/`source`/`attribution` are the finalized
 * common browse-card fields (FE-06 revision "Final Backend Contracts §2")
 * — this replaces the earlier FE-06 implementation's unconfirmed guess at
 * these same field names.
 */
export interface RecipeSearchResult {
  recipe_id: number;
  recipe_name: string;
  alt_title: string | null;
  restaurant: string | null;
  source: string | null;
  attribution: string | null;
  matched_ingredient: string | null;
  match_type: string | null;
  match_reason: Record<string, unknown> | null;
  matched_term: string | null;
}

export interface RecipeSearchResponse {
  results: RecipeSearchResult[];
}

export type MatchCategory = "perfect" | "imperfect" | "pantry_only";

/**
 * Finalized shape (FE-06 revision "Final Backend Contracts §3"):
 * `location` is now authoritative and explicit — never inferred from
 * `fridge_match_count`, array position, or anything else. This replaces
 * the earlier FE-06 implementation's incorrect assumption that
 * `matched_ingredients` was fridge-only; it can now contain both fridge
 * and pantry entries, distinguished only by this field.
 */
export interface MatchedIngredient {
  recipe_ingredient_id: string;
  ingredient_name_original: string;
  inventory_item_id: number;
  location: "fridge" | "pantry";
  match_type: string;
  reason: Record<string, unknown>;
}

/**
 * Finalized shape. `name` is part of the real transport contract but is
 * explicitly NOT used for display — `ingredient_name_original` is the
 * only field rendered (FE-06 revision "Missing list").
 */
export interface MissingIngredient {
  ingredient_id: string;
  ingredient_name_original: string;
  name: string;
}

/**
 * Finalized common browse fields (FE-06 revision "Final Backend
 * Contracts §3") — replaces the earlier FE-06 implementation's
 * unconfirmed guess at these same field names.
 */
export interface RecipeMatch {
  recipe_id: number;
  recipe_name: string;
  alt_title: string | null;
  restaurant: string | null;
  source: string | null;
  attribution: string | null;
  match_category: MatchCategory;
  matched_ingredients: MatchedIngredient[];
  missing_ingredients: MissingIngredient[];
  fridge_match_count: number;
  pantry_match_count: number;
}

export interface RecipeMatchResponse {
  results: RecipeMatch[];
}

// ---- Favorites (FE-06, revised) ----
//
// Finalized shape per the FE-06 revision's "Final Backend Contracts §1":
// no servings/yield fields exist on this response at all (the original
// FE-06 implementation modeled them from an earlier, superseded sample —
// removed here). source/attribution are now confirmed present.

export interface FavoriteRecipe {
  recipe_id: number;
  recipe_name: string;
  alt_title: string | null;
  restaurant: string | null;
  source: string | null;
  attribution: string | null;
  favorite: boolean;
}

export interface FavoriteRecipesResponse {
  results: FavoriteRecipe[];
}

// ---- Recipe detail (FE-05) ----
//
// Modeled directly on the backend's RecipeResponse/RecipeSectionResponse/
// RecipeIngredientResponse/RecipeInstructionResponse contract (FE-05 work
// order §3). `quantity`, `grams`, `notes`, `optional`, `alt_group_id`, and
// `alt_kind` are kept as distinct fields here — never collapsed into a
// single formatted string at this boundary (work order §5); composing a
// display string happens in features/recipe/, not here.
//
// Field-type notes (backend gave names but not explicit types for every
// field — see the FE-05 completion report's "Assumptions" section):
// `servings` is modeled as `string | number | null` since the work order
// didn't specify which; everything else nullable/optional per the
// backend model description is typed as `T | null`.

export interface RecipeInstruction {
  /** 1-based, backend-assigned presentation sequence — never renumbered from array position. */
  number: number;
  text: string;
}

export interface RecipeIngredient {
  ingredient_id: string | null;
  name: string;
  quantity: string | null;
  grams: number | null;
  notes: string | null;
  optional: boolean;
  /** Structured alternative-ingredient grouping. Not yet rendered — see report. */
  alt_group_id: string | null;
  /** Structured alternative-ingredient kind. Not yet rendered — see report. */
  alt_kind: string | null;
}

export interface RecipeSection {
  id: number;
  name: string | null;
  ingredients: RecipeIngredient[];
  instructions: RecipeInstruction[];
}

export interface RecipeDetail {
  id: number;
  recipe_name: string;
  alt_title: string | null;
  author: string | null;
  attribution: string | null;
  source: string | null;
  restaurant: string | null;
  url: string | null;
  /** Modeled per the transport contract; no display requirement was specified for FE-05. */
  video: string | null;
  servings: string | number | null;
  yield: string | null;
  /** Recipe-level notes, distinct from per-ingredient notes. */
  notes: string | null;
  /** Modeled per the transport contract; not a FE-05 display concern. */
  state: string | null;
  /** Modeled per the transport contract; not a FE-05 display concern. */
  ingestion_method: string | null;
  sections: RecipeSection[];
  /** 1 = favorited, null = not favorited. Modeled only — no toggle UI in FE-05. */
  favorites: number | null;
}
