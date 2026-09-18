import { fetchJson } from "./client";
import type {
  FavoriteRecipe,
  FavoriteRecipesResponse,
  RecipeDetail,
  RecipeMatch,
  RecipeMatchResponse,
  RecipeSearchResponse,
  RecipeSearchResult,
  RecipeSearchType,
} from "../types/recipe";

/**
 * Recipe-specific HTTP operations. Feature/UI code never calls `fetch()`
 * or `fetchJson()` directly (FE-01 §1.4) — this module is the only thing
 * that knows the recipe endpoint shapes.
 */

export async function searchRecipes(
  query: string,
  searchType: RecipeSearchType,
): Promise<RecipeSearchResult[]> {
  const params = new URLSearchParams({ q: query, type: searchType });
  const response = await fetchJson<RecipeSearchResponse>(
    `/api/recipes/search?${params.toString()}`,
  );
  return response.results;
}

export async function getRecipeMatches(): Promise<RecipeMatch[]> {
  const response = await fetchJson<RecipeMatchResponse>("/api/recipes/matches");
  return response.results;
}

/**
 * Favorites are static recipe data for this MVP — no toggle/mutation
 * exists here or anywhere else in FE-06 (work order §1).
 */
export async function getFavoriteRecipes(): Promise<FavoriteRecipe[]> {
  const response = await fetchJson<FavoriteRecipesResponse>("/api/recipes/favorites");
  return response.results;
}

/**
 * Fetches the full recipe (sections, ingredients, instructions). Returns
 * the decoded response as-is — no reshaping, no client-side
 * interpretation (FE-05 work order §4).
 */
export async function getRecipe(recipeId: number): Promise<RecipeDetail> {
  return fetchJson<RecipeDetail>(`/api/recipes/${recipeId}`);
}
