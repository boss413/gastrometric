import { useQuery } from "@tanstack/react-query";
import { getFavoriteRecipes, getRecipeMatches, searchRecipes } from "../../api/recipeApi";
import type { ApiError } from "../../api/client";
import type {
  FavoriteRecipe,
  RecipeMatch,
  RecipeSearchResult,
  RecipeSearchType,
} from "../../types/recipe";

export const recipeMatchesQueryKey = ["recipeMatches"] as const;
export const recipeFavoritesQueryKey = ["recipeFavorites"] as const;

const MIN_QUERY_LENGTH = 2;

export function useRecipeMatches() {
  return useQuery<RecipeMatch[], ApiError>({
    queryKey: recipeMatchesQueryKey,
    queryFn: getRecipeMatches,
  });
}

/**
 * Independent from matches and search — its own query, own loading/error
 * state, never blocked by or blocking the others (work order "Query/state
 * behavior").
 */
export function useFavoriteRecipes() {
  return useQuery<FavoriteRecipe[], ApiError>({
    queryKey: recipeFavoritesQueryKey,
    queryFn: getFavoriteRecipes,
  });
}

/**
 * Search is explicit, not live: `query`/`searchType` here must be the
 * *submitted* values, not whatever the user is currently typing. The
 * query only runs once `enabled` is true and the query meets the minimum
 * length — typing alone must never trigger a request (work order §4/§9).
 */
export function useRecipeSearch(
  query: string,
  searchType: RecipeSearchType,
  enabled: boolean,
) {
  return useQuery<RecipeSearchResult[], ApiError>({
    queryKey: ["recipeSearch", searchType, query] as const,
    queryFn: () => searchRecipes(query, searchType),
    enabled: enabled && query.trim().length >= MIN_QUERY_LENGTH,
  });
}
