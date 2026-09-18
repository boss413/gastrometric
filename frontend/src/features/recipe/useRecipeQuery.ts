import { useQuery } from "@tanstack/react-query";
import { getRecipe } from "../../api/recipeApi";
import type { ApiError } from "../../api/client";
import type { RecipeDetail } from "../../types/recipe";

/**
 * Enabled only when a valid (positive integer) recipe ID is present —
 * a malformed route param must not trigger a request (work order §6).
 */
export function useRecipeQuery(recipeId: number | null) {
  return useQuery<RecipeDetail, ApiError>({
    queryKey: ["recipe", recipeId] as const,
    queryFn: () => getRecipe(recipeId as number),
    enabled: recipeId !== null,
  });
}

/** Parses a route param into a valid recipe id, or null if it isn't one. */
export function parseRecipeId(rawRecipeId: string | undefined): number | null {
  if (!rawRecipeId) return null;
  const parsed = Number(rawRecipeId);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}
