import { useState } from "react";
import { RecipeFavoritesList } from "../features/recipes/RecipeFavoritesList";
import { RecipeMatchList } from "../features/recipes/RecipeMatchList";
import { RecipeSearchResults, type SubmittedSearch } from "../features/recipes/RecipeSearchResults";
import type { RecipeSearchType } from "../types/recipe";

/**
 * Recipes screen (also the app's home route). Three content modes:
 * Favorites (first) + Kitchen Matches make up the normal browse view;
 * submitting a search replaces that browse content entirely rather than
 * mixing search results into it (work order "Query/state behavior").
 * Each of the three is backed by its own independent TanStack Query.
 */
export function RecipesPage() {
  const [submittedSearch, setSubmittedSearch] = useState<SubmittedSearch | null>(null);

  function handleSearchSubmit(query: string, type: RecipeSearchType) {
    setSubmittedSearch({ query, type });
  }

  function handleSearchClear() {
    setSubmittedSearch(null);
  }

  return (
    <section aria-labelledby="recipes-heading">
      <h1 id="recipes-heading">Recipes</h1>

      <RecipeSearchResults
        submitted={submittedSearch}
        onSubmit={handleSearchSubmit}
        onClear={handleSearchClear}
      />

      {submittedSearch === null && (
        <>
          <RecipeFavoritesList />
          <RecipeMatchList />
        </>
      )}
    </section>
  );
}
