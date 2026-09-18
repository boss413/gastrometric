import { Link } from "react-router-dom";
import { RecipeCardHeader } from "./RecipeCardHeader";
import { useFavoriteRecipes } from "./useRecipeQueries";

/**
 * Favorites are static recipe data for this MVP (work order §1) — no
 * favorite button, toggle, or mutation exists here or anywhere else.
 * Ordering is whatever the backend returns (recipe_id ascending, per the
 * backend's own contract) — never resorted client-side.
 */
export function RecipeFavoritesList() {
  const favoritesQuery = useFavoriteRecipes();

  return (
    <section aria-labelledby="recipe-favorites-heading">
      <h2 id="recipe-favorites-heading">Favorites</h2>

      {favoritesQuery.isPending && <p role="status">Loading favorites...</p>}

      {favoritesQuery.isError && (
        <div role="alert" className="operation-error">
          <p>{favoritesQuery.error.message}</p>
          <button type="button" onClick={() => favoritesQuery.refetch()}>
            Retry
          </button>
        </div>
      )}

      {favoritesQuery.isSuccess && favoritesQuery.data.length === 0 && (
        <p>No favorite recipes yet.</p>
      )}

      {favoritesQuery.isSuccess && favoritesQuery.data.length > 0 && (
        <ul className="recipe-favorites-list recipe-search-result-list">
          {favoritesQuery.data.map((favorite) => (
            <li key={favorite.recipe_id}>
              <Link
                to={`/recipes/${favorite.recipe_id}`}
                className="recipe-browse-card recipe-search-result"
              >
                <RecipeCardHeader
                  recipeName={favorite.recipe_name}
                  altTitle={favorite.alt_title}
                  restaurant={favorite.restaurant}
                  source={favorite.source}
                  attribution={favorite.attribution}
                />
              </Link>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
