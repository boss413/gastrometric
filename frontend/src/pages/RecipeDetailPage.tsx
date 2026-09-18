import { useParams } from "react-router-dom";
import { BackButton } from "../components/BackButton";
import { RecipeHeader } from "../features/recipe/RecipeHeader";
import { RecipeSectionBlock } from "../features/recipe/RecipeSectionBlock";
import { parseRecipeId, useRecipeQuery } from "../features/recipe/useRecipeQuery";

const HEADING_ID = "recipe-detail-heading";

/**
 * Recipe detail screen. Reads and displays the route parameter so routing
 * can be verified. Fetching and rendering actual recipe content is FE-04
 * scope.
 */
export function RecipeDetailPage() {
  const { recipeId: rawRecipeId } = useParams<{ recipeId: string }>();
  const recipeId = parseRecipeId(rawRecipeId);
  const recipeQuery = useRecipeQuery(recipeId);

  const isNotFound = recipeQuery.isError && recipeQuery.error.code === "not_found";

  return (
    <section aria-labelledby={HEADING_ID}>
      <BackButton fallbackTo="/recipes" label="Recipes" />

      {recipeId === null && (
        <>
          <h1 id={HEADING_ID}>Recipe</h1>
          <p role="alert">Recipe not found.</p>
        </>
      )}

      {recipeId !== null && recipeQuery.isPending && (
        <>
          <h1 id={HEADING_ID}>Recipe</h1>
          <p role="status">Loading recipe...</p>
        </>
      )}

      {recipeId !== null && recipeQuery.isError && (
        <>
          <h1 id={HEADING_ID}>Recipe</h1>
          <div role="alert" className="operation-error">
            <p>{isNotFound ? "Recipe not found." : recipeQuery.error.message}</p>
            {!isNotFound && (
              <button type="button" onClick={() => recipeQuery.refetch()}>
                Retry
              </button>
            )}
          </div>
        </>
      )}

      {recipeId !== null && recipeQuery.isSuccess && (
        <>
          <RecipeHeader recipe={recipeQuery.data} headingId={HEADING_ID} />

          {recipeQuery.data.sections.map((section) => (
            <RecipeSectionBlock key={section.id} section={section} />
          ))}

          {recipeQuery.data.notes && (
            <section aria-labelledby="recipe-notes-heading" className="recipe-notes">
              <h2 id="recipe-notes-heading">Notes</h2>
              <p>{recipeQuery.data.notes}</p>
            </section>
          )}
        </>
      )}
    </section>
  );
}
