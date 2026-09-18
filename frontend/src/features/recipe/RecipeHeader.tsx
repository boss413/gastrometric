import type { RecipeDetail } from "../../types/recipe";

interface RecipeHeaderProps {
  recipe: RecipeDetail;
  /** id applied to the primary heading, so the page section can keep a stable aria-labelledby target. */
  headingId: string;
}

/**
 * Field order is authoritative (work order §8): name, alt_title
 * (immediately after name), author, attribution, servings, yield
 * (immediately after servings), source, restaurant. Each field renders
 * as its own labeled row — never collapsed into a generic metadata
 * string. Missing/null fields are omitted entirely, never shown as
 * "Unknown"/"N/A".
 *
 * ASSUMPTION (flagged in the completion report): the work order doesn't
 * say which field `url` attaches to. This links `source` when both are
 * present, since that's the most common "where this recipe came from"
 * pattern — `restaurant` and the recipe name are left unlinked.
 */
export function RecipeHeader({ recipe, headingId }: RecipeHeaderProps) {
  return (
    <header className="recipe-header">
      <h1 id={headingId}>
        {recipe.recipe_name}
        {recipe.alt_title && <span className="recipe-alt-title"> ({recipe.alt_title})</span>}
      </h1>

      <dl className="recipe-meta">
        {recipe.author && (
          <div className="recipe-meta-row">
            <dt>Author</dt>
            <dd>{recipe.author}</dd>
          </div>
        )}
        {recipe.attribution && (
          <div className="recipe-meta-row">
            <dt>Attribution</dt>
            <dd>{recipe.attribution}</dd>
          </div>
        )}
        {recipe.servings != null && (
          <div className="recipe-meta-row">
            <dt>Servings</dt>
            <dd>{recipe.servings}</dd>
          </div>
        )}
        {recipe.yield && (
          <div className="recipe-meta-row">
            <dt>Yield</dt>
            <dd>{recipe.yield}</dd>
          </div>
        )}
        {recipe.source && (
          <div className="recipe-meta-row">
            <dt>Source</dt>
            <dd>
              {recipe.url ? (
                <a href={recipe.url} target="_blank" rel="noreferrer">
                  {recipe.source}
                </a>
              ) : (
                recipe.source
              )}
            </dd>
          </div>
        )}
        {recipe.restaurant && (
          <div className="recipe-meta-row">
            <dt>Restaurant</dt>
            <dd>{recipe.restaurant}</dd>
          </div>
        )}
      </dl>
    </header>
  );
}
