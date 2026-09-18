import type { MatchCategory, RecipeMatch } from "../../types/recipe";
import { RecipeMatchCard } from "./RecipeMatchCard";
import { useRecipeMatches } from "./useRecipeQueries";

/**
 * Fixed section order mirrors the backend's own category ordering
 * (perfect → imperfect → pantry_only). Filtering into per-category groups
 * preserves each item's relative order within its category — nothing is
 * resorted (work order §2/§6).
 */
const CATEGORY_ORDER: MatchCategory[] = ["perfect", "imperfect", "pantry_only"];

const CATEGORY_LABELS: Record<MatchCategory, string> = {
  perfect: "Perfect",
  imperfect: "Imperfect",
  pantry_only: "Pantry only",
};

export function RecipeMatchList() {
  const matchesQuery = useRecipeMatches();

  return (
    <section aria-labelledby="recipe-matches-heading">
      <h2 id="recipe-matches-heading">Recipes from your kitchen</h2>

      {matchesQuery.isPending && <p role="status">Finding recipes...</p>}

      {matchesQuery.isError && (
        <div role="alert" className="operation-error">
          <p>{matchesQuery.error.message}</p>
          <button type="button" onClick={() => matchesQuery.refetch()}>
            Retry
          </button>
        </div>
      )}

      {matchesQuery.isSuccess && matchesQuery.data.length === 0 && (
        <p>No recipes match your current kitchen inventory.</p>
      )}

      {matchesQuery.isSuccess && matchesQuery.data.length > 0 && (
        <MatchCategorySections matches={matchesQuery.data} />
      )}
    </section>
  );
}

function MatchCategorySections({ matches }: { matches: RecipeMatch[] }) {
  return (
    <>
      {CATEGORY_ORDER.map((category) => {
        const items = matches.filter((match) => match.match_category === category);
        if (items.length === 0) return null;

        return (
          <section key={category} aria-labelledby={`match-category-${category}`}>
            <h3 id={`match-category-${category}`}>{CATEGORY_LABELS[category]}</h3>
            <ul className="recipe-match-list">
              {items.map((match) => (
                <li key={match.recipe_id}>
                  <RecipeMatchCard match={match} />
                </li>
              ))}
            </ul>
          </section>
        );
      })}
    </>
  );
}
