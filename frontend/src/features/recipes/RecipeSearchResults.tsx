import { Link } from "react-router-dom";
import type { RecipeSearchType } from "../../types/recipe";
import { RecipeCardHeader } from "./RecipeCardHeader";
import { RecipeSearchForm } from "./RecipeSearchForm";
import { highlightQuery } from "./highlightQuery";
import { useRecipeSearch } from "./useRecipeQueries";

export interface SubmittedSearch {
  query: string;
  type: RecipeSearchType;
}

interface RecipeSearchResultsProps {
  /** Owned by the page — search being active/inactive drives whether Favorites+Matches show (work order "Query/state behavior"). */
  submitted: SubmittedSearch | null;
  onSubmit: (query: string, type: RecipeSearchType) => void;
  onClear: () => void;
}

/**
 * Search is a separate, independent server query from favorites and
 * inventory matching — it never touches either of those queries, and a
 * search failure never affects them. While a search is active, it
 * replaces the browse content (the page hides Favorites/Matches); it
 * never mixes results into them.
 */
export function RecipeSearchResults({ submitted, onSubmit, onClear }: RecipeSearchResultsProps) {
  const searchQuery = useRecipeSearch(
    submitted?.query ?? "",
    submitted?.type ?? "name",
    submitted !== null,
  );

  function handleRetry() {
    searchQuery.refetch();
  }

  return (
    <section aria-labelledby="recipe-search-heading">
      <h2 id="recipe-search-heading">Search recipes</h2>

      <RecipeSearchForm onSubmit={onSubmit} isSearching={searchQuery.isFetching} />

      {submitted !== null && (
        <button type="button" className="recipe-search-clear" onClick={onClear}>
          Clear search
        </button>
      )}

      {submitted !== null && searchQuery.isFetching && <p role="status">Searching...</p>}

      {submitted !== null && !searchQuery.isFetching && searchQuery.isError && (
        <div role="alert" className="operation-error">
          <p>{searchQuery.error.message}</p>
          <button type="button" onClick={handleRetry}>
            Retry
          </button>
        </div>
      )}

      {submitted !== null &&
        !searchQuery.isFetching &&
        searchQuery.isSuccess &&
        searchQuery.data.length === 0 && <p>No recipes found.</p>}

      {submitted !== null &&
        !searchQuery.isFetching &&
        searchQuery.isSuccess &&
        searchQuery.data.length > 0 && (
          <ul className="recipe-search-result-list">
            {searchQuery.data.map((result) => (
              <li key={result.recipe_id}>
                <Link
                  to={`/recipes/${result.recipe_id}`}
                  className="recipe-search-result recipe-browse-card"
                >
                  <RecipeCardHeader
                    recipeName={
                      submitted.type === "name" ? (
                        <HighlightedText text={result.recipe_name} query={submitted.query} />
                      ) : (
                        result.recipe_name
                      )
                    }
                    altTitle={result.alt_title}
                    restaurant={result.restaurant}
                    source={result.source}
                    attribution={result.attribution}
                  />
                  {result.matched_ingredient && (
                    <p className="recipe-search-result-meta">
                      Uses: <strong>{result.matched_ingredient}</strong>
                    </p>
                  )}
                </Link>
              </li>
            ))}
          </ul>
        )}
    </section>
  );
}

function HighlightedText({ text, query }: { text: string; query: string }) {
  const segments = highlightQuery(text, query);
  return (
    <>
      {segments.map((segment, index) =>
        segment.bold ? (
          <strong key={index}>{segment.text}</strong>
        ) : (
          <span key={index}>{segment.text}</span>
        ),
      )}
    </>
  );
}
