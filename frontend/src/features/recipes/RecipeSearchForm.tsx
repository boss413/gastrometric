import { useId, useState, type FormEvent } from "react";
import type { RecipeSearchType } from "../../types/recipe";

interface RecipeSearchFormProps {
  onSubmit: (query: string, type: RecipeSearchType) => void;
  /** Disables the Search button and blocks submission while a search is already in flight. */
  isSearching: boolean;
}

const MIN_QUERY_LENGTH = 2;

/**
 * Owns only transient input state. The submitted query/type live in the
 * parent (`RecipeSearchResults`) so they can drive the TanStack Query
 * hook — typing here never triggers a request (work order §4).
 */
export function RecipeSearchForm({ onSubmit, isSearching }: RecipeSearchFormProps) {
  const [query, setQuery] = useState("");
  const [type, setType] = useState<RecipeSearchType>("name");
  const [validationError, setValidationError] = useState<string | null>(null);

  const inputId = useId();
  const typeLegendId = useId();

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (isSearching) return;

    const trimmed = query.trim();
    if (trimmed.length < MIN_QUERY_LENGTH) {
      setValidationError(`Enter at least ${MIN_QUERY_LENGTH} characters.`);
      return;
    }

    setValidationError(null);
    onSubmit(trimmed, type);
  }

  return (
    <form role="search" className="recipe-search-form" onSubmit={handleSubmit}>
      <div className="form-field">
        <label htmlFor={inputId}>Find a recipe</label>
        <input
          id={inputId}
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="e.g. chicken, tomato, pasta"
        />
      </div>

      <fieldset className="form-field" aria-labelledby={typeLegendId}>
        <legend id={typeLegendId}>Search by</legend>
        <label className="radio-label">
          <input
            type="radio"
            name="recipe-search-type"
            checked={type === "name"}
            onChange={() => setType("name")}
          />
          Recipe name
        </label>
        <label className="radio-label">
          <input
            type="radio"
            name="recipe-search-type"
            checked={type === "ingredient"}
            onChange={() => setType("ingredient")}
          />
          Ingredient
        </label>
      </fieldset>

      <button type="submit" disabled={isSearching}>
        {isSearching ? "Searching..." : "Search"}
      </button>

      {validationError && (
        <p role="alert" className="operation-error">
          {validationError}
        </p>
      )}
    </form>
  );
}
