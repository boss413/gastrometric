import { Link } from "react-router-dom";
import type { RecipeMatch } from "../../types/recipe";
import { RecipeCardHeader } from "./RecipeCardHeader";

interface RecipeMatchCardProps {
  match: RecipeMatch;
}

/**
 * `matched_ingredients` now contains both fridge- and pantry-located
 * entries, distinguished only by the backend's authoritative `location`
 * field (FE-06 revision "Final Backend Contracts §3") — never inferred
 * from `fridge_match_count`, array order, or anything else. Only
 * `location === "fridge"` entries render in the fridge line; pantry
 * entries within `matched_ingredients` are intentionally not rendered as
 * their own line (revision "Pantry matches": the existing contract calls
 * for the fridge line specifically, not a generic "available" line).
 * Missing ingredients come only from `missing_ingredients`. Both render
 * using `ingredient_name_original` only — never identity fields, never
 * `name`, never reconstructed, never quantified.
 *
 * The old generic "Available in your kitchen" text (FE-04) is dropped for
 * perfect matches in favor of the concrete fridge list, which now conveys
 * the same thing with real ingredient names.
 */
export function RecipeMatchCard({ match }: RecipeMatchCardProps) {
  const fridgeNames = match.matched_ingredients
    .filter((ingredient) => ingredient.location === "fridge")
    .map((ingredient) => ingredient.ingredient_name_original);
  const missingNames = match.missing_ingredients.map((i) => i.ingredient_name_original);

  return (
    <Link to={`/recipes/${match.recipe_id}`} className="recipe-match-card recipe-browse-card">
      <RecipeCardHeader
        recipeName={match.recipe_name}
        altTitle={match.alt_title}
        restaurant={match.restaurant}
        source={match.source}
        attribution={match.attribution}
      />

      {match.match_category === "pantry_only" && <p>Pantry</p>}

      {fridgeNames.length > 0 && (
        <p className="recipe-fridge-line">
          <strong>fridge:</strong> {fridgeNames.join(", ")}
        </p>
      )}

      {missingNames.length > 0 && (
        <p className="recipe-missing-line">
          <strong>missing:</strong> {missingNames.join(", ")}
        </p>
      )}

      <p className="recipe-match-counts">
        {match.fridge_match_count} from fridge · {match.pantry_match_count} from pantry
      </p>
    </Link>
  );
}
