import type { ReactNode } from "react";

interface RecipeCardHeaderProps {
  /** Usually the plain recipe name; search passes a highlighted (bolded) version for name-type results. */
  recipeName: ReactNode;
  altTitle: string | null;
  restaurant: string | null;
  source: string | null;
  attribution: string | null;
}

/**
 * The one base card structure shared by Favorites, Kitchen Matches, and
 * Search results (work order "Base recipe card presentation"). Field
 * order is fixed and each field is its own element — never collapsed
 * into a single string — and missing fields are omitted entirely rather
 * than shown as a placeholder.
 *
 * Restaurant and source share one line (comma-joined, restaurant first);
 * attribution is a separate line beneath. Servings/yield are
 * deliberately never accepted here — the browse card never shows them.
 */
export function RecipeCardHeader({
  recipeName,
  altTitle,
  restaurant,
  source,
  attribution,
}: RecipeCardHeaderProps) {
  const restaurantAndSource = [restaurant, source].filter(Boolean).join(", ");

  return (
    <>
      <h4 className="recipe-card-name">
        {recipeName}
        {altTitle && <span className="recipe-alt-title"> ({altTitle})</span>}
      </h4>

      {restaurantAndSource && <p className="recipe-card-meta">{restaurantAndSource}</p>}
      {attribution && <p className="recipe-card-attribution">{attribution}</p>}
    </>
  );
}
