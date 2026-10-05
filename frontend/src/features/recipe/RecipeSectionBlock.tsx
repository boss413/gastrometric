import type { RecipeSection } from "../../types/recipe";
import { formatIngredientAnnotation, formatIngredientQuantity } from "./formatIngredient";

interface RecipeSectionBlockProps {
  section: RecipeSection;
}

/**
 * Ingredients and instructions are backend-separate lists within a
 * section — the backend does not pair a specific ingredient to a
 * specific instruction. Rather than fabricate a false per-row pairing
 * across all four "conceptual columns" at once, this renders Section as
 * a heading, then Ingredient+Quantity as a table, then Instructions as
 * an ordered list beneath it — covering all four concepts without
 * inventing structure the backend doesn't provide (work order §9/§11).
 *
 * Ordering is never touched: ingredients render in array order, and
 * instructions use the backend's own `number` as the `<li value>` so the
 * displayed ordinal is the backend's number, not array position — while
 * still rendering in the backend's given array order.
 *
 * WO-2026-017: a section is presentation-only content. If it carries
 * neither ingredients nor instructions, there is nothing to present, so
 * the component renders nothing at all (no heading, no empty table, no
 * empty list) rather than a placeholder shell. Likewise, the ingredient
 * table and instruction list are each independently gated on their own
 * collection being non-empty, so a section with only one of the two
 * never renders an empty counterpart for the other.
 */
export function RecipeSectionBlock({ section }: RecipeSectionBlockProps) {
  const hasIngredients = section.ingredients.length > 0;
  const hasInstructions = section.instructions.length > 0;

  if (!hasIngredients && !hasInstructions) {
    return null;
  }

  const headingId = section.name ? `recipe-section-${section.id}` : undefined;

  return (
    <section className="recipe-section" aria-labelledby={headingId}>
      {section.name && (
        <h2 id={headingId} className="recipe-section-name">
          {section.name}
        </h2>
      )}

      {hasIngredients && (
        <table className="recipe-ingredient-table">
          <caption className="visually-hidden">Ingredients</caption>
          <thead>
            <tr>
              <th scope="col">Ingredient</th>
              <th scope="col">Quantity</th>
            </tr>
          </thead>
          <tbody>
            {section.ingredients.map((ingredient, index) => {
              const annotation = formatIngredientAnnotation(ingredient.optional, ingredient.notes);
              const quantity = formatIngredientQuantity(ingredient.quantity, ingredient.grams);

              return (
                <tr key={ingredient.ingredient_id ?? `${section.id}-${index}`}>
                  <td>
                    <div className="recipe-ingredient-name">{ingredient.name}</div>
                    {annotation && (
                      <div className="recipe-ingredient-annotation">{annotation}</div>
                    )}
                  </td>
                  <td className="recipe-ingredient-quantity">{quantity ?? ""}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      {hasInstructions && (
        <ol className="recipe-instructions">
          {section.instructions.map((instruction) => (
            <li key={instruction.number} value={instruction.number}>
              {instruction.text}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
