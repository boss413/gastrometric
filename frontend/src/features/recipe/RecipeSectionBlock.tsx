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
 */
export function RecipeSectionBlock({ section }: RecipeSectionBlockProps) {
  const headingId = section.name ? `recipe-section-${section.id}` : undefined;

  return (
    <section className="recipe-section" aria-labelledby={headingId}>
      {section.name && (
        <h2 id={headingId} className="recipe-section-name">
          {section.name}
        </h2>
      )}

      {section.ingredients.length > 0 && (
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

      {section.instructions.length > 0 && (
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
