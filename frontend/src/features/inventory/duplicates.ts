import type { InventoryItem } from "../../types/inventory";

/**
 * Returns the set of `ingredient_id`s that appear in both fridge and
 * pantry. This is purely for display (work order §14) — it never merges,
 * deletes, or moves records. Items without a resolved `ingredient_id`
 * (unresolved/ambiguous) can't reliably participate in an identity-based
 * duplicate check, so they're excluded rather than falsely flagged.
 */
export function findDuplicateIngredientIds(items: InventoryItem[]): Set<string> {
  const locationsByIngredient = new Map<string, Set<InventoryItem["location"]>>();

  for (const item of items) {
    if (!item.ingredient_id) continue;
    const locations = locationsByIngredient.get(item.ingredient_id) ?? new Set();
    locations.add(item.location);
    locationsByIngredient.set(item.ingredient_id, locations);
  }

  const duplicates = new Set<string>();
  for (const [ingredientId, locations] of locationsByIngredient) {
    if (locations.has("fridge") && locations.has("pantry")) {
      duplicates.add(ingredientId);
    }
  }

  return duplicates;
}
