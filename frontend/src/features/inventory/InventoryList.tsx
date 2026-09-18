import type { InventoryItem } from "../../types/inventory";
import { findDuplicateIngredientIds } from "./duplicates";
import { InventoryItemRow } from "./InventoryItemRow";

interface InventoryListProps {
  items: InventoryItem[];
}

/**
 * Groups the server's inventory collection into Fridge/Pantry sections.
 * Grouping is derived purely from each item's `location` field, in the
 * order the backend returned them — no client-side sorting by inferred
 * freshness (work order §13).
 */
export function InventoryList({ items }: InventoryListProps) {
  if (items.length === 0) {
    return (
      <p className="inventory-empty">
        Your kitchen is empty. Add an item above to get started.
      </p>
    );
  }

  const duplicateIngredientIds = findDuplicateIngredientIds(items);
  const fridgeItems = items.filter((item) => item.location === "fridge");
  const pantryItems = items.filter((item) => item.location === "pantry");

  function isDuplicate(item: InventoryItem): boolean {
    return item.ingredient_id !== null && duplicateIngredientIds.has(item.ingredient_id);
  }

  return (
    <div className="inventory-groups">
      <section aria-labelledby="fridge-heading">
        <h2 id="fridge-heading">Fridge</h2>
        {fridgeItems.length === 0 ? (
          <p>Nothing in the fridge yet.</p>
        ) : (
          <ul className="inventory-item-list">
            {fridgeItems.map((item) => (
              <InventoryItemRow key={item.id} item={item} isDuplicate={isDuplicate(item)} />
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="pantry-heading">
        <h2 id="pantry-heading">Pantry</h2>
        {pantryItems.length === 0 ? (
          <p>Nothing in the pantry yet.</p>
        ) : (
          <ul className="inventory-item-list">
            {pantryItems.map((item) => (
              <InventoryItemRow key={item.id} item={item} isDuplicate={isDuplicate(item)} />
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
