import { Link } from "react-router-dom";
import { InventoryForm } from "../features/inventory/InventoryForm";
import { InventoryList } from "../features/inventory/InventoryList";
import { useInventoryQuery } from "../features/inventory/useInventoryQueries";

/**
 * Inventory screen: current fridge/pantry state, an add-item form, and
 * per-item edit/delete. The inventory query is the single source of
 * truth — nothing here mirrors it into local state (work order §21).
 */
export function InventoryPage() {
  const inventoryQuery = useInventoryQuery();

  return (
    <section aria-labelledby="inventory-heading">
      <h1 id="inventory-heading">Inventory</h1>

      <InventoryForm />

      {inventoryQuery.isPending && <p role="status">Loading inventory...</p>}

      {inventoryQuery.isError && (
        <div role="alert" className="operation-error">
          <p>{inventoryQuery.error.message}</p>
          <button type="button" onClick={() => inventoryQuery.refetch()}>
            Retry
          </button>
        </div>
      )}

      {inventoryQuery.isSuccess && <InventoryList items={inventoryQuery.data} />}

      <p>
        <Link to="/recipes">← Back to Recipes</Link>
      </p>
    </section>
  );
}
