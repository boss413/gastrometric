import { useId, useState, type FormEvent } from "react";
import { useCreateInventoryItem } from "./useInventoryQueries";
import type { InventoryLocation } from "../../types/inventory";

/**
 * Add-inventory form. Server-confirmed only: the item is never inserted
 * locally; once the mutation succeeds, invalidating the inventory query
 * is what makes the new item appear via the server's own response
 * (work order §12). On failure, the form stays populated so the user can
 * simply submit again — no separate retry affordance is needed for that.
 */
export function InventoryForm() {
  const [originalInput, setOriginalInput] = useState("");
  const [location, setLocation] = useState<InventoryLocation>("fridge");
  const [quantity, setQuantity] = useState("");
  const [unit, setUnit] = useState("");

  const createMutation = useCreateInventoryItem();

  const inputId = useId();
  const locationLegendId = useId();
  const quantityId = useId();
  const unitId = useId();

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (createMutation.isPending) return;

    const trimmedInput = originalInput.trim();
    if (!trimmedInput) return;

    const parsedQuantity = quantity.trim() ? Number(quantity) : undefined;
    const hasValidQuantity = parsedQuantity !== undefined && !Number.isNaN(parsedQuantity);

    createMutation.mutate(
      {
        original_input: trimmedInput,
        location,
        ...(hasValidQuantity ? { quantity: parsedQuantity } : {}),
        ...(unit.trim() ? { unit: unit.trim() } : {}),
      },
      {
        onSuccess: () => {
          setOriginalInput("");
          setQuantity("");
          setUnit("");
          // Location is intentionally left as-is: adding several items to
          // the same location in a row is the common case in a kitchen.
        },
      },
    );
  }

  return (
    <form className="inventory-form" onSubmit={handleSubmit}>
      <h2>Add an item</h2>

      <div className="form-field">
        <label htmlFor={inputId}>What do you have?</label>
        <input
          id={inputId}
          type="text"
          value={originalInput}
          onChange={(e) => setOriginalInput(e.target.value)}
          placeholder="e.g. 2 red bell peppers"
          required
        />
      </div>

      <fieldset className="form-field" aria-labelledby={locationLegendId}>
        <legend id={locationLegendId}>Where is it?</legend>
        <label className="radio-label">
          <input
            type="radio"
            name="location"
            value="fridge"
            checked={location === "fridge"}
            onChange={() => setLocation("fridge")}
          />
          Fridge
        </label>
        <label className="radio-label">
          <input
            type="radio"
            name="location"
            value="pantry"
            checked={location === "pantry"}
            onChange={() => setLocation("pantry")}
          />
          Pantry
        </label>
      </fieldset>

      <div className="form-row">
        <div className="form-field">
          <label htmlFor={quantityId}>Quantity (optional)</label>
          <input
            id={quantityId}
            type="number"
            inputMode="decimal"
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
          />
        </div>
        <div className="form-field">
          <label htmlFor={unitId}>Unit (optional)</label>
          <input
            id={unitId}
            type="text"
            value={unit}
            onChange={(e) => setUnit(e.target.value)}
            placeholder="e.g. each, lb"
          />
        </div>
      </div>

      <button type="submit" disabled={createMutation.isPending}>
        {createMutation.isPending ? "Saving..." : "Add item"}
      </button>

      {createMutation.isPending && (
        <p role="status" className="operation-status">
          Saving...
        </p>
      )}

      {createMutation.isError && (
        <p role="alert" className="operation-error">
          {createMutation.error.message} You can try submitting again.
        </p>
      )}
    </form>
  );
}
