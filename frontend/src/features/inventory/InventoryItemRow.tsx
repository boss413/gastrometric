import { useId, useState, type FormEvent } from "react";
import type { InventoryItem, UpdateInventoryItemRequest } from "../../types/inventory";
import { buildInterpretationViewModel } from "./analysisResult";
import { useDeleteInventoryItem, useUpdateInventoryItem } from "./useInventoryQueries";

interface InventoryItemRowProps {
  item: InventoryItem;
  /** Whether this item's resolved ingredient also exists in the other location. */
  isDuplicate: boolean;
}

/**
 * Renders one inventory item: what was entered, the backend's
 * interpretation, and edit/delete controls. Each row owns its own
 * mutation instances so one row's Saving/Updating/Deleting state never
 * blocks interaction with any other row (work order §16/§21).
 */
export function InventoryItemRow({ item, isDuplicate }: InventoryItemRowProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);
  const [draftInput, setDraftInput] = useState(item.original_input);
  const [draftLocation, setDraftLocation] = useState(item.location);
  const [draftQuantity, setDraftQuantity] = useState(
    item.quantity != null ? String(item.quantity) : "",
  );
  const [draftUnit, setDraftUnit] = useState(item.unit ?? "");

  const updateMutation = useUpdateInventoryItem();
  const deleteMutation = useDeleteInventoryItem();

  const interpretation = buildInterpretationViewModel(item.analysis_result);

  const inputId = useId();
  const quantityId = useId();
  const unitId = useId();
  const locationLegendId = useId();

  function resetDraftToItem() {
    setDraftInput(item.original_input);
    setDraftLocation(item.location);
    setDraftQuantity(item.quantity != null ? String(item.quantity) : "");
    setDraftUnit(item.unit ?? "");
  }

  function startEditing() {
    resetDraftToItem();
    updateMutation.reset();
    setIsEditing(true);
  }

  function useCandidateAsEntry(label: string) {
    resetDraftToItem();
    updateMutation.reset();
    setDraftInput(label);
    setIsEditing(true);
  }

  function cancelEditing() {
    setIsEditing(false);
    resetDraftToItem();
    updateMutation.reset();
  }

  function handleSubmitEdit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (updateMutation.isPending) return;

    const trimmedInput = draftInput.trim();
    if (!trimmedInput) return;

    const patch: UpdateInventoryItemRequest = {};

    if (trimmedInput !== item.original_input) {
      patch.original_input = trimmedInput;
    }
    if (draftLocation !== item.location) {
      patch.location = draftLocation;
    }

    const parsedQuantity = draftQuantity.trim() ? Number(draftQuantity) : null;
    const nextQuantity =
      parsedQuantity === null || Number.isNaN(parsedQuantity) ? null : parsedQuantity;
    if (nextQuantity !== item.quantity) {
      patch.quantity = nextQuantity;
    }

    const trimmedUnit = draftUnit.trim() || null;
    if (trimmedUnit !== item.unit) {
      patch.unit = trimmedUnit;
    }

    if (Object.keys(patch).length === 0) {
      setIsEditing(false);
      return;
    }

    updateMutation.mutate(
      { id: item.id, patch },
      { onSuccess: () => setIsEditing(false) },
    );
  }

  function handleDelete() {
    if (deleteMutation.isPending) return;
    deleteMutation.mutate(item.id);
  }

  if (isEditing) {
    return (
      <li className="inventory-item inventory-item--editing">
        <form onSubmit={handleSubmitEdit}>
          <div className="form-field">
            <label htmlFor={inputId}>What do you have?</label>
            <input
              id={inputId}
              type="text"
              value={draftInput}
              onChange={(e) => setDraftInput(e.target.value)}
              required
            />
          </div>

          <fieldset className="form-field" aria-labelledby={locationLegendId}>
            <legend id={locationLegendId}>Where is it?</legend>
            <label className="radio-label">
              <input
                type="radio"
                name={`location-${item.id}`}
                checked={draftLocation === "fridge"}
                onChange={() => setDraftLocation("fridge")}
              />
              Fridge
            </label>
            <label className="radio-label">
              <input
                type="radio"
                name={`location-${item.id}`}
                checked={draftLocation === "pantry"}
                onChange={() => setDraftLocation("pantry")}
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
                value={draftQuantity}
                onChange={(e) => setDraftQuantity(e.target.value)}
              />
            </div>
            <div className="form-field">
              <label htmlFor={unitId}>Unit (optional)</label>
              <input
                id={unitId}
                type="text"
                value={draftUnit}
                onChange={(e) => setDraftUnit(e.target.value)}
              />
            </div>
          </div>

          <div className="inventory-item-actions">
            <button type="submit" disabled={updateMutation.isPending}>
              {updateMutation.isPending ? "Updating..." : "Save"}
            </button>
            <button type="button" onClick={cancelEditing} disabled={updateMutation.isPending}>
              Cancel
            </button>
          </div>

          {updateMutation.isPending && (
            <p role="status" className="operation-status">
              Updating...
            </p>
          )}
          {updateMutation.isError && (
            <p role="alert" className="operation-error">
              {updateMutation.error.message} Your changes were not saved — you can try again.
            </p>
          )}
        </form>
      </li>
    );
  }

  return (
    <li className="inventory-item">
      <p className="inventory-item-entry">{item.original_input}</p>

      {interpretation.resolvedLabel ? (
        <p>
          Interpreted as: <strong>{interpretation.resolvedLabel}</strong>
        </p>
      ) : (
        <p>No confirmed interpretation yet.</p>
      )}

      <p className="resolution-status">
        Status: <strong>{item.resolution_status}</strong>
        {isDuplicate && (
          <span className="duplicate-badge">
            {" "}
            · * also in {item.location === "fridge" ? "the pantry" : "the fridge"}
          </span>
        )}
      </p>

      {(item.quantity != null || item.unit) && (
        <p className="inventory-item-quantity">
          {item.quantity ?? ""} {item.unit ?? ""}
        </p>
      )}

      {interpretation.candidates.length > 0 && (
        <div className="interpretation-candidates">
          <p>Possible matches (tap to use, then confirm):</p>
          <ul>
            {interpretation.candidates.map((candidate) => (
              <li key={candidate.id}>
                <button type="button" onClick={() => useCandidateAsEntry(candidate.label)}>
                  {candidate.label}
                  {candidate.isSelected ? " (best guess)" : ""}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {interpretation.unresolvedTokens.length > 0 && (
        <p className="interpretation-unresolved">
          Not understood: {interpretation.unresolvedTokens.join(", ")}
        </p>
      )}

      {interpretation.notes.length > 0 && (
        <ul className="interpretation-notes">
          {interpretation.notes.map((note, index) => (
            <li key={index}>{note}</li>
          ))}
        </ul>
      )}

      <div className="inventory-item-actions">
        <button type="button" onClick={startEditing}>
          Edit
        </button>

        {isConfirmingDelete ? (
          <>
            <span>Delete this item?</span>
            <button type="button" onClick={handleDelete} disabled={deleteMutation.isPending}>
              {deleteMutation.isPending ? "Deleting..." : "Confirm delete"}
            </button>
            <button
              type="button"
              onClick={() => setIsConfirmingDelete(false)}
              disabled={deleteMutation.isPending}
            >
              Cancel
            </button>
          </>
        ) : (
          <button type="button" onClick={() => setIsConfirmingDelete(true)}>
            Delete
          </button>
        )}
      </div>

      {deleteMutation.isPending && (
        <p role="status" className="operation-status">
          Deleting...
        </p>
      )}
      {deleteMutation.isError && (
        <p role="alert" className="operation-error">
          {deleteMutation.error.message} The item was not deleted — you can try again.
        </p>
      )}
    </li>
  );
}
