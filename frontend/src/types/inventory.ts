/**
 * Inventory transport types, matching the backend's serialization shape
 * (see inventory_models.py). Kept close to the wire shape per FE-01 §1.6;
 * `analysis_result` stays loosely typed here and is narrowed only where
 * it's actually rendered (features/inventory/analysisResult.ts), since its
 * backend schema is not yet finalized (work order §9).
 */

export type InventoryLocation = "fridge" | "pantry";

/**
 * The backend's resolution_status is authoritative and not a fixed
 * frontend enum — the frontend must not hardcode a taxonomy that could
 * drift from the backend (work order §28). Known values observed/expected
 * are widened for convenience but any string is accepted.
 */
export type ResolutionStatus =
  | "resolved"
  | "partially_resolved"
  | "unresolved"
  | "ambiguous"
  | (string & {});

export interface InventoryItem {
  id: number;
  original_input: string;
  ingredient_id: string | null;
  location: InventoryLocation;
  quantity: number | null;
  unit: string | null;
  resolution_status: ResolutionStatus;
  analysis_result: Record<string, unknown> | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface InventoryListResponse {
  items: InventoryItem[];
}

/** Only fields the client may send. Never ingredient_id/resolution_status/analysis_result. */
export interface CreateInventoryItemRequest {
  original_input: string;
  location: InventoryLocation;
  quantity?: number;
  unit?: string;
}

/** Only editable fields; PATCH sends solely the ones the user actually changed. */
export interface UpdateInventoryItemRequest {
  original_input?: string;
  location?: InventoryLocation;
  quantity?: number | null;
  unit?: string | null;
}
