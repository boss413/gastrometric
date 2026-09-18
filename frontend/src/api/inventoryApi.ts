import { fetchJson } from "./client";
import type {
  CreateInventoryItemRequest,
  InventoryItem,
  InventoryListResponse,
  UpdateInventoryItemRequest,
} from "../types/inventory";

/**
 * Inventory-specific HTTP operations. UI/feature code never calls
 * `fetch()` or `fetchJson()` directly — this module is the only thing
 * that knows the inventory endpoint shapes (FE-01 §1.4).
 */

export async function getInventory(): Promise<InventoryItem[]> {
  const response = await fetchJson<InventoryListResponse>("/api/inventory");
  return response.items;
}

export async function createInventoryItem(
  input: CreateInventoryItemRequest,
): Promise<InventoryItem> {
  return fetchJson<InventoryItem>("/api/inventory", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export async function updateInventoryItem(
  id: number,
  patch: UpdateInventoryItemRequest,
): Promise<InventoryItem> {
  return fetchJson<InventoryItem>(`/api/inventory/${id}`, {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
}

export async function deleteInventoryItem(id: number): Promise<void> {
  await fetchJson<undefined>(`/api/inventory/${id}`, {
    method: "DELETE",
  });
}
