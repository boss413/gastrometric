import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createInventoryItem,
  deleteInventoryItem,
  getInventory,
  updateInventoryItem,
} from "../../api/inventoryApi";
import type { ApiError } from "../../api/client";
import type {
  CreateInventoryItemRequest,
  InventoryItem,
  UpdateInventoryItemRequest,
} from "../../types/inventory";

export const inventoryQueryKey = ["inventory"] as const;

/**
 * FE-04 will register the actual `['recipes','matches']` query. Inventory
 * mutations must invalidate it once it exists (work order §22) — until
 * then, invalidating a query key with no active observers is a harmless
 * no-op, so this is safe to wire up now.
 */
export const recipeMatchesQueryKey = ["recipes", "matches"] as const;

export function useInventoryQuery() {
  return useQuery<InventoryItem[], ApiError>({
    queryKey: inventoryQueryKey,
    queryFn: getInventory,
  });
}

function useInvalidateAfterInventoryChange() {
  const queryClient = useQueryClient();
  return () => {
    queryClient.invalidateQueries({ queryKey: inventoryQueryKey });
    queryClient.invalidateQueries({ queryKey: recipeMatchesQueryKey });
  };
}

/**
 * Each of these is intended to be called from its own component instance
 * (e.g. once per rendered row) so mutation state — `isPending`, `error` —
 * stays independent per item rather than shared across the whole list.
 */

export function useCreateInventoryItem() {
  const invalidate = useInvalidateAfterInventoryChange();
  return useMutation<InventoryItem, ApiError, CreateInventoryItemRequest>({
    mutationFn: (input) => createInventoryItem(input),
    onSuccess: invalidate,
  });
}

export function useUpdateInventoryItem() {
  const invalidate = useInvalidateAfterInventoryChange();
  return useMutation<
    InventoryItem,
    ApiError,
    { id: number; patch: UpdateInventoryItemRequest }
  >({
    mutationFn: ({ id, patch }) => updateInventoryItem(id, patch),
    onSuccess: invalidate,
  });
}

export function useDeleteInventoryItem() {
  const invalidate = useInvalidateAfterInventoryChange();
  return useMutation<void, ApiError, number>({
    mutationFn: (id) => deleteInventoryItem(id),
    onSuccess: invalidate,
  });
}
