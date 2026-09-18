import { describe, expect, it } from "vitest";
import type { InventoryItem } from "../../types/inventory";
import { findDuplicateIngredientIds } from "./duplicates";

function makeItem(overrides: Partial<InventoryItem>): InventoryItem {
  return {
    id: 1,
    original_input: "test",
    ingredient_id: null,
    location: "fridge",
    quantity: null,
    unit: null,
    resolution_status: "resolved",
    analysis_result: null,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

describe("findDuplicateIngredientIds", () => {
  it("flags an ingredient present in both fridge and pantry", () => {
    const items = [
      makeItem({ id: 1, ingredient_id: "bell_pepper", location: "fridge" }),
      makeItem({ id: 2, ingredient_id: "bell_pepper", location: "pantry" }),
    ];
    expect(findDuplicateIngredientIds(items)).toEqual(new Set(["bell_pepper"]));
  });

  it("does not flag an ingredient present in only one location", () => {
    const items = [
      makeItem({ id: 1, ingredient_id: "bell_pepper", location: "fridge" }),
      makeItem({ id: 2, ingredient_id: "onion", location: "pantry" }),
    ];
    expect(findDuplicateIngredientIds(items)).toEqual(new Set());
  });

  it("does not treat two unresolved items (null ingredient_id) as duplicates of each other", () => {
    const items = [
      makeItem({ id: 1, ingredient_id: null, location: "fridge" }),
      makeItem({ id: 2, ingredient_id: null, location: "pantry" }),
    ];
    expect(findDuplicateIngredientIds(items)).toEqual(new Set());
  });

  it("does not merge or remove records — it only reports identity overlap", () => {
    const items = [
      makeItem({ id: 1, ingredient_id: "onion", location: "fridge" }),
      makeItem({ id: 2, ingredient_id: "onion", location: "fridge" }),
    ];
    // Same location twice is not a fridge/pantry duplicate.
    expect(findDuplicateIngredientIds(items)).toEqual(new Set());
    expect(items).toHaveLength(2);
  });
});
