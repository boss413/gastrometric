import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import {
  createInventoryItem,
  deleteInventoryItem,
  getInventory,
  updateInventoryItem,
} from "./inventoryApi";

function jsonResponse(body: unknown, init?: { status?: number }) {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    headers: { "Content-Type": "application/json" },
  });
}

describe("inventoryApi", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("getInventory parses the items array out of the list response", async () => {
    const items = [
      {
        id: 1,
        original_input: "2 red bell peppers",
        ingredient_id: "bell_pepper",
        location: "fridge",
        quantity: 2,
        unit: "each",
        resolution_status: "resolved",
        analysis_result: null,
        created_at: null,
        updated_at: null,
      },
    ];
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(jsonResponse({ items }));

    const result = await getInventory();

    expect(result).toEqual(items);
    const [url] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("/api/inventory");
  });

  it("createInventoryItem sends only user-supplied fields, never server-derived ones", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse(
        {
          id: 2,
          original_input: "2 red bell peppers",
          ingredient_id: null,
          location: "fridge",
          quantity: 2,
          unit: "each",
          resolution_status: "unresolved",
          analysis_result: null,
          created_at: null,
          updated_at: null,
        },
        { status: 201 },
      ),
    );

    await createInventoryItem({
      original_input: "2 red bell peppers",
      location: "fridge",
      quantity: 2,
      unit: "each",
    });

    const [, init] = fetchMock.mock.calls[0];
    expect(init?.method).toBe("POST");
    const sentBody = JSON.parse(init?.body as string);
    expect(sentBody).toEqual({
      original_input: "2 red bell peppers",
      location: "fridge",
      quantity: 2,
      unit: "each",
    });
    expect(sentBody).not.toHaveProperty("ingredient_id");
    expect(sentBody).not.toHaveProperty("resolution_status");
    expect(sentBody).not.toHaveProperty("analysis_result");
  });

  it("updateInventoryItem PATCHes only the fields included in the patch", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse({
        id: 3,
        original_input: "bell pepper",
        ingredient_id: "bell_pepper",
        location: "pantry",
        quantity: null,
        unit: null,
        resolution_status: "resolved",
        analysis_result: null,
        created_at: null,
        updated_at: null,
      }),
    );

    await updateInventoryItem(3, { location: "pantry" });

    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("/api/inventory/3");
    expect(init?.method).toBe("PATCH");
    expect(JSON.parse(init?.body as string)).toEqual({ location: "pantry" });
  });

  it("deleteInventoryItem resolves cleanly on 204 No Content", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 204 }));

    await expect(deleteInventoryItem(5)).resolves.toBeUndefined();
  });

  it("surfaces a human-readable ApiError on failure, not a raw response", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() =>
      Promise.resolve(
        jsonResponse(
          { error: { code: "not_found", message: "Inventory item not found." } },
          { status: 404 },
        ),
      ),
    );

    let caught: unknown;
    try {
      await deleteInventoryItem(999);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("Inventory item not found.");
  });
});
