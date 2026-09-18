import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import * as inventoryApi from "../api/inventoryApi";
import type { InventoryItem } from "../types/inventory";
import { InventoryPage } from "./InventoryPage";

vi.mock("../api/inventoryApi");

const mockedApi = vi.mocked(inventoryApi);

function makeItem(overrides: Partial<InventoryItem>): InventoryItem {
  return {
    id: 1,
    original_input: "2 red bell peppers",
    ingredient_id: "bell_pepper",
    location: "fridge",
    quantity: 2,
    unit: "each",
    resolution_status: "resolved",
    analysis_result: {
      status: "resolved",
      interpretations: [
        {
          id: "interp_1",
          status: "resolved",
          score: 1.0,
          references: [
            {
              id: "r1",
              ingredient: { id: "bell pepper", original_text: "red bell peppers" },
              source_spans: ["red bell peppers"],
            },
          ],
          evidence: [{ kind: "exact_ingredient_match", record_id: "bell pepper", effect: "supporting" }],
        },
      ],
      selected_interpretation: "interp_1",
    },
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

function renderInventoryPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <InventoryPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("InventoryPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows a loading state and then the loaded inventory", async () => {
    mockedApi.getInventory.mockResolvedValue([makeItem({})]);

    renderInventoryPage();

    expect(screen.getByText("Loading inventory...")).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText("2 red bell peppers")).toBeInTheDocument();
    });
    expect(screen.getByText("bell pepper")).toBeInTheDocument();
  });

  it("shows an empty-state message, not an error, when inventory is empty", async () => {
    mockedApi.getInventory.mockResolvedValue([]);

    renderInventoryPage();

    await waitFor(() => {
      expect(screen.getByText(/kitchen is empty/i)).toBeInTheDocument();
    });
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows a human-readable error with retry when the initial load fails", async () => {
    mockedApi.getInventory.mockRejectedValueOnce(
      new ApiError("internal_error", "Something went wrong loading your inventory."),
    );
    mockedApi.getInventory.mockResolvedValueOnce([makeItem({})]);

    renderInventoryPage();

    await waitFor(() => {
      expect(
        screen.getByText("Something went wrong loading your inventory."),
      ).toBeInTheDocument();
    });
    // Not rendered as an empty list.
    expect(screen.queryByText(/kitchen is empty/i)).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /retry/i }));

    await waitFor(() => {
      expect(screen.getByText("2 red bell peppers")).toBeInTheDocument();
    });
  });

  it("creates an item via the form without sending server-derived fields", async () => {
    mockedApi.getInventory.mockResolvedValue([]);
    mockedApi.createInventoryItem.mockResolvedValue(makeItem({ id: 42 }));

    renderInventoryPage();
    await waitFor(() => expect(screen.getByText(/kitchen is empty/i)).toBeInTheDocument());

    await userEvent.type(
      screen.getByLabelText(/what do you have/i),
      "2 red bell peppers",
    );
    await userEvent.click(screen.getByRole("radio", { name: /fridge/i }));
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));

    await waitFor(() => {
      expect(mockedApi.createInventoryItem).toHaveBeenCalledWith({
        original_input: "2 red bell peppers",
        location: "fridge",
      });
    });
  });

  it("shows a retryable error when creation fails, and does not clear the form", async () => {
    mockedApi.getInventory.mockResolvedValue([]);
    mockedApi.createInventoryItem.mockRejectedValue(
      new ApiError("validation_error", "original_input is required."),
    );

    renderInventoryPage();
    await waitFor(() => expect(screen.getByText(/kitchen is empty/i)).toBeInTheDocument());

    const input = screen.getByLabelText(/what do you have/i);
    await userEvent.type(input, "mystery leftovers");
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));

    await waitFor(() => {
      expect(screen.getByText(/original_input is required/i)).toBeInTheDocument();
    });
    expect(input).toHaveValue("mystery leftovers");
  });

  it("edits an item's original input and PATCHes only the changed field", async () => {
    mockedApi.getInventory.mockResolvedValue([makeItem({})]);
    mockedApi.updateInventoryItem.mockResolvedValue(
      makeItem({ original_input: "bell pepper", resolution_status: "resolved" }),
    );

    renderInventoryPage();
    await waitFor(() => screen.getByText("2 red bell peppers"));

    await userEvent.click(screen.getByRole("button", { name: "Edit" }));
    // The add-item form above also has a "What do you have?" field; the
    // edit form for this row is the second one in document order.
    const editInput = screen.getAllByLabelText(/what do you have/i)[1];
    await userEvent.clear(editInput);
    await userEvent.type(editInput, "bell pepper");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockedApi.updateInventoryItem).toHaveBeenCalledWith(1, {
        original_input: "bell pepper",
      });
    });
  });

  it("keeps the item visible and shows an error when update fails", async () => {
    mockedApi.getInventory.mockResolvedValue([makeItem({})]);
    mockedApi.updateInventoryItem.mockRejectedValue(
      new ApiError("internal_error", "Could not save your changes."),
    );

    renderInventoryPage();
    await waitFor(() => screen.getByText("2 red bell peppers"));

    await userEvent.click(screen.getByRole("button", { name: "Edit" }));
    const editInput = screen.getAllByLabelText(/what do you have/i)[1];
    await userEvent.clear(editInput);
    await userEvent.type(editInput, "something else");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByText(/could not save your changes/i)).toBeInTheDocument();
    });
    // Still in the edit form — nothing was silently discarded.
    expect(screen.getAllByLabelText(/what do you have/i)[1]).toHaveValue("something else");
  });

  it("deletes an item only after explicit confirmation and server success", async () => {
    mockedApi.getInventory
      .mockResolvedValueOnce([makeItem({})])
      .mockResolvedValueOnce([]);
    mockedApi.deleteInventoryItem.mockResolvedValue(undefined);

    renderInventoryPage();
    await waitFor(() => screen.getByText("2 red bell peppers"));

    await userEvent.click(screen.getByRole("button", { name: "Delete" }));
    // Not deleted yet — awaiting confirmation.
    expect(mockedApi.deleteInventoryItem).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole("button", { name: /confirm delete/i }));

    await waitFor(() => {
      expect(mockedApi.deleteInventoryItem).toHaveBeenCalledWith(1);
    });
    await waitFor(() => {
      expect(screen.getByText(/kitchen is empty/i)).toBeInTheDocument();
    });
  });

  it("indicates when the same ingredient exists in both fridge and pantry, without merging", async () => {
    mockedApi.getInventory.mockResolvedValue([
      makeItem({ id: 1, location: "fridge", ingredient_id: "onion", original_input: "onion" }),
      makeItem({ id: 2, location: "pantry", ingredient_id: "onion", original_input: "onion" }),
    ]);

    renderInventoryPage();

    await waitFor(() => {
      expect(screen.getAllByText("onion")).toHaveLength(2);
    });

    const fridgeSection = screen.getByRole("heading", { name: "Fridge" }).closest("section")!;
    const pantrySection = screen.getByRole("heading", { name: "Pantry" }).closest("section")!;
    expect(within(fridgeSection).getByText(/also in the pantry/i)).toBeInTheDocument();
    expect(within(pantrySection).getByText(/also in the fridge/i)).toBeInTheDocument();
  });

  it("does not present an unresolved item's status as resolved", async () => {
    mockedApi.getInventory.mockResolvedValue([
      makeItem({
        id: 1,
        ingredient_id: null,
        resolution_status: "unresolved",
        analysis_result: null,
        original_input: "some weird thing",
      }),
    ]);

    renderInventoryPage();

    await waitFor(() => {
      expect(screen.getByText("No confirmed interpretation yet.")).toBeInTheDocument();
    });
    expect(screen.getByText("unresolved")).toBeInTheDocument();
  });
});
