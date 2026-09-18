import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as inventoryApi from "../../api/inventoryApi";
import type { InventoryItem } from "../../types/inventory";
import { InventoryForm } from "./InventoryForm";

vi.mock("../../api/inventoryApi");
const mockedApi = vi.mocked(inventoryApi);

function renderForm() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <InventoryForm />
    </QueryClientProvider>,
  );
}

const responseItem: InventoryItem = {
  id: 1,
  original_input: "flour",
  ingredient_id: null,
  location: "pantry",
  quantity: null,
  unit: null,
  resolution_status: "unresolved",
  analysis_result: null,
  created_at: null,
  updated_at: null,
};

describe("InventoryForm", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("does not submit when the free-text entry is empty", async () => {
    renderForm();
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));
    expect(mockedApi.createInventoryItem).not.toHaveBeenCalled();
  });

  it("defaults location to a selected value so a location is always sent", async () => {
    mockedApi.createInventoryItem.mockResolvedValue(responseItem);
    renderForm();

    await userEvent.type(screen.getByLabelText(/what do you have/i), "flour");
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));

    await waitFor(() => {
      expect(mockedApi.createInventoryItem).toHaveBeenCalledWith(
        expect.objectContaining({ location: "fridge" }),
      );
    });
  });

  it("omits quantity and unit entirely when left blank", async () => {
    mockedApi.createInventoryItem.mockResolvedValue(responseItem);
    renderForm();

    await userEvent.type(screen.getByLabelText(/what do you have/i), "flour");
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));

    await waitFor(() => {
      const [payload] = mockedApi.createInventoryItem.mock.calls[0];
      expect(payload).not.toHaveProperty("quantity");
      expect(payload).not.toHaveProperty("unit");
    });
  });

  it("includes quantity and unit when provided", async () => {
    mockedApi.createInventoryItem.mockResolvedValue(responseItem);
    renderForm();

    await userEvent.type(screen.getByLabelText(/what do you have/i), "flour");
    await userEvent.type(screen.getByLabelText(/quantity/i), "5");
    await userEvent.type(screen.getByLabelText(/unit/i), "lb");
    await userEvent.click(screen.getByRole("button", { name: /add item/i }));

    await waitFor(() => {
      expect(mockedApi.createInventoryItem).toHaveBeenCalledWith(
        expect.objectContaining({ quantity: 5, unit: "lb" }),
      );
    });
  });
});
