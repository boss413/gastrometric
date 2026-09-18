import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import * as recipeApi from "../../api/recipeApi";
import type { FavoriteRecipe } from "../../types/recipe";
import { RecipeFavoritesList } from "./RecipeFavoritesList";

vi.mock("../../api/recipeApi");
const mockedApi = vi.mocked(recipeApi);

function renderFavorites() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <RecipeFavoritesList />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const FAVORITE_A: FavoriteRecipe = {
  recipe_id: 5,
  recipe_name: "Zucchini Bread",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  favorite: true,
};

const FAVORITE_B: FavoriteRecipe = {
  recipe_id: 120,
  recipe_name: "Brown Sugar Cookies",
  alt_title: "Grandma's Cookies",
  restaurant: "Joe's Diner",
  source: "Example Source",
  attribution: "Example Author",
  favorite: true,
};

describe("RecipeFavoritesList", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows Loading favorites... while pending", () => {
    mockedApi.getFavoriteRecipes.mockReturnValue(new Promise(() => {}));
    renderFavorites();
    expect(screen.getByText("Loading favorites...")).toBeInTheDocument();
  });

  it("shows an explicit empty state, not an error, for zero favorites", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([]);
    renderFavorites();

    await waitFor(() => {
      expect(screen.getByText("No favorite recipes yet.")).toBeInTheDocument();
    });
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows a human-readable error with Retry on failure", async () => {
    mockedApi.getFavoriteRecipes.mockRejectedValueOnce(
      new ApiError("internal_error", "Unable to load favorites."),
    );
    mockedApi.getFavoriteRecipes.mockResolvedValueOnce([FAVORITE_A]);

    renderFavorites();

    await waitFor(() => {
      expect(screen.getByText("Unable to load favorites.")).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: /retry/i }));

    await waitFor(() => screen.getByText("Zucchini Bread"));
  });

  it("preserves backend ordering exactly, without alphabetizing", async () => {
    // Deliberately not alphabetical — Zucchini before Brown — matching backend recipe_id order.
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE_A, FAVORITE_B]);
    renderFavorites();

    await waitFor(() => screen.getByText("Zucchini Bread"));
    const names = screen.getAllByRole("heading", { level: 4 }).map((h) => h.textContent);
    expect(names).toEqual(["Zucchini Bread", "Brown Sugar Cookies (Grandma's Cookies)"]);
  });

  it("shows alt_title, restaurant, source, and attribution, and never servings/yield since the finalized contract doesn't return them", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE_B]);
    renderFavorites();

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    expect(screen.getByText("(Grandma's Cookies)")).toBeInTheDocument();
    expect(screen.getByText("Joe's Diner, Example Source")).toBeInTheDocument();
    expect(screen.getByText("Example Author")).toBeInTheDocument();
    expect(screen.queryByText("24")).not.toBeInTheDocument();
    expect(screen.queryByText("24 cookies")).not.toBeInTheDocument();
  });

  it("navigates to /recipes/:recipeId", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE_A]);
    renderFavorites();

    await waitFor(() => screen.getByText("Zucchini Bread"));
    const link = screen.getByRole("link", { name: /Zucchini Bread/i });
    expect(link).toHaveAttribute("href", "/recipes/5");
  });

  it("has no favorite toggle/button of any kind", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE_A]);
    renderFavorites();

    await waitFor(() => screen.getByText("Zucchini Bread"));
    expect(screen.queryByRole("button", { name: /favorite/i })).not.toBeInTheDocument();
  });
});
