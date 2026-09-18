import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import * as recipeApi from "../api/recipeApi";
import { RecipesPage } from "./RecipesPage";

vi.mock("../api/recipeApi");
const mockedApi = vi.mocked(recipeApi);

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <RecipesPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const FAVORITE = {
  recipe_id: 120,
  recipe_name: "Brown Sugar Cookies",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  favorite: true,
};

const MATCH = {
  recipe_id: 200,
  recipe_name: "Weeknight Pasta",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "perfect" as const,
  matched_ingredients: [],
  missing_ingredients: [],
  fridge_match_count: 0,
  pantry_match_count: 0,
};

const SEARCH_HIT = {
  recipe_id: 999,
  recipe_name: "Search Hit",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  matched_ingredient: null,
  match_type: null,
  match_reason: null,
  matched_term: null,
};

describe("RecipesPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedApi.getFavoriteRecipes.mockResolvedValue([]);
    mockedApi.getRecipeMatches.mockResolvedValue([]);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders the recipe heading as the page's h1", () => {
    renderPage();
    expect(screen.getByRole("heading", { level: 1, name: "Recipes" })).toBeInTheDocument();
  });

  it("shows Favorites before Kitchen Matches in the browse view", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE]);
    mockedApi.getRecipeMatches.mockResolvedValue([MATCH]);

    renderPage();

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    await waitFor(() => screen.getByText("Weeknight Pasta"));

    const headings = screen.getAllByRole("heading", { level: 2 }).map((h) => h.textContent);
    const favoritesIndex = headings.indexOf("Favorites");
    const matchesIndex = headings.findIndex((h) => h?.toLowerCase().includes("kitchen"));
    expect(favoritesIndex).toBeGreaterThanOrEqual(0);
    expect(matchesIndex).toBeGreaterThan(favoritesIndex);
  });

  it("keeps Favorites and Kitchen Matches as independent queries — one failing doesn't affect the other", async () => {
    mockedApi.getFavoriteRecipes.mockRejectedValue(
      new ApiError("internal_error", "Unable to load favorites."),
    );
    mockedApi.getRecipeMatches.mockResolvedValue([MATCH]);

    renderPage();

    await waitFor(() => {
      expect(screen.getByText("Unable to load favorites.")).toBeInTheDocument();
    });
    await waitFor(() => screen.getByText("Weeknight Pasta"));
  });

  it("shows the Favorites empty state distinctly from an error", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([]);
    mockedApi.getRecipeMatches.mockResolvedValue([MATCH]);

    renderPage();

    await waitFor(() => {
      expect(screen.getByText("No favorite recipes yet.")).toBeInTheDocument();
    });
  });

  it("replaces Favorites + Kitchen Matches with search results while a search is active", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE]);
    mockedApi.getRecipeMatches.mockResolvedValue([MATCH]);
    mockedApi.searchRecipes.mockResolvedValue([SEARCH_HIT]);

    renderPage();
    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    await waitFor(() => screen.getByText("Weeknight Pasta"));

    await userEvent.type(screen.getByLabelText(/find a recipe/i), "xyzsearch{Enter}");

    await waitFor(() => screen.getByText("Search Hit"));
    expect(screen.queryByText("Brown Sugar Cookies")).not.toBeInTheDocument();
    expect(screen.queryByText("Weeknight Pasta")).not.toBeInTheDocument();
  });

  it("returns to the Favorites + Kitchen Matches browse view when the search is cleared", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE]);
    mockedApi.getRecipeMatches.mockResolvedValue([MATCH]);
    mockedApi.searchRecipes.mockResolvedValue([SEARCH_HIT]);

    renderPage();
    await userEvent.type(screen.getByLabelText(/find a recipe/i), "xyzsearch{Enter}");
    await waitFor(() => screen.getByText("Search Hit"));

    await userEvent.click(screen.getByRole("button", { name: /clear search/i }));

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    await waitFor(() => screen.getByText("Weeknight Pasta"));
    expect(screen.queryByText("Search Hit")).not.toBeInTheDocument();
  });

  it("recipe cards navigate to /recipes/:recipeId", async () => {
    mockedApi.getFavoriteRecipes.mockResolvedValue([FAVORITE]);
    mockedApi.getRecipeMatches.mockResolvedValue([]);

    renderPage();
    await waitFor(() => screen.getByText("Brown Sugar Cookies"));

    const link = screen.getByRole("link", { name: /Brown Sugar Cookies/i });
    expect(link).toHaveAttribute("href", "/recipes/120");
  });
});
