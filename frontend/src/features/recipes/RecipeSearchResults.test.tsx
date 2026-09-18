import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import * as recipeApi from "../../api/recipeApi";
import type { SubmittedSearch } from "./RecipeSearchResults";
import { RecipeSearchResults } from "./RecipeSearchResults";

vi.mock("../../api/recipeApi");
const mockedApi = vi.mocked(recipeApi);

function renderSearch(submitted: SubmittedSearch | null = null) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const onSubmit = vi.fn();
  const onClear = vi.fn();
  const utils = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <RecipeSearchResults submitted={submitted} onSubmit={onSubmit} onClear={onClear} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...utils, onSubmit, onClear };
}

describe("RecipeSearchResults", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows no result state and no Clear button when idle (submitted is null)", () => {
    renderSearch(null);
    expect(screen.queryByText("Searching...")).not.toBeInTheDocument();
    expect(screen.queryByText("No recipes found.")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /clear search/i })).not.toBeInTheDocument();
    expect(mockedApi.searchRecipes).not.toHaveBeenCalled();
  });

  it("shows name-search results, bolding the submitted query within the recipe name", async () => {
    mockedApi.searchRecipes.mockResolvedValue([
      {
        recipe_id: 85,
        recipe_name: "Avacado Chicken Salad",
        alt_title: null,
        restaurant: null,
        source: null,
        attribution: null,
        matched_ingredient: null,
        match_type: null,
        match_reason: null,
        matched_term: null,
      },
    ]);

    renderSearch({ query: "chicken", type: "name" });

    await waitFor(() => {
      expect(screen.getByRole("heading", { level: 4 })).toHaveTextContent(
        "Avacado Chicken Salad",
      );
    });
    expect(mockedApi.searchRecipes).toHaveBeenCalledWith("chicken", "name");

    expect(screen.getByText("Chicken", { selector: "strong" })).toBeInTheDocument();
    expect(screen.queryByText(/Uses:/)).not.toBeInTheDocument();
  });

  it("shows ingredient-search results with the matched ingredient bolded, and does not bold the name", async () => {
    mockedApi.searchRecipes.mockResolvedValue([
      {
        recipe_id: 83,
        recipe_name: 'British Indian Curry Shop "Base Gravy"',
        alt_title: null,
        restaurant: null,
        source: null,
        attribution: null,
        matched_ingredient: "tomato",
        match_type: "exact",
        match_reason: { type: "identity" },
        matched_term: "tomato",
      },
    ]);

    renderSearch({ query: "tomato", type: "ingredient" });

    await waitFor(() => screen.getByText('British Indian Curry Shop "Base Gravy"'));
    expect(mockedApi.searchRecipes).toHaveBeenCalledWith("tomato", "ingredient");
    expect(screen.getByText("tomato", { selector: "strong" })).toBeInTheDocument();
  });

  it("renders the base card fields (alt_title, restaurant, source, attribution) and never a favorite indicator", async () => {
    mockedApi.searchRecipes.mockResolvedValue([
      {
        recipe_id: 90,
        recipe_name: "Pie",
        alt_title: "Grandma's Pie",
        restaurant: "Joe's Diner",
        source: "Family Recipe Box",
        attribution: "From Aunt May",
        matched_ingredient: null,
        match_type: null,
        match_reason: null,
        matched_term: null,
      },
    ]);

    renderSearch({ query: "pie", type: "name" });

    await waitFor(() => screen.getByText("(Grandma's Pie)"));
    expect(screen.getByText("Joe's Diner, Family Recipe Box")).toBeInTheDocument();
    expect(screen.getByText("From Aunt May")).toBeInTheDocument();
    expect(screen.queryByText(/favorite/i)).not.toBeInTheDocument();
  });

  it("shows a distinct no-results state for zero results", async () => {
    mockedApi.searchRecipes.mockResolvedValue([]);
    renderSearch({ query: "zzzzz", type: "name" });

    await waitFor(() => {
      expect(screen.getByText("No recipes found.")).toBeInTheDocument();
    });
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows an error with Retry on failure, distinct from no-results", async () => {
    mockedApi.searchRecipes.mockRejectedValue(
      new ApiError("internal_error", "Unable to search recipes."),
    );

    renderSearch({ query: "chicken", type: "name" });

    await waitFor(() => {
      expect(screen.getByText("Unable to search recipes.")).toBeInTheDocument();
    });
    expect(screen.queryByText("No recipes found.")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
  });

  it("shows a Clear search control once a search is active, and calls onClear", async () => {
    mockedApi.searchRecipes.mockResolvedValue([]);
    const { onClear } = renderSearch({ query: "chicken", type: "name" });
    const clearButton = await screen.findByRole("button", { name: /clear search/i });
    clearButton.click();
    expect(onClear).toHaveBeenCalled();
  });
});
