import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import * as recipeApi from "../../api/recipeApi";
import type { RecipeMatch } from "../../types/recipe";
import { RecipeMatchList } from "./RecipeMatchList";

vi.mock("../../api/recipeApi");
const mockedApi = vi.mocked(recipeApi);

function renderList() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <RecipeMatchList />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const PERFECT_MATCH: RecipeMatch = {
  recipe_id: 120,
  recipe_name: "Brown Sugar Cookies",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "perfect",
  matched_ingredients: [
    {
      recipe_ingredient_id: "butter",
      ingredient_name_original: "butter",
      inventory_item_id: 169,
      location: "fridge",
      match_type: "exact",
      reason: { type: "identity" },
    },
  ],
  missing_ingredients: [],
  fridge_match_count: 1,
  pantry_match_count: 7,
};

// A single match whose matched_ingredients contains BOTH fridge and
// pantry entries — the exact case the FE-06 revision requires
// `location` to disambiguate, replacing the old (incorrect) assumption
// that matched_ingredients was fridge-only.
const MIXED_LOCATION_MATCH: RecipeMatch = {
  recipe_id: 121,
  recipe_name: "Tomato Basil Soup",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "perfect",
  matched_ingredients: [
    {
      recipe_ingredient_id: "tomato",
      ingredient_name_original: "tomatoes",
      inventory_item_id: 1,
      location: "fridge",
      match_type: "exact",
      reason: { type: "identity" },
    },
    {
      recipe_ingredient_id: "chicken_breast",
      ingredient_name_original: "chicken breasts",
      inventory_item_id: 2,
      location: "fridge",
      match_type: "exact",
      reason: { type: "identity" },
    },
    {
      recipe_ingredient_id: "flour",
      ingredient_name_original: "flour",
      inventory_item_id: 3,
      location: "pantry",
      match_type: "exact",
      reason: { type: "identity" },
    },
  ],
  missing_ingredients: [],
  fridge_match_count: 2,
  pantry_match_count: 1,
};

const IMPERFECT_MATCH: RecipeMatch = {
  recipe_id: 102,
  recipe_name: "101 Cookbooks Tomato Sauce",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "imperfect",
  matched_ingredients: [
    {
      recipe_ingredient_id: "tomato",
      ingredient_name_original: "tomatoes",
      inventory_item_id: 1,
      location: "fridge",
      match_type: "exact",
      reason: { type: "identity" },
    },
  ],
  missing_ingredients: [
    { ingredient_id: "lemon", ingredient_name_original: "lemon", name: "lemon" },
  ],
  fridge_match_count: 1,
  pantry_match_count: 4,
};

const IMPERFECT_MULTI_MISSING: RecipeMatch = {
  recipe_id: 200,
  recipe_name: "Complicated Stew",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "imperfect",
  matched_ingredients: [],
  missing_ingredients: [
    { ingredient_id: "lemon", ingredient_name_original: "lemon", name: "lemon" },
    { ingredient_id: "thyme", ingredient_name_original: "thyme", name: "thyme" },
  ],
  fridge_match_count: 2,
  pantry_match_count: 1,
};

const PANTRY_ONLY_MATCH: RecipeMatch = {
  recipe_id: 13,
  recipe_name: "Buttermilk Substitute (Lemon Juice)",
  alt_title: null,
  restaurant: null,
  source: null,
  attribution: null,
  match_category: "pantry_only",
  matched_ingredients: [
    {
      recipe_ingredient_id: "buttermilk",
      ingredient_name_original: "buttermilk",
      inventory_item_id: 9,
      location: "pantry",
      match_type: "exact",
      reason: { type: "identity" },
    },
  ],
  missing_ingredients: [],
  fridge_match_count: 0,
  pantry_match_count: 1,
};

const FULL_CARD_MATCH: RecipeMatch = {
  recipe_id: 300,
  recipe_name: "Grandma's Pie",
  alt_title: "Holiday Pie",
  restaurant: "Joe's Diner",
  source: "Family Recipe Box",
  attribution: "From Aunt May",
  match_category: "perfect",
  matched_ingredients: [
    {
      recipe_ingredient_id: "flour",
      ingredient_name_original: "flour",
      inventory_item_id: 5,
      location: "fridge",
      match_type: "exact",
      reason: { type: "identity" },
    },
  ],
  missing_ingredients: [],
  fridge_match_count: 1,
  pantry_match_count: 0,
};

describe("RecipeMatchList", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows Finding recipes... while loading", () => {
    mockedApi.getRecipeMatches.mockReturnValue(new Promise(() => {}));
    renderList();
    expect(screen.getByText("Finding recipes...")).toBeInTheDocument();
  });

  it("renders a perfect match under Perfect with its fridge ingredients", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([PERFECT_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    expect(screen.getByRole("heading", { name: "Perfect" })).toBeInTheDocument();
    const fridgeLine = screen.getByText((_, el) => el?.textContent === "fridge: butter");
    expect(fridgeLine).toBeInTheDocument();
  });

  it("uses location === 'fridge' to build the fridge line, excluding pantry-located entries in the same array", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([MIXED_LOCATION_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("Tomato Basil Soup"));
    const fridgeLine = screen.getByText(
      (_, el) => el?.textContent === "fridge: tomatoes, chicken breasts",
    );
    expect(fridgeLine).toBeInTheDocument();
    // "flour" is location: "pantry" in this fixture — must not leak into the fridge line.
    expect(screen.queryByText(/flour/)).not.toBeInTheDocument();
    // Must render the recipe-facing phrase, not the identity slug.
    expect(screen.queryByText(/chicken_breast/)).not.toBeInTheDocument();
  });

  it("renders an imperfect match with both fridge and missing lines", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([IMPERFECT_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("101 Cookbooks Tomato Sauce"));
    expect(screen.getByRole("heading", { name: "Imperfect" })).toBeInTheDocument();
    expect(screen.getByText((_, el) => el?.textContent === "fridge: tomatoes")).toBeInTheDocument();
    expect(screen.getByText((_, el) => el?.textContent === "missing: lemon")).toBeInTheDocument();
  });

  it("comma-delimits multiple missing ingredients using ingredient_name_original, not name", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([IMPERFECT_MULTI_MISSING]);
    renderList();

    await waitFor(() => screen.getByText("Complicated Stew"));
    expect(
      screen.getByText((_, el) => el?.textContent === "missing: lemon, thyme"),
    ).toBeInTheDocument();
  });

  it("does not add a preparation phrase or quantity to a fridge/missing ingredient name", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([IMPERFECT_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("101 Cookbooks Tomato Sauce"));
    // Exact text match (not "tomatoes, diced" or "2 tomatoes") proves nothing was appended.
    expect(screen.getByText((_, el) => el?.textContent === "fridge: tomatoes")).toBeInTheDocument();
  });

  it("renders a pantry-only match without a fridge or missing line, even though matched_ingredients is non-empty", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([PANTRY_ONLY_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("Buttermilk Substitute (Lemon Juice)"));
    expect(screen.getByRole("heading", { name: "Pantry only" })).toBeInTheDocument();
    expect(screen.getByText("Pantry")).toBeInTheDocument();
    // Its one matched ingredient is location: "pantry" — must not appear as a fridge line.
    expect(screen.queryByText(/fridge:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/missing:/)).not.toBeInTheDocument();
  });

  it("renders the full base card (alt_title, restaurant, source, attribution) in the specified order", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([FULL_CARD_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("Grandma's Pie"));
    expect(screen.getByText("(Holiday Pie)")).toBeInTheDocument();
    expect(screen.getByText("Joe's Diner, Family Recipe Box")).toBeInTheDocument();
    expect(screen.getByText("From Aunt May")).toBeInTheDocument();
  });

  it("preserves backend ordering and groups by category without resorting", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([
      PERFECT_MATCH,
      IMPERFECT_MATCH,
      IMPERFECT_MULTI_MISSING,
      PANTRY_ONLY_MATCH,
    ]);
    renderList();

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));

    const headings = screen.getAllByRole("heading", { level: 3 }).map((h) => h.textContent);
    expect(headings).toEqual(["Perfect", "Imperfect", "Pantry only"]);

    const imperfectHeading = screen.getByRole("heading", { name: "Imperfect" });
    const imperfectSection = imperfectHeading.closest("section")!;
    const namesInOrder = Array.from(
      within(imperfectSection).getAllByRole("heading", { level: 4 }),
    ).map((el) => el.textContent);
    expect(namesInOrder).toEqual(["101 Cookbooks Tomato Sauce", "Complicated Stew"]);
  });

  it("shows an empty state, not an error, when there are no matches", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([]);
    renderList();

    await waitFor(() => {
      expect(
        screen.getByText("No recipes match your current kitchen inventory."),
      ).toBeInTheDocument();
    });
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows a human-readable error with Retry on failure", async () => {
    mockedApi.getRecipeMatches.mockRejectedValueOnce(
      new ApiError("internal_error", "Unable to load recipe matches."),
    );
    mockedApi.getRecipeMatches.mockResolvedValueOnce([PERFECT_MATCH]);

    renderList();

    await waitFor(() => {
      expect(screen.getByText("Unable to load recipe matches.")).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: /retry/i }));

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
  });

  it("navigates to /recipes/:recipeId when a match card is selected", async () => {
    mockedApi.getRecipeMatches.mockResolvedValue([PERFECT_MATCH]);
    renderList();

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    const link = screen.getByRole("link", { name: /Brown Sugar Cookies/i });
    expect(link).toHaveAttribute("href", "/recipes/120");
  });
});
