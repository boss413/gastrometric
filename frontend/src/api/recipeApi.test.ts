import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import { getFavoriteRecipes, getRecipe, getRecipeMatches, searchRecipes } from "./recipeApi";

function jsonResponse(body: unknown, init?: { status?: number }) {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    headers: { "Content-Type": "application/json" },
  });
}

describe("recipeApi", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("searchRecipes builds a name-search request and returns the results array", async () => {
    const results = [
      {
        recipe_id: 85,
        recipe_name: "Avacado Chicken Salad",
        matched_ingredient: null,
        match_type: null,
        match_reason: null,
        matched_term: null,
      },
    ];
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(jsonResponse({ results }));

    const result = await searchRecipes("chicken", "name");

    expect(result).toEqual(results);
    const [url] = fetchMock.mock.calls[0];
    const requestUrl = new URL(String(url), "http://localhost");
    expect(requestUrl.pathname.endsWith("/api/recipes/search")).toBe(true);
    expect(requestUrl.searchParams.get("q")).toBe("chicken");
    expect(requestUrl.searchParams.get("type")).toBe("name");
  });

  it("searchRecipes builds an ingredient-search request with type=ingredient", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse({
        results: [
          {
            recipe_id: 83,
            recipe_name: 'British Indian Curry Shop "Base Gravy"',
            matched_ingredient: "tomato",
            match_type: "exact",
            match_reason: { type: "identity" },
            matched_term: "tomato",
          },
        ],
      }),
    );

    await searchRecipes("tomato", "ingredient");

    const [url] = fetchMock.mock.calls[0];
    const requestUrl = new URL(String(url), "http://localhost");
    expect(requestUrl.searchParams.get("type")).toBe("ingredient");
    expect(requestUrl.searchParams.get("q")).toBe("tomato");
  });

  it("getRecipeMatches requests /api/recipes/matches and returns the results array", async () => {
    const results = [
      {
        recipe_id: 120,
        recipe_name: "Brown Sugar Cookies",
        match_category: "perfect",
        matched_ingredients: [
          {
            recipe_ingredient_id: "butter",
            inventory_item_id: 169,
            match_type: "exact",
            reason: { type: "identity" },
          },
        ],
        missing_ingredients: [],
        fridge_match_count: 1,
        pantry_match_count: 7,
      },
    ];
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(jsonResponse({ results }));

    const result = await getRecipeMatches();

    expect(result).toEqual(results);
    const [url] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("/api/recipes/matches");
  });

  it("propagates a human-readable ApiError through fetchJson on failure", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse(
        { error: { code: "internal_error", message: "Search is temporarily unavailable." } },
        { status: 500 },
      ),
    );

    let caught: unknown;
    try {
      await searchRecipes("chicken", "name");
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("Search is temporarily unavailable.");
  });

  it("getRecipe requests /api/recipes/{id} and returns the decoded response as-is", async () => {
    const recipe = {
      id: 120,
      recipe_name: "Brown Sugar Cookies",
      alt_title: null,
      author: "Jane Doe",
      attribution: null,
      source: "Some Blog",
      restaurant: null,
      url: null,
      video: null,
      servings: 24,
      yield: null,
      notes: null,
      state: "published",
      ingestion_method: "manual",
      sections: [],
      favorites: null,
    };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse(recipe));

    const result = await getRecipe(120);

    expect(result).toEqual(recipe);
    const [url] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("/api/recipes/120");
  });

  it("getRecipe propagates a human-readable ApiError on failure", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse(
        { error: { code: "not_found", message: "Recipe not found." } },
        { status: 404 },
      ),
    );

    let caught: unknown;
    try {
      await getRecipe(999999);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("Recipe not found.");
  });

  it("getFavoriteRecipes requests /api/recipes/favorites and returns the results array", async () => {
    const results = [
      {
        recipe_id: 120,
        recipe_name: "Brown Sugar Cookies",
        alt_title: null,
        restaurant: null,
        source: "Example Source",
        attribution: "Example Author",
        favorite: true,
      },
    ];
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(jsonResponse({ results }));

    const result = await getFavoriteRecipes();

    expect(result).toEqual(results);
    const [url] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("/api/recipes/favorites");
  });

  it("getFavoriteRecipes propagates a human-readable ApiError on failure", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse(
        { error: { code: "internal_error", message: "Unable to load favorites." } },
        { status: 500 },
      ),
    );

    let caught: unknown;
    try {
      await getFavoriteRecipes();
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("Unable to load favorites.");
  });
});
