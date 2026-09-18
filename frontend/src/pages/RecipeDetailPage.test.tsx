import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import * as recipeApi from "../api/recipeApi";
import type { RecipeDetail } from "../types/recipe";
import { RecipeDetailPage } from "./RecipeDetailPage";

vi.mock("../api/recipeApi");
const mockedApi = vi.mocked(recipeApi);

function renderAt(path: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path="/recipes/:recipeId" element={<RecipeDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const SAMPLE_RECIPE: RecipeDetail = {
  id: 42,
  recipe_name: "Brown Sugar Cookies",
  alt_title: "Grandma's Cookies",
  author: "Jane Doe",
  attribution: "Adapted from Joy of Cooking",
  source: "Joy of Cooking",
  restaurant: null,
  url: "https://example.com/cookies",
  video: null,
  servings: 24,
  yield: "2 dozen cookies",
  notes: "Best served warm.",
  state: "published",
  ingestion_method: "manual",
  favorites: null,
  sections: [
    {
      id: 1,
      name: "Dough",
      ingredients: [
        {
          ingredient_id: "butter",
          name: "butter, softened",
          quantity: "1 cup",
          grams: 227,
          notes: null,
          optional: false,
          alt_group_id: null,
          alt_kind: null,
        },
        {
          ingredient_id: "vanilla",
          name: "vanilla extract",
          quantity: "1 tsp",
          grams: null,
          notes: "For flavor",
          optional: true,
          alt_group_id: null,
          alt_kind: null,
        },
      ],
      instructions: [
        { number: 1, text: "Cream the butter and sugar." },
        { number: 2, text: "Mix in the vanilla." },
      ],
    },
    {
      id: 2,
      name: "Topping",
      ingredients: [
        {
          ingredient_id: "sugar",
          name: "brown sugar",
          quantity: "2 tbsp",
          grams: null,
          notes: null,
          optional: false,
          alt_group_id: null,
          alt_kind: null,
        },
      ],
      instructions: [{ number: 1, text: "Sprinkle over the dough." }],
    },
  ],
};

describe("RecipeDetailPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows Loading recipe... while pending", () => {
    mockedApi.getRecipe.mockReturnValue(new Promise(() => {}));
    renderAt("/recipes/42");
    expect(screen.getByText("Loading recipe...")).toBeInTheDocument();
  });

  it("has a visible Back control", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");
    expect(screen.getByRole("button", { name: /recipes/i })).toBeInTheDocument();
  });

  it("renders the recipe name as the primary heading, with alt_title alongside it", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { level: 1, name: /Brown Sugar Cookies/ }),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("(Grandma's Cookies)")).toBeInTheDocument();
  });

  it("displays author, attribution, source, and yield when supplied", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Jane Doe"));
    expect(screen.getByText("Adapted from Joy of Cooking")).toBeInTheDocument();
    expect(screen.getByText("Joy of Cooking")).toBeInTheDocument();
    expect(screen.getByText("2 dozen cookies")).toBeInTheDocument();
    expect(screen.getByText("24")).toBeInTheDocument();
  });

  it("omits null metadata fields rather than showing a placeholder", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE); // restaurant: null
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Jane Doe"));
    expect(screen.queryByText("Restaurant")).not.toBeInTheDocument();
    expect(screen.queryByText(/unknown/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/n\/a/i)).not.toBeInTheDocument();
  });

  it("links the source using url when both are supplied", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Joy of Cooking"));
    const link = screen.getByRole("link", { name: "Joy of Cooking" });
    expect(link).toHaveAttribute("href", "https://example.com/cookies");
  });

  it("preserves section names, ingredient order, and separates quantity from ingredient name", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Dough"));
    expect(screen.getByText("Topping")).toBeInTheDocument();

    const ingredientNames = screen
      .getAllByText(/butter, softened|vanilla extract|brown sugar/)
      .map((el) => el.textContent);
    expect(ingredientNames).toEqual(["butter, softened", "vanilla extract", "brown sugar"]);

    // Quantity is a separate cell, not appended to the name.
    expect(screen.getByText("butter, softened")).not.toHaveTextContent("1 cup");
    expect(screen.getByText("1 cup (227g)")).toBeInTheDocument();
  });

  it("shows ingredient notes and optional state without relying on color alone", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("(Optional. For flavor)"));
  });

  it("preserves backend instruction order and numbering", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Cream the butter and sugar."));
    const items = screen.getAllByRole("listitem").filter((el) => el.tagName === "LI");
    const doughInstructions = items.filter((el) =>
      ["Cream the butter and sugar.", "Mix in the vanilla."].includes(el.textContent ?? ""),
    );
    expect(doughInstructions.map((el) => el.getAttribute("value"))).toEqual(["1", "2"]);
  });

  it("shows recipe-level notes in a separate section from ingredient notes", async () => {
    mockedApi.getRecipe.mockResolvedValue(SAMPLE_RECIPE);
    renderAt("/recipes/42");

    await waitFor(() => screen.getByText("Best served warm."));
    expect(screen.getByRole("heading", { name: "Notes" })).toBeInTheDocument();
  });

  it("shows a human-readable error with Retry, and retry refetches the same recipe", async () => {
    mockedApi.getRecipe.mockRejectedValueOnce(
      new ApiError("internal_error", "Unable to load this recipe."),
    );
    mockedApi.getRecipe.mockResolvedValueOnce(SAMPLE_RECIPE);

    renderAt("/recipes/42");

    await waitFor(() => {
      expect(screen.getByText("Unable to load this recipe.")).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: /retry/i }));

    await waitFor(() => screen.getByText("Brown Sugar Cookies"));
    expect(mockedApi.getRecipe).toHaveBeenCalledTimes(2);
    expect(mockedApi.getRecipe).toHaveBeenNthCalledWith(2, 42);
  });

  it("shows a concise 'Recipe not found' presentation for a not_found ApiError", async () => {
    mockedApi.getRecipe.mockRejectedValue(new ApiError("not_found", "Recipe not found."));
    renderAt("/recipes/999999");

    await waitFor(() => {
      expect(screen.getByText("Recipe not found.")).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: /retry/i })).not.toBeInTheDocument();
  });

  it("does not call the API for a malformed recipe id", async () => {
    renderAt("/recipes/not-a-number");

    await waitFor(() => {
      expect(screen.getByText("Recipe not found.")).toBeInTheDocument();
    });
    expect(mockedApi.getRecipe).not.toHaveBeenCalled();
  });
});
