import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { RecipeSectionBlock } from "./RecipeSectionBlock";
import type { RecipeIngredient, RecipeInstruction, RecipeSection } from "../../types/recipe";

function makeIngredient(overrides: Partial<RecipeIngredient> = {}): RecipeIngredient {
  return {
    ingredient_id: "ing-1",
    name: "Flour",
    quantity: "2 cups",
    grams: null,
    notes: null,
    optional: false,
    alt_group_id: null,
    alt_kind: null,
    ...overrides,
  };
}

function makeInstruction(overrides: Partial<RecipeInstruction> = {}): RecipeInstruction {
  return {
    number: 1,
    text: "Mix the dry ingredients.",
    ...overrides,
  };
}

function makeSection(overrides: Partial<RecipeSection> = {}): RecipeSection {
  return {
    id: "sec-1",
    name: "Main",
    ingredients: [],
    instructions: [],
    ...overrides,
  };
}

describe("RecipeSectionBlock", () => {
  it("renders no section UI for a section with no ingredients and no instructions", () => {
    const section = makeSection({ ingredients: [], instructions: [] });
    const { container } = render(<RecipeSectionBlock section={section} />);

    expect(container).toBeEmptyDOMElement();
    expect(screen.queryByRole("heading")).not.toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
  });

  it("renders ingredients and no instruction list when instructions are empty", () => {
    const section = makeSection({
      ingredients: [makeIngredient({ name: "Flour" })],
      instructions: [],
    });
    render(<RecipeSectionBlock section={section} />);

    expect(screen.getByText("Flour")).toBeInTheDocument();
    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
  });

  it("renders instructions and no ingredient table when ingredients are empty", () => {
    const section = makeSection({
      ingredients: [],
      instructions: [makeInstruction({ number: 1, text: "Preheat the oven." })],
    });
    render(<RecipeSectionBlock section={section} />);

    expect(screen.getByText("Preheat the oven.")).toBeInTheDocument();
    expect(screen.getByRole("list")).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("renders both ingredients and instructions when both are present", () => {
    const section = makeSection({
      ingredients: [makeIngredient({ name: "Sugar" })],
      instructions: [makeInstruction({ number: 1, text: "Cream the butter and sugar." })],
    });
    render(<RecipeSectionBlock section={section} />);

    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.getByText("Sugar")).toBeInTheDocument();
    expect(screen.getByRole("list")).toBeInTheDocument();
    expect(screen.getByText("Cream the butter and sugar.")).toBeInTheDocument();
  });

  it("displays the backend-supplied instruction number, not the array position", () => {
    const section = makeSection({
      ingredients: [],
      instructions: [
        makeInstruction({ number: 7, text: "Rest the dough for an hour." }),
      ],
    });
    const { container } = render(<RecipeSectionBlock section={section} />);

    const listItem = container.querySelector("li");
    expect(listItem).not.toBeNull();
    // The instruction is the only (zeroth) array element, but its backend
    // `number` is 7 — the rendered `value` attribute must reflect that,
    // not the element's position in the array.
    expect(listItem).toHaveAttribute("value", "7");
    expect(screen.getByText("Rest the dough for an hour.")).toBeInTheDocument();
  });
});
