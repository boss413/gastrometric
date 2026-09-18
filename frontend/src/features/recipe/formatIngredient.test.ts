import { describe, expect, it } from "vitest";
import { formatIngredientAnnotation, formatIngredientQuantity } from "./formatIngredient";

describe("formatIngredientQuantity", () => {
  it("returns the author-facing quantity with grams appended in parentheses", () => {
    expect(formatIngredientQuantity("2 large, 1 cup", 150)).toBe("2 large, 1 cup (150g)");
  });

  it("returns the quantity as-is when grams is absent", () => {
    expect(formatIngredientQuantity("2 large", null)).toBe("2 large");
  });

  it("never omits the author-facing quantity in favor of grams", () => {
    const result = formatIngredientQuantity("1 cup", 240);
    expect(result).toContain("1 cup");
    expect(result?.startsWith("1 cup")).toBe(true);
  });

  it("falls back to grams alone only when no quantity was supplied at all", () => {
    expect(formatIngredientQuantity(null, 100)).toBe("100g");
  });

  it("returns null when neither quantity nor grams is present", () => {
    expect(formatIngredientQuantity(null, null)).toBeNull();
  });

  it("treats an empty-string quantity as absent", () => {
    expect(formatIngredientQuantity("", 50)).toBe("50g");
  });
});

describe("formatIngredientAnnotation", () => {
  it("combines optional and notes as 'Optional. <note>'", () => {
    expect(formatIngredientAnnotation(true, "For serving")).toBe("(Optional. For serving)");
  });

  it("shows just '(Optional)' when only optional is true", () => {
    expect(formatIngredientAnnotation(true, null)).toBe("(Optional)");
  });

  it("shows just the note in parentheses when only a note is present", () => {
    expect(formatIngredientAnnotation(false, "For serving")).toBe("(For serving)");
  });

  it("returns null when neither optional nor notes is present", () => {
    expect(formatIngredientAnnotation(false, null)).toBeNull();
  });
});
