import { describe, expect, it } from "vitest";
import { highlightQuery } from "./highlightQuery";

describe("highlightQuery", () => {
  it("bolds a single case-insensitive occurrence", () => {
    expect(highlightQuery("Avacado Chicken Salad", "chicken")).toEqual([
      { text: "Avacado ", bold: false },
      { text: "Chicken", bold: true },
      { text: " Salad", bold: false },
    ]);
  });

  it("bolds multiple occurrences", () => {
    expect(highlightQuery("Chicken Chicken Bake", "chicken")).toEqual([
      { text: "Chicken", bold: true },
      { text: " ", bold: false },
      { text: "Chicken", bold: true },
      { text: " Bake", bold: false },
    ]);
  });

  it("returns the whole text unbolded when the query does not appear", () => {
    expect(highlightQuery("Tomato Sauce", "chicken")).toEqual([
      { text: "Tomato Sauce", bold: false },
    ]);
  });

  it("returns the whole text unbolded when the query is blank", () => {
    expect(highlightQuery("Tomato Sauce", "  ")).toEqual([{ text: "Tomato Sauce", bold: false }]);
  });

  it("handles a match at the very start and very end of the text", () => {
    expect(highlightQuery("cake", "cake")).toEqual([{ text: "cake", bold: true }]);
  });
});
