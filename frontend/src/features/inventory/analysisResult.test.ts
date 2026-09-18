import { describe, expect, it } from "vitest";
import { buildInterpretationViewModel, parseAnalysisResult } from "./analysisResult";

// Real payload confirmed from a backend inventory row (id=5, "pepper" -> black pepper).
const RESOLVED_SAMPLE = {
  status: "resolved",
  interpretations: [
    {
      id: "interp_1",
      status: "resolved",
      score: 1.0,
      references: [
        {
          id: "r1",
          ingredient: { id: "black pepper", original_text: "pepper" },
          source_spans: ["pepper"],
        },
      ],
      evidence: [
        { kind: "exact_ingredient_match", record_id: "black pepper", effect: "supporting" },
      ],
    },
  ],
  selected_interpretation: "interp_1",
};

describe("parseAnalysisResult", () => {
  it("parses the confirmed resolved shape structurally", () => {
    const parsed = parseAnalysisResult(RESOLVED_SAMPLE);
    expect(parsed).not.toBeNull();
    expect(parsed?.status).toBe("resolved");
    expect(parsed?.selected_interpretation).toBe("interp_1");
    expect(parsed?.interpretations).toHaveLength(1);
    expect(parsed?.interpretations[0].references[0].ingredient).toEqual({
      id: "black pepper",
      original_text: "pepper",
    });
    expect(parsed?.interpretations[0].evidence[0]).toEqual({
      kind: "exact_ingredient_match",
      record_id: "black pepper",
      effect: "supporting",
    });
  });

  it("returns null for null input rather than fabricating a result", () => {
    expect(parseAnalysisResult(null)).toBeNull();
  });

  it("returns null when the top-level shape doesn't match at all", () => {
    expect(parseAnalysisResult({ some_other_shape: true })).toBeNull();
  });

  it("drops malformed interpretations rather than throwing", () => {
    const parsed = parseAnalysisResult({
      status: "resolved",
      interpretations: [{ not_a_valid_interpretation: true }],
      selected_interpretation: null,
    });
    expect(parsed?.interpretations).toEqual([]);
  });

  it("parses modifiers on the ingredient reference when present", () => {
    const parsed = parseAnalysisResult({
      status: "resolved",
      interpretations: [
        {
          id: "interp_1",
          status: "resolved",
          score: 1.0,
          references: [
            {
              id: "r1",
              ingredient: {
                id: "bell pepper",
                original_text: "diced red bell peppers",
                modifiers: ["diced", "red"],
              },
              source_spans: ["diced red bell peppers"],
            },
          ],
          evidence: [],
        },
      ],
      selected_interpretation: "interp_1",
    });
    expect(parsed?.interpretations[0].references[0].ingredient.modifiers).toEqual([
      "diced",
      "red",
    ]);
  });
});

describe("buildInterpretationViewModel", () => {
  it("surfaces the resolved label from the selected interpretation", () => {
    const view = buildInterpretationViewModel(RESOLVED_SAMPLE);
    expect(view.resolvedLabel).toBe("black pepper");
    expect(view.candidates).toEqual([]);
  });

  it("returns an empty, non-resolved view for null analysis_result", () => {
    const view = buildInterpretationViewModel(null);
    expect(view.resolvedLabel).toBeNull();
    expect(view.candidates).toEqual([]);
    expect(view.unresolvedTokens).toEqual([]);
    expect(view.notes).toEqual([]);
  });

  it("never reports a resolved label unless top-level status is resolved", () => {
    const ambiguous = {
      status: "ambiguous",
      interpretations: [
        {
          id: "interp_1",
          status: "ambiguous",
          score: 0.6,
          references: [
            { id: "r1", ingredient: { id: "bell pepper", original_text: "pepper" }, source_spans: ["pepper"] },
          ],
          evidence: [],
        },
        {
          id: "interp_2",
          status: "ambiguous",
          score: 0.4,
          references: [
            { id: "r1", ingredient: { id: "black pepper", original_text: "pepper" }, source_spans: ["pepper"] },
          ],
          evidence: [],
        },
      ],
      selected_interpretation: "interp_1",
    };

    const view = buildInterpretationViewModel(ambiguous);
    expect(view.resolvedLabel).toBeNull();
    expect(view.candidates).toHaveLength(2);
    expect(view.candidates.map((c) => c.label)).toEqual(["bell pepper", "black pepper"]);
    expect(view.candidates.find((c) => c.id === "interp_1")?.isSelected).toBe(true);
    expect(view.candidates.find((c) => c.id === "interp_2")?.isSelected).toBe(false);
  });

  it("surfaces unresolved tokens without presenting the item as resolved", () => {
    const partial = {
      status: "partially_resolved",
      interpretations: [
        {
          id: "interp_1",
          status: "partially_resolved",
          score: 0.5,
          references: [
            { id: "r1", ingredient: { id: "onion", original_text: "onion" }, source_spans: ["onion"] },
          ],
          evidence: [],
        },
      ],
      selected_interpretation: "interp_1",
      unresolved: ["xyzzy"],
    };

    const view = buildInterpretationViewModel(partial);
    expect(view.resolvedLabel).toBeNull();
    expect(view.unresolvedTokens).toEqual(["xyzzy"]);
  });

  it("surfaces top-level notes when present", () => {
    const withNotes = {
      ...RESOLVED_SAMPLE,
      notes: ["user abbreviation expanded"],
    };
    const view = buildInterpretationViewModel(withNotes);
    expect(view.notes).toEqual(["user abbreviation expanded"]);
  });
});
