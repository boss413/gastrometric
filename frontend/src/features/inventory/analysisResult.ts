/**
 * `analysis_result` (JSON stored in `inventory_items.analysis_result_json`)
 * has a real, confirmed shape — this module models it as a proper
 * discriminated schema and parses it structurally, rather than scanning
 * for conventionally-named fields.
 *
 * Confirmed by real backend rows (a resolved, single-word case):
 *
 *   {
 *     "status": "resolved",
 *     "interpretations": [
 *       {
 *         "id": "interp_1",
 *         "status": "resolved",
 *         "score": 1.0,
 *         "references": [
 *           {
 *             "id": "r1",
 *             "ingredient": { "id": "black pepper", "original_text": "pepper" },
 *             "source_spans": ["pepper"]
 *           }
 *         ],
 *         "evidence": [
 *           { "kind": "exact_ingredient_match", "record_id": "black pepper", "effect": "supporting" }
 *         ]
 *       }
 *     ],
 *     "selected_interpretation": "interp_1"
 *   }
 *
 * NOT yet confirmed by a real sample (no ambiguous/partial/multi-word
 * payload was available at implementation time): `modifiers` on
 * `ingredient`, top-level/interpretation `notes`, interpretation
 * `quantity`, and top-level `unresolved`. These are modeled as optional
 * fields per the FE-03 work order's field list, but their exact placement
 * is a documented assumption — see the completion report's "Backend
 * contract gaps" section. Everything here tolerates their absence.
 */

// ---- Raw schema (shape of the parsed JSON, as confirmed/assumed above) ----

export type InterpretationStatus =
  | "resolved"
  | "ambiguous"
  | "partially_resolved"
  | "unresolved"
  | (string & {});

export interface AnalyzedIngredientRef {
  id: string;
  original_text: string;
  /** Unconfirmed placement — see module doc comment. */
  modifiers?: string[];
}

export interface InterpretationReference {
  id: string;
  ingredient: AnalyzedIngredientRef;
  source_spans: string[];
}

export interface InterpretationEvidence {
  kind: string;
  record_id: string;
  effect: string;
}

export interface Interpretation {
  id: string;
  status: InterpretationStatus;
  score: number;
  references: InterpretationReference[];
  evidence: InterpretationEvidence[];
  /** Unconfirmed placement — see module doc comment. */
  quantity?: string | null;
  /** Unconfirmed placement — see module doc comment. */
  notes?: string[];
}

export interface AnalysisResult {
  status: InterpretationStatus;
  interpretations: Interpretation[];
  selected_interpretation: string | null;
  /** Unconfirmed placement — see module doc comment. Leftover unmatched tokens/spans. */
  unresolved?: string[];
  /** Unconfirmed placement — see module doc comment. */
  notes?: string[];
}

// ---- Structural parsing (validates shape; never invents values) ----

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((v): v is string => typeof v === "string");
}

function parseIngredientRef(value: unknown): AnalyzedIngredientRef | null {
  if (!isRecord(value)) return null;
  if (typeof value.id !== "string" || typeof value.original_text !== "string") return null;
  const modifiers = parseStringArray(value.modifiers);
  return {
    id: value.id,
    original_text: value.original_text,
    ...(modifiers.length > 0 ? { modifiers } : {}),
  };
}

function parseReference(value: unknown): InterpretationReference | null {
  if (!isRecord(value)) return null;
  if (typeof value.id !== "string") return null;
  const ingredient = parseIngredientRef(value.ingredient);
  if (!ingredient) return null;
  return {
    id: value.id,
    ingredient,
    source_spans: parseStringArray(value.source_spans),
  };
}

function parseEvidence(value: unknown): InterpretationEvidence | null {
  if (!isRecord(value)) return null;
  if (
    typeof value.kind !== "string" ||
    typeof value.record_id !== "string" ||
    typeof value.effect !== "string"
  ) {
    return null;
  }
  return { kind: value.kind, record_id: value.record_id, effect: value.effect };
}

function parseInterpretation(value: unknown): Interpretation | null {
  if (!isRecord(value)) return null;
  if (typeof value.id !== "string" || typeof value.status !== "string") return null;

  const references = Array.isArray(value.references)
    ? value.references.map(parseReference).filter((r): r is InterpretationReference => r !== null)
    : [];
  const evidence = Array.isArray(value.evidence)
    ? value.evidence.map(parseEvidence).filter((e): e is InterpretationEvidence => e !== null)
    : [];
  const notes = parseStringArray(value.notes);

  return {
    id: value.id,
    status: value.status,
    score: typeof value.score === "number" ? value.score : 0,
    references,
    evidence,
    ...(typeof value.quantity === "string" ? { quantity: value.quantity } : {}),
    ...(notes.length > 0 ? { notes } : {}),
  };
}

/**
 * Parses the raw stored JSON into the typed `AnalysisResult` schema.
 * Returns `null` if the top-level shape doesn't match at all (e.g. the
 * item predates this schema, or a genuinely different structure) rather
 * than fabricating a partial result.
 */
export function parseAnalysisResult(raw: Record<string, unknown> | null): AnalysisResult | null {
  if (!raw) return null;
  if (typeof raw.status !== "string" || !Array.isArray(raw.interpretations)) return null;

  const interpretations = raw.interpretations
    .map(parseInterpretation)
    .filter((i): i is Interpretation => i !== null);

  const selectedInterpretation =
    typeof raw.selected_interpretation === "string" ? raw.selected_interpretation : null;

  const unresolved = parseStringArray(raw.unresolved);
  const notes = parseStringArray(raw.notes);

  return {
    status: raw.status,
    interpretations,
    selected_interpretation: selectedInterpretation,
    ...(unresolved.length > 0 ? { unresolved } : {}),
    ...(notes.length > 0 ? { notes } : {}),
  };
}

// ---- Presentation view model (what components actually render) ----

export interface InterpretationCandidateView {
  id: string;
  /** Canonical ingredient identity — the label the user would resubmit if they pick this candidate. */
  label: string;
  status: InterpretationStatus;
  score: number;
  isSelected: boolean;
}

export interface InventoryInterpretationViewModel {
  status: InterpretationStatus;
  /** Only set when the backend's top-level status is "resolved". Never inferred otherwise. */
  resolvedLabel: string | null;
  /** Alternate interpretations. Populated whenever more than one exists, or the sole one isn't resolved. */
  candidates: InterpretationCandidateView[];
  unresolvedTokens: string[];
  notes: string[];
}

const EMPTY_VIEW: InventoryInterpretationViewModel = {
  status: "unresolved",
  resolvedLabel: null,
  candidates: [],
  unresolvedTokens: [],
  notes: [],
};

function labelFor(interpretation: Interpretation): string {
  const ids = interpretation.references.map((r) => r.ingredient.id);
  const unique = Array.from(new Set(ids));
  return unique.join(", ");
}

/**
 * Builds the view model a component actually renders from. Structural
 * parsing happens here (via `parseAnalysisResult`); components never see
 * the raw JSON.
 */
export function buildInterpretationViewModel(
  rawAnalysisResult: Record<string, unknown> | null,
): InventoryInterpretationViewModel {
  const parsed = parseAnalysisResult(rawAnalysisResult);
  if (!parsed) return EMPTY_VIEW;

  const selected = parsed.interpretations.find((i) => i.id === parsed.selected_interpretation);

  const resolvedLabel =
    parsed.status === "resolved" && selected ? labelFor(selected) || null : null;

  // Alternates are useful whenever the item isn't cleanly resolved to a
  // single label — ambiguous (multiple interpretations) or
  // partially_resolved (one interpretation, not yet confirmed).
  const candidates: InterpretationCandidateView[] =
    resolvedLabel === null
      ? parsed.interpretations
          .map((interpretation) => {
            const label = labelFor(interpretation);
            if (!label) return null;
            const candidate: InterpretationCandidateView = {
              id: interpretation.id,
              label,
              status: interpretation.status,
              score: interpretation.score,
              isSelected: interpretation.id === parsed.selected_interpretation,
            };
            return candidate;
          })
          .filter((c): c is InterpretationCandidateView => c !== null)
      : [];

  return {
    status: parsed.status,
    resolvedLabel,
    candidates,
    unresolvedTokens: parsed.unresolved ?? [],
    notes: parsed.notes ?? selected?.notes ?? [],
  };
}
