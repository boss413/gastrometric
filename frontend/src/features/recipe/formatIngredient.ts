/**
 * Composes the quantity column's display text.
 *
 * The backend model (FE-05 work order §3) exposes a single author-facing
 * `quantity` string (already ordered: natural portion/package, then
 * imperial — the backend's concern, not ours) plus a separate `grams`
 * value. Per §9/§14, grams is supplementary metric information appended
 * in parentheses; it never replaces or is displayed instead of the
 * author-facing quantity.
 *
 * ASSUMPTION (flagged in the completion report): the backend contract
 * available at implementation time only exposes one `quantity` string —
 * there are no separate natural-portion/package/imperial fields to
 * compose ourselves. If a future backend revision splits `quantity` into
 * those parts, this function's ordering logic would need to move up a
 * level to compose them; today it only ever concatenates the given
 * `quantity` string with `grams`.
 */
export function formatIngredientQuantity(
  quantity: string | null,
  grams: number | null,
): string | null {
  const hasQuantity = quantity !== null && quantity.trim().length > 0;
  const hasGrams = grams !== null;

  if (hasQuantity && hasGrams) {
    return `${quantity} (${grams}g)`;
  }
  if (hasQuantity) {
    return quantity;
  }
  if (hasGrams) {
    // No author-facing quantity was supplied at all — showing grams alone
    // isn't "replacing" a quantity that doesn't exist, and dropping real
    // backend data silently would be worse.
    return `${grams}g`;
  }
  return null;
}

/**
 * Composes the optional/notes parenthetical shown beneath an ingredient's
 * name (work order §10):
 *   both      -> "(Optional. <note>)"
 *   optional only -> "(Optional)"
 *   note only -> "(<note>)"
 *   neither   -> null (render nothing)
 */
export function formatIngredientAnnotation(
  optional: boolean,
  notes: string | null,
): string | null {
  const trimmedNotes = notes?.trim() || null;

  if (optional && trimmedNotes) {
    return `(Optional. ${trimmedNotes})`;
  }
  if (optional) {
    return "(Optional)";
  }
  if (trimmedNotes) {
    return `(${trimmedNotes})`;
  }
  return null;
}
