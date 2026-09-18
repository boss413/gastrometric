/**
 * Bolds the submitted query text within a recipe name for name-type
 * search results.
 *
 * ASSUMPTION (flagged in the FE-06 completion report): the work order
 * says to bold "the keyword match" using "backend-provided search
 * metadata," but no such metadata field (e.g. match offsets) exists on
 * the current `RecipeSearchResult` contract, and no updated sample
 * payload was supplied. This highlights a literal, case-insensitive
 * occurrence of the exact query the user submitted — a deterministic
 * text operation using data already on the client, not semantic
 * matching. It is only ever applied to the name the backend itself
 * already decided matched, never used to filter/rank/decide relevance.
 */
export interface TextSegment {
  text: string;
  bold: boolean;
}

export function highlightQuery(text: string, query: string): TextSegment[] {
  const trimmedQuery = query.trim();
  if (!trimmedQuery) {
    return [{ text, bold: false }];
  }

  const lowerText = text.toLowerCase();
  const lowerQuery = trimmedQuery.toLowerCase();

  const firstMatch = lowerText.indexOf(lowerQuery);
  if (firstMatch === -1) {
    return [{ text, bold: false }];
  }

  const segments: TextSegment[] = [];
  let cursor = 0;
  let matchIndex = firstMatch;

  while (matchIndex !== -1) {
    if (matchIndex > cursor) {
      segments.push({ text: text.slice(cursor, matchIndex), bold: false });
    }
    segments.push({ text: text.slice(matchIndex, matchIndex + trimmedQuery.length), bold: true });
    cursor = matchIndex + trimmedQuery.length;
    matchIndex = lowerText.indexOf(lowerQuery, cursor);
  }

  if (cursor < text.length) {
    segments.push({ text: text.slice(cursor), bold: false });
  }

  return segments;
}
