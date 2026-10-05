#!/usr/bin/env bash
#
# gastrometric/scripts/verify-wo-2026-017.sh
#
# Verifies WO-2026-017 (Recipe Detail Empty Content Rendering) against the
# ACTUAL repository, not an isolated copy of the file. Run this from the
# repo checkout that a reviewer actually has on disk.
#
# What this does, and why:
#   1. Locates the real RecipeSectionBlock.tsx / .test.tsx in the repo tree
#      (does not assume a fixed path, since "the file change did not fix
#      the bug" may mean the patch landed somewhere other than expected,
#      or that a stale/duplicate copy exists elsewhere in the tree).
#   2. Statically greps the *actual on-disk* component for the guard
#      clause the fix depends on. If this fails, the running app is very
#      likely serving a build that does NOT contain the intended patch —
#      the most common reason a "fix" appears to do nothing (stale file,
#      wrong path, patch applied to a different component, build cache).
#   3. Checks for a second/duplicate RecipeSectionBlock implementation
#      elsewhere in the tree, or a parent component that also renders
#      section chrome (heading/table/list) independently — either of
#      which would explain "fixed the file, bug still visible."
#   4. Runs the actual test suite, lint, and build from package.json
#      (not simulated) and reports real pass/fail.
#   5. Exits non-zero if ANY check fails, so it can gate CI.
#
# Usage:
#   ./verify-wo-2026-017.sh [path-to-frontend-project-root]
#
# If no path is given, it searches upward/downward from the current
# directory for a directory containing both package.json and a
# RecipeSectionBlock.tsx under src/.

set -uo pipefail

FRONTEND_ROOT="${1:-}"
FAIL=0
WARN=0

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=1; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; WARN=1; }
section() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
section "0. Locate frontend project root"
# ---------------------------------------------------------------------------
if [[ -z "$FRONTEND_ROOT" ]]; then
  CANDIDATE=$(find . -maxdepth 6 -type f -name package.json \
    -exec grep -l '"vitest"' {} \; 2>/dev/null | head -n1)
  if [[ -n "$CANDIDATE" ]]; then
    FRONTEND_ROOT="$(dirname "$CANDIDATE")"
  fi
fi

if [[ -z "$FRONTEND_ROOT" || ! -f "$FRONTEND_ROOT/package.json" ]]; then
  fail "Could not locate a frontend project root automatically."
  echo "       Re-run as: ./verify-wo-2026-017.sh /path/to/frontend"
  exit 1
fi
pass "Using frontend root: $FRONTEND_ROOT"

# ---------------------------------------------------------------------------
section "1. Locate the component and test file on disk"
# ---------------------------------------------------------------------------
COMPONENT_MATCHES=$(find "$FRONTEND_ROOT/src" -type f -name 'RecipeSectionBlock.tsx' 2>/dev/null)
TEST_MATCHES=$(find "$FRONTEND_ROOT/src" -type f -name 'RecipeSectionBlock.test.tsx' 2>/dev/null)

COMPONENT_COUNT=$(echo "$COMPONENT_MATCHES" | grep -c . || true)
TEST_COUNT=$(echo "$TEST_MATCHES" | grep -c . || true)

if [[ "$COMPONENT_COUNT" -eq 0 ]]; then
  fail "No RecipeSectionBlock.tsx found under $FRONTEND_ROOT/src"
elif [[ "$COMPONENT_COUNT" -gt 1 ]]; then
  warn "Multiple RecipeSectionBlock.tsx files found — a stale/duplicate copy can make a fix appear not to work if the wrong one is imported:"
  echo "$COMPONENT_MATCHES" | sed 's/^/         /'
else
  pass "Found component: $COMPONENT_MATCHES"
fi

if [[ "$TEST_COUNT" -eq 0 ]]; then
  fail "No RecipeSectionBlock.test.tsx found under $FRONTEND_ROOT/src (WO-2026-017 requires this file to exist)"
else
  pass "Found test file: $TEST_MATCHES"
fi

# ---------------------------------------------------------------------------
section "2. Static check: does the on-disk component actually contain the guard?"
# ---------------------------------------------------------------------------
# This is the single most common reason a "correct-looking" patch does not
# fix the observed bug: the file on disk (or the file the bundler actually
# resolves) does not contain the change at all.
if [[ "$COMPONENT_COUNT" -ge 1 ]]; then
  while IFS= read -r COMPONENT_FILE; do
    [[ -z "$COMPONENT_FILE" ]] && continue
    echo "  Checking: $COMPONENT_FILE"

    if grep -Eq 'ingredients\.length\s*===\s*0.*instructions\.length\s*===\s*0|hasIngredients.*&&.*!hasInstructions|!hasIngredients\s*&&\s*!hasInstructions' "$COMPONENT_FILE"; then
      pass "    contains an empty-section guard condition"
    else
      fail "    does NOT contain an empty-section guard (ingredients.length===0 && instructions.length===0, or equivalent) — this file will render an empty section regardless of any patch you believe was applied"
    fi

    if grep -Eq 'return\s+null' "$COMPONENT_FILE"; then
      pass "    contains an early 'return null'"
    else
      fail "    does NOT contain 'return null' anywhere — if the guard exists but there's no early return, the empty section will still render"
    fi

    # Detect the specific bug this WO targets: table/list rendered
    # unconditionally on their OWN length, independent of the other
    # collection. If these gates are missing, ingredients-only or
    # instructions-only sections will still render an empty counterpart.
    if grep -Eq 'ingredients\.length\s*>\s*0' "$COMPONENT_FILE"; then
      pass "    ingredient table is gated on ingredients.length > 0"
    else
      fail "    ingredient table is NOT gated on ingredients.length > 0 — an empty ingredient table can still render"
    fi

    if grep -Eq 'instructions\.length\s*>\s*0' "$COMPONENT_FILE"; then
      pass "    instruction list is gated on instructions.length > 0"
    else
      fail "    instruction list is NOT gated on instructions.length > 0 — a stray '1.' can still render"
    fi
  done <<< "$COMPONENT_MATCHES"
fi

# ---------------------------------------------------------------------------
section "3. Check for a second render path (parent component doing its own gating)"
# ---------------------------------------------------------------------------
# If a parent (e.g. the recipe detail page) also decides section visibility,
# or wraps RecipeSectionBlock in its own heading/divider, the child fix can
# be entirely correct while the bug is still visible from a different file.
PARENT_HITS=$(grep -rl 'RecipeSectionBlock' "$FRONTEND_ROOT/src" 2>/dev/null | grep -v 'RecipeSectionBlock\.tsx$' | grep -v 'RecipeSectionBlock\.test\.tsx$')
if [[ -n "$PARENT_HITS" ]]; then
  echo "  Files that reference RecipeSectionBlock (inspect for independent section chrome):"
  echo "$PARENT_HITS" | sed 's/^/    /'
  for f in $PARENT_HITS; do
    if grep -Eq '<h2|recipe-ingredient-table|recipe-instructions' "$f"; then
      warn "    $f appears to render its own section heading/table/list markup — this can reproduce the bug independently of RecipeSectionBlock's fix"
    fi
  done
else
  pass "No other file renders section chrome independently of RecipeSectionBlock"
fi

# ---------------------------------------------------------------------------
section "4. Run the real test suite (this file's tests only)"
# ---------------------------------------------------------------------------
pushd "$FRONTEND_ROOT" > /dev/null || exit 1

if [[ -f package-lock.json || -f npm-shrinkwrap.json ]]; then
  INSTALL_CMD="npm ci"
else
  INSTALL_CMD="npm install"
fi

if [[ ! -d node_modules ]]; then
  echo "  Installing dependencies ($INSTALL_CMD)..."
  if ! $INSTALL_CMD > /tmp/wo-2026-017-install.log 2>&1; then
    fail "Dependency install failed — see /tmp/wo-2026-017-install.log"
  fi
fi

echo "  Running: npx vitest run --reporter=verbose -- RecipeSectionBlock"
if npx vitest run --reporter=verbose RecipeSectionBlock > /tmp/wo-2026-017-test.log 2>&1; then
  pass "vitest: RecipeSectionBlock tests passed"
else
  fail "vitest: RecipeSectionBlock tests FAILED — see /tmp/wo-2026-017-test.log"
fi
tail -n 40 /tmp/wo-2026-017-test.log | sed 's/^/    /'

# ---------------------------------------------------------------------------
section "5. Lint"
# ---------------------------------------------------------------------------
if npm run lint > /tmp/wo-2026-017-lint.log 2>&1; then
  pass "npm run lint passed"
else
  fail "npm run lint FAILED — see /tmp/wo-2026-017-lint.log"
  tail -n 20 /tmp/wo-2026-017-lint.log | sed 's/^/    /'
fi

# ---------------------------------------------------------------------------
section "6. Build"
# ---------------------------------------------------------------------------
if npm run build > /tmp/wo-2026-017-build.log 2>&1; then
  pass "npm run build passed"
else
  fail "npm run build FAILED — see /tmp/wo-2026-017-build.log"
  tail -n 30 /tmp/wo-2026-017-build.log | sed 's/^/    /'
fi

popd > /dev/null || exit 1

# ---------------------------------------------------------------------------
section "Summary"
# ---------------------------------------------------------------------------
if [[ "$FAIL" -eq 1 ]]; then
  echo "  One or more checks FAILED. See PASS/FAIL lines above and the logs in /tmp/wo-2026-017-*.log."
  echo "  If section 2 failed but section 4 passed, the repo's tests are exercising a"
  echo "  different file than the one actually bundled/served — check for duplicate"
  echo "  components, a stale build cache, or an import pointing at the wrong path."
  exit 1
elif [[ "$WARN" -eq 1 ]]; then
  echo "  All hard checks passed, but warnings above should be reviewed (duplicate files"
  echo "  or a parent component that may independently reproduce the bug)."
  exit 0
else
  echo "  All checks passed."
  exit 0
fi