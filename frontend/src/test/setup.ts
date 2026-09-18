import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";
import "@testing-library/jest-dom/vitest";

// Testing Library doesn't auto-unmount between tests unless the test
// runner's global `afterEach` is wired up; do it once here for every file.
afterEach(() => {
  cleanup();
});
