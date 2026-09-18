import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Listen on all interfaces so the app is reachable from a phone on the
    // same LAN as the developer's machine, not just localhost.
    host: true,
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
  },
})
