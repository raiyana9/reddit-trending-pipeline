import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  server: { port: 5173 },
  build: {
    chunkSizeWarningLimit: 600,
    rollupOptions: {
      output: { manualChunks: { react: ["react", "react-dom"], recharts: ["recharts"] } },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
  },
});
