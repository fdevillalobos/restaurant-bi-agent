import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    globals: true
  },
  build: {
    chunkSizeWarningLimit: 1300,
    rollupOptions: {
      output: {
        manualChunks: {
          charts: ["echarts", "echarts-for-react"],
          assistant: ["@assistant-ui/react"],
          vendor: ["react", "react-dom", "lucide-react"]
        }
      }
    }
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8000"
    }
  }
});
