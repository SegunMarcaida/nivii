import { defineConfig } from "vite"
import vue from "@vitejs/plugin-vue"

export default defineConfig({
  plugins: [vue()],
  server: {
    host: "0.0.0.0",
    port: 3000,
    proxy: {
      "/query": {
        target: process.env.VITE_API_URL || "http://api:8000",
        changeOrigin: true,
      },
    },
  },
})
