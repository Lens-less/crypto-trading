/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, type Plugin } from "vite";

function sanitizeReactRouterEmbeddedUrls(): Plugin {
  return {
    name: "sanitize-react-router-embedded-urls",
    enforce: "pre",
    transform(code, id) {
      if (!/[\\/]react-router[\\/]/.test(id)) {
        return null;
      }

      let normalized = code;
      for (const [source, replacement] of [
        ['"http://localhost"', '"http://[::1]"'],
        [
          "https://reactrouter.com/en/main/routers/picking-a-router",
          "the React Router documentation",
        ],
        [
          "https://github.com/ungap/url-search-params",
          "the URLSearchParams polyfill documentation",
        ],
      ] as const) {
        normalized = normalized.replaceAll(source, replacement);
      }
      return normalized === code ? null : { code: normalized, map: null };
    },
  };
}

// 开发服务器只绑定 loopback,与后端(axum,127.0.0.1:8787)保持同样的
// 「永不暴露到局域网」姿态;/api 走代理,避免浏览器跨源与凭据泄漏。
export default defineConfig({
  plugins: [sanitizeReactRouterEmbeddedUrls(), react(), tailwindcss()],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: "dist",
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
