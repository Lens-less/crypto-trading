// ESLint 9 flat config:JS 推荐规则 + typescript-eslint 推荐规则。
import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/", "node_modules/", "public/"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    // 供 CI 使用的 Node 供应链检查脚本:声明 Node 全局,避免 no-undef 误报。
    files: ["scripts/**/*.mjs"],
    languageOptions: {
      globals: {
        console: "readonly",
        process: "readonly",
        URL: "readonly",
      },
    },
  },
  {
    files: ["**/*.{ts,tsx}"],
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
    },
  },
);
