import { defineConfig } from "@playwright/test";

/**
 * 真浏览器合约(Playwright)。
 *
 * 被测对象不是 Vite dev server,而是真实交付物:嵌入了 frontend/dist 的
 * `crypto-trading-web` 二进制。每个 spec 自己 spawn/kill 后端进程
 * (e2e/backend.ts),因此这里不配置 webServer,并串行执行以避免端口与
 * 进程生命周期互相干扰。
 *
 * 前置条件(见 README「Web 控制面」):
 *   1. pnpm build                        # 生成 frontend/dist
 *   2. cargo build -p crypto-trading-web-app --bin crypto-trading-web
 *      (在 rust/ 下;dist 会被 build.rs 嵌入)
 *   3. pnpm exec playwright install chromium
 * 二进制路径可用 CT_WEB_BIN 覆盖;默认 rust/target/debug/crypto-trading-web。
 *
 * CI 只在 ubuntu 上跑(frontend.yml 的 e2e job);本地 Windows 直接
 * `pnpm e2e` 即可。
 */
export default defineConfig({
  testDir: "./e2e",
  // 每个 spec 独占一个后端进程与端口;串行是契约的一部分。
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 120_000,
  expect: { timeout: 15_000 },
  reporter: process.env.CI ? [["list"], ["github"]] : [["list"]],
  use: {
    trace: "retain-on-failure",
  },
});
