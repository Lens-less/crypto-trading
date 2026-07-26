/**
 * 真浏览器合约 ②:通知通道降级与自动恢复。
 *
 * 杀掉后端 → 徽标经「重连中」降级为「通知不可用」(连续失败 ≥3);
 * 在同一端口重启后端 → 指数退避的下一次重连成功,徽标自动恢复为
 * 「已连接 · 仅通知」,全程无需刷新页面。
 */
import { expect, test } from "@playwright/test";
import { startBackend, WEB_API_JOURNAL, type Backend } from "./backend";

const PORT = 8792;

let backend: Backend;

test.afterEach(async () => {
  await backend?.stop();
});

test("后端消失徽标降级,重启后自动恢复", async ({ page }) => {
  backend = await startBackend({ port: PORT, historyPath: WEB_API_JOURNAL });
  await page.goto(`${backend.baseUrl}/overview`);

  const label = page
    .getByTestId("notification-channel")
    .locator("span.inline-flex");
  await expect(label).toHaveText("已连接 · 仅通知", { timeout: 30_000 });

  // 后端消失:三次连续重连失败后降级为「通知不可用」。
  await backend.stop();
  await expect(label).toHaveText("通知不可用", { timeout: 45_000 });

  // 同端口重启:下一次退避重连自动恢复,页面不刷新。
  backend = await startBackend({ port: PORT, historyPath: WEB_API_JOURNAL });
  await expect(label).toHaveText("已连接 · 仅通知", { timeout: 90_000 });
});
