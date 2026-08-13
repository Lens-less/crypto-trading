/**
 * 真浏览器合约 ①:只读模式下的权限脊柱、历史投影事实、通知徽标与
 * 浏览器存储纪律。
 *
 * 后端:嵌入 dist 的真实二进制 + rust/fixtures/web-api/journal.jsonl
 * (monitor / tasks / executions 均非空)。
 */
import { expect, test } from "@playwright/test";
import { startBackend, WEB_API_JOURNAL, type Backend } from "./backend";

const PORT = 8791;

/** 通知通道徽标允许出现的全部文案(NotificationChannelBadge 三态)。 */
const BADGE_LABELS = ["已连接 · 仅通知", "重连中", "通知不可用"];

let backend: Backend;

test.beforeAll(async () => {
  backend = await startBackend({
    port: PORT,
    historyPath: WEB_API_JOURNAL,
    extraArgs: ["--allow-open-loopback-read-api"],
  });
});

test.afterAll(async () => {
  await backend?.stop();
});

test("权限脊柱在 3 秒内可辨 PAPER 与后端声明的 live 授权,且服务的是嵌入 bundle", async ({
  page,
}) => {
  await page.goto(`${backend.baseUrl}/overview`);

  // 嵌入模式守卫:绝不允许 e2e 在占位 shell 上「通过」。
  await expect(page.locator("body")).not.toContainText("UI 资产未构建");

  // live-manual 姿态:manifest 声明 live 授权,但 Web 界面保持只读,
  // 浏览器永不构造 live 权限(权限态只来自 /api/v1/capabilities)。
  const authority = page.getByTestId("authority");
  await expect(authority).toContainText("PAPER");
  await expect(authority).toContainText("live 授权由后端声明");
  await expect(authority).toContainText("本界面仍为只读");
});

test("总览 monitor 卡呈现 recorded_at(记录时间)与 market generation", async ({
  page,
}) => {
  await page.goto(`${backend.baseUrl}/overview`);

  await expect(page.getByText("recorded_at(记录时间)")).toBeVisible();
  await expect(page.getByText("market generation", { exact: true })).toBeVisible();
  // 历史投影语义脚注:monitor 数据永不冒充实时行情。
  await expect(
    page.getByText(/不代表当前实时行情仍然新鲜/).first(),
  ).toBeVisible();
});

test("SSE 徽标文案始终属于三态集合,并最终达到「已连接 · 仅通知」", async ({
  page,
}) => {
  await page.goto(`${backend.baseUrl}/overview`);

  const badge = page.getByTestId("notification-channel");
  await expect(badge).toBeVisible();
  const label = badge.locator("span.inline-flex");
  await expect(label).toHaveText(new RegExp(`^(${BADGE_LABELS.join("|")})$`));
  await expect(label).toHaveText("已连接 · 仅通知", { timeout: 30_000 });
});

test("浏览器持久化存储的键集合 ⊆ {ct-theme}", async ({ page }) => {
  await page.goto(`${backend.baseUrl}/overview`);
  await expect(page.getByTestId("authority")).toContainText("PAPER");

  // 遍历数据页并切换主题,逼出所有可能的存储写入。
  for (const label of ["执行", "设置"]) {
    await page.getByRole("link", { name: label, exact: true }).click();
  }
  await page.getByRole("button", { name: /主题:/ }).click();

  const storageKeys = await page.evaluate(() => ({
    local: Object.keys(window.localStorage),
    session: Object.keys(window.sessionStorage),
  }));
  expect(storageKeys.local).toEqual(["ct-theme"]);
  expect(storageKeys.session).toEqual([]);

  // cookie 同样不承载任何业务态。
  expect(await page.context().cookies()).toEqual([]);
});
