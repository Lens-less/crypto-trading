/**
 * 真浏览器合约 ③:受信 Paper 写路径(W4 人工验证流程的固化)。
 *
 * 后端以 --enable-paper-writes + replay-backed 网格 profile 启动,
 * journal 使用 m2 fixture 的临时副本(写模式会向 journal 追加事实)。
 * 流程:/settings 输入 bearer token(仅内存)→ SPA 导航到 /strategies
 * → start_paper_grid → durable receipt「已写入」→ 任务投影出现 →
 * stop_task 二次确认 → receipt「已写入」。
 */
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import {
  copyJournalToTemp,
  REPO_ROOT,
  startBackend,
  type Backend,
} from "./backend";

const PORT = 8793;
const TOKEN = "e2e-playwright-paper-writes-token-000001";
const TOKEN_ENV = "CT_E2E_WEB_TOKEN";

const GRID_TASK_ID = "paper-grid-owner";

let backend: Backend;

test.beforeAll(async () => {
  const historyPath = copyJournalToTemp(
    join(REPO_ROOT, "rust", "fixtures", "m2-operator-journal.jsonl"),
  );
  backend = await startBackend({
    port: PORT,
    historyPath,
    env: { [TOKEN_ENV]: TOKEN },
    extraArgs: [
      "--bearer-token-env",
      TOKEN_ENV,
      "--enable-paper-writes",
      "--paper-account-risk-config",
      join(REPO_ROOT, "rust", "config", "paper", "account-risk.example.yaml"),
      "--paper-grid-task-id",
      GRID_TASK_ID,
      "--paper-grid-strategy-id",
      "grid.strategy",
      "--paper-grid-strategy-revision",
      "grid.v1",
      "--paper-grid-config",
      join(REPO_ROOT, "rust", "config", "grid", "paper-once-btc.yaml"),
      "--paper-grid-replay",
      join(REPO_ROOT, "rust", "fixtures", "m4-grid-paper-replay.jsonl"),
    ],
  });
});

test.afterAll(async () => {
  await backend?.stop();
});

test("start_paper_grid → stop_task 全流程通过受信 submit 生效", async ({
  page,
}) => {
  // 1. /settings:令牌只进内存;应用后重建会话与通知流。
  await page.goto(`${backend.baseUrl}/settings`);
  await page.getByLabel("bearer token").fill(TOKEN);
  await page.getByRole("button", { name: "应用令牌并重建流" }).click();

  // 2. SPA 导航(整页刷新会丢内存令牌,这本身就是契约)。
  await page.getByRole("link", { name: "策略" }).click();
  await expect(
    page.getByRole("heading", { name: "只读连续任务明细" }),
  ).toBeVisible();

  const gridCard = page
    .locator("section")
    .filter({ has: page.getByRole("heading", { name: "网格 Paper 任务" }) })
    .last();
  await expect(gridCard).toBeVisible();

  // 写能力已探测:profile 预填自 /api/v1/settings。
  await expect(gridCard.getByLabel("任务 ID / task_id")).toHaveValue(
    GRID_TASK_ID,
  );

  // 3. paper_only 风险确认 + 启动网格。
  await gridCard.getByRole("checkbox").check();
  await gridCard.getByRole("button", { name: "启动网格" }).click();

  // durable receipt:applied → 状态 pill「已写入」,并回显 journal 证据来源。
  const appliedPill = gridCard
    .locator("span.inline-flex")
    .filter({ hasText: /^已写入$/ });
  await expect(appliedPill).toBeVisible({ timeout: 30_000 });
  await expect(gridCard.getByText("submit_command_v1")).toBeVisible();
  await expect(gridCard.getByText("durable_journal")).toBeVisible();

  // 4. 任务投影(GET /api/v1/tasks)出现该任务:结果只信 durable 投影。
  const taskTable = page.getByRole("region", {
    name: "只读连续任务明细,可横向滚动",
  });
  await expect(taskTable).toContainText(GRID_TASK_ID, { timeout: 30_000 });
  await expect(
    gridCard.getByText(/生效判定:已生效/),
  ).toBeVisible({ timeout: 30_000 });

  // 5. stop_task:干预必须二次确认,确认文案含任务身份。
  await gridCard.getByRole("button", { name: "停止任务" }).click();
  const confirm = gridCard.getByRole("alertdialog", { name: "确认任务干预" });
  await expect(confirm).toBeVisible();
  await expect(confirm).toContainText(GRID_TASK_ID);
  await confirm.getByRole("button", { name: "确认停止" }).click();

  // stop 的 durable receipt 同样为「已写入」,且确认框已关闭。
  await expect(confirm).toBeHidden();
  await expect(
    gridCard.locator("span.inline-flex").filter({ hasText: /^停止任务$/ }),
  ).toBeVisible({ timeout: 30_000 });
  await expect(appliedPill).toBeVisible({ timeout: 30_000 });
});
