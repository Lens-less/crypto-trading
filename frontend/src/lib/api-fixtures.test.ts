/**
 * Fixture 交叉契约(前端半边)。
 *
 * 后端半边是 `rust/crates/web/tests/api_fixture_contract.rs`:它把每个只读
 * 端点在 `rust/fixtures/web-api/journal.jsonl` 上的响应逐字节锁进
 * `rust/fixtures/web-api/*.json`。本文件读取同一批快照,用 `api-types.ts`
 * 的 zod schema 全量解析:
 * - 后端改序列化 → Rust 侧字节比对红;
 * - 前端收紧 schema 使其不再接受后端字节 → 本文件红。
 * 双端由同一份签入文件钉死,schema 升版必然双端同红,直到一起更新。
 *
 * 快照再生成:`UPDATE_FIXTURES=1 cargo test -p crypto-trading-web
 * --test api_fixture_contract`,并与 schema 变更同一提交评审。
 */
import { describe, expect, it } from "vitest";
import {
  arbitrageMonitorReadModelSchema,
  capabilityManifestSchema,
  controlPlaneEventsPageSchema,
  executionsResponseSchema,
  readOnlyTaskReadModelSchema,
  riskResponseSchema,
  settingsResponseSchema,
  systemResponseSchema,
} from "./api-types";

// Vite 原生 glob 导入:无需 Node fs,jsdom 环境同样可用;
// 新快照文件(如 risk.json)出现即自动进入本契约。
const FIXTURES = import.meta.glob("../../../rust/fixtures/web-api/*.json", {
  eager: true,
  import: "default",
}) as Record<string, unknown>;

function readFixture(name: string): unknown {
  const key = `../../../rust/fixtures/web-api/${name}`;
  const fixture = FIXTURES[key];
  if (fixture === undefined) {
    throw new Error(
      `missing snapshot rust/fixtures/web-api/${name}; regenerate with ` +
        "UPDATE_FIXTURES=1 cargo test -p crypto-trading-web --test api_fixture_contract",
    );
  }
  return fixture;
}

describe("web-api fixture snapshots parse with the production zod schemas", () => {
  it("GET /api/v1/system → system.json", () => {
    const system = systemResponseSchema.parse(readFixture("system.json"));
    expect(system.projection_status).toBe("complete");
  });

  it("GET /api/v1/capabilities → capabilities.json", () => {
    const manifest = capabilityManifestSchema.parse(
      readFixture("capabilities.json"),
    );
    // live-manual:后端声明操作员监督的一次性 CLI lifecycle;
    // 浏览器侧仍然只读,永不构造 live 权限。
    expect(manifest.release_stage).toBe("live-manual");
    expect(manifest.live_trading_enabled).toBe(true);
  });

  it("GET /api/v1/monitor → monitor.json(latest 非空)", () => {
    const monitor = arbitrageMonitorReadModelSchema.parse(
      readFixture("monitor.json"),
    );
    expect(monitor.latest).not.toBeNull();
  });

  it("GET /api/v1/tasks → tasks.json(任务非空)", () => {
    const tasks = readOnlyTaskReadModelSchema.parse(readFixture("tasks.json"));
    expect(tasks.tasks.length).toBeGreaterThan(0);
  });

  it("GET /api/v1/settings → settings.json", () => {
    const settings = settingsResponseSchema.parse(readFixture("settings.json"));
    expect(settings.paper_principal_id).toBeNull();
  });

  it("GET /api/v1/executions → executions.json(批次与游标非空)", () => {
    const raw = readFixture("executions.json");
    const executions = executionsResponseSchema.parse(raw);
    expect(executions.operator.batches.length).toBeGreaterThan(0);
    // SSE 与 REST 共用同一 events-page 形态;changes 单独再过一次流 schema。
    const changes = controlPlaneEventsPageSchema.parse(
      (raw as { changes: unknown }).changes,
    );
    expect(changes.next_cursor).not.toBeNull();
  });

  it("GET /api/v1/risk → risk.json(账户敞口 + 账户风控均非空)", () => {
    const risk = riskResponseSchema.parse(readFixture("risk.json"));
    expect(risk.paper_accounts.projection_status).toBe("complete");
    expect(risk.paper_accounts.accounts.length).toBeGreaterThan(0);
    expect(risk.account_risk.projection_status).toBe("complete");
    expect(risk.account_risk.scopes.length).toBeGreaterThan(0);
  });
});
