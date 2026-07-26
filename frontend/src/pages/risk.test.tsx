// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type {
  PaperAccountReadModel,
  PaperAccountSnapshot,
  SystemResponse,
} from "../lib/api-types";
import { jsonResponse, renderWithQueryClient, routedFetch } from "./pageTestUtils";
import { Component as RiskPage } from "./risk";

function account(overrides: Partial<PaperAccountSnapshot> = {}): PaperAccountSnapshot {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    projection_status: "complete",
    invalid_event_count: 0,
    account_id: "paper-usdc",
    initial_available: "10000",
    available: "9000.5",
    pending_reserved: "500.25",
    uncertain_reserved: "100",
    committed_exposure: "399.25",
    reservations: [
      {
        reservation_id: "11111111-1111-4111-8111-111111111111",
        task_id: "paper-grid-btc",
        idempotency_key: "paper-key-1",
        batch_id: "22222222-2222-4222-8222-222222222222",
        cost_model: { version: 1, fee_bps: 10, funding_buffer_bps: 5, slippage_bps: 5 },
        legs: [],
        reserved_exposure: "500.25",
        held_exposure: "500.25",
        phase: "pending",
        first_sequence: 3,
        last_sequence: 4,
        reconciliation: null,
      },
    ],
    ...overrides,
  };
}

function model(overrides: Partial<PaperAccountReadModel> = {}): PaperAccountReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    projection_status: "complete",
    invalid_event_count: 0,
    accounts: [account()],
    ...overrides,
  };
}

const SYSTEM: SystemResponse = {
  schema_version: 1,
  product_version: "0.1.0",
  release_stage: "paper-only",
  live_trading_enabled: false,
  access_scope: "loopback",
  authentication_required: true,
  projection_status: "complete",
  journal_id: "journal-a",
  head_sequence: 10,
  execution_batch_count: 0,
  recovery_required_count: 0,
  conflict_count: 0,
  warning_count: 0,
  truncation: { batches: false, warnings: false },
  kill_switch: "not_available",
  market_data_freshness: "not_available",
  adapter_health: "not_available",
};

function stubRisk(payload: PaperAccountReadModel, withSystem = true): void {
  vi.stubGlobal(
    "fetch",
    routedFetch({
      "/api/v1/risk": () => jsonResponse(payload),
      ...(withSystem ? { "/api/v1/system": () => jsonResponse(SYSTEM) } : {}),
    }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("/risk", () => {
  it("完整视图:账户字段、精确聚合与预留明细(阶段用专用文案)", async () => {
    stubRisk(model());
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getAllByText("paper-usdc").length).toBeGreaterThan(0);
    });
    // 账户表列全字段
    expect(screen.getByText("9000.5")).toBeTruthy();
    expect(screen.getByText("10000")).toBeTruthy();
    // 聚合:500.25 + 100 + 399.25 = 999.5(BigInt 十进制求和,非浮点)
    expect(screen.getByText("999.5")).toBeTruthy();
    // 预留明细:阶段 pending 用「待处理」而不是预警的「最后记录:未决」
    expect(screen.getByText("待处理")).toBeTruthy();
    expect(screen.queryByText("最后记录:未决")).toBeNull();
    expect(screen.getByText("未对账")).toBeTruthy();
    expect(
      screen.getByRole("region", { name: "Paper 预留明细,可横向滚动" }),
    ).toBeTruthy();
  });

  it("kill switch 展示位:system 提供 not_available 时显式陈述,不造假数据", async () => {
    stubRisk(model());
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText(/暂不可用/)).toBeTruthy();
    });
    expect(screen.getByText(/受监督前不声明健康/)).toBeTruthy();
  });

  it("system 投影缺席时 kill switch 显示「当前投影未提供」一等状态", async () => {
    stubRisk(model(), false);
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("当前投影未提供")).toBeTruthy();
    });
  });

  it("degraded 投影:danger 横幅置于数字上方", async () => {
    stubRisk(model({ projection_status: "degraded" }));
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toContain(
        "Paper 账户投影已降级",
      );
    });
  });

  it("空态:没有账户时说明查过 /api/v1/risk", async () => {
    stubRisk(model({ accounts: [] }));
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("尚未投影出 Paper 账户。")).toBeTruthy();
    });
    expect(
      screen.getByText(/journal 中没有可验证的 paper_account 事实/),
    ).toBeTruthy();
  });

  it("对账记录:released 展示结论与证据序号", async () => {
    const withReconciliation = model();
    withReconciliation.accounts[0]!.reservations[0]!.reconciliation = {
      outcome: "released",
      proof: {},
      evidence_sequence: 42,
    };
    stubRisk(withReconciliation);
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("已释放 · 证据 #42")).toBeTruthy();
    });
  });
});
