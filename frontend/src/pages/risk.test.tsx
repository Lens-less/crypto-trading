// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type {
  AccountRiskStateView,
  PaperAccountReadModel,
  PaperAccountSnapshot,
  RiskResponse,
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

function paperModel(
  overrides: Partial<PaperAccountReadModel> = {},
): PaperAccountReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    projection_status: "complete",
    invalid_event_count: 0,
    accounts: [account()],
    ...overrides,
  };
}

function scope(overrides: Partial<AccountRiskStateView> = {}): AccountRiskStateView {
  return {
    schema_version: 1,
    scope_id: "paper",
    paused: false,
    pause_reason: null,
    kill_switch_engaged: false,
    kill_switch_reason: null,
    trade_date_utc: "2026-07-25",
    daily_trade_count: 3,
    open_positions: [
      {
        task_id: "paper-grid-btc",
        symbol: "BTC-USDC-PERP",
        opened_at: "2026-07-25T00:07:00Z",
      },
    ],
    admitted_count: 5,
    rejected_count: 2,
    last_rejection: "daily trade limit reached",
    last_recorded_at: "2026-07-25T00:09:00Z",
    ...overrides,
  };
}

function riskPayload(
  overrides: Partial<RiskResponse> = {},
): RiskResponse {
  return {
    schema_version: 1,
    paper_accounts: paperModel(),
    account_risk: {
      schema_version: 1,
      journal_id: "journal-a",
      projection_status: "complete",
      invalid_event_count: 0,
      scopes: [scope()],
    },
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

function stubRisk(payload: RiskResponse, withSystem = true): void {
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
    stubRisk(riskPayload());
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

  it("账户风控卡:UTC 交易日、当日计数、准入/拒绝计数与 last_rejection", async () => {
    stubRisk(riskPayload());
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(
        screen.getByRole("region", { name: "账户风控 scope paper" }),
      ).toBeTruthy();
    });
    expect(screen.getByText("2026-07-25")).toBeTruthy();
    expect(screen.getByText("3")).toBeTruthy(); // daily_trade_count
    expect(screen.getByText("5")).toBeTruthy(); // admitted_count
    expect(screen.getByText("2")).toBeTruthy(); // rejected_count
    expect(screen.getByText("daily trade limit reached")).toBeTruthy();
    // open positions 表
    expect(
      screen.getByRole("region", {
        name: "scope paper 的 open positions,可横向滚动",
      }),
    ).toBeTruthy();
    expect(screen.getByText("BTC-USDC-PERP")).toBeTruthy();
    // 未触发 / 未暂停:中性色,不声明健康
    expect(screen.getByText("未触发")).toBeTruthy();
    expect(screen.getByText("未暂停")).toBeTruthy();
    expect(screen.getByText("准入开放")).toBeTruthy();
  });

  it("kill switch engaged:safe-danger 且文字明示「闩锁,不可解除」与原因", async () => {
    stubRisk(
      riskPayload({
        account_risk: {
          schema_version: 1,
          journal_id: "journal-a",
          projection_status: "complete",
          invalid_event_count: 0,
          scopes: [
            scope({
              kill_switch_engaged: true,
              kill_switch_reason: "operator drill",
            }),
          ],
        },
      }),
    );
    renderWithQueryClient(<RiskPage />);
    // 总览行 + scope 卡都必须明示闩锁语义
    await waitFor(() => {
      expect(screen.getAllByText(/闩锁,不可解除/).length).toBeGreaterThanOrEqual(2);
    });
    for (const element of screen.getAllByText(/闩锁,不可解除/)) {
      expect(element.className).toContain("text-safe-danger");
    }
    expect(screen.getByText(/原因:operator drill/)).toBeTruthy();
    expect(screen.getByText("kill switch 已闩锁")).toBeTruthy();
    // 只读页永不提供解除入口
    expect(screen.queryByRole("button", { name: /解除/ })).toBeNull();
  });

  it("暂停状态:safe-warning 文字 + 原因,并说明存量持仓不受影响", async () => {
    stubRisk(
      riskPayload({
        account_risk: {
          schema_version: 1,
          journal_id: "journal-a",
          projection_status: "complete",
          invalid_event_count: 0,
          scopes: [
            scope({ paused: true, pause_reason: "exchange maintenance window" }),
          ],
        },
      }),
    );
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText(/拒绝新准入,存量持仓不受影响/)).toBeTruthy();
    });
    const paused = screen.getByText(/拒绝新准入/);
    expect(paused.textContent).toContain("已暂停");
    expect(paused.className).toContain("text-safe-warning");
    expect(screen.getByText(/原因:exchange maintenance window/)).toBeTruthy();
  });

  it("scopes 存在且未触发时,总览 kill switch 来自持久风控投影", async () => {
    stubRisk(riskPayload());
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText(/来自持久账户风控投影/)).toBeTruthy();
    });
    expect(screen.queryByText(/受监督前不声明健康/)).toBeNull();
  });

  it("scopes 为空时保留 W4 降级语义:system not_available 显式陈述", async () => {
    stubRisk(
      riskPayload({
        account_risk: {
          schema_version: 1,
          journal_id: "journal-a",
          projection_status: "complete",
          invalid_event_count: 0,
          scopes: [],
        },
      }),
    );
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText(/暂不可用/)).toBeTruthy();
    });
    expect(screen.getByText(/受监督前不声明健康/)).toBeTruthy();
    // 账户风控卡对空集合给一等空态,说明查过哪份事实
    expect(screen.getByText("尚未投影出账户风控事实。")).toBeTruthy();
    expect(
      screen.getByText(/account_risk\.scopes;空集合表示 journal 中没有可验证的/),
    ).toBeTruthy();
  });

  it("scopes 为空且 system 投影缺席时,kill switch 显示「当前投影未提供」一等状态", async () => {
    stubRisk(
      riskPayload({
        account_risk: {
          schema_version: 1,
          journal_id: "journal-a",
          projection_status: "complete",
          invalid_event_count: 0,
          scopes: [],
        },
      }),
      false,
    );
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("当前投影未提供")).toBeTruthy();
    });
  });

  it("degraded 投影:danger 横幅置于数字上方", async () => {
    stubRisk(
      riskPayload({ paper_accounts: paperModel({ projection_status: "degraded" }) }),
    );
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      const alerts = screen.getAllByRole("alert");
      expect(
        alerts.some((alert) =>
          (alert.textContent ?? "").includes("Paper 账户投影已降级"),
        ),
      ).toBe(true);
    });
  });

  it("账户风控投影降级:独立 danger 横幅,状态不被解释为当前风控状态", async () => {
    stubRisk(
      riskPayload({
        account_risk: {
          schema_version: 1,
          journal_id: "journal-a",
          projection_status: "degraded",
          invalid_event_count: 1,
          scopes: [scope()],
        },
      }),
    );
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      const alerts = screen.getAllByRole("alert");
      expect(
        alerts.some((alert) =>
          (alert.textContent ?? "").includes("账户风控投影已降级"),
        ),
      ).toBe(true);
    });
    expect(screen.getByText(/1 条 account_risk 事件未通过校验/)).toBeTruthy();
  });

  it("空态:没有账户时说明查过 /api/v1/risk", async () => {
    stubRisk(riskPayload({ paper_accounts: paperModel({ accounts: [] }) }));
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("尚未投影出 Paper 账户。")).toBeTruthy();
    });
    expect(
      screen.getByText(/journal 中没有可验证的 paper_account 事实/),
    ).toBeTruthy();
  });

  it("对账记录:released 展示结论与证据序号", async () => {
    const payload = riskPayload();
    payload.paper_accounts.accounts[0]!.reservations[0]!.reconciliation = {
      outcome: "released",
      proof: {},
      evidence_sequence: 42,
    };
    stubRisk(payload);
    renderWithQueryClient(<RiskPage />);
    await waitFor(() => {
      expect(screen.getByText("已释放 · 证据 #42")).toBeTruthy();
    });
  });
});
