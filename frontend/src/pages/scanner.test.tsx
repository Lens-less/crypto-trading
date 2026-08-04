// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type {
  VirtualGridScannerReadModel,
  VirtualGridScanRowView,
} from "../lib/api-types";
import { jsonResponse, renderWithQueryClient, routedFetch } from "./pageTestUtils";
import { Component as ScannerPage } from "./scanner";

function row(overrides: Partial<VirtualGridScanRowView> = {}): VirtualGridScanRowView {
  return {
    rank: 1,
    priority: "benchmark",
    instrument: { exchange: "binance", symbol: "BTCUSDT", market_type: "spot" },
    started_at: "2026-07-25T00:00:00Z",
    last_observed_at: "2026-07-26T00:00:00Z",
    observation_count: 120,
    last_observation_sequence: 88,
    current_price: "65000.5",
    lower_price: "60000",
    upper_price: "70000",
    pending_buy_price: "64000",
    pending_sell_price: "66000",
    grid_width_percent: "16.6",
    grid_interval_percent: "0.8",
    grid_count: 20,
    running_seconds: 86400,
    buy_crosses: 12,
    sell_crosses: 11,
    total_crosses: 23,
    complete_cycles: 11,
    recent_five_minute_cycles: 1,
    cycles_per_hour: "0.45",
    estimated_apr: "12.5",
    estimated_apr_kind: "heuristic",
    volume_24h_usdc: "1000000",
    rating_grade: "s",
    rating_score: "91.2",
    price_change_24h_percent: null,
    ...overrides,
  };
}

function model(
  overrides: Partial<VirtualGridScannerReadModel> = {},
): VirtualGridScannerReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 9,
    projection_status: "complete",
    latest: {
      source_sequence: 9,
      event_id: "00000000-0000-4000-8000-000000000009",
      recorded_at: "2026-07-26T00:00:00Z",
      run_id: "run-7",
      ranking_policy: "explicit_benchmark_then_apr_desc",
      apr_window_seconds: 3600,
      estimated_apr_kind: "heuristic",
      estimated_apr_assumptions: {
        order_notional_usdc: "100",
        round_trip_fee_percent: "0.2",
      },
      min_complete_cycles: 2,
      row_limit: 128,
      candidate_count: 4,
      eligible_count: 2,
      filtered_by_cycles_count: 2,
      truncated: false,
      rows: [
        row(),
        row({
          rank: 2,
          priority: "standard",
          rating_grade: "d",
          estimated_apr: "3.1",
          instrument: { exchange: "okx", symbol: "ETHUSDT", market_type: "spot" },
        }),
      ],
    },
    invalid_event_count: 0,
    ...overrides,
  };
}

function stubScanner(payload: VirtualGridScannerReadModel): void {
  vi.stubGlobal(
    "fetch",
    routedFetch({ "/api/v1/scanner": () => jsonResponse(payload) }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("/scanner", () => {
  it("渲染排行、APR 假设和回放免责声明，不把指标当成实盘信号", async () => {
    stubScanner(model());
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(screen.getByText("BTCUSDT")).toBeTruthy();
    });
    expect(
      screen.getByRole("region", { name: "确定性虚拟网格排行明细，可横向滚动" }),
    ).toBeTruthy();
    const apr = screen.getByText("12.5%");
    expect(apr.className).toContain("numeric");
    expect(
      screen.getByText(/benchmark 只是展示优先级，不是评分加成/),
    ).toBeTruthy();
    expect(screen.getByText("启发式估算 / Heuristic estimate")).toBeTruthy();
    expect(screen.getByText(/每格名义 100 USDC · 往返费率 0.2%/)).toBeTruthy();
    expect(
      screen.getByRole("columnheader", { name: "APR 估算 / Rating" }),
    ).toBeTruthy();
    expect(screen.getByText(/APR 估算不代表可交易收益/)).toBeTruthy();
    expect(screen.getByText(/不构成投资建议/)).toBeTruthy();
  });

  it("兼容旧版 v1 unknown APR，并明确展示恢复公式而非启发式标签", async () => {
    stubScanner(
      model({
        latest: {
          ...model().latest!,
          estimated_apr_kind: "unknown",
          estimated_apr_assumptions: {
            order_notional_usdc: "10",
            round_trip_fee_percent: "0.004",
          },
          rows: [row({ estimated_apr_kind: "unknown" })],
        },
      }),
    );
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(
        screen.getByText("未知（旧版 v1 恢复） / Unknown (restored from legacy v1)"),
      ).toBeTruthy();
    });
    expect(
      screen.getByText(/旧版记录未显式声明APR类型，按v1固定公式恢复：10 USDC \/ 0.004%/),
    ).toBeTruthy();
    expect(
      screen.getByText(
        /Legacy record omitted explicit APR kind; restored with the fixed v1 formula: 10 USDC \/ 0.004%/,
      ),
    ).toBeTruthy();
    expect(screen.queryByText("启发式估算 / Heuristic estimate")).toBeNull();
  });

  it("评级徽章保持中性语义，不把 S/D 误导成安全状态", async () => {
    stubScanner(model());
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(screen.getAllByTestId("scanner-grade").length).toBe(2);
    });
    for (const badge of screen.getAllByTestId("scanner-grade")) {
      expect(badge.className).not.toContain("safe-ok");
      expect(badge.className).not.toContain("safe-warning");
      expect(badge.className).not.toContain("safe-danger");
    }
    const sBadge = screen
      .getAllByTestId("scanner-grade")
      .find((badge) => badge.getAttribute("data-grade") === "s");
    expect(sBadge?.className).toContain("text-primary");
    const dBadge = screen
      .getAllByTestId("scanner-grade")
      .find((badge) => badge.getAttribute("data-grade") === "d");
    expect(dBadge?.className).toContain("text-muted-foreground");
  });

  it("degraded 投影保留最后有效历史排行，但明确不是当前结果", async () => {
    stubScanner(model({ projection_status: "degraded" }));
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toContain("scanner 投影已降级");
    });
    expect(screen.getByText("BTCUSDT")).toBeTruthy();
    expect(screen.getByText("ETHUSDT")).toBeTruthy();
  });

  it("truncated 排行显示截断横幅，并说明只展示部分候选", async () => {
    const truncated = model();
    truncated.latest = { ...truncated.latest!, truncated: true, eligible_count: 9 };
    stubScanner(truncated);
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(screen.getByText("排行已截断")).toBeTruthy();
    });
  });

  it("latest=null 时明确说明只检查过 read model，不伪装健康状态", async () => {
    stubScanner(model({ latest: null }));
    renderWithQueryClient(<ScannerPage />);
    await waitFor(() => {
      expect(screen.getByText("尚无确定性虚拟网格排行。")).toBeTruthy();
    });
    expect(
      screen.getByText(/已检查完整 \/api\/v1\/scanner read model/),
    ).toBeTruthy();
  });
});
