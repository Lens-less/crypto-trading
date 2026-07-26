// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type {
  ArbitrageMonitorReadModel,
  SettingsResponse,
} from "../lib/api-types";
import { jsonResponse, renderWithQueryClient, routedFetch } from "./pageTestUtils";
import { Component as ReplayPage } from "./replay";

function monitorModel(
  overrides: Partial<ArbitrageMonitorReadModel> = {},
): ArbitrageMonitorReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 12,
    projection_status: "complete",
    latest: {
      source_sequence: 12,
      event_id: "00000000-0000-4000-8000-00000000000c",
      recorded_at: "2026-07-26T00:00:00Z",
      monitor_sequence: 7,
      market_generation: 3,
      symbol: "BTCUSDT",
      state: "opportunity",
      left: { exchange: "binance", symbol: "BTCUSDT", market_type: "spot" },
      right: { exchange: "okx", symbol: "BTCUSDT", market_type: "perpetual" },
      projection: {
        type: "opportunity",
        buy_exchange: "binance",
        sell_exchange: "okx",
        buy_price: "64000",
        sell_price: "64100",
        absolute_spread: "100",
        spread_percent: "0.156",
        threshold_percent: "0.1",
      },
    },
    invalid_event_count: 0,
    ...overrides,
  };
}

function settingsModel(
  overrides: Partial<SettingsResponse> = {},
): SettingsResponse {
  return {
    schema_version: 1,
    data_directory: "data",
    journal_path: "data/journal.jsonl",
    log_sink: "stdout_stderr",
    notification_evidence: "journal_projection",
    credentials: {
      web_bearer: "configured",
      binance_testnet: "not_configured",
      mainnet: "not_accepted",
    },
    paper_principal_id: null,
    paper_profiles: [],
    request_limit: { maximum_requests: 240, window_seconds: 60 },
    ...overrides,
  };
}

function stubReplay(
  monitor: ArbitrageMonitorReadModel,
  settings: SettingsResponse,
): void {
  vi.stubGlobal(
    "fetch",
    routedFetch({
      "/api/v1/monitor": () => jsonResponse(monitor),
      "/api/v1/settings": () => jsonResponse(settings),
    }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("/replay", () => {
  it("常驻说明:这是持久化历史投影,不是实时行情(独立横幅,始终可见)", async () => {
    stubReplay(monitorModel({ latest: null }), settingsModel());
    renderWithQueryClient(<ReplayPage />);
    expect(
      screen.getByText("这是持久化历史投影,不是实时行情"),
    ).toBeTruthy();
    await waitFor(() => {
      expect(screen.getByText("尚未持久化任何监控事件。")).toBeTruthy();
    });
  });

  it("完整视图:pair 双侧、价差、recorded_at 与 market generation", async () => {
    stubReplay(monitorModel(), settingsModel());
    renderWithQueryClient(<ReplayPage />);
    await waitFor(() => {
      expect(screen.getByText("binance/BTCUSDT · 现货")).toBeTruthy();
    });
    expect(screen.getByText("okx/BTCUSDT · 永续")).toBeTruthy();
    expect(screen.getByText("recorded_at(记录时间)")).toBeTruthy();
    expect(screen.getByText("market generation(市场代次)")).toBeTruthy();
    expect(screen.getByText("0.156% / 0.1%")).toBeTruthy();
    expect(screen.getByText("binance → okx")).toBeTruthy();
    expect(
      screen.getByText(/不代表当前实时行情仍然新鲜/),
    ).toBeTruthy();
  });

  it("degraded 投影:隐藏 latest,显式陈述保留事实已隐藏", async () => {
    stubReplay(monitorModel({ projection_status: "degraded" }), settingsModel());
    renderWithQueryClient(<ReplayPage />);
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toContain("监控投影已降级");
    });
    expect(screen.getByText("已隐藏")).toBeTruthy();
    expect(screen.queryByText("binance → okx")).toBeNull();
  });

  it("settings 提供 paper_profiles 时展示 replay 文件路径", async () => {
    stubReplay(
      monitorModel(),
      settingsModel({
        paper_profiles: [
          {
            kind: "grid",
            task_id: "paper-grid-btc",
            strategy_id: "paper-grid",
            strategy_revision: "2026-07-25",
            configuration_files: ["config/grid/paper-once-btc.yaml"],
            replay_file: "fixtures/m4-grid-paper-replay.jsonl",
          },
        ],
      }),
    );
    renderWithQueryClient(<ReplayPage />);
    await waitFor(() => {
      expect(
        screen.getByText(/fixtures\/m4-grid-paper-replay\.jsonl/),
      ).toBeTruthy();
    });
    expect(screen.getByText(/config\/grid\/paper-once-btc\.yaml/)).toBeTruthy();
  });

  it("无 paper profile:一等空态说明查过 /api/v1/settings", async () => {
    stubReplay(monitorModel(), settingsModel());
    renderWithQueryClient(<ReplayPage />);
    await waitFor(() => {
      expect(
        screen.getByText("当前运行实例没有配置 Paper profile。"),
      ).toBeTruthy();
    });
  });
});
