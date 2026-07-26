// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { getBearerToken, setBearerToken } from "../lib/api";
import {
  currentSessionGeneration,
  resetSessionStateForTests,
} from "../lib/useOperationEvents";
import type { SettingsResponse, SystemResponse } from "../lib/api-types";
import { jsonResponse, renderWithQueryClient, routedFetch } from "./pageTestUtils";
import { Component as SettingsPage } from "./settings";

function settingsModel(overrides: Partial<SettingsResponse> = {}): SettingsResponse {
  return {
    schema_version: 1,
    data_directory: "C:/data/crypto",
    journal_path: "C:/data/crypto/journal.jsonl",
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

function stubSettings(payload: SettingsResponse): void {
  vi.stubGlobal(
    "fetch",
    routedFetch({
      "/api/v1/settings": () => jsonResponse(payload),
      "/api/v1/system": () => jsonResponse(SYSTEM),
    }),
  );
}

beforeEach(() => {
  resetSessionStateForTests();
  setBearerToken(null);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  setBearerToken(null);
});

describe("/settings", () => {
  it("运行时设置展示:目录、journal 路径、限流与凭据配置状态", async () => {
    stubSettings(settingsModel());
    renderWithQueryClient(<SettingsPage />);
    await waitFor(() => {
      expect(screen.getByText("C:/data/crypto")).toBeTruthy();
    });
    expect(screen.getByText("C:/data/crypto/journal.jsonl")).toBeTruthy();
    expect(screen.getByText("240 次 / 60s")).toBeTruthy();
    expect(screen.getByText(/已配置 \/ 未配置 \/ 不接受/)).toBeTruthy();
    expect(screen.getByText("未启用写入")).toBeTruthy();
    expect(screen.getByText(/429 并携带 Retry-After/)).toBeTruthy();
  });

  it("bearer token 输入:应用后仅存内存、推进会话代际、输入框立即清空", async () => {
    stubSettings(settingsModel());
    renderWithQueryClient(<SettingsPage />);
    const generationBefore = currentSessionGeneration();
    const input = screen.getByLabelText("bearer token") as HTMLInputElement;
    expect(input.type).toBe("password");
    fireEvent.change(input, { target: { value: "secret-token-0123456789abcdef" } });
    fireEvent.click(screen.getByRole("button", { name: "应用令牌并重建流" }));
    await waitFor(() => {
      expect(getBearerToken()).toBe("secret-token-0123456789abcdef");
    });
    expect(currentSessionGeneration()).toBe(generationBefore + 1);
    // 输入框清空:值不再出现在 DOM 中。
    expect(input.value).toBe("");
    expect(screen.getByText("Bearer 绑定:仅页内存")).toBeTruthy();
  });

  it("清除令牌:回到未绑定状态", async () => {
    stubSettings(settingsModel());
    setBearerToken("secret-token-0123456789abcdef");
    renderWithQueryClient(<SettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "清除令牌" }));
    await waitFor(() => {
      expect(getBearerToken()).toBeNull();
    });
    expect(screen.getByText("未绑定")).toBeTruthy();
  });

  it("主题切换按钮存在且可点击(持久化白名单只有 ct-theme)", async () => {
    stubSettings(settingsModel());
    renderWithQueryClient(<SettingsPage />);
    const button = screen.getByRole("button", { name: /当前:(深色|浅色)/ });
    fireEvent.click(button);
    expect(screen.getByRole("button", { name: /当前:(深色|浅色)/ })).toBeTruthy();
  });

  it("paper_principal_id 发布时如实展示(写路径已开启的事实)", async () => {
    stubSettings(settingsModel({ paper_principal_id: "local-paper-operator" }));
    renderWithQueryClient(<SettingsPage />);
    await waitFor(() => {
      expect(screen.getByText("local-paper-operator")).toBeTruthy();
    });
  });
});
