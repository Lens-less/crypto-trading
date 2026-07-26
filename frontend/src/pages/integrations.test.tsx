// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type {
  AdapterFacetSupport,
  CapabilityManifest,
  SettingsResponse,
} from "../lib/api-types";
import { jsonResponse, renderWithQueryClient, routedFetch } from "./pageTestUtils";
import { Component as IntegrationsPage } from "./integrations";

function facet(overrides: Partial<AdapterFacetSupport> = {}): AdapterFacetSupport {
  return { level: "unavailable", blockers: [], evidence: [], ...overrides };
}

function manifest(overrides: Partial<CapabilityManifest> = {}): CapabilityManifest {
  return {
    schema_version: 2,
    product_version: "0.1.0",
    release_stage: "paper-only",
    live_trading_enabled: false,
    adapters: [
      {
        id: "binance",
        name: "Binance",
        public_data: facet({
          level: "implemented",
          evidence: ["rust/crates/exchange/src/binance.rs"],
        }),
        testnet_protocol: facet({ level: "protocol-only" }),
        authenticated: facet({ level: "request-only" }),
        reconcile: facet(),
        live: facet({ blockers: ["live trading is closed"] }),
      },
    ],
    capabilities: [
      {
        id: "control-plane.web",
        area: "control-plane",
        level: "read-only",
        scope: { environments: ["local"], access: "local" },
        summary: "本地只读控制面",
        blockers: ["写路径需要显式启用"],
        evidence: ["rust/crates/web/src/api.rs", "rust/crates/web/src/server.rs"],
      },
    ],
    ...overrides,
  };
}

function settingsModel(): SettingsResponse {
  return {
    schema_version: 1,
    data_directory: "data",
    journal_path: "data/journal.jsonl",
    log_sink: "stdout_stderr",
    notification_evidence: "journal_projection",
    credentials: {
      web_bearer: "configured",
      binance_testnet: "partial",
      mainnet: "not_accepted",
    },
    paper_principal_id: null,
    paper_profiles: [],
    request_limit: { maximum_requests: 240, window_seconds: 60 },
  };
}

function stubIntegrations(payload: CapabilityManifest): void {
  vi.stubGlobal(
    "fetch",
    routedFetch({
      "/api/v1/capabilities": () => jsonResponse(payload),
      "/api/v1/settings": () => jsonResponse(settingsModel()),
    }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("/integrations", () => {
  it("适配器矩阵:五个能力面逐格展示强度与证据计数;不可用保持中性", async () => {
    stubIntegrations(manifest());
    renderWithQueryClient(<IntegrationsPage />);
    await waitFor(() => {
      expect(screen.getByText("Binance")).toBeTruthy();
    });
    expect(
      screen.getByRole("region", { name: "适配器支持矩阵,可横向滚动" }),
    ).toBeTruthy();
    expect(screen.getByText("已实现")).toBeTruthy();
    expect(screen.getByText("仅协议")).toBeTruthy();
    expect(screen.getByText("仅请求")).toBeTruthy();
    // 不可用 → 中性色(不是 danger,也绝不是 ok)。
    const unavailable = screen.getAllByText("不可用");
    for (const pill of unavailable) {
      expect(pill.className).toContain("text-safe-neutral");
    }
  });

  it("能力账本:折叠行展开后可见 evidence 文件名与阻塞项", async () => {
    stubIntegrations(manifest());
    renderWithQueryClient(<IntegrationsPage />);
    await waitFor(() => {
      expect(screen.getByText("control-plane.web")).toBeTruthy();
    });
    // <details> 内容在 DOM 中即可断言(展开交互由原生 details 承担)。
    expect(screen.getByText("rust/crates/web/src/api.rs")).toBeTruthy();
    expect(screen.getByText("rust/crates/web/src/server.rs")).toBeTruthy();
    expect(screen.getByText("写路径需要显式启用")).toBeTruthy();
    expect(screen.getByText("证据文件(2)")).toBeTruthy();
  });

  it("凭据配置状态:只显示完整性词汇,绝不显示明文值", async () => {
    stubIntegrations(manifest());
    renderWithQueryClient(<IntegrationsPage />);
    await waitFor(() => {
      expect(screen.getByText("已配置")).toBeTruthy();
    });
    expect(screen.getByText("部分配置")).toBeTruthy();
    expect(screen.getByText("不接受")).toBeTruthy();
    expect(screen.getByText(/状态只表示配置完整性,不返回值/)).toBeTruthy();
  });

  it("空适配器清单:一等空态", async () => {
    stubIntegrations(manifest({ adapters: [] }));
    renderWithQueryClient(<IntegrationsPage />);
    await waitFor(() => {
      expect(
        screen.getByText("能力清单没有声明任何交易所适配器。"),
      ).toBeTruthy();
    });
  });
});
