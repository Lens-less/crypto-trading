import { describe, expect, it } from "vitest";
import type {
  PaperAccountReadModel,
  VirtualGridScannerReadModel,
  VirtualGridScanView,
} from "./api-types";
import { riskBanners, scannerBanners } from "./banners";

function scanView(overrides: Partial<VirtualGridScanView> = {}): VirtualGridScanView {
  return {
    source_sequence: 4,
    event_id: "00000000-0000-4000-8000-000000000004",
    recorded_at: "2026-07-26T00:00:00Z",
    run_id: "run-1",
    ranking_policy: "explicit_benchmark_then_apr_desc",
    apr_window_seconds: 3600,
    estimated_apr_kind: "heuristic",
    estimated_apr_assumptions: {
      order_notional_usdc: "100",
      round_trip_fee_percent: "0.2",
    },
    min_complete_cycles: 2,
    row_limit: 128,
    candidate_count: 3,
    eligible_count: 3,
    filtered_by_cycles_count: 0,
    truncated: false,
    rows: [],
    ...overrides,
  };
}

function scannerModel(
  overrides: Partial<VirtualGridScannerReadModel> = {},
): VirtualGridScannerReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 8,
    projection_status: "complete",
    latest: scanView(),
    invalid_event_count: 0,
    ...overrides,
  };
}

function riskModel(
  overrides: Partial<PaperAccountReadModel> = {},
): PaperAccountReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    projection_status: "complete",
    invalid_event_count: 0,
    accounts: [],
    ...overrides,
  };
}

describe("scannerBanners", () => {
  it("complete 且未截断:没有横幅", () => {
    expect(scannerBanners(scannerModel())).toEqual([]);
  });

  it("degraded:danger 横幅,保留最后有效历史排行而非隐藏", () => {
    const banners = scannerBanners(scannerModel({ projection_status: "degraded" }));
    const degraded = banners.find((banner) => banner.key === "scanner-degraded");
    expect(degraded?.tone).toBe("danger");
    expect(degraded?.tag).toBe("保留最后有效历史排行");
    expect(degraded?.message).toContain("不把它解释为当前结果");
  });

  it("truncated:窗口化横幅说明展示范围(N / eligible)", () => {
    const banners = scannerBanners(
      scannerModel({ latest: scanView({ truncated: true, eligible_count: 9 }) }),
    );
    const truncated = banners.find((banner) => banner.key === "scanner-truncated");
    expect(truncated?.tone).toBe("warning");
    expect(truncated?.message).toContain("0 / 9");
  });
});

describe("riskBanners", () => {
  it("complete 且无无效事件:没有横幅", () => {
    expect(riskBanners(riskModel())).toEqual([]);
  });

  it("degraded:danger 横幅,数字只是最后有效事实", () => {
    const banners = riskBanners(riskModel({ projection_status: "degraded" }));
    const degraded = banners.find((banner) => banner.key === "risk-degraded");
    expect(degraded?.tone).toBe("danger");
    expect(degraded?.message).toContain("不把这些敞口解释为当前可用额度");
  });

  it("invalid_event_count > 0:warning 横幅陈述拒绝计入", () => {
    const banners = riskBanners(riskModel({ invalid_event_count: 2 }));
    const invalid = banners.find((banner) => banner.key === "risk-invalid-events");
    expect(invalid?.tone).toBe("warning");
    expect(invalid?.message).toContain("2 条");
  });
});
