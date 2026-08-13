import { describe, expect, it } from "vitest";
import type { PaperAccountReadModel } from "./api-types";
import { riskBanners } from "./banners";

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
