import { describe, expect, it } from "vitest";
import {
  MAX_ALERT_OCCURRENCES,
  type AlertDeliveryStatus,
  type AlertOccurrenceView,
  type PriceAlertReadModel,
} from "./api-types";
import {
  alertOccurrenceSeverity,
  alertProjectionLabel,
  countPendingAlertDeliveries,
  isTrustedAlertProjection,
  summarizeDeliveries,
  toneForAlertDelivery,
  visibleAlertOccurrences,
} from "./alerts";

function occurrence(
  sequence: number,
  overrides: Partial<AlertOccurrenceView> = {},
): AlertOccurrenceView {
  return {
    source_sequence: sequence,
    event_id: `event-${sequence}`,
    alert_sequence: sequence,
    recorded_at: "2026-07-26T00:00:00Z",
    exchange: "binance",
    symbol: "BTCUSDT",
    market_type: "spot",
    kind: "upper_limit",
    price: "50000",
    change_percent: null,
    acknowledged_at: null,
    deliveries: [],
    ...overrides,
  };
}

function model(overrides: Partial<PriceAlertReadModel> = {}): PriceAlertReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 10,
    boundary: { kind: "snapshot_end" },
    projection_status: "complete",
    occurrences: [occurrence(1)],
    occurrences_truncated: false,
    invalid_event_count: 0,
    ...overrides,
  };
}

describe("isTrustedAlertProjection", () => {
  it("complete 且完整性自洽:可信", () => {
    expect(isTrustedAlertProjection(model())).toBe(true);
  });

  it("degraded:不可信", () => {
    expect(isTrustedAlertProjection(model({ projection_status: "degraded" }))).toBe(false);
  });

  it("超出 256 条窗口(契约不一致):不可信", () => {
    const many = Array.from({ length: MAX_ALERT_OCCURRENCES + 1 }, (_, i) =>
      occurrence(i + 1),
    );
    expect(isTrustedAlertProjection(model({ occurrences: many }))).toBe(false);
  });

  it("invalid_event_count 非零:不可信", () => {
    expect(isTrustedAlertProjection(model({ invalid_event_count: 1 }))).toBe(false);
  });

  it("边界不是 snapshot_end(部分尾记录):不可信", () => {
    expect(
      isTrustedAlertProjection(
        model({ boundary: { kind: "partial_tail", offset: 10, bytes: 3 } }),
      ),
    ).toBe(false);
  });

  it("windowed 必须与 occurrences_truncated 互相印证", () => {
    expect(
      isTrustedAlertProjection(
        model({ projection_status: "windowed", occurrences_truncated: true }),
      ),
    ).toBe(true);
    expect(
      isTrustedAlertProjection(
        model({ projection_status: "windowed", occurrences_truncated: false }),
      ),
    ).toBe(false);
    expect(
      isTrustedAlertProjection(
        model({ projection_status: "complete", occurrences_truncated: true }),
      ),
    ).toBe(false);
  });
});

describe("visibleAlertOccurrences", () => {
  it("可信投影返回全部 occurrence", () => {
    expect(visibleAlertOccurrences(model())).toHaveLength(1);
  });

  it("降级投影停止展示(空列表)", () => {
    expect(
      visibleAlertOccurrences(model({ projection_status: "degraded" })),
    ).toEqual([]);
  });
});

describe("alertProjectionLabel", () => {
  it("契约不一致显式标为「降级 / 契约不一致」", () => {
    expect(alertProjectionLabel(model({ invalid_event_count: 2 }))).toBe(
      "降级 / 契约不一致",
    );
  });

  it("可信 windowed 标为窗口化", () => {
    expect(
      alertProjectionLabel(
        model({ projection_status: "windowed", occurrences_truncated: true }),
      ),
    ).toBe("窗口化");
  });
});

describe("countPendingAlertDeliveries", () => {
  it("统计可信投影中停在 pending 的投递记录", () => {
    const delivery = (status: AlertDeliveryStatus) => ({
      adapter_id: `adapter-${status}`,
      status,
      failure: null,
      updated_at: "2026-07-26T00:00:00Z",
    });
    const trusted = model({
      occurrences: [
        occurrence(1, { deliveries: [delivery("pending"), delivery("succeeded")] }),
        occurrence(2, { deliveries: [delivery("pending")] }),
      ],
    });
    expect(countPendingAlertDeliveries(trusted)).toBe(2);
  });

  it("不可信投影不产生未决计数(已停止展示)", () => {
    const degraded = model({
      projection_status: "degraded",
      occurrences: [
        occurrence(1, {
          deliveries: [
            {
              adapter_id: "a",
              status: "pending",
              failure: null,
              updated_at: "2026-07-26T00:00:00Z",
            },
          ],
        }),
      ],
    });
    expect(countPendingAlertDeliveries(degraded)).toBe(0);
  });
});

describe("severity 与安全色映射", () => {
  it("投递失败 → danger;未决 → warning;待确认 → warning;已确认 → ok", () => {
    const failed = occurrence(1, {
      deliveries: [
        { adapter_id: "a", status: "failed", failure: "rejected", updated_at: "t" },
      ],
    });
    expect(alertOccurrenceSeverity(failed)).toEqual({
      tone: "danger",
      label: "投递失败",
    });

    const pending = occurrence(2, {
      deliveries: [
        { adapter_id: "a", status: "pending", failure: null, updated_at: "t" },
      ],
    });
    expect(alertOccurrenceSeverity(pending)).toEqual({
      tone: "warning",
      label: "投递未决",
    });

    expect(alertOccurrenceSeverity(occurrence(3))).toEqual({
      tone: "warning",
      label: "待确认",
    });
    expect(
      alertOccurrenceSeverity(
        occurrence(4, { acknowledged_at: "2026-07-26T00:01:00Z" }),
      ),
    ).toEqual({ tone: "ok", label: "已确认" });
  });

  it("投递状态色调沿承旧映射(succeeded→ok,failed/timed_out→danger,dropped/pending→warning)", () => {
    expect(toneForAlertDelivery("succeeded")).toBe("ok");
    expect(toneForAlertDelivery("failed")).toBe("danger");
    expect(toneForAlertDelivery("timed_out")).toBe("danger");
    expect(toneForAlertDelivery("dropped")).toBe("warning");
    expect(toneForAlertDelivery("pending")).toBe("warning");
  });

  it("投递摘要:按状态计数,pending 文案是「最后记录:未决」", () => {
    expect(summarizeDeliveries([])).toBe("未发送");
    expect(
      summarizeDeliveries([{ status: "succeeded" }, { status: "pending" }]),
    ).toBe("已送达 1 / 最后记录:未决 1");
  });
});
