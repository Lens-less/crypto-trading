/**
 * 预警投影的信任判定与派生统计(沿承旧 app.js 的关键语义)。
 *
 * 不可协商语义:
 * - read model 最多保留 256 条 occurrence(MAX_ALERT_OCCURRENCES);
 *   超窗即为契约不一致,必须停止展示;
 * - 只有 complete / windowed 且完整性自洽的投影才可信;
 *   降级投影停止展示所有 occurrence,不猜测最新事实;
 * - windowed 必须与 occurrences_truncated 互相印证,否则视为降级。
 */
import {
  MAX_ALERT_OCCURRENCES,
  type AlertDeliveryStatus,
  type AlertOccurrenceView,
  type PriceAlertReadModel,
} from "./api-types";
import { humanizeToken } from "./labels";

export type SafeTone = "ok" | "warning" | "danger" | "neutral";

const TRUSTED_ALERT_PROJECTION_IDS = new Set(["complete", "windowed"]);

/** 投影是否可信:不可信时界面必须隐藏 occurrence 并挂降级横幅。 */
export function isTrustedAlertProjection(
  model: PriceAlertReadModel | null | undefined,
): boolean {
  if (
    !model ||
    !TRUSTED_ALERT_PROJECTION_IDS.has(model.projection_status) ||
    !Array.isArray(model.occurrences) ||
    model.occurrences.length > MAX_ALERT_OCCURRENCES ||
    model.invalid_event_count !== 0 ||
    model.boundary.kind !== "snapshot_end"
  ) {
    return false;
  }
  return model.projection_status === "windowed"
    ? model.occurrences_truncated === true
    : model.occurrences_truncated === false;
}

/** 可展示的 occurrence:不可信投影一律返回空,绝不展示可疑事实。 */
export function visibleAlertOccurrences(
  model: PriceAlertReadModel | null | undefined,
): AlertOccurrenceView[] {
  if (!model || !isTrustedAlertProjection(model)) {
    return [];
  }
  return model.occurrences;
}

/** 投影状态标签:契约不一致时显式标为「降级 / 契约不一致」。 */
export function alertProjectionLabel(
  model: PriceAlertReadModel | null | undefined,
): string {
  if (!model) {
    return humanizeToken("loading");
  }
  if (model.projection_status === "degraded") {
    return humanizeToken("degraded");
  }
  return isTrustedAlertProjection(model)
    ? humanizeToken(model.projection_status)
    : "降级 / 契约不一致";
}

/** 冻结 journal 中最后停在 pending 的 adapter 投递记录数。 */
export function countPendingAlertDeliveries(
  model: PriceAlertReadModel | null | undefined,
): number {
  return visibleAlertOccurrences(model).reduce(
    (count, occurrence) =>
      count +
      occurrence.deliveries.filter((delivery) => delivery.status === "pending")
        .length,
    0,
  );
}

/** 投递状态 → 安全色调(沿承旧 toneForAlertDelivery 映射)。 */
export function toneForAlertDelivery(status: AlertDeliveryStatus): SafeTone {
  switch (status) {
    case "succeeded":
      return "ok";
    case "failed":
    case "timed_out":
      return "danger";
    case "dropped":
    case "pending":
      return "warning";
    default:
      return "neutral";
  }
}

/**
 * 单条 occurrence 的 severity(用于告警流着色)。
 * 状态 = 文字 + 颜色:label 必与 tone 同时呈现,颜色永不单独承载含义。
 */
export function alertOccurrenceSeverity(occurrence: AlertOccurrenceView): {
  tone: SafeTone;
  label: string;
} {
  const deliveries = occurrence.deliveries;
  if (
    deliveries.some(
      (delivery) =>
        delivery.status === "failed" || delivery.status === "timed_out",
    )
  ) {
    return { tone: "danger", label: "投递失败" };
  }
  if (deliveries.some((delivery) => delivery.status === "pending")) {
    return { tone: "warning", label: "投递未决" };
  }
  if (occurrence.acknowledged_at === null) {
    return { tone: "warning", label: "待确认" };
  }
  return { tone: "ok", label: "已确认" };
}

/** 投递结果的按状态计数摘要(如「已送达 2 / 最后记录:未决 1」)。 */
export function summarizeDeliveries(
  deliveries: readonly { status: AlertDeliveryStatus }[],
): string {
  if (deliveries.length === 0) {
    return "未发送";
  }
  const counts = new Map<AlertDeliveryStatus, number>();
  for (const delivery of deliveries) {
    counts.set(delivery.status, (counts.get(delivery.status) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([status, count]) => `${humanizeToken(status)} ${count}`)
    .join(" / ");
}
