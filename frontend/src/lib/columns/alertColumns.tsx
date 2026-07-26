import { StatusPill } from "../../components/StatusPill";
import type { AlertOccurrenceView } from "../api-types";
import {
  alertOccurrenceSeverity,
  toneForAlertDelivery,
} from "../alerts";
import { formatDateTime } from "../format";
import { humanizeToken } from "../labels";
import type { ColumnDef } from "./types";

/**
 * change_percent 的方向呈现:方向色恒伴随 +/− 符号,
 * 方向色(--up/--down)只表达涨跌,与安全色严格分离。
 */
export function ChangePercent({ value }: { value: string | null }) {
  if (value === null || value === "") {
    return <span className="numeric text-muted-foreground">--</span>;
  }
  const negative = value.startsWith("-");
  const magnitude = negative ? value.slice(1) : value;
  return (
    <span className={negative ? "numeric text-down" : "numeric text-up"}>
      {negative ? "−" : "+"}
      {magnitude}%
    </span>
  );
}

/** 预警明细列定义(occurrence 按序号倒序展示)。 */
export const alertColumns: ColumnDef<AlertOccurrenceView>[] = [
  {
    id: "sequence",
    header: "序号",
    numeric: true,
    cell: (occurrence) => (
      <span className="numeric">{occurrence.alert_sequence}</span>
    ),
  },
  {
    id: "instrument",
    header: "标的",
    cell: (occurrence) => (
      <span className="block">
        <span className="numeric block">
          {occurrence.exchange}/{occurrence.symbol}
        </span>
        <span className="block text-xs text-muted-foreground">
          {humanizeToken(occurrence.market_type)}
        </span>
      </span>
    ),
  },
  {
    id: "kind",
    header: "类型 / severity",
    cell: (occurrence) => {
      const severity = alertOccurrenceSeverity(occurrence);
      return (
        <span className="flex flex-wrap gap-1">
          <StatusPill tone="neutral" label={humanizeToken(occurrence.kind)} />
          <StatusPill tone={severity.tone} label={severity.label} />
        </span>
      );
    },
  },
  {
    id: "price",
    header: "价格 / 波动",
    numeric: true,
    cell: (occurrence) => (
      <span className="block">
        <span className="numeric block">{occurrence.price}</span>
        <ChangePercent value={occurrence.change_percent} />
      </span>
    ),
  },
  {
    id: "deliveries",
    header: "通知结果",
    cell: (occurrence) => {
      const deliveries = occurrence.deliveries;
      if (deliveries.length === 0) {
        return <StatusPill tone="neutral" label="未发送" />;
      }
      return (
        <span className="block space-y-1">
          <span className="flex flex-wrap gap-1">
            {deliveries.map((delivery) => (
              <StatusPill
                key={delivery.adapter_id}
                tone={toneForAlertDelivery(delivery.status)}
                label={`${delivery.adapter_id}: ${humanizeToken(delivery.status)}`}
              />
            ))}
          </span>
          {deliveries.some((delivery) => delivery.failure !== null) && (
            <span className="block text-xs text-muted-foreground">
              {deliveries
                .filter((delivery) => delivery.failure !== null)
                .map(
                  (delivery) =>
                    `${delivery.adapter_id}=${humanizeToken(delivery.failure)}`,
                )
                .join(" / ")}
            </span>
          )}
          <span className="numeric block text-xs text-muted-foreground">
            {deliveries
              .map(
                (delivery) =>
                  `${delivery.adapter_id} ${formatDateTime(delivery.updated_at)}`,
              )
              .join(" / ")}
          </span>
        </span>
      );
    },
  },
  {
    id: "timing",
    header: "触发 / 确认",
    numeric: true,
    cell: (occurrence) => (
      <span className="block space-y-1">
        <StatusPill
          tone={occurrence.acknowledged_at !== null ? "ok" : "warning"}
          label={occurrence.acknowledged_at !== null ? "已确认" : "待确认"}
        />
        <span className="numeric block text-xs text-muted-foreground">
          触发 {formatDateTime(occurrence.recorded_at)}
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          {occurrence.acknowledged_at !== null
            ? `确认 ${formatDateTime(occurrence.acknowledged_at)}`
            : "确认 --"}
        </span>
      </span>
    ),
  },
];
