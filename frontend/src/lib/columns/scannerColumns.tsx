import type { ScannerRatingGrade, VirtualGridScanRowView } from "../api-types";
import { cn } from "../cn";
import { formatDateTime } from "../format";
import { humanizeToken } from "../labels";
import type { ColumnDef } from "./types";

/**
 * 评级徽章:评级(S/A/B/C/D)是估算证据,不是安全状态。
 * 依据安全语义分离原则,这里只使用中性/强调色(border/primary/muted),
 * 永不使用 --safe-ok / --safe-warning / --safe-danger。
 */
export function ScannerGradeBadge({ grade }: { grade: ScannerRatingGrade }) {
  const emphasized = grade === "s" || grade === "a";
  return (
    <span
      data-testid="scanner-grade"
      data-grade={grade}
      className={cn(
        "numeric inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-medium",
        emphasized
          ? "border-primary/50 bg-primary/10 text-primary"
          : "border-border bg-muted/40 text-muted-foreground",
      )}
    >
      {grade.toUpperCase()}
    </span>
  );
}

/** 确定性虚拟网格排行列(980px 级宽表,APR/评分等宽数字)。 */
export const scannerColumns: ColumnDef<VirtualGridScanRowView>[] = [
  {
    id: "market",
    header: "排名 / 市场",
    cell: (row) => (
      <span className="block">
        <span className="flex items-baseline gap-2">
          <span className="numeric text-xs text-muted-foreground">#{row.rank}</span>
          <span className="numeric font-medium">{row.instrument.symbol}</span>
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          {row.instrument.exchange} / {humanizeToken(row.instrument.market_type)}
        </span>
      </span>
    ),
  },
  {
    id: "priority",
    header: "优先级",
    cell: (row) => (
      // benchmark 是展示优先级,不是评分加成;同样不占用安全色。
      <span
        className={cn(
          "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs",
          row.priority === "benchmark"
            ? "border-primary/50 bg-primary/10 text-primary"
            : "border-border bg-muted/40 text-muted-foreground",
        )}
      >
        {humanizeToken(row.priority)}
      </span>
    ),
  },
  {
    id: "apr",
    header: "APR / Rating",
    numeric: true,
    cell: (row) => (
      <span className="block space-y-1">
        <span className="numeric block">{row.estimated_apr}%</span>
        <span className="flex flex-wrap justify-end gap-1">
          <ScannerGradeBadge grade={row.rating_grade} />
          <span className="numeric text-xs text-muted-foreground">
            评分 {row.rating_score}
          </span>
        </span>
      </span>
    ),
  },
  {
    id: "cycles",
    header: "循环证据",
    numeric: true,
    cell: (row) => (
      <span className="block">
        <span className="numeric block text-xs">
          {row.complete_cycles} 完整 / {row.recent_five_minute_cycles} 近 5m
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          {row.cycles_per_hour} cycles/h · 买 {row.buy_crosses} / 卖 {row.sell_crosses}
        </span>
      </span>
    ),
  },
  {
    id: "grid",
    header: "价格 / 网格",
    numeric: true,
    cell: (row) => (
      <span className="block">
        <span className="numeric block text-xs">{row.current_price}</span>
        <span className="numeric block text-xs text-muted-foreground">
          {row.lower_price} — {row.upper_price}
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          宽 {row.grid_width_percent}% · 间距 {row.grid_interval_percent}% ·{" "}
          {row.grid_count} 格
        </span>
      </span>
    ),
  },
  {
    id: "evidence",
    header: "回放证据",
    numeric: true,
    cell: (row) => (
      <span className="block">
        <span className="numeric block text-xs">
          seq {row.last_observation_sequence} / {row.observation_count} samples
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          {formatDateTime(row.last_observed_at)}
        </span>
        <span className="numeric block text-xs text-muted-foreground">
          24h 量 {row.volume_24h_usdc}
        </span>
      </span>
    ),
  },
];
