import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  virtualGridScannerReadModelSchema,
  type ScannerAprEstimateAssumptions,
  type ScannerAprEstimateKind,
  type VirtualGridScannerReadModel,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { scannerBanners } from "../lib/banners";
import { formatDateTime } from "../lib/format";
import { humanizeToken } from "../lib/labels";
import { scannerColumns } from "../lib/columns/scannerColumns";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import { DataAsOf, QueryStateBody } from "../components/QueryStateBody";

function aprKindLabel(kind: ScannerAprEstimateKind): string {
  if (kind === "unknown") {
    return "未知（旧版 v1 恢复） / Unknown (restored from legacy v1)";
  }
  return "启发式估算 / Heuristic estimate";
}

function aprAssumptionSummary(
  kind: ScannerAprEstimateKind,
  assumptions: ScannerAprEstimateAssumptions,
): string {
  if (kind === "unknown") {
    return `旧版记录未显式声明APR类型，按v1固定公式恢复：${assumptions.order_notional_usdc} USDC / ${assumptions.round_trip_fee_percent}% · Legacy record omitted explicit APR kind; restored with the fixed v1 formula: ${assumptions.order_notional_usdc} USDC / ${assumptions.round_trip_fee_percent}%.`;
  }
  return `每格名义 ${assumptions.order_notional_usdc} USDC · 往返费率 ${assumptions.round_trip_fee_percent}%`;
}

export function Component() {
  const scanner = useQuery({
    queryKey: queryKeys.scanner,
    queryFn: ({ signal }) =>
      request<VirtualGridScannerReadModel>("/api/v1/scanner", {
        schema: virtualGridScannerReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">扫描</h1>
        <p className="text-sm text-muted-foreground">
          确定性虚拟网格排行（/api/v1/scanner）；仅展示最后一次离线历史回放，不生成订单意图，也不推断当前行情。
        </p>
      </header>

      <DataCard
        title="确定性虚拟网格排行"
        subtitle="网格穿越频率只是波动代理；APR 估算与 Rating 都只是回放证据，不是实盘信号。"
      >
        <QueryStateBody query={scanner} skeletonRows={7}>
          {(model) => {
            const banners = scannerBanners(model);
            const latest = model.latest;
            if (latest === null) {
              return (
                <div className="space-y-2">
                  {banners.map((banner) => (
                    <DegradedBanner key={banner.key} banner={banner} />
                  ))}
                  <EmptyState
                    message="尚无确定性虚拟网格排行。"
                    checkedFact="已检查完整 /api/v1/scanner read model；没有记录、启动按钮或在线行情推断。"
                  />
                </div>
              );
            }
            return (
              <div className="space-y-3">
                {banners.map((banner) => (
                  <DegradedBanner key={banner.key} banner={banner} />
                ))}
                <div className="flex flex-wrap items-center gap-2">
                  <StatusPill
                    tone={
                      model.projection_status === "complete"
                        ? "ok"
                        : model.projection_status === "windowed"
                          ? "warning"
                          : "danger"
                    }
                    label={`投影：${humanizeToken(model.projection_status)}`}
                  />
                  <StatusPill tone="neutral" label="读取方式：历史快照" />
                </div>
                <div className="grid gap-x-6 gap-y-1 sm:grid-cols-2">
                  <FactRow label="评估时间" value={formatDateTime(latest.recorded_at)} />
                  <FactRow label="Run ID" value={latest.run_id} />
                  <FactRow
                    label="候选 / 入榜 / 循环过滤"
                    value={`${latest.candidate_count} / ${latest.eligible_count} / ${latest.filtered_by_cycles_count}`}
                  />
                  <FactRow label="APR 窗口" value={`${latest.apr_window_seconds}s`} />
                  <FactRow
                    label="APR 类型"
                    value={aprKindLabel(latest.estimated_apr_kind)}
                  />
                  <FactRow
                    label="APR 假设"
                    value={aprAssumptionSummary(
                      latest.estimated_apr_kind,
                      latest.estimated_apr_assumptions,
                    )}
                  />
                  <FactRow label="最小完整循环" value={String(latest.min_complete_cycles)} />
                  <FactRow
                    label="返回行 / 行上限"
                    value={`${latest.rows.length} / ${latest.row_limit}`}
                  />
                  <FactRow label="无效事件" value={String(model.invalid_event_count)} />
                  <DataAsOf updatedAt={scanner.dataUpdatedAt} />
                </div>
                <div className="rounded-md border border-border bg-muted/30 px-3 py-2">
                  <p className="text-xs font-medium text-muted-foreground">排行策略</p>
                  <p className="mt-0.5 text-xs">
                    显式 benchmark 优先，其余按 APR 估算降序；并列时按 exact instrument
                    稳定排序。benchmark 只是展示优先级，不是评分加成。
                  </p>
                </div>
                {latest.rows.length === 0 ? (
                  <EmptyState
                    message="本次排行没有返回行。"
                    checkedFact="可能所有标准候选都未达到最小完整循环；benchmark 例外仍需显式配置。"
                  />
                ) : (
                  <DataTable
                    columns={scannerColumns}
                    rows={latest.rows}
                    rowKey={(row) => String(row.rank)}
                    ariaLabel="确定性虚拟网格排行明细，可横向滚动"
                    minWidth="62rem"
                  />
                )}
                <p className="text-xs text-muted-foreground">
                  所有数值来自最后一次离线历史回放；APR
                  估算不代表可交易收益，也不证明 scanner 进程仍存活或行情仍然新鲜。本页没有启动、停止、重连或交易控件，也不构成投资建议。
                </p>
              </div>
            );
          }}
        </QueryStateBody>
      </DataCard>
    </section>
  );
}
