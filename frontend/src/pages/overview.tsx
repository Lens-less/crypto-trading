import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { request } from "../lib/api";
import {
  arbitrageMonitorReadModelSchema,
  capabilityManifestSchema,
  executionsResponseSchema,
  healthResponseSchema,
  priceAlertReadModelSchema,
  readOnlyTaskReadModelSchema,
  systemResponseSchema,
  type ArbitrageMonitorReadModel,
  type CapabilityLevel,
  type CapabilityManifest,
  type ExecutionsResponse,
  type HealthResponse,
  type PriceAlertReadModel,
  type ProjectionStatus,
  type ReadOnlyTaskReadModel,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { alertBanners, monitorBanner, taskBanners } from "../lib/banners";
import { alertOccurrenceSeverity, visibleAlertOccurrences } from "../lib/alerts";
import { errorPresentation } from "../lib/errorPresentation";
import { formatDateTime, shortId } from "../lib/format";
import { humanizeToken } from "../lib/labels";
import { DataCard, FactRow } from "../components/DataCard";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState, SkeletonRows } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import { toneForBatchState } from "../lib/columns/executionColumns";

const PROJECTION_TONES: Record<ProjectionStatus, "ok" | "warning" | "danger"> = {
  complete: "ok",
  windowed: "warning",
  degraded: "danger",
};

/** 卡片体的一等状态外壳:loading / error(脱敏)交给这里,成功再渲染事实。 */
function QueryStateBody<T>({
  query,
  skeletonRows,
  children,
}: {
  query: UseQueryResult<T>;
  skeletonRows: number;
  children: (data: T) => ReactNode;
}) {
  if (query.isPending) {
    return <SkeletonRows rows={skeletonRows} />;
  }
  if (query.isError && query.data === undefined) {
    const presentation = errorPresentation(query.error);
    return (
      <div className="space-y-2">
        <StatusPill tone={presentation.tone} label={presentation.label} />
        <p className="text-xs text-muted-foreground">{presentation.detail}</p>
      </div>
    );
  }
  if (query.data === undefined) {
    return <SkeletonRows rows={skeletonRows} />;
  }
  return (
    <>
      {query.isError && (
        <DegradedBanner
          banner={{
            key: "stale-snapshot",
            tone: "warning",
            title: "快照刷新失败",
            tag: "保留旧快照",
            message:
              "最后一个通过校验的快照仍然可见;重新读取成功前,不把它解释为最新状态。",
          }}
        />
      )}
      {children(query.data)}
    </>
  );
}

/** 「数据截至」= 本页读取投影的时间,永不代表外部行情时间。 */
function DataAsOf({ updatedAt }: { updatedAt: number }) {
  return (
    <FactRow
      label="数据截至"
      value={updatedAt === 0 ? "--" : formatDateTime(updatedAt)}
    />
  );
}

function SystemCard({
  system,
  health,
}: {
  system: UseQueryResult<SystemResponse>;
  health: UseQueryResult<HealthResponse>;
}) {
  return (
    <DataCard
      title="系统"
      subtitle="/api/v1/system · journal 投影的运行事实"
      className="lg:col-span-2"
    >
      <QueryStateBody query={system} skeletonRows={6}>
        {(data) => (
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <StatusPill
                tone={PROJECTION_TONES[data.projection_status]}
                label={`投影:${humanizeToken(data.projection_status)}`}
              />
              {health.isSuccess && <StatusPill tone="ok" label="就绪" />}
              <StatusPill
                tone={data.live_trading_enabled ? "warning" : "neutral"}
                label={data.live_trading_enabled ? "live 已声明" : "LIVE CLOSED"}
              />
            </div>
            <FactRow label="journal_id" value={data.journal_id} />
            <FactRow
              label="generation(头序列)"
              value={
                data.head_sequence === null ? "空 journal" : String(data.head_sequence)
              }
            />
            <FactRow
              label="批次 / 需恢复 / 冲突 / 警告"
              value={`${data.execution_batch_count} / ${data.recovery_required_count} / ${data.conflict_count} / ${data.warning_count}`}
            />
            {(data.truncation.batches || data.truncation.warnings) && (
              <div className="flex flex-wrap gap-2">
                {data.truncation.batches && (
                  <StatusPill tone="warning" label="批次窗口化:已截断" />
                )}
                {data.truncation.warnings && (
                  <StatusPill tone="warning" label="警告窗口化:已截断" />
                )}
              </div>
            )}
            <FactRow
              label="kill switch / 行情新鲜度 / 适配器健康"
              numeric={false}
              value={
                <span className="text-xs text-safe-neutral">
                  {`${humanizeToken(data.kill_switch)} / ${humanizeToken(
                    data.market_data_freshness,
                  )} / ${humanizeToken(data.adapter_health)}(受监督前不声明健康)`}
                </span>
              }
            />
            <DataAsOf updatedAt={system.dataUpdatedAt} />
            <p className="text-xs text-muted-foreground">
              「数据截至」为本页读取投影的时间,不代表外部行情时间
            </p>
          </div>
        )}
      </QueryStateBody>
    </DataCard>
  );
}

const CAPABILITY_LEVELS: CapabilityLevel[] = [
  "available",
  "read-only",
  "paper-once",
  "validate-only",
  "contract-only",
  "unavailable",
];

function CapabilitiesCard({
  capabilities,
}: {
  capabilities: UseQueryResult<CapabilityManifest>;
}) {
  return (
    <DataCard title="权限总览" subtitle="/api/v1/capabilities · 浏览器永不构造 live 权限">
      <QueryStateBody query={capabilities} skeletonRows={5}>
        {(manifest) => {
          const paperAvailable =
            manifest.release_stage === "paper-only" ||
            manifest.capabilities.some(
              (capability) =>
                capability.scope.access === "paper-trading" &&
                capability.level !== "unavailable",
            );
          const counts = new Map<CapabilityLevel, number>();
          for (const capability of manifest.capabilities) {
            counts.set(capability.level, (counts.get(capability.level) ?? 0) + 1);
          }
          return (
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <StatusPill
                  tone={paperAvailable ? "ok" : "warning"}
                  label={paperAvailable ? "PAPER 可用" : "Paper 暂不可用"}
                />
                <StatusPill
                  tone={manifest.live_trading_enabled ? "warning" : "neutral"}
                  label={manifest.live_trading_enabled ? "live 由后端声明" : "LIVE CLOSED"}
                />
              </div>
              {CAPABILITY_LEVELS.map((level) => (
                <FactRow
                  key={level}
                  label={humanizeToken(level)}
                  value={String(counts.get(level) ?? 0)}
                />
              ))}
              <FactRow label="能力项总数" value={String(manifest.capabilities.length)} />
              <DataAsOf updatedAt={capabilities.dataUpdatedAt} />
            </div>
          );
        }}
      </QueryStateBody>
    </DataCard>
  );
}

function MonitorCard({
  monitor,
}: {
  monitor: UseQueryResult<ArbitrageMonitorReadModel>;
}) {
  return (
    <DataCard
      title="只读套利监控"
      subtitle="/api/v1/monitor · 持久化历史投影,不代表当前实时行情仍然新鲜"
      className="lg:col-span-2"
    >
      <QueryStateBody query={monitor} skeletonRows={5}>
        {(model) => {
          const degraded = monitorBanner(model);
          if (degraded !== null) {
            return (
              <div className="space-y-2">
                <DegradedBanner banner={degraded} />
                <FactRow label="投影" value={humanizeToken(model.projection_status)} />
                <FactRow label="无效事件" value={String(model.invalid_event_count)} />
                <FactRow
                  label="保留事实"
                  value={model.latest !== null ? "已隐藏" : "无"}
                />
              </div>
            );
          }
          const latest = model.latest;
          if (latest === null) {
            return (
              <EmptyState
                message="尚未观察到只读套利监控事件。"
                checkedFact="已检查 /api/v1/monitor;监控投影不会把缺失行情提升为健康状态。"
              />
            );
          }
          const projection = latest.projection;
          return (
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <StatusPill tone="neutral" label={humanizeToken(latest.state)} />
                <StatusPill tone="neutral" label="读取方式:历史快照" />
              </div>
              <FactRow
                label="监控对"
                value={`${latest.left.exchange}/${latest.left.symbol} ↔ ${latest.right.exchange}/${latest.right.symbol}`}
              />
              <FactRow label="market generation" value={String(latest.market_generation)} />
              <FactRow label="recorded_at(记录时间)" value={formatDateTime(latest.recorded_at)} />
              {projection.type === "waiting" && (
                <>
                  <FactRow
                    label="等待腿"
                    value={`${projection.instrument.exchange}/${projection.instrument.symbol}`}
                  />
                  <FactRow
                    label="新鲜度 / 连续性"
                    value={`${humanizeToken(projection.freshness)} / ${humanizeToken(projection.continuity)}`}
                  />
                </>
              )}
              {(projection.type === "opportunity" ||
                projection.type === "no_opportunity") && (
                <>
                  <FactRow
                    label="方向"
                    value={`${projection.buy_exchange} → ${projection.sell_exchange}`}
                  />
                  <FactRow
                    label="价差 / 阈值"
                    value={`${projection.spread_percent}% / ${projection.threshold_percent}%`}
                  />
                </>
              )}
              {projection.type === "analysis_rejected" && (
                <FactRow label="拒绝分类" value={humanizeToken(projection.failure)} />
              )}
              <DataAsOf updatedAt={monitor.dataUpdatedAt} />
              <p className="text-xs text-muted-foreground">
                这是持久化监控事件的最后一次投影,不代表当前实时行情仍然新鲜。
              </p>
            </div>
          );
        }}
      </QueryStateBody>
    </DataCard>
  );
}

function TasksCard({ tasks }: { tasks: UseQueryResult<ReadOnlyTaskReadModel> }) {
  return (
    <DataCard title="只读连续任务" subtitle="/api/v1/tasks · 最后持久阶段,不证明进程仍存活">
      <QueryStateBody query={tasks} skeletonRows={5}>
        {(model) => {
          const banners = taskBanners(model);
          if (model.tasks.length === 0 && banners.length === 0) {
            return (
              <EmptyState
                message="尚未投影出连续任务登记。"
                checkedFact="已检查 /api/v1/tasks;没有运行按钮,也不会把缺失事实解释成健康。"
              />
            );
          }
          const countByPhase = (phase: string): number =>
            model.tasks.filter((task) => task.phase === phase).length;
          return (
            <div className="space-y-2">
              {banners.map((banner) => (
                <DegradedBanner key={banner.key} banner={banner} />
              ))}
              <FactRow label="最后记录:运行中" value={String(countByPhase("running"))} />
              <FactRow label="已停止" value={String(countByPhase("stopped"))} />
              <FactRow label="失败" value={String(countByPhase("failed"))} />
              <FactRow label="任务总数" value={String(model.tasks.length)} />
              <FactRow label="无效事件" value={String(model.invalid_event_count)} />
              <DataAsOf updatedAt={tasks.dataUpdatedAt} />
              <p className="text-xs text-muted-foreground">
                running / stopping 只代表 journal 最后记录,不证明进程仍存活;本页没有启动、停止、重连或自动恢复入口。
              </p>
            </div>
          );
        }}
      </QueryStateBody>
    </DataCard>
  );
}

const SIDEBAR_ALERT_COUNT = 8;

function AlertsSidebarCard({
  alerts,
}: {
  alerts: UseQueryResult<PriceAlertReadModel>;
}) {
  return (
    <DataCard title="告警流" subtitle="/api/v1/alerts · 最近 occurrence,降级即停止展示">
      <QueryStateBody query={alerts} skeletonRows={6}>
        {(model) => {
          const banners = alertBanners(model, { refreshFailed: alerts.isError });
          const visible = visibleAlertOccurrences(model)
            .slice(-SIDEBAR_ALERT_COUNT)
            .reverse();
          return (
            <div className="space-y-2">
              {banners.map((banner) => (
                <DegradedBanner key={banner.key} banner={banner} />
              ))}
              {visible.length === 0 ? (
                <EmptyState
                  message="当前冻结快照中还没有可展示的价格预警。"
                  checkedFact="已检查 /api/v1/alerts 返回的 occurrences;不可信投影不会展示可疑事实。"
                />
              ) : (
                <ul className="space-y-2">
                  {visible.map((occurrence) => {
                    const severity = alertOccurrenceSeverity(occurrence);
                    return (
                      <li
                        key={occurrence.alert_sequence}
                        className="rounded-md border border-border px-3 py-2"
                      >
                        <div className="flex flex-wrap items-center gap-1.5">
                          <StatusPill tone={severity.tone} label={severity.label} />
                          <span className="text-xs">{humanizeToken(occurrence.kind)}</span>
                        </div>
                        <p className="numeric mt-1 text-xs">
                          {occurrence.exchange}/{occurrence.symbol} · {occurrence.price}
                        </p>
                        <p className="numeric mt-0.5 text-xs text-muted-foreground">
                          触发 {formatDateTime(occurrence.recorded_at)}
                        </p>
                      </li>
                    );
                  })}
                </ul>
              )}
              <Link
                to="/alerts"
                className="inline-block text-xs text-primary underline-offset-2 hover:underline"
              >
                查看预警明细
              </Link>
            </div>
          );
        }}
      </QueryStateBody>
    </DataCard>
  );
}

const SIDEBAR_BATCH_COUNT = 5;

function ExecutionsSidebarCard({
  executions,
}: {
  executions: UseQueryResult<ExecutionsResponse>;
}) {
  return (
    <DataCard title="最近执行" subtitle="/api/v1/executions · 有界执行账本摘要">
      <QueryStateBody query={executions} skeletonRows={5}>
        {(data) => {
          const batches = [...data.operator.batches]
            .sort((left, right) => right.last_sequence - left.last_sequence)
            .slice(0, SIDEBAR_BATCH_COUNT);
          if (batches.length === 0) {
            return (
              <EmptyState
                message="这个有界快照中没有执行批次。"
                checkedFact="已检查 /api/v1/executions 返回的 operator.batches。"
              />
            );
          }
          return (
            <div className="space-y-2">
              <ul className="space-y-2">
                {batches.map((batch) => (
                  <li
                    key={batch.batch_id}
                    className="rounded-md border border-border px-3 py-2"
                  >
                    <div className="flex flex-wrap items-center gap-1.5">
                      <StatusPill
                        tone={toneForBatchState(batch.state)}
                        label={humanizeToken(batch.state)}
                      />
                      <span className="numeric text-xs" title={batch.batch_id}>
                        {shortId(batch.batch_id)}
                      </span>
                    </div>
                    <p className="numeric mt-1 text-xs text-muted-foreground">
                      {batch.strategy} · {batch.symbol}
                    </p>
                    <p className="numeric mt-0.5 text-xs text-muted-foreground">
                      更新 {formatDateTime(batch.updated_at)}
                    </p>
                  </li>
                ))}
              </ul>
              <Link
                to="/executions"
                className="inline-block text-xs text-primary underline-offset-2 hover:underline"
              >
                查看执行账本
              </Link>
            </div>
          );
        }}
      </QueryStateBody>
    </DataCard>
  );
}

export function Component() {
  const system = useQuery({
    queryKey: queryKeys.system,
    queryFn: ({ signal }) =>
      request<SystemResponse>("/api/v1/system", {
        schema: systemResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const health = useQuery({
    queryKey: queryKeys.health,
    queryFn: ({ signal }) =>
      request<HealthResponse>("/api/v1/health", {
        schema: healthResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const capabilities = useQuery({
    queryKey: queryKeys.capabilities,
    queryFn: ({ signal }) =>
      request<CapabilityManifest>("/api/v1/capabilities", {
        schema: capabilityManifestSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const monitor = useQuery({
    queryKey: queryKeys.monitor,
    queryFn: ({ signal }) =>
      request<ArbitrageMonitorReadModel>("/api/v1/monitor", {
        schema: arbitrageMonitorReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const tasks = useQuery({
    queryKey: queryKeys.tasks,
    queryFn: ({ signal }) =>
      request<ReadOnlyTaskReadModel>("/api/v1/tasks", {
        schema: readOnlyTaskReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const alerts = useQuery({
    queryKey: queryKeys.alerts,
    queryFn: ({ signal }) =>
      request<PriceAlertReadModel>("/api/v1/alerts", {
        schema: priceAlertReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const executions = useQuery({
    queryKey: queryKeys.executions(null),
    queryFn: ({ signal }) =>
      request<ExecutionsResponse>("/api/v1/executions", {
        schema: executionsResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">总览</h1>
        <p className="text-sm text-muted-foreground">
          journal 投影的系统事实;本页数据是持久化投影,不代表外部行情新鲜度
        </p>
      </header>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_20rem]">
        <div className="grid content-start gap-4 grid-cols-1 lg:grid-cols-3">
          <SystemCard system={system} health={health} />
          <CapabilitiesCard capabilities={capabilities} />
          <MonitorCard monitor={monitor} />
          <TasksCard tasks={tasks} />
        </div>
        <aside aria-label="告警与执行侧栏" className="space-y-4">
          <AlertsSidebarCard alerts={alerts} />
          <ExecutionsSidebarCard executions={executions} />
        </aside>
      </div>
    </section>
  );
}
