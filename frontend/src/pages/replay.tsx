import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  arbitrageMonitorReadModelSchema,
  settingsResponseSchema,
  systemResponseSchema,
  type ArbitrageMonitorReadModel,
  type SettingsResponse,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { monitorBanner } from "../lib/banners";
import { formatDateTime } from "../lib/format";
import { humanizeToken } from "../lib/labels";
import { DataCard, FactRow } from "../components/DataCard";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import { DataAsOf, QueryStateBody } from "../components/QueryStateBody";

function MonitorLegFacts({
  label,
  leg,
}: {
  label: string;
  leg: { exchange: string; symbol: string; market_type: string };
}) {
  return (
    <FactRow
      label={label}
      value={`${leg.exchange}/${leg.symbol} · ${humanizeToken(leg.market_type)}`}
    />
  );
}

export function Component() {
  const monitor = useQuery({
    queryKey: queryKeys.monitor,
    queryFn: ({ signal }) =>
      request<ArbitrageMonitorReadModel>("/api/v1/monitor", {
        schema: arbitrageMonitorReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const system = useQuery({
    queryKey: queryKeys.system,
    queryFn: ({ signal }) =>
      request<SystemResponse>("/api/v1/system", {
        schema: systemResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });
  const settings = useQuery({
    queryKey: queryKeys.settings,
    queryFn: ({ signal }) =>
      request<SettingsResponse>("/api/v1/settings", {
        schema: settingsResponseSchema,
        signal,
      }),
    refetchInterval: 60_000,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">回放</h1>
        <p className="text-sm text-muted-foreground">
          持久化历史投影(/api/v1/monitor);不是实时撮合回放,也不能提交工作
        </p>
      </header>

      <DegradedBanner
        banner={{
          key: "replay-not-live",
          tone: "neutral",
          title: "这是持久化历史投影,不是实时行情",
          tag: "历史快照",
          message:
            "本页所有数字来自 journal 中最后一次持久化的监控事实;recorded_at 是记录时间,不代表当前外部行情仍然新鲜。",
        }}
      />

      <div className="grid gap-4 lg:grid-cols-2">
        <DataCard
          title="监控历史投影"
          subtitle="/api/v1/monitor · pair 双侧、价差与市场代次的最后一次投影"
        >
          <QueryStateBody query={monitor} skeletonRows={8}>
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
                    message="尚未持久化任何监控事件。"
                    checkedFact="已检查 /api/v1/monitor;缺失行情不会被提升为健康状态。"
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
                  <MonitorLegFacts label="左腿" leg={latest.left} />
                  <MonitorLegFacts label="右腿" leg={latest.right} />
                  <FactRow label="symbol" value={latest.symbol} />
                  <FactRow
                    label="recorded_at(记录时间)"
                    value={formatDateTime(latest.recorded_at)}
                  />
                  <FactRow
                    label="market generation(市场代次)"
                    value={String(latest.market_generation)}
                  />
                  <FactRow label="监控序号" value={String(latest.monitor_sequence)} />
                  <FactRow label="来源序号" value={String(latest.source_sequence)} />
                  <FactRow label="event_id" value={latest.event_id} />
                  {(projection.type === "opportunity" ||
                    projection.type === "no_opportunity") && (
                    <>
                      <FactRow
                        label="方向"
                        value={`${projection.buy_exchange} → ${projection.sell_exchange}`}
                      />
                      <FactRow
                        label="买 / 卖价"
                        value={`${projection.buy_price} / ${projection.sell_price}`}
                      />
                      <FactRow
                        label="绝对价差"
                        value={projection.absolute_spread}
                      />
                      <FactRow
                        label="价差 / 阈值"
                        value={`${projection.spread_percent}% / ${projection.threshold_percent}%`}
                      />
                    </>
                  )}
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

        <div className="space-y-4">
          <DataCard
            title="历史投影水位"
            subtitle="每行标出一个只读投影最后保留的事实,确认页面实际描述的时间范围"
          >
            <QueryStateBody query={monitor} skeletonRows={4}>
              {(model) => (
                <div className="space-y-2">
                  <FactRow label="monitor journal" value={model.journal_id} />
                  <FactRow
                    label="monitor 头序号"
                    value={
                      model.journal_head_sequence === null
                        ? "空 journal"
                        : String(model.journal_head_sequence)
                    }
                  />
                  <FactRow
                    label="monitor 投影"
                    value={humanizeToken(model.projection_status)}
                  />
                  {system.data !== undefined && (
                    <>
                      <FactRow label="system journal" value={system.data.journal_id} />
                      <FactRow
                        label="system 头序号"
                        value={
                          system.data.head_sequence === null
                            ? "空 journal"
                            : String(system.data.head_sequence)
                        }
                      />
                    </>
                  )}
                </div>
              )}
            </QueryStateBody>
          </DataCard>

          <DataCard
            title="回放文件来源"
            subtitle="/api/v1/settings · paper_profiles 声明的 replay fixture 与配置文件"
          >
            <QueryStateBody query={settings} skeletonRows={4}>
              {(data) =>
                data.paper_profiles.length === 0 ? (
                  <EmptyState
                    message="当前运行实例没有配置 Paper profile。"
                    checkedFact="已检查 /api/v1/settings 的 paper_profiles;不会从表单默认值推断后端所有权。"
                  />
                ) : (
                  <ul className="space-y-2">
                    {data.paper_profiles.map((profile) => (
                      <li
                        key={profile.task_id}
                        className="rounded-md border border-border px-3 py-2"
                      >
                        <div className="flex flex-wrap items-center gap-1.5">
                          <StatusPill
                            tone="neutral"
                            label={humanizeToken(profile.kind)}
                          />
                          <span className="numeric text-xs">{profile.task_id}</span>
                        </div>
                        <p className="numeric mt-1 break-all text-xs text-muted-foreground">
                          回放文件:{profile.replay_file}
                        </p>
                        <p className="numeric mt-0.5 break-all text-xs text-muted-foreground">
                          配置:{profile.configuration_files.join(", ")}
                        </p>
                      </li>
                    ))}
                  </ul>
                )
              }
            </QueryStateBody>
          </DataCard>
        </div>
      </div>
    </section>
  );
}
