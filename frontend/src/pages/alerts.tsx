import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  priceAlertReadModelSchema,
  type PriceAlertReadModel,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import {
  alertProjectionLabel,
  isTrustedAlertProjection,
  visibleAlertOccurrences,
} from "../lib/alerts";
import { alertBanners } from "../lib/banners";
import { errorPresentation } from "../lib/errorPresentation";
import { formatDateTime } from "../lib/format";
import { humanizeToken } from "../lib/labels";
import { alertColumns } from "../lib/columns/alertColumns";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState, SkeletonRows } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";

function ProjectionCard({
  model,
  updatedAt,
}: {
  model: PriceAlertReadModel;
  updatedAt: number;
}) {
  const trusted = isTrustedAlertProjection(model);
  const occurrences = visibleAlertOccurrences(model);
  return (
    <DataCard
      title="预警投影状态"
      subtitle="只消费有界 read model;窗口化、降级、部分尾记录与确认状态都在这里显式说明"
    >
      <div className="grid gap-x-6 gap-y-2 sm:grid-cols-2">
        <FactRow
          label="投影"
          numeric={false}
          value={
            <StatusPill
              tone={trusted ? (model.projection_status === "windowed" ? "warning" : "ok") : "danger"}
              label={alertProjectionLabel(model)}
            />
          }
        />
        <FactRow
          label="可展示 occurrence"
          value={trusted ? String(occurrences.length) : "已隐藏"}
        />
        <FactRow label="无效事件" value={String(model.invalid_event_count)} />
        <FactRow
          label="窗口截断"
          value={model.occurrences_truncated ? "是" : "否(未发生窗口截断)"}
        />
        <FactRow label="边界" value={humanizeToken(model.boundary.kind)} />
        <FactRow
          label="头序号"
          value={
            model.journal_head_sequence === null
              ? "空 journal"
              : String(model.journal_head_sequence)
          }
        />
        <FactRow label="规则定义" value="当前投影未提供" numeric={false} />
        <FactRow label="冷却状态" value="当前投影未提供" numeric={false} />
      </div>
      <FactRow
        label="数据截至"
        value={updatedAt === 0 ? "--" : formatDateTime(updatedAt)}
      />
      <p className="text-xs text-muted-foreground">
        「数据截至」为本页读取投影的时间;预警是持久化历史事实,不代表外部行情新鲜度
      </p>
    </DataCard>
  );
}

export function Component() {
  const alerts = useQuery({
    queryKey: queryKeys.alerts,
    queryFn: ({ signal }) =>
      request<PriceAlertReadModel>("/api/v1/alerts", {
        schema: priceAlertReadModelSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });

  const model = alerts.data;
  const banners = alertBanners(model, { refreshFailed: alerts.isError });
  const occurrences =
    model !== undefined ? [...visibleAlertOccurrences(model)].reverse() : [];
  const errorState =
    alerts.isError && model === undefined ? errorPresentation(alerts.error) : null;

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">预警</h1>
        <p className="text-sm text-muted-foreground">
          价格预警明细(/api/v1/alerts);occurrence 窗口有界,降级即停止展示不可信结果
        </p>
      </header>

      {banners.map((banner) => (
        <DegradedBanner key={banner.key} banner={banner} />
      ))}

      {alerts.isPending && (
        <DataCard title="预警投影状态" subtitle="正在读取有界 read model">
          <SkeletonRows rows={8} />
        </DataCard>
      )}
      {errorState !== null && (
        <DataCard title="预警投影状态" subtitle="/api/v1/alerts 读取失败">
          <StatusPill tone={errorState.tone} label={errorState.label} />
          <p className="text-xs text-muted-foreground">{errorState.detail}</p>
        </DataCard>
      )}

      {model !== undefined && (
        <>
          <ProjectionCard model={model} updatedAt={alerts.dataUpdatedAt} />

          <DataCard
            title="预警 occurrence"
            subtitle="按 occurrence 序号倒序展示触发时间、种类、severity、确认与各 adapter 的最后已记录投递状态;不暴露写入口"
          >
            {!isTrustedAlertProjection(model) ? (
              <p className="text-xs text-muted-foreground">
                预警投影未通过完整性校验;界面不会把不可信的最近预警提升成可操作事实。
              </p>
            ) : occurrences.length === 0 ? (
              <EmptyState
                message="当前冻结快照中还没有价格预警 occurrence。"
                checkedFact="已检查 /api/v1/alerts 返回的 occurrences;没有写路径会在这里补造状态。"
              />
            ) : (
              <DataTable
                columns={alertColumns}
                rows={occurrences}
                rowKey={(occurrence) => String(occurrence.alert_sequence)}
                ariaLabel="价格预警明细,可横向滚动"
                minWidth="56rem"
              />
            )}
          </DataCard>
        </>
      )}
    </section>
  );
}
