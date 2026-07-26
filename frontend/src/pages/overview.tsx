import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { asApiError, request } from "../lib/api";
import {
  healthResponseSchema,
  systemResponseSchema,
  type HealthResponse,
  type ProjectionStatus,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { StatusPill } from "../components/StatusPill";

const TIME_FORMAT = new Intl.DateTimeFormat("zh-CN", {
  hour12: false,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const PROJECTION_LABELS: Record<
  ProjectionStatus,
  { label: string; tone: "ok" | "warning" | "danger" }
> = {
  complete: { label: "完整", tone: "ok" },
  windowed: { label: "窗口化", tone: "warning" },
  degraded: { label: "降级", tone: "danger" },
};

/** 把归一化 API 错误转成脱敏、可辨别的一等状态文案。 */
function errorPresentation(error: unknown): {
  label: string;
  tone: "warning" | "danger" | "neutral";
  detail: string;
} {
  const apiError = asApiError(error);
  switch (apiError?.kind) {
    case "unauthorized":
      return {
        label: "需要令牌",
        tone: "warning",
        detail: "后端要求 bearer 认证(HTTP 401),令牌只保存在内存中",
      };
    case "unavailable":
      return {
        label: "暂不可用",
        tone: "warning",
        detail: "operation journal 暂时不可读(HTTP 503)",
      };
    case "rate_limited":
      return {
        label: "限流",
        tone: "warning",
        detail: "已达到本地 API 请求上限,稍后自动重试",
      };
    case "network":
      return {
        label: "无法连接",
        tone: "danger",
        detail: "无法连接 127.0.0.1:8787,后端可能未运行",
      };
    case "invalid_body":
      return {
        label: "响应异常",
        tone: "danger",
        detail: "响应不符合已知 schema_version",
      };
    default:
      return {
        label: "错误",
        tone: "danger",
        detail: apiError?.code ?? "请求失败",
      };
  }
}

function CardFrame({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-5">
      <h2 className="text-sm font-medium text-muted-foreground">{title}</h2>
      <div className="mt-3 space-y-2">{children}</div>
    </div>
  );
}

function Fact({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-4">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="numeric break-all text-right text-sm">{value}</span>
    </div>
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

  return (
    <section className="mx-auto max-w-5xl space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">总览</h1>
        <p className="text-sm text-muted-foreground">
          journal 投影的系统事实;本页数据是持久化投影,不代表外部行情新鲜度
        </p>
      </header>

      <div className="grid gap-4 md:grid-cols-2">
        <CardFrame title="就绪探针 /api/v1/health">
          {health.isPending && <StatusPill tone="neutral" label="正在加载" />}
          {health.isError && (
            <>
              <StatusPill
                tone={errorPresentation(health.error).tone}
                label={errorPresentation(health.error).label}
              />
              <p className="text-xs text-muted-foreground">
                {errorPresentation(health.error).detail}
              </p>
            </>
          )}
          {health.isSuccess && (
            <>
              <div className="flex items-center gap-2">
                <StatusPill tone="ok" label="就绪" />
                <StatusPill
                  tone={health.data.live_trading_enabled ? "warning" : "neutral"}
                  label={
                    health.data.live_trading_enabled
                      ? "live 已声明"
                      : "LIVE CLOSED"
                  }
                />
              </div>
              <Fact
                label="数据截至"
                value={TIME_FORMAT.format(new Date(health.dataUpdatedAt))}
              />
            </>
          )}
        </CardFrame>

        <CardFrame title="系统摘要 /api/v1/system">
          {system.isPending && <StatusPill tone="neutral" label="正在加载" />}
          {system.isError && (
            <>
              <StatusPill
                tone={errorPresentation(system.error).tone}
                label={errorPresentation(system.error).label}
              />
              <p className="text-xs text-muted-foreground">
                {errorPresentation(system.error).detail}
              </p>
            </>
          )}
          {system.isSuccess && (
            <>
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">投影状态</span>
                <StatusPill
                  tone={PROJECTION_LABELS[system.data.projection_status].tone}
                  label={PROJECTION_LABELS[system.data.projection_status].label}
                />
              </div>
              <Fact label="journal_id" value={system.data.journal_id} />
              <Fact
                label="generation(头序列)"
                value={
                  system.data.head_sequence === null
                    ? "空 journal"
                    : String(system.data.head_sequence)
                }
              />
              <Fact
                label="执行批次 / 需恢复 / 冲突"
                value={`${system.data.execution_batch_count} / ${system.data.recovery_required_count} / ${system.data.conflict_count}`}
              />
              <Fact
                label="数据截至"
                value={TIME_FORMAT.format(new Date(system.dataUpdatedAt))}
              />
              <p className="text-xs text-muted-foreground">
                「数据截至」为本页读取投影的时间,不代表外部行情时间
              </p>
            </>
          )}
        </CardFrame>
      </div>
    </section>
  );
}
