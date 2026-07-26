import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  paperAccountReadModelSchema,
  systemResponseSchema,
  type PaperAccountReadModel,
  type PaperAccountSnapshot,
  type PaperReservationView,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { riskBanners } from "../lib/banners";
import { sumDecimalStrings } from "../lib/decimal";
import { humanizeToken, reservationPhaseLabel } from "../lib/labels";
import type { ColumnDef } from "../lib/columns/types";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import {
  DataAsOf,
  NotProjectedFact,
  QueryStateBody,
} from "../components/QueryStateBody";

const accountColumns: ColumnDef<PaperAccountSnapshot>[] = [
  {
    id: "account",
    header: "账户",
    cell: (account) => <span className="numeric">{account.account_id}</span>,
  },
  {
    id: "initial",
    header: "initial_available",
    numeric: true,
    cell: (account) => <span className="numeric">{account.initial_available}</span>,
  },
  {
    id: "available",
    header: "available",
    numeric: true,
    cell: (account) => <span className="numeric">{account.available}</span>,
  },
  {
    id: "pending",
    header: "pending_reserved",
    numeric: true,
    cell: (account) => <span className="numeric">{account.pending_reserved}</span>,
  },
  {
    id: "uncertain",
    header: "uncertain_reserved",
    numeric: true,
    cell: (account) => <span className="numeric">{account.uncertain_reserved}</span>,
  },
  {
    id: "committed",
    header: "committed_exposure",
    numeric: true,
    cell: (account) => <span className="numeric">{account.committed_exposure}</span>,
  },
];

interface ReservationRow extends PaperReservationView {
  accountId: string;
}

function reconciliationSummary(reservation: PaperReservationView): string {
  const record = reservation.reconciliation;
  if (record === null) {
    return "未对账";
  }
  const outcome = record.outcome === "released" ? "已释放" : "失败";
  return `${outcome} · 证据 #${record.evidence_sequence}`;
}

const reservationColumns: ColumnDef<ReservationRow>[] = [
  {
    id: "identity",
    header: "账户 / 任务",
    cell: (row) => (
      <span className="block">
        <span className="numeric block">{row.accountId}</span>
        <span className="numeric block text-xs text-muted-foreground">
          {row.task_id}
        </span>
      </span>
    ),
  },
  {
    id: "reservation",
    header: "reservation / batch",
    cell: (row) => (
      <span className="block">
        <span className="numeric block text-xs">{row.reservation_id}</span>
        <span className="numeric block text-xs text-muted-foreground">
          {row.batch_id}
        </span>
      </span>
    ),
  },
  {
    id: "phase",
    header: "阶段",
    cell: (row) => (
      <StatusPill tone="neutral" label={reservationPhaseLabel(row.phase)} />
    ),
  },
  {
    id: "legs",
    header: "腿",
    numeric: true,
    cell: (row) => <span className="numeric">{row.legs.length}</span>,
  },
  {
    id: "reserved",
    header: "预留敞口",
    numeric: true,
    cell: (row) => <span className="numeric">{row.reserved_exposure}</span>,
  },
  {
    id: "held",
    header: "当前占用",
    numeric: true,
    cell: (row) => <span className="numeric">{row.held_exposure}</span>,
  },
  {
    id: "reconciliation",
    header: "对账状态",
    cell: (row) => <span className="text-xs">{reconciliationSummary(row)}</span>,
  },
];

export function Component() {
  const risk = useQuery({
    queryKey: queryKeys.risk,
    queryFn: ({ signal }) =>
      request<PaperAccountReadModel>("/api/v1/risk", {
        schema: paperAccountReadModelSchema,
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

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">风险</h1>
        <p className="text-sm text-muted-foreground">
          Paper 账户读模型(/api/v1/risk);本页只读,不提供释放、提交或 reconcile 操作
        </p>
      </header>

      <DataCard
        title="风险总览"
        subtitle="关闭优先:预留、kill switch 或凭证事实缺失时保持不可用,不做推断"
      >
        <QueryStateBody query={risk} skeletonRows={6}>
          {(model) => {
            const accounts = model.accounts;
            const pendingReserved = sumDecimalStrings(
              accounts.map((account) => account.pending_reserved),
            );
            const uncertainReserved = sumDecimalStrings(
              accounts.map((account) => account.uncertain_reserved),
            );
            const committedExposure = sumDecimalStrings(
              accounts.map((account) => account.committed_exposure),
            );
            const totalExposure = sumDecimalStrings([
              pendingReserved,
              uncertainReserved,
              committedExposure,
            ]);
            return (
              <div className="space-y-2">
                {riskBanners(model).map((banner) => (
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
                    label={`投影:${humanizeToken(model.projection_status)}`}
                  />
                </div>
                <FactRow label="账户数" value={String(accounts.length)} />
                <FactRow label="待处理预留(pending_reserved)" value={pendingReserved} />
                <FactRow
                  label="不确定预留(uncertain_reserved)"
                  value={uncertainReserved}
                />
                <FactRow
                  label="已确认敞口(committed_exposure)"
                  value={committedExposure}
                />
                <FactRow label="账户级总敞口" value={totalExposure} />
                {system.data !== undefined ? (
                  <FactRow
                    label="kill switch"
                    numeric={false}
                    value={
                      <span className="text-xs text-safe-neutral">
                        {humanizeToken(system.data.kill_switch)}
                        (后端显式声明 not_available,受监督前不声明健康)
                      </span>
                    }
                  />
                ) : (
                  <NotProjectedFact label="kill switch" />
                )}
                <FactRow label="无效事件" value={String(model.invalid_event_count)} />
                <DataAsOf updatedAt={risk.dataUpdatedAt} />
              </div>
            );
          }}
        </QueryStateBody>
      </DataCard>

      <DataCard
        title="Paper 账户"
        subtitle="数值来自 durable PaperAccountReadModel;Money 保持后端给定的规范十进制字符串"
      >
        <QueryStateBody query={risk} skeletonRows={4}>
          {(model) =>
            model.accounts.length === 0 ? (
              <EmptyState
                message="尚未投影出 Paper 账户。"
                checkedFact="已检查 /api/v1/risk;空集合表示 journal 中没有可验证的 paper_account 事实。"
              />
            ) : (
              <DataTable
                columns={accountColumns}
                rows={model.accounts}
                rowKey={(account) => account.account_id}
                ariaLabel="Paper 账户明细,可横向滚动"
                minWidth="48rem"
              />
            )
          }
        </QueryStateBody>
      </DataCard>

      <DataCard
        title="预留明细(reservations)"
        subtitle="每条 reservation 是有界账本事实;总敞口以上方账户级聚合为准"
      >
        <QueryStateBody query={risk} skeletonRows={4}>
          {(model) => {
            const reservations: ReservationRow[] = model.accounts.flatMap(
              (account) =>
                account.reservations.map((reservation) => ({
                  ...reservation,
                  accountId: account.account_id,
                })),
            );
            if (reservations.length === 0) {
              return (
                <EmptyState
                  message="账户尚无 reservation 事实。"
                  checkedFact="已检查 /api/v1/risk 各账户的 reservations 集合。"
                />
              );
            }
            return (
              <DataTable
                columns={reservationColumns}
                rows={reservations}
                rowKey={(row) => row.reservation_id}
                ariaLabel="Paper 预留明细,可横向滚动"
                minWidth="64rem"
              />
            );
          }}
        </QueryStateBody>
      </DataCard>
    </section>
  );
}
