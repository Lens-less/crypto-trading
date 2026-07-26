import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  riskResponseSchema,
  systemResponseSchema,
  type AccountRiskOpenPositionView,
  type AccountRiskStateView,
  type PaperAccountSnapshot,
  type PaperReservationView,
  type RiskResponse,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { riskBanners, type BannerDescriptor } from "../lib/banners";
import { sumDecimalStrings } from "../lib/decimal";
import { formatDateTime } from "../lib/format";
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

const openPositionColumns: ColumnDef<AccountRiskOpenPositionView>[] = [
  {
    id: "task",
    header: "任务",
    cell: (row) => <span className="numeric">{row.task_id}</span>,
  },
  {
    id: "symbol",
    header: "标的",
    cell: (row) => <span className="numeric">{row.symbol}</span>,
  },
  {
    id: "opened",
    header: "opened_at(持仓时钟起点)",
    cell: (row) => <span className="numeric">{formatDateTime(row.opened_at)}</span>,
  },
];

/**
 * 账户风控投影横幅:与 paper 账户一致,降级是一等状态;
 * kill switch / pause 事实缺失时不把 scope 解释为健康。
 */
function accountRiskBanners(model: RiskResponse["account_risk"]): BannerDescriptor[] {
  const banners: BannerDescriptor[] = [];
  if (model.projection_status !== "complete") {
    banners.push({
      key: "account-risk-degraded",
      tone: "danger",
      title: "账户风控投影已降级",
      tag: "最后有效事实",
      message:
        "pause、kill switch 与计数只反映最后通过校验的持久事实;修复 journal 前,不把它们解释为当前风控状态。",
    });
  }
  if (model.invalid_event_count > 0) {
    banners.push({
      key: "account-risk-invalid-events",
      tone: "warning",
      title: "存在无效账户风控事件",
      tag: "已拒绝计入",
      message: `journal 中有 ${model.invalid_event_count} 条 account_risk 事件未通过校验,已被拒绝计入投影;状态不包含这些事实。`,
    });
  }
  return banners;
}

/** kill switch 是闩锁事实:engaged 只用安全色 + 文字明示,不提供解除入口。 */
function KillSwitchFact({ scope }: { scope: AccountRiskStateView }) {
  if (scope.kill_switch_engaged) {
    return (
      <FactRow
        label="kill switch"
        numeric={false}
        value={
          <span className="text-xs text-safe-danger">
            已触发(闩锁,不可解除)
            {scope.kill_switch_reason !== null
              ? ` · 原因:${scope.kill_switch_reason}`
              : ""}
          </span>
        }
      />
    );
  }
  return (
    <FactRow
      label="kill switch"
      numeric={false}
      value={<span className="text-xs text-safe-neutral">未触发</span>}
    />
  );
}

function PauseFact({ scope }: { scope: AccountRiskStateView }) {
  if (scope.paused) {
    return (
      <FactRow
        label="暂停状态"
        numeric={false}
        value={
          <span className="text-xs text-safe-warning">
            已暂停(拒绝新准入,存量持仓不受影响)
            {scope.pause_reason !== null ? ` · 原因:${scope.pause_reason}` : ""}
          </span>
        }
      />
    );
  }
  return (
    <FactRow
      label="暂停状态"
      numeric={false}
      value={<span className="text-xs text-safe-neutral">未暂停</span>}
    />
  );
}

function AccountRiskScopeCard({ scope }: { scope: AccountRiskStateView }) {
  return (
    <section
      aria-label={`账户风控 scope ${scope.scope_id}`}
      className="space-y-2 rounded-md border border-border px-3 py-2"
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="numeric text-sm">{scope.scope_id}</span>
        {scope.kill_switch_engaged && (
          <StatusPill tone="danger" label="kill switch 已闩锁" />
        )}
        {scope.paused && <StatusPill tone="warning" label="已暂停" />}
        {!scope.kill_switch_engaged && !scope.paused && (
          <StatusPill tone="neutral" label="准入开放" />
        )}
      </div>
      <KillSwitchFact scope={scope} />
      <PauseFact scope={scope} />
      <FactRow
        label="UTC 交易日(trade_date_utc)"
        value={scope.trade_date_utc ?? "--"}
      />
      <FactRow label="当日准入计数" value={String(scope.daily_trade_count)} />
      <FactRow label="累计准入(admitted_count)" value={String(scope.admitted_count)} />
      <FactRow label="累计拒绝(rejected_count)" value={String(scope.rejected_count)} />
      <FactRow
        label="最近拒绝原因(last_rejection)"
        numeric={false}
        value={
          scope.last_rejection === null ? (
            <span className="text-xs text-safe-neutral">无</span>
          ) : (
            <span className="text-xs">{scope.last_rejection}</span>
          )
        }
      />
      <FactRow
        label="最后记录时间"
        value={
          scope.last_recorded_at === null
            ? "--"
            : formatDateTime(scope.last_recorded_at)
        }
      />
      {scope.open_positions.length === 0 ? (
        <p className="text-xs text-muted-foreground">
          无 open position 时钟;已检查该 scope 的 open_positions 集合。
        </p>
      ) : (
        <DataTable
          columns={openPositionColumns}
          rows={scope.open_positions}
          rowKey={(row) => `${row.task_id}/${row.symbol}`}
          ariaLabel={`scope ${scope.scope_id} 的 open positions,可横向滚动`}
          minWidth="32rem"
        />
      )}
    </section>
  );
}

export function Component() {
  const risk = useQuery({
    queryKey: queryKeys.risk,
    queryFn: ({ signal }) =>
      request<RiskResponse>("/api/v1/risk", {
        schema: riskResponseSchema,
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
          Paper 账户与账户风控读模型(/api/v1/risk);本页只读,不提供释放、恢复或解除
          kill switch 的操作
        </p>
      </header>

      <DataCard
        title="风险总览"
        subtitle="关闭优先:预留、kill switch 或凭证事实缺失时保持不可用,不做推断"
      >
        <QueryStateBody query={risk} skeletonRows={6}>
          {(data) => {
            const model = data.paper_accounts;
            const accounts = model.accounts;
            const scopes = data.account_risk.scopes;
            const engagedScopes = scopes.filter(
              (scope) => scope.kill_switch_engaged,
            );
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
                {scopes.length > 0 ? (
                  <FactRow
                    label="kill switch"
                    numeric={false}
                    value={
                      engagedScopes.length > 0 ? (
                        <span className="text-xs text-safe-danger">
                          已触发(闩锁,不可解除):
                          {engagedScopes.map((scope) => scope.scope_id).join("、")}
                        </span>
                      ) : (
                        <span className="text-xs text-safe-neutral">
                          未触发(来自持久账户风控投影,{scopes.length} 个 scope)
                        </span>
                      )
                    }
                  />
                ) : system.data !== undefined ? (
                  <FactRow
                    label="kill switch"
                    numeric={false}
                    value={
                      <span className="text-xs text-safe-neutral">
                        {humanizeToken(system.data.kill_switch)}
                        (journal 中没有 account_risk 事实;受监督前不声明健康)
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
        title="账户风控(account_risk)"
        subtitle="durable AccountRiskReadModel:pause / kill switch(闩锁)/ UTC 日计数 / 持仓时钟;本页不提供任何解除操作"
      >
        <QueryStateBody query={risk} skeletonRows={5}>
          {(data) => (
            <div className="space-y-3">
              {accountRiskBanners(data.account_risk).map((banner) => (
                <DegradedBanner key={banner.key} banner={banner} />
              ))}
              {data.account_risk.scopes.length === 0 ? (
                <EmptyState
                  message="尚未投影出账户风控事实。"
                  checkedFact="已检查 /api/v1/risk 的 account_risk.scopes;空集合表示 journal 中没有可验证的 account_risk 事实。"
                />
              ) : (
                data.account_risk.scopes.map((scope) => (
                  <AccountRiskScopeCard key={scope.scope_id} scope={scope} />
                ))
              )}
            </div>
          )}
        </QueryStateBody>
      </DataCard>

      <DataCard
        title="Paper 账户"
        subtitle="数值来自 durable PaperAccountReadModel;Money 保持后端给定的规范十进制字符串"
      >
        <QueryStateBody query={risk} skeletonRows={4}>
          {(data) =>
            data.paper_accounts.accounts.length === 0 ? (
              <EmptyState
                message="尚未投影出 Paper 账户。"
                checkedFact="已检查 /api/v1/risk;空集合表示 journal 中没有可验证的 paper_account 事实。"
              />
            ) : (
              <DataTable
                columns={accountColumns}
                rows={data.paper_accounts.accounts}
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
          {(data) => {
            const reservations: ReservationRow[] =
              data.paper_accounts.accounts.flatMap((account) =>
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
