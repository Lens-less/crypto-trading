import { useEffect, useMemo, useReducer, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { request } from "../lib/api";
import {
  executionsResponseSchema,
  type ExecutionBatchView,
  type ExecutionsResponse,
  type ReadModelWarning,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { executionBanners } from "../lib/banners";
import {
  cursorPagerReducer,
  initialCursorPagerState,
} from "../lib/cursorPager";
import { errorPresentation } from "../lib/errorPresentation";
import { formatDateTime, formatOptionalNumber } from "../lib/format";
import { humanizeToken } from "../lib/labels";
import {
  executionColumns,
  toneForBatchState,
  toneForPhase,
  toneForRecovery,
} from "../lib/columns/executionColumns";
import { noticeColumns } from "../lib/columns/noticeColumns";
import type { ColumnDef } from "../lib/columns/types";
import { CursorPager } from "../components/CursorPager";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { DegradedBanner } from "../components/DegradedBanner";
import { DetailDrawer } from "../components/DetailDrawer";
import { EmptyState, SkeletonRows } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";

type BatchFilter =
  | "all"
  | "attention"
  | "completed"
  | "partial"
  | "failed"
  | "conflict"
  | "unknown";

const BATCH_FILTERS: ReadonlyArray<{ id: BatchFilter; label: string }> = [
  { id: "all", label: "全部批次" },
  { id: "attention", label: "需要关注" },
  { id: "completed", label: "已完成" },
  { id: "partial", label: "部分完成" },
  { id: "failed", label: "失败" },
  { id: "conflict", label: "冲突" },
  { id: "unknown", label: "结果未知" },
];

function matchesBatchFilter(batch: ExecutionBatchView, filter: BatchFilter): boolean {
  switch (filter) {
    case "attention":
      return batch.recovery !== "none" || batch.state === "conflict";
    case "completed":
      return batch.state === "completed";
    case "partial":
      return batch.state === "partial";
    case "failed":
      return batch.state === "failed";
    case "conflict":
      return batch.state === "conflict";
    case "unknown":
      return batch.state === "outcome_unknown";
    default:
      return true;
  }
}

/** 抽屉:计划与结果事实(展示批次的全部投影字段)。 */
function DrawerFacts({ batch }: { batch: ExecutionBatchView }) {
  const facts: ReadonlyArray<[string, string]> = [
    ["策略", batch.strategy],
    ["交易对", batch.symbol],
    ["首个序号", String(batch.first_sequence)],
    ["最新序号", String(batch.last_sequence)],
    ["首次观察", formatDateTime(batch.first_seen_at)],
    ["最近更新", formatDateTime(batch.updated_at)],
    ["计划时间", formatDateTime(batch.planned_at)],
    ["结果时间", formatDateTime(batch.outcome_at)],
    ["交易腿数量", formatOptionalNumber(batch.leg_count)],
    ["回执数量", formatOptionalNumber(batch.receipt_count)],
    ["预期回执", formatOptionalNumber(batch.expected_receipt_count)],
    ["失败索引", formatOptionalNumber(batch.failed_index)],
    ["未尝试数量", formatOptionalNumber(batch.unattempted_count)],
    ["对账观察", formatOptionalNumber(batch.reconciliation_observation_count)],
    ["对账错误", formatOptionalNumber(batch.reconciliation_error_count)],
    ["已记录失败", batch.failure_recorded ? "是" : "否"],
  ];
  return (
    <section aria-label="计划与结果事实" className="space-y-1.5">
      <h3 className="text-xs font-medium text-muted-foreground">计划与结果事实</h3>
      {facts.map(([label, value]) => (
        <FactRow key={label} label={label} value={value} />
      ))}
      <p className="text-xs text-muted-foreground">
        状态摘要:{batch.status_summary !== "" ? batch.status_summary : "--"}
      </p>
    </section>
  );
}

/** 抽屉:事件封套元数据(envelope / 投影水位)。 */
function DrawerEnvelope({ data }: { data: ExecutionsResponse }) {
  return (
    <section aria-label="投影封套元数据" className="space-y-1.5">
      <h3 className="text-xs font-medium text-muted-foreground">投影封套元数据</h3>
      <FactRow label="schema_version" value={String(data.schema_version)} />
      <FactRow label="journal_id" value={data.operator.journal_id} />
      <FactRow
        label="head_sequence"
        value={
          data.operator.head_sequence === null
            ? "空 journal"
            : String(data.operator.head_sequence)
        }
      />
      <FactRow label="head_event_id" value={data.operator.head_event_id ?? "--"} />
      <FactRow
        label="投影状态"
        numeric={false}
        value={
          <StatusPill
            tone={
              data.operator.projection_status === "complete"
                ? "ok"
                : data.operator.projection_status === "windowed"
                  ? "warning"
                  : "danger"
            }
            label={humanizeToken(data.operator.projection_status)}
          />
        }
      />
      <FactRow label="变更页边界" value={humanizeToken(data.changes.boundary.kind)} />
    </section>
  );
}

function DrawerPhases({ batch }: { batch: ExecutionBatchView }) {
  return (
    <section aria-label="持久化阶段带" className="space-y-1.5">
      <h3 className="text-xs font-medium text-muted-foreground">持久化阶段带</h3>
      {batch.phases.length === 0 ? (
        <EmptyState
          message="这个批次没有投影出持久化阶段。"
          checkedFact="批次存在,但缺少阶段证据。"
        />
      ) : (
        <ol className="space-y-1">
          {batch.phases.map((phase, index) => (
            <li
              key={`${phase}-${index}`}
              className="flex items-center justify-between gap-3"
            >
              <span className="text-xs text-muted-foreground">阶段 {index + 1}</span>
              <StatusPill tone={toneForPhase(phase)} label={humanizeToken(phase)} />
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

function DrawerWarnings({
  batch,
  warnings,
}: {
  batch: ExecutionBatchView;
  warnings: ReadModelWarning[];
}) {
  const scoped = warnings.filter((warning) => warning.batch_id === batch.batch_id);
  return (
    <section aria-label="批次范围警告" className="space-y-1.5">
      <h3 className="text-xs font-medium text-muted-foreground">批次范围警告</h3>
      {scoped.length === 0 ? (
        <EmptyState
          message="当前批次没有关联的投影警告。"
          checkedFact="已按 batch_id 筛选并检查 operator.warnings。"
        />
      ) : (
        <ul className="space-y-1">
          {scoped.map((warning, index) => (
            <li key={index} className="flex items-center justify-between gap-3">
              <span className="text-xs">{humanizeToken(warning.code)}</span>
              <span className="numeric text-xs text-muted-foreground">
                {warning.sequence !== null ? `#${warning.sequence}` : "--"}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export function Component() {
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const rawFilter = searchParams.get("state");
  const filter: BatchFilter = BATCH_FILTERS.some((entry) => entry.id === rawFilter)
    ? (rawFilter as BatchFilter)
    : "all";
  const selectedBatchId = searchParams.get("batch");

  const executions = useQuery({
    queryKey: queryKeys.executions(null),
    queryFn: ({ signal }) =>
      request<ExecutionsResponse>("/api/v1/executions", {
        schema: executionsResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });

  // 通知分页:不透明游标只存内存(reducer state),永不进入 URL。
  const [pager, dispatch] = useReducer(cursorPagerReducer, initialCursorPagerState);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState<unknown>(null);

  useEffect(() => {
    if (executions.data !== undefined) {
      dispatch({ type: "apply_page", page: executions.data.changes });
    }
  }, [executions.data]);

  const loadMore = async (): Promise<void> => {
    const cursor = pager.nextCursor;
    if (cursor === null || loadingMore) {
      return;
    }
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const page = await request<ExecutionsResponse>(
        `/api/v1/executions?cursor=${encodeURIComponent(cursor)}`,
        { schema: executionsResponseSchema },
      );
      dispatch({ type: "apply_page", page: page.changes });
    } catch (error) {
      setLoadMoreError(error);
      if (errorPresentation(error).cursorInvalidated) {
        // 游标过期/无效:丢弃本地游标状态并整体失效,走既有失效协议重建。
        dispatch({ type: "reset" });
        void queryClient.invalidateQueries({ queryKey: ["executions"] });
      }
    } finally {
      setLoadingMore(false);
    }
  };

  const operator = executions.data?.operator;
  const batches = useMemo(
    () => (operator?.batches ?? []).filter((batch) => matchesBatchFilter(batch, filter)),
    [operator, filter],
  );
  const selectedBatch =
    selectedBatchId !== null
      ? (operator?.batches.find((batch) => batch.batch_id === selectedBatchId) ?? null)
      : null;

  const closeDrawer = (): void => {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        next.delete("batch");
        return next;
      },
      { replace: true },
    );
  };
  const openDrawer = (batch: ExecutionBatchView): void => {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        next.set("batch", batch.batch_id);
        return next;
      },
      { replace: true },
    );
  };

  const columns: ColumnDef<ExecutionBatchView>[] = [
    ...executionColumns,
    {
      id: "inspect",
      header: "检查",
      cell: (batch) => (
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            openDrawer(batch);
          }}
          className="min-h-10 rounded-md border border-border px-3 py-1.5 text-xs transition-colors hover:bg-muted focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
        >
          {selectedBatchId === batch.batch_id ? "已选择" : "打开详情"}
        </button>
      ),
    },
  ];

  const banners = executionBanners(operator);
  const errorState =
    executions.isError && executions.data === undefined
      ? errorPresentation(executions.error)
      : null;
  const loadMorePresentation =
    loadMoreError !== null ? errorPresentation(loadMoreError) : null;

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">执行</h1>
        <p className="text-sm text-muted-foreground">
          有界执行账本(/api/v1/executions);持久化投影,不构造任何交易权限
        </p>
      </header>

      {banners.map((banner) => (
        <DegradedBanner key={banner.key} banner={banner} />
      ))}
      {executions.isError && executions.data !== undefined && (
        <DegradedBanner
          banner={{
            key: "executions-refresh-failed",
            tone: "warning",
            title: "执行快照刷新失败",
            tag: "保留旧快照",
            message:
              "最后一个通过校验的执行快照仍然可见;重新读取成功前,不把它解释为最新状态。",
          }}
        />
      )}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto]">
        <div className="min-w-0 space-y-6">
          <DataCard
            title="执行账本"
            subtitle="全宽账本;选择批次后在右侧抽屉检查计划与恢复事实"
          >
            <div className="flex flex-wrap items-center gap-2">
              <label htmlFor="batch-filter" className="text-xs text-muted-foreground">
                执行状态
              </label>
              <select
                id="batch-filter"
                value={filter}
                onChange={(event) => {
                  setSearchParams(
                    (previous) => {
                      const next = new URLSearchParams(previous);
                      if (event.target.value === "all") {
                        next.delete("state");
                      } else {
                        next.set("state", event.target.value);
                      }
                      next.delete("batch");
                      return next;
                    },
                    { replace: true },
                  );
                }}
                className="min-h-10 rounded-md border border-border bg-card px-2 py-1.5 text-sm"
              >
                {BATCH_FILTERS.map((entry) => (
                  <option key={entry.id} value={entry.id}>
                    {entry.label}
                  </option>
                ))}
              </select>
            </div>

            {executions.isPending && <SkeletonRows rows={8} />}
            {errorState !== null && (
              <div className="space-y-2">
                <StatusPill tone={errorState.tone} label={errorState.label} />
                <p className="text-xs text-muted-foreground">{errorState.detail}</p>
              </div>
            )}
            {operator !== undefined &&
              (batches.length === 0 ? (
                <EmptyState
                  message={
                    filter === "all"
                      ? "这个有界快照中没有执行批次。"
                      : "没有执行批次符合当前筛选。"
                  }
                  checkedFact={
                    filter === "all"
                      ? "已检查 /api/v1/executions 返回的 operator.batches。"
                      : "清除筛选,或等待下一次有界投影刷新。"
                  }
                />
              ) : (
                <DataTable
                  columns={columns}
                  rows={batches}
                  rowKey={(batch) => batch.batch_id}
                  ariaLabel="执行账本,可横向滚动"
                  minWidth="64rem"
                  onRowClick={openDrawer}
                  selectedRowKey={selectedBatchId}
                />
              ))}
          </DataCard>

          <DataCard
            title="最近事件通知"
            subtitle="事件页不携带原始载荷;浏览器只读取通知元数据,再重新获取快照。不透明恢复游标只保存在页面内存。"
          >
            {loadMorePresentation !== null && (
              <DegradedBanner
                banner={{
                  key: "load-more-error",
                  tone: loadMorePresentation.cursorInvalidated ? "warning" : "danger",
                  title: loadMorePresentation.cursorInvalidated
                    ? "游标已失效"
                    : "加载更多失败",
                  tag: loadMorePresentation.label,
                  message: loadMorePresentation.detail,
                }}
              />
            )}
            {pager.notices.length === 0 ? (
              <EmptyState
                message="当前游标之后没有新的操作通知。"
                checkedFact={`已检查边界:${humanizeToken(pager.boundary ?? "snapshot_end")}。`}
              />
            ) : (
              <DataTable
                columns={noticeColumns}
                rows={[...pager.notices].reverse()}
                rowKey={(notice) => String(notice.sequence)}
                ariaLabel="最近事件通知,可横向滚动"
                minWidth="40rem"
              />
            )}
            <CursorPager
              state={pager}
              loading={loadingMore}
              onLoadMore={() => void loadMore()}
            />
          </DataCard>
        </div>

        {selectedBatch !== null && executions.data !== undefined && (
          <DetailDrawer
            title="执行详情"
            identifier={selectedBatch.batch_id}
            onClose={closeDrawer}
            headerExtra={
              <div className="flex flex-wrap gap-1.5">
                <StatusPill
                  tone={toneForBatchState(selectedBatch.state)}
                  label={humanizeToken(selectedBatch.state)}
                />
                <StatusPill
                  tone={toneForRecovery(selectedBatch.recovery)}
                  label={humanizeToken(selectedBatch.recovery)}
                />
              </div>
            }
          >
            <DrawerFacts batch={selectedBatch} />
            <DrawerPhases batch={selectedBatch} />
            <DrawerWarnings
              batch={selectedBatch}
              warnings={executions.data.operator.warnings}
            />
            <DrawerEnvelope data={executions.data} />
          </DetailDrawer>
        )}
      </div>
    </section>
  );
}
