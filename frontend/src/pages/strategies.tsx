import {
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import {
  useQuery,
  useQueryClient,
  type UseQueryResult,
} from "@tanstack/react-query";
import { asApiError, request } from "../lib/api";
import {
  readOnlyTaskReadModelSchema,
  settingsResponseSchema,
  type ReadOnlyTaskKind,
  type ReadOnlyTaskReadModel,
  type SettingsResponse,
  type SubmitCommandKind,
  type SubmitStatus,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { taskBanners } from "../lib/banners";
import { errorPresentation } from "../lib/errorPresentation";
import { humanizeToken } from "../lib/labels";
import { taskColumns } from "../lib/columns/taskColumns";
import {
  assessSubmissionEffect,
  buildSubmission,
  createSubmitFormState,
  latestTaskForKind,
  postSubmitEnvelope,
  submitActionGate,
  submitFormReducer,
  submitGate,
  submitWriteCapability,
  SubmitProblem,
  type SubmitAction,
  type SubmitFormState,
  type SubmitWriteCapability,
} from "../lib/submit";
import {
  currentSessionGeneration,
  invalidateSession,
  subscribeSessionChanges,
} from "../lib/useOperationEvents";
import { formatDateTime } from "../lib/format";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { DegradedBanner } from "../components/DegradedBanner";
import { EmptyState } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import { DataAsOf, QueryStateBody } from "../components/QueryStateBody";

interface StrategyControlDefinition {
  formKey: "grid" | "arbitrage";
  title: string;
  taskKind: ReadOnlyTaskKind;
  startCommandKind: "start_paper_grid" | "start_paper_arbitrage";
  startLabel: string;
  defaultStrategyId: string;
  summary: string;
}

const DEFAULT_STRATEGY_REVISION = "2026-07-25";

const STRATEGY_CONTROL_DEFS: readonly StrategyControlDefinition[] = [
  {
    formKey: "grid",
    title: "网格 Paper 任务",
    taskKind: "grid_paper",
    startCommandKind: "start_paper_grid",
    startLabel: "启动网格",
    defaultStrategyId: "paper-grid",
    summary:
      "只提交 Paper 网格任务的 start/stop/cancel,不提供 live、reconcile 或下单权限。",
  },
  {
    formKey: "arbitrage",
    title: "套利 Paper 任务",
    taskKind: "arbitrage_paper",
    startCommandKind: "start_paper_arbitrage",
    startLabel: "启动套利",
    defaultStrategyId: "paper-arbitrage",
    summary:
      "只提交 Paper 套利任务的 start/stop/cancel,不开放实盘、对账或下单控制。",
  },
];

function commandLabel(kind: SubmitCommandKind | null): string {
  switch (kind) {
    case "start_paper_grid":
      return "启动网格";
    case "start_paper_arbitrage":
      return "启动套利";
    case "stop_task":
      return "停止任务";
    case "cancel_task":
      return "取消任务";
    default:
      return "受信 submit";
  }
}

function receiptTone(status: SubmitStatus): "ok" | "warning" | "danger" {
  switch (status) {
    case "applied":
      return "ok";
    case "outcome_unknown":
      return "warning";
    default:
      return "danger";
  }
}

function actionLabel(action: SubmitAction, startLabel: string): string {
  return action === "start" ? startLabel : action === "stop" ? "停止任务" : "取消任务";
}

/** 生效判定三态的可读呈现。 */
function EffectAssessment({
  form,
  definition,
  tasks,
}: {
  form: SubmitFormState;
  definition: StrategyControlDefinition;
  tasks: ReadOnlyTaskReadModel | undefined;
}) {
  const submission = form.lastSubmission;
  const effect = assessSubmissionEffect(form, submission, tasks, definition.taskKind);
  if (effect === null || form.lastReceipt === null) {
    return null;
  }
  if (effect === "outcome_unknown_locked") {
    return (
      <DegradedBanner
        banner={{
          key: `${definition.formKey}-outcome-unknown`,
          tone: "warning",
          title: "提交结果不明,表单已锁定",
          tag: "outcome_unknown",
          message:
            "指令已写入 durable journal,但结果仍需在 /api/v1/tasks 投影中确认;确认前本表单不会生成新指令,修改 task_id / strategy_id / strategy_revision 可显式开启新命令。",
        }}
      />
    );
  }
  return (
    <p className="text-xs text-muted-foreground" data-testid="effect-assessment">
      生效判定:
      {effect === "projection_advanced"
        ? "已生效 —— 任务投影指纹已相对提交前基线变化。"
        : "未观察到变化 —— 任务投影指纹仍与提交前基线一致,等待投影推进。"}
    </p>
  );
}

function SubmitResult({ form }: { form: SubmitFormState }) {
  if (form.lastError !== null) {
    return (
      <div className="space-y-2 rounded-md border border-safe-danger/40 bg-safe-danger/5 px-3 py-2">
        <div className="flex flex-wrap gap-1.5">
          <StatusPill tone="danger" label={commandLabel(form.lastAction)} />
          <StatusPill tone="warning" label={form.lastError.code} />
        </div>
        <p className="text-xs">{form.lastError.message}</p>
        {form.pendingSubmission !== null && (
          <>
            <FactRow
              label="command_id"
              value={form.pendingSubmission.envelope.command_id}
            />
            <FactRow
              label="idempotency_key"
              value={form.pendingSubmission.envelope.idempotency_key}
            />
            <FactRow
              label="task_id"
              value={form.pendingSubmission.envelope.target_task_id}
            />
            <p className="text-xs text-muted-foreground">
              当前错误仍保留同一个 pending envelope;只要 task_id / strategy_id /
              strategy_revision 不变,下一次同动作会复用相同的 command_id 和
              idempotency_key。
            </p>
          </>
        )}
      </div>
    );
  }
  if (form.lastReceipt !== null) {
    return (
      <div className="space-y-2 rounded-md border border-border px-3 py-2">
        <div className="flex flex-wrap gap-1.5">
          <StatusPill tone="neutral" label={commandLabel(form.lastAction)} />
          <StatusPill
            tone={receiptTone(form.lastReceipt.status)}
            label={humanizeToken(form.lastReceipt.status)}
          />
        </div>
        <FactRow label="command_id" value={form.lastReceipt.command_id} />
        <FactRow label="task_id" value={form.lastReceipt.target_task_id} />
        <FactRow
          label="journal_projection"
          value={form.lastReceipt.journal_projection}
        />
        <FactRow label="source" value={form.lastReceipt.source} />
        {form.lastReceipt.status === "outcome_unknown" && (
          <p className="text-xs text-muted-foreground">
            outcome_unknown 表示 submit 已被 durable journal
            接纳,但结果仍需回到任务与执行账本中人工核对。
          </p>
        )}
      </div>
    );
  }
  return (
    <EmptyState
      message="尚未提交受信 Paper 指令。"
      checkedFact="只允许 start_paper_grid、start_paper_arbitrage、stop_task 和 cancel_task 四种 lifecycle 动作。"
    />
  );
}

function TextField({
  label,
  value,
  placeholder,
  disabled,
  onChange,
}: {
  label: string;
  value: string;
  placeholder: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block min-w-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <input
        type="text"
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        autoComplete="off"
        spellCheck={false}
        onChange={(event) => onChange(event.target.value)}
        className="numeric mt-1 min-h-10 w-full rounded-md border border-border bg-card px-3 py-2 text-sm disabled:opacity-60"
      />
    </label>
  );
}

function StrategyControlCard({
  definition,
  capability,
  tasks,
  settings,
}: {
  definition: StrategyControlDefinition;
  capability: SubmitWriteCapability;
  tasks: UseQueryResult<ReadOnlyTaskReadModel>;
  settings: SettingsResponse;
}) {
  const queryClient = useQueryClient();
  const profile =
    settings.paper_profiles.find(
      (candidate) =>
        candidate.kind === (definition.formKey === "grid" ? "grid" : "arbitrage"),
    ) ?? null;
  const [form, dispatch] = useReducer(
    submitFormReducer,
    {
      taskId: profile?.task_id ?? "",
      strategyId: profile?.strategy_id ?? definition.defaultStrategyId,
      strategyRevision: profile?.strategy_revision ?? DEFAULT_STRATEGY_REVISION,
    },
    createSubmitFormState,
  );
  /** stop / cancel 的二次确认(文案含任务身份);null = 无待确认动作。 */
  const [confirmingAction, setConfirmingAction] = useState<SubmitAction | null>(null);
  const formRef = useRef(form);
  formRef.current = form;

  // 认证变更:清瞬态但保留 pending envelope 身份与 outcome_unknown 锁。
  useEffect(
    () =>
      subscribeSessionChanges(() => {
        dispatch({ type: "auth_reset" });
        setConfirmingAction(null);
      }),
    [],
  );

  // 每次任务投影刷新都尝试解锁 outcome_unknown(仅信任 complete 投影)。
  useEffect(() => {
    if (tasks.data !== undefined) {
      dispatch({
        type: "tasks_projection",
        model: tasks.data,
        taskKind: definition.taskKind,
      });
    }
  }, [tasks.data, definition.taskKind]);

  const gate = submitGate(form, capability, tasks.data);
  const latestTask = latestTaskForKind(tasks.data, definition.taskKind);
  const targetTask =
    tasks.data?.tasks.find(
      (task) =>
        task.task_id === form.taskId.trim() && task.kind === definition.taskKind,
    ) ?? null;

  const runSubmit = async (action: SubmitAction): Promise<void> => {
    const generation = currentSessionGeneration();
    const current = formRef.current;
    const actionGate = submitActionGate(
      current,
      capability,
      tasks.data,
      definition.taskKind,
      action,
    );
    if (!actionGate.allowed) {
      if (actionGate.reason === "task_terminal") {
        dispatch({
          type: "submit_failed",
          problem: new SubmitProblem(
            "task_terminal",
            "目标任务已处于持久终态;停止或取消不会再次提交。需要新生命周期时请显式启动。",
          ),
        });
      }
      return;
    }
    let submission;
    try {
      const baselineTask =
        tasks.data?.tasks.find(
          (task) =>
            task.task_id === current.taskId.trim() &&
            task.kind === definition.taskKind,
        ) ?? null;
      submission = buildSubmission({
        form: current,
        action,
        startCommandKind: definition.startCommandKind,
        principalId: capability.principalId,
        baselineTask,
      });
    } catch (error) {
      if (error instanceof SubmitProblem) {
        dispatch({ type: "submit_failed", problem: error });
      }
      return;
    }
    dispatch({ type: "submit_started", submission });
    try {
      const receipt = await postSubmitEnvelope(submission.envelope);
      if (generation !== currentSessionGeneration()) {
        return;
      }
      dispatch({ type: "receipt_received", receipt });
      void queryClient.invalidateQueries({ queryKey: queryKeys.tasks });
      void queryClient.invalidateQueries({ queryKey: ["executions"] });
    } catch (error) {
      if (generation !== currentSessionGeneration()) {
        return;
      }
      const apiError = asApiError(error);
      if (apiError?.kind === "unauthorized") {
        invalidateSession(queryClient);
        return;
      }
      if (error instanceof SubmitProblem) {
        dispatch({ type: "submit_failed", problem: error });
        return;
      }
      if (apiError !== null) {
        const presentation = errorPresentation(apiError);
        dispatch({
          type: "submit_failed",
          problem: new SubmitProblem(
            apiError.kind === "not_found" ? "submit_route_unavailable" : apiError.kind,
            apiError.kind === "not_found"
              ? "后端未开放 /api/v1/submit(HTTP 404);写路径未启用,已保留原 envelope。"
              : presentation.detail,
          ),
        });
        return;
      }
      dispatch({
        type: "submit_failed",
        problem: new SubmitProblem("submit_failed", "提交无法安全完成;已保留原 envelope。"),
      });
    }
  };

  const requestAction = (action: SubmitAction): void => {
    // stop / cancel 是对在跑任务的干预:必须二次确认,且确认文案含任务身份。
    if (action === "stop" || action === "cancel") {
      setConfirmingAction(action);
      return;
    }
    setConfirmingAction(null);
    void runSubmit(action);
  };

  const fieldDisabled = form.inFlight;
  const helpText = form.lockedByOutcomeUnknown
    ? "当前表单因 outcome_unknown 被锁定;请先核对 /api/v1/tasks,或者明确修改 task_id / strategy_id / strategy_revision 后再生成新 ID。"
    : gate.reason === "readback_blocked"
      ? "任务投影尚未就绪或已降级;在 /api/v1/tasks 恢复 complete 之前禁止提交。"
      : targetTask?.phase === "stopped" || targetTask?.phase === "failed"
        ? "目标任务已处于持久终态;停止与取消已禁用。启动会创建新的受信生命周期。"
      : "首次动作会生成 UUID command_id 和幂等键;仅在提交中、网络结果未知或 durable receipt 为 outcome_unknown 时,未修改的同动作重试才复用原身份。已拒绝 / 已写入属于闭合结果,再次点击会生成新指令。";

  return (
    <DataCard title={definition.title} subtitle={definition.summary}>
      {latestTask !== null ? (
        <div className="space-y-1.5">
          <FactRow label="最近 task_id" value={latestTask.task_id} />
          <FactRow label="持久阶段" value={humanizeToken(latestTask.phase)} />
          <FactRow label="恢复判断" value={humanizeToken(latestTask.recovery)} />
          <FactRow label="更新时间" value={formatDateTime(latestTask.updated_at)} />
        </div>
      ) : (
        <EmptyState
          message="尚未观察到这类 Paper 任务的 durable 快照。"
          checkedFact="如需 start / stop / cancel,请显式填写 task_id、strategy_id 和 strategy_revision。"
        />
      )}

      <div className="grid gap-3 sm:grid-cols-3">
        <TextField
          label="任务 ID / task_id"
          value={form.taskId}
          placeholder={profile?.task_id ?? `paper-${definition.formKey}-btc-usdt`}
          disabled={fieldDisabled}
          onChange={(value) =>
            dispatch({ type: "field_changed", field: "taskId", value })
          }
        />
        <TextField
          label="策略 ID / strategy_id"
          value={form.strategyId}
          placeholder={definition.defaultStrategyId}
          disabled={fieldDisabled}
          onChange={(value) =>
            dispatch({ type: "field_changed", field: "strategyId", value })
          }
        />
        <TextField
          label="版本 / strategy_revision"
          value={form.strategyRevision}
          placeholder={DEFAULT_STRATEGY_REVISION}
          disabled={fieldDisabled}
          onChange={(value) =>
            dispatch({ type: "field_changed", field: "strategyRevision", value })
          }
        />
      </div>

      <label className="flex min-h-10 items-center gap-2.5">
        <input
          type="checkbox"
          checked={form.confirmed}
          onChange={(event) =>
            dispatch({ type: "confirm_toggled", confirmed: event.target.checked })
          }
          className="h-4 w-4"
        />
        <span className="text-xs">
          我确认这是 paper_only 指令,只操作 Paper 任务,不启用实盘或对账释放。
        </span>
      </label>

      <p className="text-xs text-muted-foreground">{helpText}</p>

      <div className="flex flex-wrap gap-2">
        {(["start", "stop", "cancel"] as const).map((action) => {
          const busy = form.inFlight && form.pendingAction === action;
          const actionGate = submitActionGate(
            form,
            capability,
            tasks.data,
            definition.taskKind,
            action,
          );
          const lockedToAnotherAction =
            form.pendingSubmission !== null &&
            !form.lockedByOutcomeUnknown &&
            form.pendingSubmission.action !== action;
          return (
            <button
              key={action}
              type="button"
              disabled={!actionGate.allowed || lockedToAnotherAction}
              aria-busy={busy || undefined}
              onClick={() => requestAction(action)}
              className="min-h-10 rounded-md border border-border px-3 py-1.5 text-sm transition-colors hover:bg-muted disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
            >
              {busy ? "提交中…" : actionLabel(action, definition.startLabel)}
            </button>
          );
        })}
      </div>

      {confirmingAction !== null && (
        <div
          role="alertdialog"
          aria-label="确认任务干预"
          className="space-y-2 rounded-md border border-safe-warning/50 bg-safe-warning/10 px-3 py-2"
        >
          <p className="text-sm">
            确认对任务{" "}
            <span className="numeric font-medium">{form.taskId || "(未填写)"}</span>
            ({definition.title})执行「
            {confirmingAction === "stop" ? "停止任务" : "取消任务"}」?
          </p>
          <p className="text-xs text-muted-foreground">
            该指令会写入 durable journal 并计入审计账本;结果以 /api/v1/tasks
            投影为准。
          </p>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              disabled={
                !submitActionGate(
                  form,
                  capability,
                  tasks.data,
                  definition.taskKind,
                  confirmingAction,
                ).allowed
              }
              onClick={() => {
                const action = confirmingAction;
                setConfirmingAction(null);
                void runSubmit(action);
              }}
              className="min-h-10 rounded-md border border-safe-warning/60 bg-safe-warning/20 px-3 py-1.5 text-sm transition-colors hover:bg-safe-warning/30 focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
            >
              确认{confirmingAction === "stop" ? "停止" : "取消"}
            </button>
            <button
              type="button"
              onClick={() => setConfirmingAction(null)}
              className="min-h-10 rounded-md border border-border px-3 py-1.5 text-sm transition-colors hover:bg-muted focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
            >
              返回
            </button>
          </div>
        </div>
      )}

      <SubmitResult form={form} />
      <EffectAssessment form={form} definition={definition} tasks={tasks.data} />
    </DataCard>
  );
}

export function Component() {
  const tasks = useQuery({
    queryKey: queryKeys.tasks,
    queryFn: ({ signal }) =>
      request<ReadOnlyTaskReadModel>("/api/v1/tasks", {
        schema: readOnlyTaskReadModelSchema,
        signal,
      }),
    refetchInterval: 15_000,
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

  const capability = submitWriteCapability(settings.data);
  const sortedTasks = useMemo(
    () =>
      [...(tasks.data?.tasks ?? [])].sort(
        (left, right) => right.last_sequence - left.last_sequence,
      ),
    [tasks.data],
  );

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">策略</h1>
        <p className="text-sm text-muted-foreground">
          策略运行面:任务投影明细与受信 Paper submit 控制;只允许 Grid / Arbitrage
          的 paper task lifecycle,不开放 live、reconcile 或下单控制
        </p>
      </header>

      <DataCard
        title="只读连续任务明细"
        subtitle="/api/v1/tasks · 最后持久阶段、双源健康与事件计数;running / stopping 只代表 journal 最后记录,不证明进程仍存活"
      >
        <QueryStateBody query={tasks} skeletonRows={6}>
          {(model) => (
            <div className="space-y-3">
              {taskBanners(model).map((banner) => (
                <DegradedBanner key={banner.key} banner={banner} />
              ))}
              {sortedTasks.length === 0 ? (
                <EmptyState
                  message="尚未投影出连续任务登记。"
                  checkedFact="已检查 /api/v1/tasks;缺失事实不会被解释成健康。"
                />
              ) : (
                <DataTable
                  columns={taskColumns}
                  rows={sortedTasks}
                  rowKey={(task) => task.task_id}
                  ariaLabel="只读连续任务明细,可横向滚动"
                  minWidth="56rem"
                />
              )}
              <FactRow label="任务投影" value={humanizeToken(model.projection_status)} />
              <FactRow label="无效事件" value={String(model.invalid_event_count)} />
              <DataAsOf updatedAt={tasks.dataUpdatedAt} />
            </div>
          )}
        </QueryStateBody>
      </DataCard>

      {settings.isSuccess && !capability.enabled && (
        <DataCard
          title="Paper 任务控制"
          subtitle="/api/v1/submit · 写路径由后端显式开启"
        >
          <EmptyState
            message="后端未启用 Paper 写路径,本页保持只读。"
            checkedFact="已检查 /api/v1/settings:paper_principal_id 未发布(服务端未以 --enable-paper-writes 启动)。"
          />
        </DataCard>
      )}
      {settings.isPending && (
        <DataCard title="Paper 任务控制" subtitle="/api/v1/submit">
          <p className="text-xs text-muted-foreground">
            正在读取 /api/v1/settings 以探测写能力…
          </p>
        </DataCard>
      )}
      {settings.isError && (
        <DataCard title="Paper 任务控制" subtitle="/api/v1/submit">
          <EmptyState
            message="无法读取 /api/v1/settings,不能探测写能力;写控件保持关闭。"
            checkedFact={errorPresentation(settings.error).detail}
          />
        </DataCard>
      )}

      {capability.enabled && settings.data !== undefined && (
        <>
          <p className="text-xs text-muted-foreground">
            所有动作都通过 POST /api/v1/submit 发送 SubmitEnvelope v1,principal_id=
            <span className="numeric">{capability.principalId}</span>
            ,role=paper_operator,risk_confirmation=paper_only;结果只能从 durable
            journal 投影回读。
          </p>
          <div className="grid gap-4 xl:grid-cols-2">
            {STRATEGY_CONTROL_DEFS.map((definition) => (
              <StrategyControlCard
                key={definition.formKey}
                definition={definition}
                capability={capability}
                tasks={tasks}
                settings={settings.data}
              />
            ))}
          </div>
        </>
      )}
    </section>
  );
}
