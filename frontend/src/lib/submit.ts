/**
 * 受信 Paper submit 状态机(纯函数)+ POST /api/v1/submit 封装。
 *
 * 事实来源:rust/crates/control-plane/src/submit.rs(SubmitEnvelope schema)
 * 与 rust/crates/web-app/src/submit.rs(HTTP 语义)。
 *
 * 不可协商语义(逐条移植自已删除的旧原生 JS 前端,见
 * frontend/docs/ui-contract-migration.md;均有测试):
 * - crypto.randomUUID / getRandomValues 不可用 → secure_random_unavailable,
 *   拒绝生成命令(绝不用弱随机源伪造 command_id);
 * - 幂等键与 pendingSubmission 跨认证变更保留(authReset 不清 envelope 身份);
 * - 提交前记录任务投影指纹基线(taskProjectionFingerprint),回执后比对判定生效;
 * - outcome_unknown → 表单锁死重复提交,直到 /api/v1/tasks 投影确认
 *   (指纹推进 + 预期阶段)或操作者显式修改身份字段;
 * - HTTP 422 视为「已受理过」的幂等重放回执,按 receipt 校验后正常消费;
 * - tasks projection_status !== "complete" 时禁止提交(读回被降级即写路径关闭);
 * - bearer token 只存内存(复用 lib/api.ts);
 * - 浏览器只构造 paper_operator / paper_only;Reconciler 面(reconcile_release
 *   / record_reconcile_failure)是 Reconciler 角色专属,UI 永不提供。
 */
import { getBearerToken, ApiRequestError } from "./api";
import {
  SUBMIT_JOURNAL_PROJECTION,
  SUBMIT_JOURNAL_SOURCE,
  SUBMIT_SCHEMA_VERSION,
  submitReceiptSchema,
  type ReadOnlyTaskKind,
  type ReadOnlyTaskReadModel,
  type ReadOnlyTaskView,
  type SettingsResponse,
  type SubmitCommand,
  type SubmitCommandKind,
  type SubmitEnvelope,
  type SubmitReceipt,
} from "./api-types";

export const SUBMIT_ROLE = "paper_operator" as const;
export const SUBMIT_RISK_CONFIRMATION = "paper_only" as const;
const MAX_IDENTITY_BYTES = 128;

/* ------------------------------------------------------------ 写能力探测 */

export interface SubmitWriteCapability {
  enabled: boolean;
  principalId: string | null;
}

/**
 * 写路径探测:与后端契约一致的方式是 settings 投影的 paper_principal_id ——
 * 组合根仅在 --enable-paper-writes 时发布它(web-app/src/lib.rs:313),
 * 且它就是服务端受信身份必须精确匹配的 principal_id。
 * /api/v1/submit 的 404 只作为兜底错误呈现,不作为首选探测。
 */
export function submitWriteCapability(
  settings: SettingsResponse | null | undefined,
): SubmitWriteCapability {
  const principalId = settings?.paper_principal_id ?? null;
  return { enabled: principalId !== null, principalId };
}

/* ------------------------------------------------------------ 错误与身份 */

/** 提交面稳定错误(code 可测,message 面向操作者)。 */
export class SubmitProblem extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "SubmitProblem";
    this.code = code;
  }
}

export function asSubmitProblem(error: unknown): SubmitProblem | null {
  return error instanceof SubmitProblem ? error : null;
}

/** 有界身份校验(与后端 validate_identity 相同的四条规则)。 */
export function validateBoundedIdentity(value: string, label: string): string {
  const raw = String(value);
  if (raw === "") {
    throw new SubmitProblem("invalid_input", `${label}不能为空。`);
  }
  if (raw.trim() !== raw) {
    throw new SubmitProblem("invalid_input", `${label}不能包含首尾空格。`);
  }
  const hasControlCharacter = [...raw].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0;
    return codePoint < 0x20 || codePoint === 0x7f;
  });
  if (hasControlCharacter) {
    throw new SubmitProblem("invalid_input", `${label}不能包含控制字符。`);
  }
  if (new TextEncoder().encode(raw).length > MAX_IDENTITY_BYTES) {
    throw new SubmitProblem("invalid_input", `${label}必须保持在 128 字节以内。`);
  }
  return raw;
}

/** 安全随机 command_id;没有安全随机源时拒绝生成命令。 */
export function generateCommandId(
  cryptoApi: Crypto | undefined = globalThis.crypto,
): string {
  if (cryptoApi === undefined || typeof cryptoApi.getRandomValues !== "function") {
    throw new SubmitProblem(
      "secure_random_unavailable",
      "当前浏览器无法提供安全随机源,不能生成受信 command_id。",
    );
  }
  if (typeof cryptoApi.randomUUID === "function") {
    return cryptoApi.randomUUID();
  }
  const bytes = new Uint8Array(16);
  cryptoApi.getRandomValues(bytes);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}

/* ------------------------------------------------------------ 投影指纹 */

/**
 * 任务投影指纹:提交前记录基线,回执后比对判定命令是否已在
 * durable 投影中生效。指纹只含投影推进会改变的字段。
 */
export function taskProjectionFingerprint(
  task: ReadOnlyTaskView | null | undefined,
): string | null {
  if (task === null || task === undefined) {
    return null;
  }
  return JSON.stringify([
    task.first_sequence ?? null,
    task.last_sequence ?? null,
    task.updated_at ?? null,
    task.phase ?? null,
    task.recovery ?? null,
  ]);
}

export function submitSnapshotKey(
  taskId: string,
  strategyId: string,
  strategyRevision: string,
): string {
  return JSON.stringify([taskId, strategyId, strategyRevision]);
}

export function latestTaskForKind(
  model: ReadOnlyTaskReadModel | null | undefined,
  taskKind: ReadOnlyTaskKind,
): ReadOnlyTaskView | null {
  const tasks = (model?.tasks ?? []).filter((task) => task.kind === taskKind);
  if (tasks.length === 0) {
    return null;
  }
  return (
    [...tasks].sort(
      (left, right) => Date.parse(right.updated_at) - Date.parse(left.updated_at),
    )[0] ?? null
  );
}

/* ------------------------------------------------------------ 表单状态机 */

export type SubmitAction = "start" | "stop" | "cancel";

export interface PendingSubmission {
  action: SubmitAction;
  snapshotKey: string;
  commandKind: SubmitCommandKind;
  /** 提交前的任务投影指纹基线(任务不存在时为 null)。 */
  baselineTaskFingerprint: string | null;
  envelope: SubmitEnvelope;
}

export interface SubmitFormState {
  taskId: string;
  strategyId: string;
  strategyRevision: string;
  confirmed: boolean;
  inFlight: boolean;
  pendingAction: SubmitAction | null;
  pendingSubmission: PendingSubmission | null;
  /**
   * 最近一次实际发出的提交(含指纹基线);闭合回执后仍保留,
   * 供「已生效 / 未观察到变化」的生效判定使用。
   */
  lastSubmission: PendingSubmission | null;
  lockedByOutcomeUnknown: boolean;
  lastAction: SubmitCommandKind | null;
  lastReceipt: SubmitReceipt | null;
  lastError: SubmitProblem | null;
}

export function createSubmitFormState(defaults: {
  taskId?: string;
  strategyId: string;
  strategyRevision: string;
}): SubmitFormState {
  return {
    taskId: defaults.taskId ?? "",
    strategyId: defaults.strategyId,
    strategyRevision: defaults.strategyRevision,
    confirmed: false,
    inFlight: false,
    pendingAction: null,
    pendingSubmission: null,
    lastSubmission: null,
    lockedByOutcomeUnknown: false,
    lastAction: null,
    lastReceipt: null,
    lastError: null,
  };
}

export type SubmitFormEvent =
  | {
      type: "field_changed";
      field: "taskId" | "strategyId" | "strategyRevision";
      value: string;
    }
  | { type: "confirm_toggled"; confirmed: boolean }
  | { type: "submit_started"; submission: PendingSubmission }
  | { type: "receipt_received"; receipt: SubmitReceipt }
  | { type: "submit_failed"; problem: SubmitProblem }
  | { type: "auth_reset" }
  | { type: "tasks_projection"; model: ReadOnlyTaskReadModel; taskKind: ReadOnlyTaskKind };

/**
 * 纯 reducer:所有提交面状态转移都经由这里,可逐条测试。
 */
export function submitFormReducer(
  state: SubmitFormState,
  event: SubmitFormEvent,
): SubmitFormState {
  switch (event.type) {
    case "field_changed": {
      if (state[event.field] === event.value) {
        return state;
      }
      // 显式修改身份字段 = 操作者声明这是新命令:
      // 丢弃 pending envelope 与 outcome_unknown 锁,反馈一并清空。
      return {
        ...state,
        [event.field]: event.value,
        inFlight: false,
        pendingAction: null,
        pendingSubmission: null,
        lastSubmission: null,
        lockedByOutcomeUnknown: false,
        lastAction: null,
        lastReceipt: null,
        lastError: null,
      };
    }
    case "confirm_toggled":
      return { ...state, confirmed: event.confirmed };
    case "submit_started":
      return {
        ...state,
        inFlight: true,
        pendingAction: event.submission.action,
        pendingSubmission: event.submission,
        lastSubmission: event.submission,
        lastAction: event.submission.commandKind,
        lastError: null,
      };
    case "receipt_received": {
      const locked = event.receipt.status === "outcome_unknown";
      return {
        ...state,
        inFlight: false,
        pendingAction: null,
        lastReceipt: event.receipt,
        lastError: null,
        lockedByOutcomeUnknown: locked,
        // 非 outcome_unknown 即闭合结果:释放 envelope,下次动作生成新身份。
        pendingSubmission: locked ? state.pendingSubmission : null,
      };
    }
    case "submit_failed":
      return {
        ...state,
        inFlight: false,
        pendingAction: null,
        lastError: event.problem,
        // pendingSubmission 保留:身份字段不变时,同动作重试复用同一身份。
      };
    case "auth_reset":
      // 认证变更只清瞬态;pending envelope 身份与 outcome_unknown 锁必须保留,
      // 换令牌不会让一次结果不明的提交凭空闭合。
      return {
        ...state,
        inFlight: false,
        pendingAction: null,
        confirmed: false,
        lastAction: state.pendingSubmission?.commandKind ?? null,
        lastReceipt: null,
        lastError: null,
      };
    case "tasks_projection":
      return syncSubmitLock(state, event.model, event.taskKind);
    default:
      return state;
  }
}

/**
 * outcome_unknown 解锁判定:只信任 complete 的任务投影;
 * 匹配任务的指纹必须相对基线推进,并落在该动作的预期阶段集合。
 */
export function syncSubmitLock(
  state: SubmitFormState,
  model: ReadOnlyTaskReadModel | null | undefined,
  taskKind: ReadOnlyTaskKind,
): SubmitFormState {
  if (
    !state.lockedByOutcomeUnknown ||
    state.pendingSubmission === null ||
    !model ||
    model.projection_status !== "complete"
  ) {
    return state;
  }
  const pending = state.pendingSubmission;
  const matched = model.tasks.find((task) => {
    const projectionAdvanced =
      taskProjectionFingerprint(task) !== pending.baselineTaskFingerprint;
    const expectedPhase =
      pending.action === "start"
        ? ["running", "stopped", "failed"].includes(task.phase)
        : ["stopped", "failed"].includes(task.phase);
    return (
      task.task_id === pending.envelope.target_task_id &&
      task.kind === taskKind &&
      projectionAdvanced &&
      expectedPhase
    );
  });
  if (matched === undefined) {
    return state;
  }
  return { ...state, lockedByOutcomeUnknown: false, pendingSubmission: null };
}

/* ------------------------------------------------------------ 组装 envelope */

export interface BuildSubmissionInput {
  form: SubmitFormState;
  action: SubmitAction;
  startCommandKind: "start_paper_grid" | "start_paper_arbitrage";
  /** 服务端受信身份(settings.paper_principal_id),写路径未启用时为 null。 */
  principalId: string | null;
  /** 提交前的任务投影基线(latestTask 为该 task_id + kind 的匹配任务)。 */
  baselineTask: ReadOnlyTaskView | null;
  cryptoApi?: Crypto;
}

/**
 * 组装(或复用)一次受信提交。
 *
 * - 同身份 + 同动作 + 未锁定 → 复用 pending envelope(同 command_id / 幂等键);
 * - 同身份 + outcome_unknown 锁 → outcome_unknown_locked,拒绝生成新命令;
 * - 同身份 + 不同动作 → pending_submission_locked;
 * - 新身份 → 生成新 UUID command_id 与可见幂等键 paper-<kind>-<command_id>。
 */
export function buildSubmission(input: BuildSubmissionInput): PendingSubmission {
  const { form, action } = input;
  if (input.principalId === null) {
    throw new SubmitProblem(
      "submit_unavailable",
      "后端未启用 Paper 写路径(settings 未发布 paper_principal_id);本页保持只读。",
    );
  }
  const taskId = validateBoundedIdentity(form.taskId, "task_id");
  const strategyId = validateBoundedIdentity(form.strategyId, "strategy_id");
  const strategyRevision = validateBoundedIdentity(
    form.strategyRevision,
    "strategy_revision",
  );
  if (!form.confirmed) {
    throw new SubmitProblem(
      "paper_confirmation_required",
      "请先显式确认这是 Paper-only 指令。",
    );
  }
  const snapshotKey = submitSnapshotKey(taskId, strategyId, strategyRevision);
  if (form.pendingSubmission !== null && form.pendingSubmission.snapshotKey === snapshotKey) {
    if (form.lockedByOutcomeUnknown) {
      throw new SubmitProblem(
        "outcome_unknown_locked",
        "上一次结果仍为 outcome_unknown;请先核对 /api/v1/tasks,或明确修改 task_id / strategy_id / strategy_revision 后再生成新指令。",
      );
    }
    if (form.pendingSubmission.action === action) {
      return form.pendingSubmission;
    }
    throw new SubmitProblem(
      "pending_submission_locked",
      "当前表单仍保留上一次未闭合的 submit envelope;在 /api/v1/tasks 确认结果或修改 task_id / strategy_id / strategy_revision 之前,只能重试同一个动作。",
    );
  }
  const commandId = generateCommandId(input.cryptoApi);
  const commandKind: SubmitCommandKind =
    action === "start"
      ? input.startCommandKind
      : action === "stop"
        ? "stop_task"
        : "cancel_task";
  const command: SubmitCommand =
    action === "start"
      ? {
          kind: input.startCommandKind,
          strategy_id: strategyId,
          strategy_revision: strategyRevision,
        }
      : { kind: commandKind as "stop_task" | "cancel_task" };
  return {
    action,
    snapshotKey,
    commandKind,
    baselineTaskFingerprint: taskProjectionFingerprint(input.baselineTask),
    envelope: {
      schema_version: SUBMIT_SCHEMA_VERSION,
      command_id: commandId,
      idempotency_key: `paper-${commandKind}-${commandId}`,
      target_task_id: taskId,
      permission: {
        principal_id: input.principalId,
        role: SUBMIT_ROLE,
      },
      risk_confirmation: SUBMIT_RISK_CONFIRMATION,
      command,
    },
  };
}

/* ------------------------------------------------------------ 回执校验 */

/**
 * 回执必须能对上 envelope 身份并声明 durable journal 来源,
 * 否则视为不可验证:保留原 envelope,不清任何本地状态。
 */
export function validateSubmitReceipt(
  receipt: unknown,
  envelope: SubmitEnvelope,
): SubmitReceipt {
  const parsed = submitReceiptSchema.safeParse(receipt);
  if (parsed.success) {
    const candidate = receipt as SubmitReceipt;
    if (
      candidate.command_id === envelope.command_id &&
      candidate.target_task_id === envelope.target_task_id &&
      candidate.source === SUBMIT_JOURNAL_SOURCE &&
      candidate.journal_projection === SUBMIT_JOURNAL_PROJECTION
    ) {
      return candidate;
    }
  }
  throw new SubmitProblem(
    "invalid_submit_receipt",
    "受信 submit 返回了无法验证的 receipt;已保留原 envelope,请先核对 durable journal。",
  );
}

/* ------------------------------------------------------------ 生效判定 */

export type SubmissionEffect =
  | "outcome_unknown_locked"
  | "projection_advanced"
  | "no_change_observed";

/**
 * 回执后的生效三态:
 * - outcome_unknown_locked:结果不明,表单锁定直到投影确认;
 * - projection_advanced:任务投影指纹已相对基线变化(已观察到生效);
 * - no_change_observed:投影指纹与基线一致,尚未观察到变化。
 */
export function assessSubmissionEffect(
  state: SubmitFormState,
  submission: PendingSubmission | null,
  model: ReadOnlyTaskReadModel | null | undefined,
  taskKind: ReadOnlyTaskKind,
): SubmissionEffect | null {
  if (state.lockedByOutcomeUnknown) {
    return "outcome_unknown_locked";
  }
  if (submission === null) {
    return null;
  }
  const task =
    model?.tasks.find(
      (candidate) =>
        candidate.task_id === submission.envelope.target_task_id &&
        candidate.kind === taskKind,
    ) ?? null;
  return taskProjectionFingerprint(task) !== submission.baselineTaskFingerprint
    ? "projection_advanced"
    : "no_change_observed";
}

/* ------------------------------------------------------------ 提交门槛 */

export interface SubmitGate {
  allowed: boolean;
  /** 稳定原因码,页面据此渲染帮助文案。 */
  reason:
    | null
    | "write_disabled"
    | "readback_blocked"
    | "in_flight"
    | "outcome_unknown_locked";
}

/**
 * 提交门槛(纯函数):写路径未启用 / 任务读回不是 complete /
 * 提交进行中 / outcome_unknown 锁,任一命中即禁止提交。
 */
export function submitGate(
  form: SubmitFormState,
  capability: SubmitWriteCapability,
  tasksModel: ReadOnlyTaskReadModel | null | undefined,
): SubmitGate {
  if (!capability.enabled) {
    return { allowed: false, reason: "write_disabled" };
  }
  if (!tasksModel || tasksModel.projection_status !== "complete") {
    return { allowed: false, reason: "readback_blocked" };
  }
  if (form.lockedByOutcomeUnknown) {
    return { allowed: false, reason: "outcome_unknown_locked" };
  }
  if (form.inFlight) {
    return { allowed: false, reason: "in_flight" };
  }
  return { allowed: true, reason: null };
}

/* ------------------------------------------------------------ POST 封装 */

export interface PostSubmitOptions {
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
}

/**
 * 唯一的写请求:POST /api/v1/submit。
 *
 * - 422 视为「已受理过」的幂等重放(rejected receipt),照常解析回执;
 * - 200(applied)/ 202(outcome_unknown)同样回读 receipt;
 * - 其余状态归一为 ApiRequestError(401 由全局失效协议接管,
 *   404 呈现为写路径不可用)。
 */
export async function postSubmitEnvelope(
  envelope: SubmitEnvelope,
  options: PostSubmitOptions = {},
): Promise<SubmitReceipt> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const headers = new Headers({
    accept: "application/json",
    "content-type": "application/json",
  });
  const token = getBearerToken();
  if (token !== null) {
    headers.set("authorization", `Bearer ${token}`);
  }
  let response: Response;
  try {
    response = await fetchImpl("/api/v1/submit", {
      method: "POST",
      headers,
      body: JSON.stringify(envelope),
      signal: options.signal ?? null,
      credentials: "omit",
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw error;
    }
    throw new ApiRequestError("network", "网络请求失败,后端可能未运行");
  }

  if (!response.ok && response.status !== 422) {
    let code: string | null = null;
    let message: string | null = null;
    try {
      const body = (await response.json()) as {
        error?: { code?: string; message?: string };
      };
      code = body.error?.code ?? null;
      message = body.error?.message ?? null;
    } catch {
      // 无法解析的错误体不进入用户可见文案(脱敏)。
    }
    const kind =
      response.status === 401
        ? "unauthorized"
        : response.status === 404
          ? "not_found"
          : response.status === 429
            ? "rate_limited"
            : response.status === 503
              ? "unavailable"
              : response.status >= 500
                ? "server"
                : "bad_request";
    throw new ApiRequestError(kind, message ?? `请求失败(HTTP ${response.status})`, {
      status: response.status,
      code,
    });
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new SubmitProblem(
      "invalid_submit_receipt",
      "受信 submit 返回了无法验证的 receipt;已保留原 envelope,请先核对 durable journal。",
    );
  }
  return validateSubmitReceipt(body, envelope);
}
