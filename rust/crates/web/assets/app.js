const VIEW_IDS = new Set([
  "overview",
  "scanner",
  "alerts",
  "executions",
  "integrations",
  "strategies",
  "risk",
  "replay",
  "settings",
]);
const AREA_IDS = new Set([
  "all",
  "config",
  "control-plane",
  "exchange",
  "history",
  "risk",
  "runtime",
  "strategy",
]);
const FACET_IDS = new Set([
  "all",
  "public-data",
  "testnet-protocol",
  "authenticated",
  "reconcile",
  "live",
]);
const BATCH_FILTER_IDS = new Set([
  "all",
  "attention",
  "completed",
  "partial",
  "failed",
  "conflict",
  "unknown",
]);
const MAX_ALERT_OCCURRENCES = 256;
const TRUSTED_ALERT_PROJECTION_IDS = new Set(["complete", "windowed"]);
const TOKEN_LABELS = new Map([
  ["paper_operator", "\u7eb8\u9762\u64cd\u4f5c\u5458"],
  ["paper_only", "\u4ec5 Paper"],
  ["applied", "\u5df2\u5199\u5165"],
  ["rejected", "\u5df2\u62d2\u7edd"],
  ["complete", "完整"],
  ["degraded", "降级"],
  ["windowed", "窗口化"],
  ["not_available", "暂不可用"],
  ["snapshot_end", "快照末尾"],
  ["loading", "正在加载"],
  ["page_limit", "分页边界"],
  ["partial_tail", "部分尾记录"],
  ["none", "无需恢复"],
  ["planned", "已计划"],
  ["completed", "已完成"],
  ["partial", "部分完成"],
  ["incomplete", "未完成"],
  ["failed", "失败"],
  ["conflict", "冲突"],
  ["outcome_unknown", "结果未知"],
  ["reconcile_required", "需要对账"],
  ["investigate", "需要调查"],
  ["available", "可用"],
  ["read-only", "只读"],
  ["paper-once", "单次模拟"],
  ["validate-only", "仅校验"],
  ["contract-only", "仅契约"],
  ["unavailable", "不可用"],
  ["implemented", "已实现"],
  ["protocol-only", "仅协议"],
  ["request-only", "仅请求"],
  ["config-only", "仅配置"],
  ["not-applicable", "不适用"],
]);
for (const [token, label] of [
  ["arbitrage_paper", "Paper \u5957\u5229"],
  ["grid_paper", "Paper \u7f51\u683c"],
  ["start_paper_arbitrage", "\u542f\u52a8\u5957\u5229"],
  ["start_paper_grid", "\u542f\u52a8\u7f51\u683c"],
  ["stop_task", "\u505c\u6b62\u4efb\u52a1"],
  ["cancel_task", "\u53d6\u6d88\u4efb\u52a1"],
  ["pending", "最后记录：未决"],
  ["dropped", "已丢弃"],
  ["succeeded", "已送达"],
  ["timed_out", "已超时"],
  ["volatility_up", "波动上破"],
  ["volatility_down", "波动下破"],
  ["upper_limit", "上沿提醒"],
  ["lower_limit", "下沿提醒"],
  ["backpressure", "排队拥塞"],
  ["adapter_closed", "适配器已关闭"],
  ["device_unavailable", "设备不可用"],
  ["rejected", "已拒绝"],
  ["worker_failed", "通知 worker 异常终止"],
  ["timeout", "超时"],
  ["spot", "现货"],
  ["perpetual", "永续"],
  ["waiting", "等待行情"],
  ["no_opportunity", "暂无机会"],
  ["opportunity", "发现机会"],
  ["analysis_rejected", "分析拒绝"],
  ["missing", "缺失"],
  ["fresh", "新鲜"],
  ["stale", "陈旧"],
  ["future", "未来时间"],
  ["continuous", "连续"],
  ["source_gap", "数据缺口"],
  ["duplicate", "重复"],
  ["out_of_order", "乱序"],
  ["registered", "已登记"],
  ["running", "最后记录：运行中"],
  ["stopping", "最后记录：停止中"],
  ["stopped", "已停止"],
  ["starting", "最后记录：启动中"],
  ["healthy", "健康"],
  ["unknown", "未知"],
  ["stop_requested", "收到停止请求"],
  ["source_ended", "数据源结束"],
  ["shutdown_timed_out", "停止超时"],
  ["startup_failed", "启动失败"],
  ["source_contract", "数据源契约失败"],
  ["monitor_contract", "监控契约失败"],
  ["journal_unavailable", "日志不可用"],
  ["task_panicked", "任务异常终止"],
  ["task_cancelled", "任务被取消"],
  ["arbitrage_monitor", "套利监控"],
  ["benchmark", "基准优先"],
  ["standard", "标准"],
]) {
  TOKEN_LABELS.set(token, label);
}

const STREAM_RETRY_BASE_MS = 1500;
const STREAM_RETRY_MAX_MS = 10000;
const STALE_AFTER_MS = 15000;
const REFETCH_DEBOUNCE_MS = 180;
const SUBMIT_SCHEMA_VERSION = 1;
const SUBMIT_PRINCIPAL_ID = "local-paper-operator";
const SUBMIT_ROLE = "paper_operator";
const SUBMIT_RISK_CONFIRMATION = "paper_only";
const DEFAULT_STRATEGY_REVISION = "2026-07-25";
const STRATEGY_CONTROL_DEFS = {
  arbitrage: {
    title: "\u5957\u5229 Paper \u4efb\u52a1",
    taskKind: "arbitrage_paper",
    defaultStrategyId: "paper-arbitrage",
    startCommandKind: "start_paper_arbitrage",
    startLabel: "\u542f\u52a8\u5957\u5229",
    summary:
      "\u53ea\u63d0\u4ea4 Paper \u5957\u5229\u4efb\u52a1\u7684 start/stop/cancel\uff0c\u4e0d\u5f00\u653e\u5b9e\u76d8\u3001\u5bf9\u8d26\u6216\u4e0b\u5355\u63a7\u5236\u3002",
  },
  grid: {
    title: "\u7f51\u683c Paper \u4efb\u52a1",
    taskKind: "grid_paper",
    defaultStrategyId: "paper-grid",
    startCommandKind: "start_paper_grid",
    startLabel: "\u542f\u52a8\u7f51\u683c",
    summary:
      "\u53ea\u63d0\u4ea4 Paper \u7f51\u683c\u4efb\u52a1\u7684 start/stop/cancel\uff0c\u4e0d\u63d0\u4f9b live\u3001reconcile \u6216\u4e0b\u5355\u6743\u9650\u3002",
  },
};

const dom = {
  spine: document.querySelector("#risk-spine"),
  header: document.querySelector("#workspace-header"),
  main: document.querySelector("#main-view"),
  drawerHost: document.querySelector("#detail-drawer-host"),
  announcer: document.querySelector("#app-status"),
};

const state = {
  route: readRoute(),
  cursor: "",
  authToken: "",
  authInput: "",
  authRequired: false,
  sessionGeneration: 0,
  system: null,
  monitor: null,
  alerts: null,
  tasks: null,
  scanner: null,
  capabilities: null,
  executions: null,
  lastPage: null,
  lastSnapshotAt: 0,
  refreshTimer: 0,
  isRefreshing: false,
  renderedStale: false,
  drawerReturnBatch: "",
  drawerReturnAction: "detail",
  drawerFocusedBatch: "",
  strategySubmit: createStrategySubmitState(),
  loads: {
    system: "idle",
    monitor: "idle",
    alerts: "idle",
    tasks: "idle",
    scanner: "idle",
    capabilities: "idle",
    executions: "idle",
  },
  errors: {
    system: null,
    monitor: null,
    alerts: null,
    tasks: null,
    scanner: null,
    capabilities: null,
    executions: null,
  },
  stream: {
    controller: null,
    connected: false,
    lastEventAt: 0,
    lastError: null,
    retryAt: 0,
    retryCount: 0,
  },
};

window.addEventListener("popstate", () => {
  state.route = readRoute();
  render();
});

window.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && hasAnyData()) {
    void refreshOperationalTruth();
  }
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.route.batch) {
    event.preventDefault();
    closeDrawer();
  }
});

window.setInterval(() => {
  if (hasAnyData() && state.renderedStale !== isStale()) {
    render();
  }
}, 5000);

void bootstrap();

async function bootstrap() {
  render();
  await Promise.all([
    loadSystem(),
    loadMonitor(),
    loadAlerts(),
    loadTasks(),
    loadScanner(),
    loadCapabilities(),
    loadExecutions(),
  ]);
  if (!state.authRequired || state.authToken) {
    restartStream();
  }
  render();
}

function readRoute() {
  const params = new URLSearchParams(window.location.search);
  const pathView = window.location.pathname.replace(/^\/+|\/+$/g, "");
  const view = VIEW_IDS.has(pathView) ? pathView : "overview";
  const area = AREA_IDS.has(params.get("area")) ? params.get("area") : "all";
  const facet = FACET_IDS.has(params.get("facet")) ? params.get("facet") : "all";
  const batchState = BATCH_FILTER_IDS.has(params.get("status"))
    ? params.get("status")
    : "all";
  const batch = params.get("batch") || "";
  return { view, area, facet, batchState, batch };
}

function writeRoute(patch, { replace = false } = {}) {
  state.route = { ...state.route, ...patch };
  const params = new URLSearchParams();
  if (state.route.area !== "all") {
    params.set("area", state.route.area);
  }
  if (state.route.facet !== "all") {
    params.set("facet", state.route.facet);
  }
  if (state.route.batchState !== "all") {
    params.set("status", state.route.batchState);
  }
  if (state.route.batch) {
    params.set("batch", state.route.batch);
  }
  const nextPath = `/${state.route.view}`;
  const next = `${nextPath}${params.size ? `?${params.toString()}` : ""}`;
  const current = `${window.location.pathname}${window.location.search}`;
  if (next !== current) {
    history[replace ? "replaceState" : "pushState"](null, "", next);
  }
}

function navigate(patch, options) {
  writeRoute(patch, options);
  render();
}

function restartStream() {
  stopStream();
  void startStream();
}

function stopStream() {
  if (state.stream.controller) {
    state.stream.controller.abort();
    state.stream.controller = null;
  }
  state.stream.connected = false;
}

function clearProtectedState() {
  state.sessionGeneration += 1;
  stopStream();
  resetStrategySubmitTransientState();
  if (state.refreshTimer) {
    window.clearTimeout(state.refreshTimer);
    state.refreshTimer = 0;
  }
  state.cursor = "";
  state.system = null;
  state.monitor = null;
  state.alerts = null;
  state.tasks = null;
  state.scanner = null;
  state.capabilities = null;
  state.executions = null;
  state.lastPage = null;
  state.lastSnapshotAt = 0;
  state.isRefreshing = false;
  state.renderedStale = false;
  state.drawerFocusedBatch = "";
  state.loads = {
    system: "idle",
    monitor: "idle",
    alerts: "idle",
    tasks: "idle",
    scanner: "idle",
    capabilities: "idle",
    executions: "idle",
  };
  state.errors = {
    system: null,
    monitor: null,
    alerts: null,
    tasks: null,
    scanner: null,
    capabilities: null,
    executions: null,
  };
  state.stream.lastEventAt = 0;
  state.stream.lastError = null;
  state.stream.retryAt = 0;
  state.stream.retryCount = 0;
}

function markAuthenticationRequired(problem) {
  if (problem?.code !== "authentication_required") {
    return false;
  }
  clearProtectedState();
  state.authRequired = true;
  for (const key of Object.keys(state.loads)) {
    state.loads[key] = "error";
    state.errors[key] = problem;
  }
  announce("受保护的运行事实已从页面内存清除；需要有效的 Bearer 令牌。");
  return true;
}

function replaceAuthToken(nextToken) {
  clearProtectedState();
  state.authToken = nextToken;
  state.authInput = "";
  announce(
    state.authToken
      ? "Bearer 令牌已绑定到当前页面内存。"
      : "已清除内存中的 Bearer 令牌和受保护的运行事实。",
  );
  if (!state.authToken && state.authRequired) {
    render();
    return;
  }
  restartStream();
  void Promise.all([
    loadSystem(),
    loadMonitor(),
    loadAlerts(),
    loadTasks(),
    loadScanner(),
    loadCapabilities(),
    loadExecutions(),
  ]);
  render();
}

async function loadSystem() {
  const generation = state.sessionGeneration;
  state.loads.system = "loading";
  state.errors.system = null;
  render();
  try {
    const system = await fetchJson("/api/v1/system");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.system = system;
    state.authRequired = Boolean(system.authentication_required);
    state.lastSnapshotAt = Date.now();
    state.loads.system = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.system = "error";
      state.errors.system = problem;
    }
  }
  render();
}

async function loadMonitor() {
  const generation = state.sessionGeneration;
  state.loads.monitor = "loading";
  state.errors.monitor = null;
  render();
  try {
    const monitor = await fetchJson("/api/v1/monitor");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.monitor = monitor;
    state.lastSnapshotAt = Date.now();
    state.loads.monitor = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.monitor = "error";
      state.errors.monitor = problem;
    }
  }
  render();
}

async function loadAlerts() {
  const generation = state.sessionGeneration;
  state.loads.alerts = "loading";
  state.errors.alerts = null;
  render();
  try {
    const alerts = await fetchJson("/api/v1/alerts");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.alerts = alerts;
    state.lastSnapshotAt = Date.now();
    state.loads.alerts = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.alerts = "error";
      state.errors.alerts = problem;
    }
  }
  render();
}

async function loadTasks() {
  const generation = state.sessionGeneration;
  state.loads.tasks = "loading";
  state.errors.tasks = null;
  render();
  try {
    const tasks = await fetchJson("/api/v1/tasks");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.tasks = tasks;
    state.lastSnapshotAt = Date.now();
    state.loads.tasks = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.tasks = "error";
      state.errors.tasks = problem;
    }
  }
  render();
}

async function loadScanner() {
  const generation = state.sessionGeneration;
  state.loads.scanner = "loading";
  state.errors.scanner = null;
  render();
  try {
    const scanner = await fetchJson("/api/v1/scanner");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.scanner = scanner;
    state.lastSnapshotAt = Date.now();
    state.loads.scanner = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.scanner = "error";
      state.errors.scanner = problem;
    }
  }
  render();
}

async function loadCapabilities() {
  const generation = state.sessionGeneration;
  state.loads.capabilities = "loading";
  state.errors.capabilities = null;
  render();
  try {
    const capabilities = await fetchJson("/api/v1/capabilities");
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.capabilities = capabilities;
    state.loads.capabilities = "ready";
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.capabilities = "error";
      state.errors.capabilities = problem;
    }
  }
  render();
}

async function loadExecutions() {
  const generation = state.sessionGeneration;
  state.loads.executions = "loading";
  state.errors.executions = null;
  render();
  try {
    const suffix = state.cursor
      ? `?cursor=${encodeURIComponent(state.cursor)}`
      : "";
    const executions = await fetchJson(`/api/v1/executions${suffix}`);
    if (generation !== state.sessionGeneration) {
      return;
    }
    state.executions = executions;
    state.lastSnapshotAt = Date.now();
    state.loads.executions = "ready";
    if (!state.lastPage || state.executions.changes.events.length > 0) {
      state.lastPage = state.executions.changes;
    }
    if (state.executions.changes.next_cursor) {
      state.cursor = state.executions.changes.next_cursor;
    }
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    if (!markAuthenticationRequired(problem)) {
      state.loads.executions = "error";
      state.errors.executions = problem;
    }
  }
  render();
}

async function refreshOperationalTruth() {
  if (state.isRefreshing) {
    return;
  }
  state.isRefreshing = true;
  render();
  try {
    await Promise.all([
      loadSystem(),
      loadMonitor(),
      loadAlerts(),
      loadTasks(),
      loadScanner(),
      loadExecutions(),
    ]);
  } finally {
    state.isRefreshing = false;
    render();
  }
}

function scheduleOperationalRefresh() {
  if (state.refreshTimer) {
    return;
  }
  state.refreshTimer = window.setTimeout(async () => {
    state.refreshTimer = 0;
    await refreshOperationalTruth();
  }, REFETCH_DEBOUNCE_MS);
}

async function startStream() {
  const controller = new AbortController();
  state.stream.controller = controller;
  state.stream.connected = false;
  state.stream.lastError = null;
  render();
  while (!controller.signal.aborted) {
    try {
      const response = await fetchStreamResponse(controller.signal);
      state.stream.connected = true;
      state.stream.lastError = null;
      state.stream.retryAt = 0;
      state.stream.retryCount = 0;
      render();
      await parseSse(response.body, controller.signal, handleSseMessage);
      if (!controller.signal.aborted) {
        throw new Error("stream_closed");
      }
    } catch (error) {
      if (controller.signal.aborted) {
        return;
      }
      const problem = normalizeError(error);
      if (markAuthenticationRequired(problem)) {
        render();
        return;
      }
      state.stream.connected = false;
      state.stream.lastError = problem;
      state.stream.retryCount += 1;
      const waitMs = Math.min(
        STREAM_RETRY_BASE_MS * 2 ** Math.min(state.stream.retryCount - 1, 3),
        STREAM_RETRY_MAX_MS,
      );
      state.stream.retryAt = Date.now() + waitMs;
      render();
      await delay(waitMs, controller.signal);
    }
  }
}

async function fetchStreamResponse(signal) {
  const headers = {
    Accept: "text/event-stream",
  };
  if (state.authToken) {
    headers.Authorization = `Bearer ${state.authToken}`;
  }
  if (state.cursor) {
    headers["Last-Event-ID"] = state.cursor;
  }
  const suffix = state.cursor
    ? `?cursor=${encodeURIComponent(state.cursor)}`
    : "";
  const response = await fetch(`/api/v1/events${suffix}`, {
    method: "GET",
    cache: "no-store",
    headers,
    signal,
  });
  if (!response.ok) {
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    throw new ApiProblem(
      payload?.error?.code || "stream_error",
      payload?.error?.message || "The event stream could not be started.",
      response.status,
    );
  }
  if (!response.headers.get("content-type")?.includes("text/event-stream")) {
    throw new ApiProblem(
      "stream_error",
      "The event endpoint returned an unexpected content type.",
      response.status,
    );
  }
  return response;
}

function handleSseMessage(message) {
  state.stream.connected = true;
  state.stream.lastEventAt = Date.now();
  if (message.id) {
    state.cursor = message.id;
  }
  if (message.event === "operation_page" && message.data) {
    try {
      const page = JSON.parse(message.data);
      state.lastPage = page;
      if (page.next_cursor) {
        state.cursor = page.next_cursor;
      }
      announce(
        page.events.length > 0
          ? `已观察到 ${page.events.length} 条新的操作通知。`
          : "操作事件流已恢复，没有新的通知。",
      );
      scheduleOperationalRefresh();
    } catch {
      state.stream.lastError = new ApiProblem(
        "stream_error",
        "The event page could not be decoded safely.",
        0,
      );
    }
  } else if (message.event === "stream_error" && message.data) {
    try {
      const payload = JSON.parse(message.data);
      state.stream.lastError = new ApiProblem(
        payload.error?.code || "stream_error",
        payload.error?.message || "The event stream stopped.",
        0,
      );
    } catch {
      state.stream.lastError = new ApiProblem("stream_error", "The event stream stopped.", 0);
    }
  }
  render();
}

async function parseSse(stream, signal, onMessage) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (!signal.aborted) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true }).replace(/\r/g, "");
    let boundaryIndex = buffer.indexOf("\n\n");
    while (boundaryIndex >= 0) {
      const chunk = buffer.slice(0, boundaryIndex);
      buffer = buffer.slice(boundaryIndex + 2);
      const message = parseSseChunk(chunk);
      if (message) {
        onMessage(message);
      }
      boundaryIndex = buffer.indexOf("\n\n");
    }
  }
}

function parseSseChunk(chunk) {
  const lines = chunk.replace(/\r/g, "").split("\n");
  const message = {
    event: "message",
    data: "",
    id: "",
  };
  for (const line of lines) {
    if (!line || line.startsWith(":")) {
      continue;
    }
    const separator = line.indexOf(":");
    const field = separator >= 0 ? line.slice(0, separator) : line;
    const rawValue = separator >= 0 ? line.slice(separator + 1).replace(/^ /, "") : "";
    if (field === "event") {
      message.event = rawValue;
    } else if (field === "data") {
      message.data = message.data ? `${message.data}\n${rawValue}` : rawValue;
    } else if (field === "id") {
      message.id = rawValue;
    }
  }
  return message.data || message.id || message.event !== "message" ? message : null;
}

async function fetchJson(url, options = {}) {
  const {
    method = "GET",
    body,
    headers: extraHeaders = {},
    signal,
    acceptedStatuses = [],
  } = options;
  const headers = {
    Accept: "application/json",
    ...extraHeaders,
  };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  if (state.authToken) {
    headers.Authorization = `Bearer ${state.authToken}`;
  }
  const response = await fetch(url, {
    method,
    cache: "no-store",
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });
  let payload = null;
  if (response.headers.get("content-type")?.includes("application/json")) {
    payload = await response.json();
  }
  if (!response.ok && !acceptedStatuses.includes(response.status)) {
    throw new ApiProblem(
      payload?.error?.code || "internal_error",
      payload?.error?.message || "The request could not be completed.",
      response.status,
    );
  }
  return payload;
}

function normalizeError(error) {
  if (error instanceof ApiProblem) {
    return error;
  }
  if (error?.name === "AbortError") {
    return new ApiProblem("request_cancelled", "The request was cancelled.", 0);
  }
  return new ApiProblem(
    error?.code || "network_error",
    error?.message || "The request could not be completed.",
    error?.status || 0,
  );
}

function createStrategySubmitState() {
  return {
    arbitrage: createStrategySubmitModel(
      STRATEGY_CONTROL_DEFS.arbitrage.defaultStrategyId,
    ),
    grid: createStrategySubmitModel(STRATEGY_CONTROL_DEFS.grid.defaultStrategyId),
  };
}

function createStrategySubmitModel(defaultStrategyId) {
  return {
    taskId: "",
    strategyId: defaultStrategyId,
    strategyRevision: DEFAULT_STRATEGY_REVISION,
    confirmed: false,
    inFlight: false,
    pendingAction: "",
    pendingSubmission: null,
    lockedByOutcomeUnknown: false,
    lastAction: "",
    lastReceipt: null,
    lastError: null,
  };
}

function resetStrategySubmitTransientState() {
  for (const form of Object.values(state.strategySubmit)) {
    form.inFlight = false;
    form.pendingAction = "";
    form.confirmed = false;
    form.lastAction = form.pendingSubmission?.commandKind || "";
    form.lastReceipt = null;
    form.lastError = null;
  }
}

function latestTaskForKind(taskKind) {
  const tasks = (state.tasks?.tasks || []).filter((task) => task.kind === taskKind);
  if (tasks.length === 0) {
    return null;
  }
  return tasks
    .slice()
    .sort((left, right) => Date.parse(right.updated_at) - Date.parse(left.updated_at))[0];
}

function validateBoundedIdentity(value, label) {
  const raw = String(value || "");
  if (!raw) {
    throw new ApiProblem("invalid_input", `${label}\u4e0d\u80fd\u4e3a\u7a7a\u3002`, 0);
  }
  if (raw.trim() !== raw) {
    throw new ApiProblem(
      "invalid_input",
      `${label}\u4e0d\u80fd\u5305\u542b\u9996\u5c3e\u7a7a\u683c\u3002`,
      0,
    );
  }
  if (/[\u0000-\u001f\u007f]/.test(raw)) {
    throw new ApiProblem(
      "invalid_input",
      `${label}\u4e0d\u80fd\u5305\u542b\u63a7\u5236\u5b57\u7b26\u3002`,
      0,
    );
  }
  if (new TextEncoder().encode(raw).length > 128) {
    throw new ApiProblem(
      "invalid_input",
      `${label}\u5fc5\u987b\u4fdd\u6301\u5728 128 \u5b57\u8282\u4ee5\u5185\u3002`,
      0,
    );
  }
  return raw;
}

function requirePaperConfirmation(form) {
  if (!form.confirmed) {
    throw new ApiProblem(
      "paper_confirmation_required",
      "\u8bf7\u5148\u663e\u5f0f\u786e\u8ba4\u8fd9\u662f Paper-only \u6307\u4ee4\u3002",
      0,
    );
  }
}

function randomCommandId() {
  const cryptoApi = globalThis.crypto;
  if (!cryptoApi?.getRandomValues) {
    throw new ApiProblem(
      "secure_random_unavailable",
      "当前浏览器无法提供安全随机源，不能生成受信 command_id。",
      0,
    );
  }
  if (typeof cryptoApi.randomUUID === "function") {
    return cryptoApi.randomUUID();
  }
  const bytes = new Uint8Array(16);
  cryptoApi.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}

function strategySubmitSnapshotKey(taskId, strategyId, strategyRevision) {
  return JSON.stringify([taskId, strategyId, strategyRevision]);
}

function taskProjectionFingerprint(task) {
  if (!task) {
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

function clearStrategySubmissionState(form, { keepFeedback = false } = {}) {
  form.inFlight = false;
  form.pendingAction = "";
  form.pendingSubmission = null;
  form.lockedByOutcomeUnknown = false;
  if (!keepFeedback) {
    form.lastAction = "";
    form.lastReceipt = null;
    form.lastError = null;
  }
}

function updateStrategySubmitField(formKey, field, value) {
  const form = state.strategySubmit[formKey];
  if (!form || form[field] === value) {
    return;
  }
  form[field] = value;
  clearStrategySubmissionState(form);
  render();
}

function syncStrategySubmitLocks() {
  if (
    state.loads.tasks !== "ready" ||
    state.tasks?.projection_status !== "complete"
  ) {
    return;
  }
  for (const [formKey, form] of Object.entries(state.strategySubmit)) {
    if (!form.lockedByOutcomeUnknown || !form.pendingSubmission) {
      continue;
    }
    const pendingTaskId = form.pendingSubmission.envelope.target_task_id;
    const matchedTask = (state.tasks?.tasks || []).find((task) => {
      const projectionAdvanced =
        taskProjectionFingerprint(task) !==
        form.pendingSubmission.baselineTaskFingerprint;
      const expectedPhase =
        form.pendingSubmission.action === "start"
          ? ["running", "stopped", "failed"].includes(task.phase)
          : ["stopped", "failed"].includes(task.phase);
      return (
        task.task_id === pendingTaskId &&
        task.kind === STRATEGY_CONTROL_DEFS[formKey].taskKind &&
        projectionAdvanced &&
        expectedPhase
      );
    });
    if (matchedTask) {
      form.lockedByOutcomeUnknown = false;
      form.pendingSubmission = null;
    }
  }
}

function buildSubmitEnvelope(formKey, action) {
  const form = state.strategySubmit[formKey];
  const definition = STRATEGY_CONTROL_DEFS[formKey];
  const taskId = validateBoundedIdentity(form.taskId, "task_id");
  const strategyId = validateBoundedIdentity(form.strategyId, "strategy_id");
  const strategyRevision = validateBoundedIdentity(
    form.strategyRevision,
    "strategy_revision",
  );
  requirePaperConfirmation(form);
  const snapshotKey = strategySubmitSnapshotKey(
    taskId,
    strategyId,
    strategyRevision,
  );
  if (form.pendingSubmission && form.pendingSubmission.snapshotKey === snapshotKey) {
    if (form.lockedByOutcomeUnknown) {
      throw new ApiProblem(
        "outcome_unknown_locked",
        "\u4e0a\u4e00\u6b21\u7ed3\u679c\u4ecd\u4e3a outcome_unknown\uff1b\u8bf7\u5148\u6838\u5bf9 /api/v1/tasks\uff0c\u6216\u660e\u786e\u4fee\u6539 task_id / strategy_id / strategy_revision \u540e\u518d\u751f\u6210\u65b0\u6307\u4ee4\u3002",
        0,
      );
    }
    if (form.pendingSubmission.action === action) {
      return form.pendingSubmission;
    }
    throw new ApiProblem(
      "pending_submission_locked",
      "\u5f53\u524d\u8868\u5355\u4ecd\u4fdd\u7559\u4e0a\u4e00\u6b21\u672a\u95ed\u5408\u7684 submit envelope\uff1b\u5728 /api/v1/tasks \u786e\u8ba4\u7ed3\u679c\u6216\u4fee\u6539 task_id / strategy_id / strategy_revision \u4e4b\u524d\uff0c\u53ea\u80fd\u91cd\u8bd5\u540c\u4e00\u4e2a\u52a8\u4f5c\u3002",
      0,
    );
  }
  const commandId = randomCommandId();
  const commandKind =
    action === "start"
      ? definition.startCommandKind
      : action === "stop"
        ? "stop_task"
        : "cancel_task";
  const command =
    action === "start"
      ? {
          kind: commandKind,
          strategy_id: strategyId,
          strategy_revision: strategyRevision,
        }
      : { kind: commandKind };
  const baselineTask = (state.tasks?.tasks || []).find(
    (task) =>
      task.task_id === taskId &&
      task.kind === definition.taskKind,
  );
  return {
    action,
    snapshotKey,
    commandKind,
    baselineTaskFingerprint: taskProjectionFingerprint(baselineTask),
    envelope: {
      schema_version: SUBMIT_SCHEMA_VERSION,
      command_id: commandId,
      idempotency_key: `paper-${commandKind}-${commandId}`,
      target_task_id: taskId,
      permission: {
        principal_id: SUBMIT_PRINCIPAL_ID,
        role: SUBMIT_ROLE,
      },
      risk_confirmation: SUBMIT_RISK_CONFIRMATION,
      command,
    },
  };
}

function validateSubmitReceipt(receipt, envelope) {
  const validStatuses = new Set(["applied", "rejected", "outcome_unknown"]);
  if (
    !receipt ||
    receipt.schema_version !== SUBMIT_SCHEMA_VERSION ||
    receipt.command_id !== envelope.command_id ||
    receipt.target_task_id !== envelope.target_task_id ||
    !validStatuses.has(receipt.status) ||
    receipt.source !== "durable_journal" ||
    receipt.journal_projection !== "submit_command_v1"
  ) {
    throw new ApiProblem(
      "invalid_submit_receipt",
      "受信 submit 返回了无法验证的 receipt；已保留原 envelope，请先核对 durable journal。",
      0,
    );
  }
  return receipt;
}

async function submitStrategyCommand(formKey, action) {
  const form = state.strategySubmit[formKey];
  if (!form || form.inFlight) {
    return;
  }
  const generation = state.sessionGeneration;
  let submission;
  try {
    submission = buildSubmitEnvelope(formKey, action);
  } catch (error) {
    form.lastError = normalizeError(error);
    render();
    return;
  }
  form.inFlight = true;
  form.pendingAction = action;
  form.pendingSubmission = submission;
  form.lastAction = submission.commandKind;
  form.lastError = null;
  render();
  try {
    const receipt = validateSubmitReceipt(
      await fetchJson("/api/v1/submit", {
        method: "POST",
        body: submission.envelope,
        acceptedStatuses: [422],
      }),
      submission.envelope,
    );
    if (generation !== state.sessionGeneration) {
      return;
    }
    form.lastReceipt = receipt;
    form.lastError = null;
    form.lockedByOutcomeUnknown = receipt?.status === "outcome_unknown";
    if (!form.lockedByOutcomeUnknown) {
      form.pendingSubmission = null;
    }
    announce(
      receipt?.status === "outcome_unknown"
        ? "\u53d7\u4fe1 Paper \u6307\u4ee4\u5df2\u5199\u5165 durable journal\uff0c\u8bf7\u5148\u6838\u5bf9 /api/v1/tasks \u540e\u518d\u51b3\u5b9a\u4e0b\u4e00\u6b65\u3002"
        : `\u53d7\u4fe1 Paper \u6307\u4ee4\u5df2\u5199\u5165 durable journal\uff1a${submitCommandLabel(
            submission.commandKind,
          )}\u3002`,
    );
    void refreshOperationalTruth();
  } catch (error) {
    if (generation !== state.sessionGeneration) {
      return;
    }
    const problem = normalizeError(error);
    markAuthenticationRequired(problem);
    if (generation === state.sessionGeneration) {
      form.lastError = problem;
    }
  } finally {
    if (generation === state.sessionGeneration) {
      form.inFlight = false;
      form.pendingAction = "";
      render();
    }
  }
}

function submitCommandLabel(commandKind) {
  switch (commandKind) {
    case "start_paper_arbitrage":
      return "\u542f\u52a8\u5957\u5229";
    case "start_paper_grid":
      return "\u542f\u52a8\u7f51\u683c";
    case "stop_task":
      return "\u505c\u6b62\u4efb\u52a1";
    case "cancel_task":
      return "\u53d6\u6d88\u4efb\u52a1";
    default:
      return commandKind;
  }
}

function submitReceiptTone(status) {
  switch (status) {
    case "applied":
      return "success";
    case "rejected":
      return "danger";
    case "outcome_unknown":
      return "warning";
    default:
      return "neutral";
  }
}

function render() {
  state.renderedStale = isStale();
  syncStrategySubmitLocks();
  renderSpine();
  renderHeader();
  renderMain();
  renderDrawer();
}

function renderSpine() {
  dom.spine.replaceChildren(
    renderAuthorityBlock(),
    renderSpineStatusBlock(),
    renderNavigationBlock(),
    renderAccessBlock(),
  );
}

function renderAuthorityBlock() {
  const mode = state.system?.release_stage === "paper-only" ? "PAPER" : "模式未知";
  const liveLabel = state.system?.live_trading_enabled ? "LIVE 已开启" : "LIVE 已关闭";
  return el("section", { className: "spine-block authority-block" }, [
    el("div", { className: "authority-stack" }, [
      el("div", { className: "compact-label", text: "权限边界" }),
      el("div", { className: "authority-mode" }, [
        el("span", { text: mode }),
        buildTag(liveLabel, state.system?.live_trading_enabled ? "danger" : "neutral"),
      ]),
      el("div", { className: "spine-meta" }, [
        metaRow("访问", state.system?.access_scope || "loopback"),
        metaRow("认证", accessDescriptor()),
        metaRow("投影", humanizeToken(state.system?.projection_status || "unavailable")),
      ]),
    ]),
  ]);
}

function renderSpineStatusBlock() {
  const signals = [
    {
      label: "事件流",
      value: eventStreamLabel(),
      tone: eventStreamTone(),
    },
    {
      label: "行情新鲜度",
      value: humanizeToken(state.system?.market_data_freshness || "not_available"),
      tone: "neutral",
    },
    {
      label: "适配器健康",
      value: humanizeToken(state.system?.adapter_health || "not_available"),
      tone: state.system?.adapter_health === "not_available" ? "neutral" : "info",
    },
    {
      label: "Kill switch",
      value: humanizeToken(state.system?.kill_switch || "not_available"),
      tone: "neutral",
    },
    {
      label: "恢复",
      value: state.system
        ? `${state.system.recovery_required_count} 个批次`
        : "--",
      tone: state.system?.recovery_required_count ? "warning" : "success",
    },
  ];
  return el("section", { className: "spine-block risk-status-block" }, [
    el("div", { className: "compact-label", text: "风险脊柱" }),
    el(
      "ul",
      { className: "status-list", attrs: { role: "list" } },
      signals.map((signalItem) =>
        el("li", {}, [
          el("span", { text: signalItem.label }),
          buildTag(signalItem.value, signalItem.tone),
        ]),
      ),
    ),
  ]);
}

function renderNavigationBlock() {
  const entries = [
    ["strategies", "策略"],
    ["risk", "风险"],
    ["replay", "回放"],
    ["settings", "设置"],
    ["overview", "总览"],
    ["scanner", "扫描"],
    ["alerts", "预警"],
    ["executions", "执行"],
    ["integrations", "集成"],
  ];
  return el("nav", { className: "spine-nav", attrs: { "aria-label": "主要导航" } }, [
    ...entries.map(([view, label]) =>
      el("a", {
        attrs: {
          href: routeHref({ view, batch: view === "executions" ? state.route.batch : "" }),
          "aria-current": state.route.view === view ? "page" : null,
        },
        onclick: (event) => {
          event.preventDefault();
          navigate({ view, batch: view === "executions" ? state.route.batch : "" });
        },
      }, [
        el("span", { className: "spine-text", text: label }),
      ]),
    ),
  ]);
}

function renderAccessBlock() {
  const authRequired = Boolean(
    state.authRequired ||
      Object.values(state.errors).some((problem) => problem?.code === "authentication_required"),
  );
  if (!authRequired && !state.authToken) {
    return el("section", {
      className: "spine-block access-block",
      attrs: { "data-required": "false" },
    }, [
      el("div", { className: "compact-label", text: "会话访问" }),
      metaRow("Bearer 令牌", state.system ? "无需提供" : "正在检查"),
      el("p", {
        className: "field-help",
        text: "外壳不含运行数据；操作事实仅通过本机回环接口读取。",
      }),
    ]);
  }
  return el("section", {
    className: "spine-block access-block",
    attrs: { "data-required": "true" },
  }, [
    el("div", { className: "compact-label", text: "会话访问" }),
    el("form", {
      className: "spine-form",
      onsubmit: (event) => {
        event.preventDefault();
        replaceAuthToken(state.authInput.trim());
      },
    }, [
      el("label", {}, [
        el("span", {
          className: "compact-label",
          text: authRequired
            ? "服务器要求 Bearer 令牌"
            : "本机会话可选 Bearer 令牌",
        }),
        el("input", {
          attrs: {
            type: "password",
            inputmode: "text",
            autocomplete: "off",
            spellcheck: "false",
            value: state.authInput,
            placeholder: authRequired ? "仅在当前页面粘贴令牌" : "开放访问时保持为空",
          },
          oninput: (event) => {
            state.authInput = event.target.value;
          },
        }),
      ]),
      el("div", { className: "spine-actions" }, [
        el("button", { attrs: { type: "submit" }, text: "绑定到内存" }),
        el("button", {
          className: "ghost",
          attrs: { type: "button" },
          text: "清除",
          onclick: () => {
            replaceAuthToken("");
          },
        }),
      ]),
      el("p", {
        className: "field-help",
        text: "令牌只保存在当前页面内存中；刷新页面即清除。",
      }),
    ]),
  ]);
}

function renderHeader() {
  const currentPage = {
    strategies: "策略证据与受信 Paper 任务控制；不开放 live 或 reconcile。",
    risk: "关闭优先的风险状态、恢复压力与有界权限证据。",
    replay: "历史快照与事件水位上下文；不是实时撮合回放。",
    settings: "本页可见的只读外壳、访问策略与投影边界。",
    alerts: "有界价格预警投影、确认状态与本地通知结果。",
    overview: "跨域读取模型与风险优先的运行事实。",
    scanner: "离线确定性虚拟网格排行，以及可审计的 APR 与评分证据。",
    executions: "有界执行账本，以及恢复与结果证据。",
    integrations: "能力矩阵与适配器支持证据。",
  }[state.route.view];
  const stripItems = [
    ["日志代次", state.system?.journal_id || "--"],
    ["游标", state.cursor || "尚未固定"],
    ["序号", state.system?.head_sequence?.toString() || "--"],
    ["事件流", eventStreamLabel()],
  ];
  dom.header.replaceChildren(
    el("div", { className: "workspace-header-row" }, [
      el("div", { className: "headline-actions" }, [
        el("h1", { text: "加密交易控制面" }),
      ]),
      el("div", { className: "headline-actions" }, [
        el("button", {
          className: "secondary",
          text: state.isRefreshing ? "正在刷新…" : "刷新快照",
          attrs: { type: "button", disabled: state.isRefreshing ? "true" : null },
          onclick: () => {
            void refreshOperationalTruth();
          },
        }),
        el("button", {
          className: "ghost",
          text: "复制游标",
          attrs: { type: "button", disabled: state.cursor ? null : "true" },
          onclick: () => {
            void copyValue(state.cursor, "已复制当前游标。");
          },
        }),
      ]),
    ]),
    el("p", { className: "workspace-summary", text: currentPage }),
    el(
      "div",
      { className: "status-strip" },
      stripItems.map(([label, value]) =>
        el("div", { className: "status-strip-item" }, [
          el("div", { className: "compact-label", text: label }),
          el("div", {
            className: label === "日志代次" || label === "游标" ? "status-hero visually-contained mono" : "status-hero",
            text: String(value),
          }),
        ]),
      ),
    ),
  );
}

function renderMain() {
  const content = [];
  for (const band of collectBands()) {
    content.push(renderBand(band));
  }
  if (state.route.view === "overview") {
    content.push(renderOverviewView());
  } else if (state.route.view === "scanner") {
    content.push(renderScannerView());
  } else if (state.route.view === "alerts") {
    content.push(renderAlertsView());
  } else if (state.route.view === "executions") {
    content.push(renderExecutionsView());
  } else if (state.route.view === "strategies") {
    content.push(renderStrategiesView());
  } else if (state.route.view === "risk") {
    content.push(renderRiskView());
  } else if (state.route.view === "replay") {
    content.push(renderReplayView());
  } else if (state.route.view === "settings") {
    content.push(renderSettingsView());
  } else {
    content.push(renderIntegrationsView());
  }
  dom.main.replaceChildren(...content);
}

function renderOverviewView() {
  const leftColumn = el("div", { className: "view-stack" }, [
    renderRibbon(),
    renderMonitorRegion(),
    renderTasksRegion(),
    renderScannerSummaryRegion(),
    renderAlertsSummaryRegion(),
    renderExecutionsSummaryRegion(),
    renderRecentNoticesRegion(),
  ]);
  const rightColumn = el("div", { className: "view-stack" }, [
    renderProjectionRegion(),
    renderCapabilityPulseRegion(),
  ]);
  return el("section", { className: "overview-grid" }, [leftColumn, rightColumn]);
}

function renderTasksRegion() {
  const model = state.tasks;
  if (state.loads.tasks === "error" && !model) {
    return renderRegion({
      title: "只读连续任务",
      subtitle: "这里只呈现持久生命周期事实，不提供启动、停止或重连权限。",
      body: renderError(state.errors.tasks),
    });
  }
  if (state.loads.tasks === "loading" && !model) {
    return renderRegion({
      title: "只读连续任务",
      subtitle: "正在读取任务登记、源健康与最后持久终态。",
      body: renderSkeleton(5),
    });
  }
  if (!model || !Array.isArray(model.tasks) || model.tasks.length === 0) {
    return renderRegion({
      title: "只读连续任务",
      subtitle: "任务投影从 journal 冷启动重建；不会自动恢复外部连接。",
      body: renderEmpty(
        "尚未投影出连续任务登记。",
        "已检查 /api/v1/tasks；没有运行按钮，也不会把缺失事实解释成健康。",
      ),
    });
  }

  const rows = [...model.tasks]
    .sort((left, right) => (right.last_sequence || 0) - (left.last_sequence || 0))
    .slice(0, 8);
  const table = el("div", {
    className: "table-wrap",
    attrs: {
      role: "region",
      tabindex: "0",
      "aria-label": "只读连续任务明细，可横向滚动",
    },
  }, [
    el("table", { className: "task-table" }, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "任务 / 最后事实" }),
          el("th", { attrs: { scope: "col" }, text: "阶段" }),
          el("th", { attrs: { scope: "col" }, text: "数据源" }),
          el("th", { attrs: { scope: "col" }, text: "恢复判断" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        rows.map((task) =>
          el("tr", {}, [
            el("td", {}, [
              el("div", { className: "mono", text: task.task_id || "--" }),
              el("div", {
                className: "muted mono",
                text: `#${task.last_sequence ?? "--"} · ${formatDateTime(task.updated_at)}`,
              }),
              el("div", {
                className: "muted",
                text: `${humanizeToken(task.kind)} · ${task.processed_event_count ?? 0} 个事件`,
              }),
            ]),
            el("td", {}, [
              buildTag(humanizeToken(task.phase), taskTone(task)),
              task.exit
                ? el("div", { className: "muted", text: humanizeToken(task.exit) })
                : null,
              task.failure
                ? el("div", { className: "muted", text: humanizeToken(task.failure) })
                : null,
            ]),
            el(
              "td",
              {},
              (task.sources || []).map((source) =>
                el("div", { className: "task-source-line" }, [
                  el("span", {
                    className: "mono",
                    text: `${source.source_id} #${source.event_sequence}`,
                  }),
                  buildTag(
                    humanizeToken(source.health),
                    source.health === "healthy"
                      ? "success"
                      : source.health === "degraded"
                        ? "warning"
                        : "neutral",
                  ),
                ]),
              ),
            ),
            el("td", {}, [
              buildTag(
                humanizeToken(task.recovery),
                task.recovery === "none" ? "success" : "warning",
              ),
              el("div", {
                className: "muted",
                text:
                  task.recovery === "none"
                    ? "持久终态已闭合"
                    : "仅有历史事实；需核对进程",
              }),
            ]),
          ]),
        ),
      ),
    ]),
  ]);

  return renderRegion({
    title: "只读连续任务",
    subtitle:
      "显示最后持久阶段、双源健康与事件计数。running / stopping 只代表 journal 最后记录，不证明进程仍存活。",
    body: el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, [
        detailStat("任务投影", humanizeToken(model.projection_status)),
        detailStat("任务数", model.tasks.length),
        detailStat("无效事件", model.invalid_event_count ?? "--"),
        detailStat("日志头", model.journal_head_sequence ?? "--"),
      ]),
      model.projection_status !== "complete"
        ? el("p", {
            className: "muted",
            text: "任务投影已降级；保留行是最后通过校验的历史事实，不能据此判断当前 liveness。",
          })
        : null,
      table,
      model.tasks.length > rows.length
        ? el("p", {
            className: "muted",
            text: `总览仅显示最近 ${rows.length} / ${model.tasks.length} 个有界任务。`,
          })
        : null,
      el("p", {
        className: "muted",
        text: "本页没有启动、停止、重连或自动恢复入口；进程重启后只重建投影，不重放网络任务。",
      }),
    ]),
  });
}

function taskTone(task) {
  if (task.failure || task.phase === "failed") {
    return "danger";
  }
  if (task.recovery !== "none" || task.exit === "shutdown_timed_out") {
    return "warning";
  }
  if (task.phase === "stopped") {
    return "success";
  }
  return "info";
}

function renderScannerSummaryRegion() {
  const model = state.scanner;
  const latest = model?.latest;
  let body;
  if (state.loads.scanner === "error" && !model) {
    body = renderError(state.errors.scanner);
  } else if (state.loads.scanner === "loading" && !model) {
    body = renderSkeleton(4);
  } else if (!latest) {
    body = renderEmpty(
      "尚未观察到虚拟网格排行事实。",
      "已检查 /api/v1/scanner；缺失记录不会被解释为 scanner 正在运行。",
    );
  } else {
    const leader = latest.rows?.[0];
    body = el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, [
        detailStat("投影", humanizeToken(model.projection_status)),
        detailStat("评估时间", formatDateTime(latest.recorded_at)),
        detailStat("候选 / 入榜", `${latest.candidate_count} / ${latest.eligible_count}`),
        detailStat("首位", leader ? scannerMarketLabel(leader) : "无返回行"),
        detailStat("首位 APR", leader ? `${leader.estimated_apr}%` : "--"),
      ]),
      el("p", {
        className: "muted",
        text:
          "这是最后一次离线历史排行；不证明 scanner 进程仍存活，也不证明行情仍然新鲜。",
      }),
    ]);
  }
  return renderRegion({
    title: "虚拟网格 Scanner",
    subtitle:
      "用有序历史价格回放生成 APR/Rating 排行；只读、可审计，不生成订单意图。",
    body,
  });
}

function renderScannerView() {
  const model = state.scanner;
  const latest = model?.latest;
  let body;
  if (state.loads.scanner === "error" && !model) {
    body = renderError(state.errors.scanner);
  } else if (state.loads.scanner === "loading" && !model) {
    body = renderSkeleton(7);
  } else if (!latest) {
    body = renderEmpty(
      "尚无确定性虚拟网格排行。",
      "已检查完整 scanner read model；没有记录、启动按钮或在线行情推断。",
    );
  } else {
    const projectionDegraded = model.projection_status !== "complete";
    body = el("div", { className: "view-stack" }, [
      projectionDegraded
        ? el("div", {
            className: "scanner-projection-warning",
            attrs: { role: "status" },
          }, [
            buildTag("scanner 投影已降级", "danger"),
            el("p", {
              text:
                "当前仅保留最后有效历史排行；无效事实或部分尾记录修复前，不把它解释为当前结果。",
            }),
          ])
        : null,
      el("div", { className: "detail-grid" }, [
        detailStat("评估时间", formatDateTime(latest.recorded_at)),
        detailStat("Run ID", latest.run_id),
        detailStat("候选", latest.candidate_count),
        detailStat("符合过滤", latest.eligible_count),
        detailStat("循环过滤", latest.filtered_by_cycles_count),
        detailStat("APR 窗口", `${latest.apr_window_seconds}s`),
        detailStat("最小循环", latest.min_complete_cycles),
        detailStat("返回行", latest.rows?.length || 0),
      ]),
      el("div", { className: "scanner-policy-note" }, [
        el("div", { className: "compact-label", text: "排行政策" }),
        el("p", {
          text:
            "显式 benchmark 优先，其余按 APR 降序；并列时按 exact instrument 稳定排序。benchmark 是展示优先级，不是评分加成。",
        }),
      ]),
      latest.truncated
        ? el("p", {
            className: "muted",
            text: `排行已截断：展示 ${latest.rows.length} / ${latest.eligible_count} 个符合条件的候选。`,
          })
        : null,
      renderScannerTable(latest.rows || []),
      el("p", {
        className: "muted",
        text:
          "所有数值来自最后一次离线历史回放；不证明 scanner 进程仍存活，不证明行情仍然新鲜，也不是投资建议。本页没有启动、停止、重连或交易控件。",
      }),
    ]);
  }
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "确定性虚拟网格排行",
      subtitle:
        "最后一次离线历史排行：网格穿越频率是波动代理，APR 与 Rating 都是估算证据，不是实时信号。",
      body,
    }),
  ]);
}

function renderScannerTable(rows) {
  if (rows.length === 0) {
    return renderEmpty(
      "本次排行没有返回行。",
      "可能所有标准候选都未达到最小完整循环；benchmark 例外仍需显式配置。",
    );
  }
  return el("div", {
    className: "table-wrap",
    attrs: {
      role: "region",
      tabindex: "0",
      "aria-label": "确定性虚拟网格排行明细，可横向滚动",
    },
  }, [
    el("table", { className: "scanner-table" }, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "排名 / 市场" }),
          el("th", { attrs: { scope: "col" }, text: "优先级" }),
          el("th", { attrs: { scope: "col" }, text: "APR / Rating" }),
          el("th", { attrs: { scope: "col" }, text: "循环证据" }),
          el("th", { attrs: { scope: "col" }, text: "价格 / 网格" }),
          el("th", { attrs: { scope: "col" }, text: "回放证据" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        rows.map((row) =>
          el("tr", {}, [
            el("td", {}, [
              el("div", { className: "scanner-market" }, [
                el("span", { className: "scanner-rank mono", text: `#${row.rank}` }),
                el("div", {}, [
                  el("strong", {
                    className: "scanner-symbol mono",
                    text: row.instrument?.symbol || "--",
                  }),
                  el("div", {
                    className: "muted mono",
                    text: `${row.instrument?.exchange || "--"} / ${humanizeToken(
                      row.instrument?.market_type || "unknown",
                    )}`,
                  }),
                ]),
              ]),
            ]),
            el("td", {}, [
              buildTag(
                humanizeToken(row.priority),
                row.priority === "benchmark" ? "info" : "ghost",
              ),
            ]),
            el("td", {}, [
              el("div", {
                className: "scanner-apr mono",
                text: `${row.estimated_apr}%`,
              }),
              el("div", { className: "tag-row" }, [
                buildTag(
                  String(row.rating_grade || "--").toUpperCase(),
                  scannerGradeTone(row.rating_grade),
                ),
                buildTag(`评分 ${row.rating_score}`, "ghost"),
              ]),
            ]),
            el("td", {}, [
              el("div", {
                className: "mono",
                text: `${row.complete_cycles} 完整 / ${row.recent_five_minute_cycles} 近 5m`,
              }),
              el("div", {
                className: "muted mono",
                text: `${row.cycles_per_hour} cycles/h · 买 ${row.buy_crosses} / 卖 ${row.sell_crosses}`,
              }),
            ]),
            el("td", {}, [
              el("div", { className: "mono", text: row.current_price }),
              el("div", {
                className: "muted mono",
                text: `${row.lower_price} — ${row.upper_price}`,
              }),
              el("div", {
                className: "muted",
                text: `宽 ${row.grid_width_percent}% · 间距 ${row.grid_interval_percent}% · ${row.grid_count} 格`,
              }),
            ]),
            el("td", {}, [
              el("div", {
                className: "mono",
                text: `seq ${row.last_observation_sequence} / ${row.observation_count} samples`,
              }),
              el("div", {
                className: "muted",
                text: formatDateTime(row.last_observed_at),
              }),
              el("div", {
                className: "muted mono",
                text: `24h 量 ${row.volume_24h_usdc}`,
              }),
            ]),
          ]),
        ),
      ),
    ]),
  ]);
}

function scannerMarketLabel(row) {
  return `${row.instrument?.exchange || "--"}/${row.instrument?.symbol || "--"}`;
}

function scannerGradeTone(grade) {
  switch (grade) {
    case "s":
      return "success";
    case "a":
      return "info";
    case "b":
      return "neutral";
    case "c":
      return "warning";
    case "d":
      return "danger";
    default:
      return "ghost";
  }
}

function renderMonitorRegion() {
  const projectionStatus = state.monitor?.projection_status || "degraded";
  const latest = state.monitor?.latest;
  let body;
  if (state.loads.monitor === "error" && !state.monitor) {
    body = renderError(state.errors.monitor);
  } else if (state.loads.monitor === "loading" && !state.monitor) {
    body = renderSkeleton(4);
  } else if (state.monitor && projectionStatus !== "complete") {
    body = el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, [
        detailStat("投影", humanizeToken(projectionStatus)),
        detailStat("无效事件", state.monitor.invalid_event_count ?? "--"),
        detailStat("保留事实", latest ? "已隐藏" : "无"),
      ]),
      el("p", {
        className: "muted",
        text: "监控投影已降级；最后一个有效结果停止展示，直到完整 journal 再次通过投影校验。",
      }),
    ]);
  } else if (!latest) {
    body = renderEmpty(
      "尚未观察到只读套利监控事件。",
      "监控投影不会把缺失行情提升为健康状态，也不会生成订单权限。",
    );
  } else {
    const projection = latest.projection || {};
    const facts = [
      detailStat("状态", humanizeToken(latest.state)),
      detailStat("读取方式", "历史快照"),
      detailStat(
        "监控对",
        `${latest.left.exchange}/${latest.left.symbol} ↔ ${latest.right.exchange}/${latest.right.symbol}`,
      ),
      detailStat("市场代次", latest.market_generation),
      detailStat("记录时间", formatDateTime(latest.recorded_at)),
    ];
    if (latest.state === "waiting") {
      facts.push(
        detailStat(
          "等待腿",
          `${projection.instrument?.exchange || "--"}/${projection.instrument?.symbol || "--"}`,
        ),
        detailStat("新鲜度", humanizeToken(projection.freshness || "missing")),
        detailStat("连续性", humanizeToken(projection.continuity || "missing")),
      );
    } else if (
      latest.state === "opportunity" ||
      latest.state === "no_opportunity"
    ) {
      facts.push(
        detailStat(
          "方向",
          `${projection.buy_exchange || "--"} → ${projection.sell_exchange || "--"}`,
        ),
        detailStat("价差", `${projection.spread_percent || "--"}%`),
        detailStat("阈值", `${projection.threshold_percent || "--"}%`),
      );
    } else {
      facts.push(
        detailStat("拒绝分类", humanizeToken(projection.failure || "unknown")),
      );
    }
    body = el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, facts),
      el("p", {
        className: "muted",
        text: "这是持久化监控事件的最后一次投影，不代表当前实时行情仍然新鲜。",
      }),
    ]);
  }
  return renderRegion({
    title: "只读套利监控",
    subtitle:
      "展示等待、无机会、机会与分析拒绝；不携带订单意图，也不把历史事件伪装成实时健康。",
    body,
  });
}

function renderAlertsSummaryRegion() {
  return renderRegion({
    title: "只读价格预警",
    subtitle:
      "展示最新预警、确认状态与本地通知的最后记录；投影降级时宁可隐藏 occurrence，也不猜测最新事实。",
    body: renderAlertBody({ condensed: true }),
  });
}

function renderAlertsView() {
  const model = state.alerts;
  const occurrences = visibleAlertOccurrences(model);
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "预警投影状态",
      subtitle:
        "预警页面只消费有界 read model。窗口化、降级、部分尾记录与确认状态都在这里显式说明。",
      body:
        state.loads.alerts === "error" && !state.alerts
          ? renderError(state.errors.alerts)
          : model
          ? el("div", { className: "detail-grid" }, [
              detailStat("投影", alertProjectionLabel(model)),
              detailStat(
                "可展示 occurrence",
                isTrustedAlertProjection(model) ? occurrences.length : "已隐藏",
              ),
              detailStat("无效事件", model.invalid_event_count ?? "--"),
              detailStat("窗口截断", model.occurrences_truncated ? "是" : "否"),
              detailStat("边界", humanizeToken(model.boundary?.kind || "snapshot_end")),
              detailStat("头序号", model.journal_head_sequence ?? "--"),
              detailStat("规则定义", "当前投影未提供"),
              detailStat("冷却状态", "当前投影未提供"),
            ])
          : renderSkeleton(8),
    }),
    renderRegion({
      title: "预警 occurrence",
      subtitle:
        "按 occurrence sequence 展示价格、方向、触发与确认时间及各 adapter 的最后已记录状态，不暴露写入口。规则定义与冷却状态尚未进入当前投影，因此明确标记为不可用。",
      body: renderAlertBody({ condensed: false }),
    }),
  ]);
}

function renderAlertBody({ condensed }) {
  const model = state.alerts;
  const occurrences = visibleAlertOccurrences(model);
  if (state.loads.alerts === "error" && !state.alerts) {
    return renderError(state.errors.alerts);
  }
  if (state.loads.alerts === "loading" && !state.alerts) {
    return renderSkeleton(condensed ? 4 : 8);
  }
  if (model && !isTrustedAlertProjection(model)) {
    return el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, [
        detailStat("投影", alertProjectionLabel(model)),
        detailStat("无效事件", model.invalid_event_count ?? "--"),
        detailStat("可展示 occurrence", "已隐藏"),
        detailStat("窗口截断", model.occurrences_truncated ? "是" : "否"),
      ]),
      el("p", {
        className: "muted",
        text: "预警投影未通过完整性校验；界面不会把不可信的最近预警提升成可操作事实。",
      }),
    ]);
  }
  if (occurrences.length === 0) {
    return renderEmpty(
      "当前冻结快照中还没有价格预警 occurrence。",
      "已检查 /api/v1/alerts 返回的 occurrences；没有写路径会在这里补造状态。",
    );
  }
  if (condensed) {
    const latest = occurrences[occurrences.length - 1];
    return el("div", { className: "view-stack" }, [
      el("div", { className: "detail-grid" }, [
        detailStat("最新序号", latest.alert_sequence),
        detailStat("标的", `${latest.exchange}/${latest.symbol}`),
        detailStat("类型", humanizeToken(latest.kind)),
        detailStat("价格", latest.price),
        detailStat("确认", latest.acknowledged_at ? "已确认" : "待确认"),
        detailStat("通知", summarizeDeliveries(latest.deliveries)),
      ]),
      el("p", {
        className: "muted",
        text: `最近记录时间：${formatDateTime(latest.recorded_at)}；当前冻结视图保留 ${occurrences.length} 条 occurrence，${
          model.occurrences_truncated ? "更早记录已被有界淘汰。" : "未发生窗口截断。"
        }`,
      }),
    ]);
  }
  return renderAlertTable(occurrences);
}

function isTrustedAlertProjection(model) {
  if (
    !model ||
    !TRUSTED_ALERT_PROJECTION_IDS.has(model.projection_status) ||
    !Array.isArray(model.occurrences) ||
    model.occurrences.length > MAX_ALERT_OCCURRENCES ||
    model.invalid_event_count !== 0 ||
    model.boundary?.kind !== "snapshot_end"
  ) {
    return false;
  }
  return model.projection_status === "windowed"
    ? model.occurrences_truncated === true
    : model.occurrences_truncated === false;
}

function alertProjectionLabel(model) {
  if (!model) {
    return humanizeToken("loading");
  }
  if (model.projection_status === "degraded") {
    return humanizeToken("degraded");
  }
  return isTrustedAlertProjection(model)
    ? humanizeToken(model.projection_status)
    : "降级 / 契约不一致";
}

function visibleAlertOccurrences(model) {
  if (!isTrustedAlertProjection(model) || !Array.isArray(model.occurrences)) {
    return [];
  }
  return model.occurrences;
}

function renderAlertTable(occurrences) {
  return el("div", {
    className: "table-wrap",
    attrs: {
      role: "region",
      tabindex: "0",
      "aria-label": "价格预警明细，可横向滚动",
    },
  }, [
    el("table", { className: "alert-table" }, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", {
            className: "table-number",
            attrs: { scope: "col" },
            text: "序号",
          }),
          el("th", { attrs: { scope: "col" }, text: "标的" }),
          el("th", { attrs: { scope: "col" }, text: "类型" }),
          el("th", {
            className: "table-number",
            attrs: { scope: "col" },
            text: "价格 / 波动",
          }),
          el("th", { attrs: { scope: "col" }, text: "通知结果" }),
          el("th", { attrs: { scope: "col" }, text: "触发 / 确认" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        occurrences
          .slice()
          .reverse()
          .map((occurrence) => {
            const deliveries = Array.isArray(occurrence.deliveries)
              ? occurrence.deliveries
              : [];
            return el("tr", {}, [
              el("td", {
                className: "mono table-number",
                text: String(occurrence.alert_sequence),
              }),
              el("td", {}, [
                el("div", {
                  className: "mono alert-symbol",
                  text: `${occurrence.exchange}/${occurrence.symbol}`,
                }),
                el("div", { className: "muted", text: humanizeToken(occurrence.market_type) }),
              ]),
              el("td", {}, [buildTag(humanizeToken(occurrence.kind), "warning")]),
              el("td", { className: "table-number" }, [
                el("div", { className: "mono", text: occurrence.price }),
                el("div", {
                  className: "muted mono",
                  text: occurrence.change_percent ? `${occurrence.change_percent}%` : "--",
                }),
              ]),
              el("td", {}, [
                el(
                  "div",
                  { className: "tag-row" },
                  deliveries.length > 0
                    ? deliveries.map((delivery) =>
                        buildTag(
                          `${delivery.adapter_id}: ${humanizeToken(delivery.status)}`,
                          toneForAlertDelivery(delivery.status),
                        ),
                      )
                    : [buildTag("未发送", "neutral")],
                ),
                deliveries.some((delivery) => delivery.failure)
                  ? el("div", {
                      className: "muted alert-adapter-failure",
                      text: deliveries
                        .filter((delivery) => delivery.failure)
                        .map((delivery) => `${delivery.adapter_id}=${humanizeToken(delivery.failure)}`)
                        .join(" / "),
                    })
                  : null,
                deliveries.length > 0
                  ? el("div", {
                      className: "muted mono alert-adapter-failure",
                      text: deliveries
                        .map(
                          (delivery) =>
                            `${delivery.adapter_id} ${formatDateTime(delivery.updated_at)}`,
                        )
                        .join(" / "),
                    })
                  : null,
              ]),
              el("td", {}, [
                buildTag(
                  occurrence.acknowledged_at ? "已确认" : "待确认",
                  occurrence.acknowledged_at ? "success" : "warning",
                ),
                el("div", {
                  className: "muted mono",
                  text: `触发 ${formatDateTime(occurrence.recorded_at)}`,
                }),
                el("div", {
                  className: "muted mono",
                  text: occurrence.acknowledged_at
                    ? `确认 ${formatDateTime(occurrence.acknowledged_at)}`
                    : "确认 --",
                }),
              ]),
            ]);
          }),
      ),
    ]),
  ]);
}

function summarizeDeliveries(deliveries) {
  if (!deliveries || deliveries.length === 0) {
    return "未发送";
  }
  const counts = new Map();
  for (const delivery of deliveries) {
    const key = delivery.status || "unknown";
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return [...counts.entries()]
    .map(([status, count]) => `${humanizeToken(status)} ${count}`)
    .join(" / ");
}

function countPendingAlertDeliveries(model) {
  return visibleAlertOccurrences(model).reduce(
    (count, occurrence) =>
      count +
      (Array.isArray(occurrence.deliveries)
        ? occurrence.deliveries.filter((delivery) => delivery.status === "pending").length
        : 0),
    0,
  );
}

function toneForAlertDelivery(status) {
  switch (status) {
    case "succeeded":
      return "success";
    case "failed":
    case "timed_out":
      return "danger";
    case "dropped":
    case "pending":
      return "warning";
    default:
      return "neutral";
  }
}

function renderRibbon() {
  const metrics = [
    {
      label: "投影",
      value: humanizeToken(state.system?.projection_status || "loading"),
      highlight: true,
    },
    {
      label: "批次",
      value: state.system?.execution_batch_count ?? "--",
    },
    {
      label: "恢复",
      value: state.system?.recovery_required_count ?? "--",
    },
    {
      label: "警告",
      value: state.system?.warning_count ?? "--",
    },
    {
      label: "冲突",
      value: state.system?.conflict_count ?? "--",
    },
    {
      label: "事件流",
      value: compactEventStreamLabel(),
    },
  ];
  const leadCopy = state.executions?.operator
    ? state.executions.operator.batches.length > 0
      ? "风险事实固定在这里：先看权限、新鲜度、游标与恢复，再看批次详情。"
      : "读取模型已经可用，但当前日志快照尚未投影出执行批次。"
    : "正在加载有界操作快照。";
  return el("section", { className: "system-ribbon" }, [
    el(
      "div",
      { className: "system-ribbon-grid" },
      [
        el("div", { className: "ribbon-row", attrs: { "data-highlight": "true" } }, [
          el("div", { className: "compact-label", text: "跨域运行事实" }),
          el("div", { className: "metric-value", text: state.system?.live_trading_enabled ? "LIVE 已开启" : "PAPER / LIVE 已关闭" }),
          el("p", { className: "muted", text: leadCopy }),
          el("div", { className: "inline-button-row" }, [
            buildTag(accessDescriptor(), state.system?.authentication_required ? "warning" : "neutral"),
            buildTag(`操作通知：${eventStreamLabel()}`, eventStreamTone()),
            buildTag(state.cursor ? "游标已固定" : "游标待生成", state.cursor ? "info" : "ghost"),
          ]),
        ]),
        ...metrics.slice(1).map((metric) =>
          el("div", { className: "ribbon-row" }, [
            el("div", { className: "compact-label", text: metric.label }),
            el("div", { className: "metric-value", text: String(metric.value) }),
          ]),
        ),
      ],
    ),
  ]);
}

function renderExecutionsSummaryRegion() {
  const batches = filteredBatches().slice(0, 6);
  return renderRegion({
    title: "最近执行账本",
    subtitle: "展示计划、结果、恢复与稳定标识，但绝不构造交易权限。",
    body:
      state.loads.executions === "error" && !state.executions
        ? renderError(state.errors.executions)
        : state.loads.executions === "loading" && !state.executions
        ? renderSkeleton(6)
        : batches.length > 0
          ? renderBatchTable(batches, { condensed: true })
          : renderEmpty(
              "这个有界快照中没有执行批次。",
              "已检查 /api/v1/executions 返回的 operator.batches。",
            ),
  });
}

function renderRecentNoticesRegion() {
  const page = state.lastPage;
  return renderRegion({
    title: "最近事件通知",
    subtitle: "事件页不携带原始载荷；浏览器只读取通知元数据，再重新获取快照。",
    body:
      !page && state.loads.executions === "error"
        ? renderEmpty(
            "最近事件通知当前不可用。",
            "安全错误说明与重试操作已固定在页面顶部。",
          )
        : page
        ? page.events.length > 0
          ? renderNoticeTable(page)
          : renderEmpty(
              "当前游标之后没有新的操作通知。",
              `已检查边界：${humanizeToken(page.boundary?.kind || "snapshot_end")}。`,
            )
        : renderSkeleton(4),
  });
}

function renderProjectionRegion() {
  const warnings = state.executions?.operator?.warnings || [];
  const truncation = state.system?.truncation;
  const blocks = [
    detailStat("投影状态", humanizeToken(state.system?.projection_status || "loading")),
    detailStat("批次已截断", truncation?.batches ? "是" : "否"),
    detailStat("警告已截断", truncation?.warnings ? "是" : "否"),
    detailStat("最近更新", state.lastSnapshotAt ? formatDateTime(state.lastSnapshotAt) : "--"),
  ];
  return renderRegion({
    title: "投影证据",
    subtitle: "窗口化与降级是明确的产品状态，并固定显示在受影响区域上方。",
    body:
      state.loads.executions === "error" && !state.executions
        ? renderEmpty(
            "投影证据当前不可用。",
            "安全错误说明与重试操作已固定在页面顶部。",
          )
        : el("div", { className: "view-stack" }, [
            el("div", { className: "detail-grid" }, blocks),
            warnings.length > 0
              ? el(
                  "ul",
                  { className: "warning-list", attrs: { role: "list" } },
                  warnings.slice(0, 6).map((warning) =>
                    el("li", {}, [
                      el("span", { text: `${humanizeToken(warning.code)}${warning.sequence ? ` @ ${warning.sequence}` : ""}` }),
                      buildTag(warning.batch_id ? "批次范围" : "全局", warning.batch_id ? "warning" : "neutral"),
                    ]),
                  ),
                )
              : renderEmpty(
                  "当前没有有界投影警告。",
                  "已检查 /api/v1/executions 返回的 operator.warnings。",
                ),
          ]),
  });
}

function renderCapabilityPulseRegion() {
  const manifest = state.capabilities;
  const counts = summarizeCapabilityLevels(manifest?.capabilities || []);
  const body =
    state.loads.capabilities === "error" && !manifest
      ? renderError(state.errors.capabilities)
      : manifest
      ? el("div", { className: "view-stack" }, [
          el("div", { className: "metrics-grid" }, [
            detailStat("可用", counts.available),
            detailStat("只读", counts["read-only"]),
            detailStat("单次模拟", counts["paper-once"]),
            detailStat("仅校验", counts["validate-only"]),
            detailStat("不可用", counts.unavailable),
          ]),
          el("div", { className: "support-grid" }, [
            detailStat("发布阶段", manifest.release_stage),
            detailStat("实盘交易", manifest.live_trading_enabled ? "已启用" : "已禁用"),
            detailStat("适配器", manifest.adapters.length),
            detailStat("能力项", manifest.capabilities.length),
          ]),
        ])
      : renderSkeleton(4);
  return renderRegion({
    title: "能力脉冲",
    subtitle: "CLI、HTTP 与 Web 共用同一份能力事实；蓝色表示交互，而非权限。",
    body,
  });
}

function renderExecutionsView() {
  const batches = filteredBatches();
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "执行筛选",
      subtitle: "筛选与所选批次保留在 URL 中；不透明恢复游标只保存在页面内存。",
      body: el("div", { className: "filter-row" }, [
        renderSelect(
          "执行状态",
          [
            ["all", "全部批次"],
            ["attention", "需要关注"],
            ["completed", "已完成"],
            ["partial", "部分完成"],
            ["failed", "失败"],
            ["conflict", "冲突"],
            ["unknown", "结果未知"],
          ],
          state.route.batchState,
          (value) => navigate({ batchState: value, batch: "" }),
        ),
      ]),
    }),
    renderRegion({
      title: "执行账本",
      subtitle: "全宽账本保持高密度分栏；选择批次后，可在抽屉中检查计划与恢复事实。",
      body:
        state.loads.executions === "error" && !state.executions
          ? renderError(state.errors.executions)
          : state.loads.executions === "loading" && !state.executions
          ? renderSkeleton(8)
          : batches.length > 0
            ? renderBatchTable(batches, { condensed: false })
            : renderEmpty(
                "没有执行批次符合当前筛选。",
                "清除筛选，或等待下一次有界投影刷新。",
              ),
    }),
  ]);
}

function renderIntegrationsView() {
  const manifest = state.capabilities;
  const capabilities = filteredCapabilities();
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "集成筛选",
      subtitle: "按领域与能力面收窄证据矩阵，不改变任何运行权限。",
      body: el("div", { className: "filter-row" }, [
        renderSelect(
          "能力领域",
          [
            ["all", "全部领域"],
            ["config", "配置"],
            ["control-plane", "控制面"],
            ["exchange", "交易所"],
            ["history", "历史记录"],
            ["risk", "风险"],
            ["runtime", "运行时"],
            ["strategy", "策略"],
          ],
          state.route.area,
          (value) => navigate({ area: value }),
        ),
        renderSelect(
          "适配器能力面",
          [
            ["all", "全部能力面"],
            ["public-data", "公共数据"],
            ["testnet-protocol", "测试网协议"],
            ["authenticated", "鉴权访问"],
            ["reconcile", "对账"],
            ["live", "实盘"],
          ],
          state.route.facet,
          (value) => navigate({ facet: value }),
        ),
      ]),
    }),
    renderRegion({
      title: "适配器支持矩阵",
      subtitle: "不可用保持中性且明确；每个单元格展示支持强度、阻塞项和证据数量。",
      body:
        state.loads.capabilities === "error" && !manifest
          ? renderError(state.errors.capabilities)
          : manifest
          ? renderAdapterMatrix(manifest.adapters)
          : renderSkeleton(8),
    }),
    renderRegion({
      title: "能力账本",
      subtitle: "保留精确的能力 ID、访问范围与阻塞项，使 Web 与运行时权限契约一致。",
      body:
        state.loads.capabilities === "error" && !manifest
          ? renderError(state.errors.capabilities)
          : manifest
          ? renderCapabilityTable(capabilities)
          : renderSkeleton(8),
    }),
  ]);
}

function renderRiskView() {
  const riskyBatches = attentionBatches();
  const tasks = attentionTasks();
  const capabilities = capabilityRowsFor("risk");
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "风险总览",
      subtitle:
        "只展示关闭优先的风险状态；预留、Kill switch 或凭证事实缺失时保持不可用，不做推断。",
      body: el("div", { className: "detail-grid" }, [
        detailStat("Kill switch", humanizeToken(state.system?.kill_switch || "not_available")),
        detailStat("需要恢复", state.system?.recovery_required_count ?? "--"),
        detailStat("冲突", state.system?.conflict_count ?? "--"),
        detailStat("待调查任务", tasks.length),
        detailStat("风险能力", capabilities.length),
        detailStat("投影", humanizeToken(state.system?.projection_status || "unavailable")),
      ]),
    }),
    renderRegion({
      title: "恢复账本",
      subtitle:
        "这里的批次需要调查或对账；本页不提供重试、释放或提交操作。",
      body:
        state.loads.executions === "error" && !state.executions
          ? renderError(state.errors.executions)
          : state.loads.executions === "loading" && !state.executions
          ? renderSkeleton(6)
          : riskyBatches.length > 0
          ? renderBatchTable(riskyBatches, { condensed: false })
          : renderEmpty(
              "当前没有投影出恢复压力的执行批次。",
              "已检查 operator.batches 中 recovery 非 none 或状态为 conflict/partial/failed/outcome_unknown 的记录。",
            ),
    }),
    renderRegion({
      title: "任务调查账本",
      subtitle:
        "持久任务生命周期可能表明停止或失败后仍需人工核验。",
      body:
        state.loads.tasks === "error" && !state.tasks
          ? renderError(state.errors.tasks)
          : state.loads.tasks === "loading" && !state.tasks
          ? renderSkeleton(5)
          : tasks.length > 0
          ? renderTaskAttentionTable(tasks)
          : renderEmpty(
              "当前没有需要调查的任务。",
              "已检查 /api/v1/tasks 中 recovery=investigate、failure 或 failed 阶段。",
            ),
    }),
    renderRegion({
      title: "风险能力账本",
      subtitle:
        "展示当前清单中由风险域拥有的权限证据；不可用状态保持中性且明确。",
      body:
        state.loads.capabilities === "error" && !state.capabilities
          ? renderError(state.errors.capabilities)
          : state.capabilities
          ? capabilities.length > 0
            ? renderCapabilityTable(capabilities)
            : renderEmpty(
                "当前清单没有额外的风险能力项。",
                "已检查 /api/v1/capabilities 中 area=risk 的记录。",
              )
          : renderSkeleton(6),
    }),
  ]);
}

function renderReplayView() {
  const page = state.lastPage;
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "回放快照",
      subtitle:
        "这里只展示历史快照与游标水位；不是实时撮合回放，也不能提交工作。",
      body: el("div", { className: "detail-grid" }, [
        detailStat("Journal", state.system?.journal_id || "--"),
        detailStat("头部序号", state.system?.head_sequence ?? "--"),
        detailStat("游标", state.cursor || "--"),
        detailStat("事件页", page?.events?.length || 0),
        detailStat("边界", humanizeToken(page?.boundary?.kind || "snapshot_end")),
        detailStat("事件流", eventStreamLabel()),
      ]),
    }),
    renderRegion({
      title: "历史投影时间",
      subtitle:
        "每行标出一个只读投影最后保留的事实，便于确认页面实际描述的时间范围。",
      body: renderReplaySurfaceTable(),
    }),
    renderRegion({
      title: "最近操作事件",
      subtitle:
        "事件元数据经过有界与脱敏处理；游标只保留在页面内存中，不进入分享链接。",
      body:
        page
          ? page.events.length > 0
            ? renderNoticeTable(page)
            : renderEmpty(
                "当前游标水位之后没有新的操作事件。",
                `已检查边界：${humanizeToken(page.boundary?.kind || "snapshot_end")}。`,
              )
          : state.loads.executions === "error"
          ? renderError(state.errors.executions)
          : renderSkeleton(4),
    }),
  ]);
}

function renderSettingsView() {
  const alertAdapters = uniqueValues(
    visibleAlertOccurrences(state.alerts).flatMap((occurrence) =>
      Array.isArray(occurrence.deliveries)
        ? occurrence.deliveries.map((delivery) => delivery.adapter_id)
        : [],
    ),
  );
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "访问与外壳",
      subtitle:
        "这里只展示外壳可见设置；Bearer 只留在页内存，文件系统路径与适配器凭证保持隐藏。",
      body: el("div", { className: "detail-grid" }, [
        detailStat("产品版本", state.system?.product_version || "--"),
        detailStat("发布阶段", state.system?.release_stage || "--"),
        detailStat("访问范围", state.system?.access_scope || "--"),
        detailStat("需要认证", state.system?.authentication_required ? "是" : "否"),
        detailStat("Bearer 绑定", state.authToken ? "仅页内存" : "未绑定"),
        detailStat("事件流", eventStreamLabel()),
      ]),
    }),
    renderRegion({
      title: "投影来源",
      subtitle:
        "以下为嵌入式外壳当前可见的有界 journal 与读取模型水位。",
      body: el("div", { className: "evidence-grid" }, [
        renderSubregion("系统", el("div", { className: "detail-grid" }, [
          detailStat("Journal", state.system?.journal_id || "--"),
          detailStat("头部序号", state.system?.head_sequence ?? "--"),
          detailStat("投影", humanizeToken(state.system?.projection_status || "unavailable")),
          detailStat("更新时间", state.lastSnapshotAt ? formatDateTime(state.lastSnapshotAt) : "--"),
        ])),
        renderSubregion("监控", el("div", { className: "detail-grid" }, [
          detailStat("Journal", state.monitor?.journal_id || "--"),
          detailStat("头部序号", state.monitor?.journal_head_sequence ?? "--"),
          detailStat("投影", humanizeToken(state.monitor?.projection_status || "unavailable")),
          detailStat("最新记录", state.monitor?.latest ? formatDateTime(state.monitor.latest.recorded_at) : "--"),
        ])),
        renderSubregion("预警", el("div", { className: "detail-grid" }, [
          detailStat("Journal", state.alerts?.journal_id || "--"),
          detailStat("头部序号", state.alerts?.journal_head_sequence ?? "--"),
          detailStat("投影", alertProjectionLabel(state.alerts)),
          detailStat("边界", humanizeToken(state.alerts?.boundary?.kind || "snapshot_end")),
        ])),
        renderSubregion("任务 / Scanner", el("div", { className: "detail-grid" }, [
          detailStat("任务头部序号", state.tasks?.journal_head_sequence ?? "--"),
          detailStat("任务投影", humanizeToken(state.tasks?.projection_status || "unavailable")),
          detailStat("Scanner 头部序号", state.scanner?.journal_head_sequence ?? "--"),
          detailStat("Scanner 投影", humanizeToken(state.scanner?.projection_status || "unavailable")),
        ])),
      ]),
    }),
    renderRegion({
      title: "只读边界",
      subtitle:
        "外壳默认只读；仅策略页可选用同源 trusted submit 控制 Paper owner，数据目录、日志路径和适配器凭证仍不投影到浏览器。",
      body: el("div", { className: "detail-grid" }, [
        detailStat("HTML 路由", VIEW_IDS.size),
        detailStat("API 路由", "8 GET + 可选 1 POST"),
        detailStat("已见预警适配器", alertAdapters.length),
        detailStat("令牌持久化", "仅页内存"),
        detailStat("目录", "未投影"),
        detailStat("凭证", "未投影"),
      ]),
    }),
  ]);
}

function renderSubregion(title, body) {
  return el("section", { className: "subregion" }, [
    el("div", { className: "compact-label", text: title }),
    body,
  ]);
}

function renderTaskAttentionTable(tasks) {
  return el("div", { className: "table-wrap" }, [
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "任务" }),
          el("th", { attrs: { scope: "col" }, text: "阶段" }),
          el("th", { attrs: { scope: "col" }, text: "恢复" }),
          el("th", { attrs: { scope: "col" }, text: "更新时间" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        tasks.map((task) =>
          el("tr", {}, [
            el("td", {}, [
              el("div", { className: "mono", text: task.task_id || "--" }),
              el("div", { className: "muted", text: humanizeToken(task.kind) }),
            ]),
            el("td", {}, [buildTag(humanizeToken(task.phase), taskTone(task))]),
            el("td", {}, [
              buildTag(humanizeToken(task.recovery), tagToneForRecovery(task.recovery)),
              task.failure
                ? el("div", { className: "muted", text: humanizeToken(task.failure) })
                : null,
            ]),
            el("td", { className: "mono", text: formatDateTime(task.updated_at) }),
          ]),
        ),
      ),
    ]),
  ]);
}

function renderReplaySurfaceTable() {
  const latestAlert = latestAlertOccurrence();
  const rows = [
    [
      "监控",
      humanizeToken(state.monitor?.projection_status || "unavailable"),
      state.monitor?.latest ? formatDateTime(state.monitor.latest.recorded_at) : "--",
      state.monitor?.latest?.source_sequence ?? "--",
    ],
    [
      "预警",
      alertProjectionLabel(state.alerts),
      latestAlert ? formatDateTime(latestAlert.recorded_at) : "--",
      latestAlert?.source_sequence ?? "--",
    ],
    [
      "任务",
      humanizeToken(state.tasks?.projection_status || "unavailable"),
      latestTaskUpdateAt(),
      latestTaskSequence(),
    ],
    [
      "Scanner",
      humanizeToken(state.scanner?.projection_status || "unavailable"),
      state.scanner?.latest ? formatDateTime(state.scanner.latest.recorded_at) : "--",
      state.scanner?.latest?.source_sequence ?? "--",
    ],
    [
      "执行",
      humanizeToken(state.system?.projection_status || "unavailable"),
      state.lastSnapshotAt ? formatDateTime(state.lastSnapshotAt) : "--",
      state.system?.head_sequence ?? "--",
    ],
  ];
  return el("div", { className: "table-wrap" }, [
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "运行面" }),
          el("th", { attrs: { scope: "col" }, text: "投影" }),
          el("th", { attrs: { scope: "col" }, text: "最新事实" }),
          el("th", { attrs: { scope: "col" }, text: "序号" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        rows.map(([surface, projection, recordedAt, sequence]) =>
          el("tr", {}, [
            el("td", { text: surface }),
            el("td", {}, [buildTag(String(projection), projectionToneForLabel(projection))]),
            el("td", { className: "mono", text: String(recordedAt) }),
            el("td", { className: "mono", text: String(sequence) }),
          ]),
        ),
      ),
    ]),
  ]);
}

function renderDrawer() {
  const batch = selectedBatch();
  const isMobile = window.matchMedia("(max-width: 671px)").matches;
  if (!batch || state.route.view !== "executions") {
    state.drawerFocusedBatch = "";
    dom.drawerHost.hidden = true;
    dom.drawerHost.replaceChildren();
    return;
  }
  dom.drawerHost.hidden = false;
  const closeButton = el("button", {
    className: "secondary",
    text: isMobile ? "收起详情" : "关闭抽屉",
    attrs: { type: "button" },
    onclick: closeDrawer,
  });
  const panel = el("section", {
    className: "drawer-panel",
    attrs: {
      "data-open": "true",
      "aria-labelledby": "drawer-title",
      role: "dialog",
    },
  }, [
    el("div", { className: "drawer-header" }, [
      el("div", { className: "compact-label", text: "执行详情" }),
      el("div", { className: "drawer-title mono visually-contained", attrs: { id: "drawer-title" }, text: batch.batch_id }),
      el("div", { className: "tag-row" }, [
        buildBatchStateTag(batch.state),
        buildRecoveryTag(batch.recovery),
        buildTag(batch.status_summary, tagToneForRecovery(batch.recovery)),
      ]),
      el("div", { className: "drawer-actions" }, [
        el("button", {
          className: "ghost",
          text: "复制批次 ID",
          attrs: { type: "button" },
          onclick: () => void copyValue(batch.batch_id, "已复制批次 ID。"),
        }),
        el("button", {
          className: "ghost",
          text: "复制游标",
          attrs: { type: "button" },
          onclick: () => void copyValue(state.cursor, "已复制当前游标。"),
        }),
        closeButton,
      ]),
    ]),
    el("div", { className: "drawer-grid" }, [
      renderDrawerFactSection(batch),
      renderDrawerTimeline(batch),
      renderDrawerWarnings(batch),
    ]),
  ]);
  dom.drawerHost.replaceChildren(panel);
  if (state.drawerFocusedBatch !== batch.batch_id) {
    state.drawerFocusedBatch = batch.batch_id;
    window.requestAnimationFrame(() => closeButton.focus());
  }
}

function closeDrawer() {
  const returnBatch = state.drawerReturnBatch || state.route.batch;
  state.drawerFocusedBatch = "";
  navigate({ batch: "" });
  window.requestAnimationFrame(() => {
    const returnTarget = returnBatch
      ? findBatchActionTarget(returnBatch, state.drawerReturnAction)
      : null;
    (returnTarget || dom.main.closest("main"))?.focus();
  });
}

function findBatchActionTarget(batchId, action) {
  return (
    Array.from(
      document.querySelectorAll("[data-batch-id][data-batch-action]"),
    ).find(
      (element) =>
        element.dataset.batchId === batchId &&
        element.dataset.batchAction === action,
    ) || null
  );
}

function renderDrawerFactSection(batch) {
  const facts = [
    ["策略", batch.strategy],
    ["交易对", batch.symbol],
    ["首个序号", String(batch.first_sequence)],
    ["最新序号", String(batch.last_sequence)],
    ["首次观察", formatDateTime(batch.first_seen_at)],
    ["最近更新", formatDateTime(batch.updated_at)],
    ["计划时间", formatOptionalDate(batch.planned_at)],
    ["结果时间", formatOptionalDate(batch.outcome_at)],
    ["交易腿数量", formatOptionalNumber(batch.leg_count)],
    ["回执数量", formatOptionalNumber(batch.receipt_count)],
    ["预期回执", formatOptionalNumber(batch.expected_receipt_count)],
    ["失败索引", formatOptionalNumber(batch.failed_index)],
    ["未尝试数量", formatOptionalNumber(batch.unattempted_count)],
    ["对账观察", formatOptionalNumber(batch.reconciliation_observation_count)],
    ["对账错误", formatOptionalNumber(batch.reconciliation_error_count)],
    ["已记录失败", batch.failure_recorded ? "是" : "否"],
  ];
  return el("section", { className: "region" }, [
    el("div", { className: "region-title", text: "计划与结果事实" }),
    el(
      "div",
      { className: "drawer-facts" },
      facts.map(([label, value]) =>
        el("div", { className: "drawer-fact-row" }, [
          el("span", { className: "compact-label", text: label }),
          el("span", {
            className: typeof value === "string" && looksMonospaced(value) ? "mono align-right visually-contained" : "align-right visually-contained",
            text: value,
          }),
        ]),
      ),
    ),
  ]);
}

function renderDrawerTimeline(batch) {
  const phases = (batch.phases || []).map((phase, index) =>
    el("div", { className: "timeline-item" }, [
      el("div", { className: "drawer-fact-row" }, [
        el("span", { text: `阶段 ${index + 1}` }),
        buildTag(humanizeToken(phase), tagToneForPhase(phase)),
      ]),
    ]),
  );
  return el("section", { className: "region" }, [
    el("div", { className: "region-title", text: "持久化阶段带" }),
    phases.length > 0
      ? el("div", { className: "timeline" }, phases)
      : renderEmpty("这个批次没有投影出持久化阶段。", "批次存在，但缺少阶段证据。"),
  ]);
}

function renderDrawerWarnings(batch) {
  const warnings = (state.executions?.operator?.warnings || []).filter(
    (warning) => warning.batch_id === batch.batch_id,
  );
  return el("section", { className: "region" }, [
    el("div", { className: "region-title", text: "批次范围警告" }),
    warnings.length > 0
      ? el(
          "ul",
          { className: "warning-list", attrs: { role: "list" } },
          warnings.map((warning) =>
            el("li", {}, [
              el("span", { text: humanizeToken(warning.code) }),
              el("span", { className: "mono", text: warning.sequence ? `#${warning.sequence}` : "--" }),
            ]),
          ),
        )
      : renderEmpty(
          "当前批次没有关联的投影警告。",
          "已按 batch_id 筛选并检查 operator.warnings。",
        ),
  ]);
}

function renderBand(band) {
  return el("section", { className: "state-band", attrs: { "data-tone": band.tone } }, [
    el("div", { className: "region-head" }, [
      el("div", { className: "region-title", text: band.title }),
      buildTag(band.tag, band.tone),
    ]),
    el("p", { text: band.message }),
    band.action
      ? el("div", { className: "banner-actions" }, [
          el("button", {
            className: band.action.ghost ? "ghost" : "secondary",
            attrs: { type: "button" },
            text: band.action.label,
            onclick: band.action.onClick,
          }),
        ])
      : null,
  ]);
}

function renderRegion({ title, subtitle, body }) {
  return el("section", { className: "region" }, [
    el("div", { className: "region-head" }, [
      el("div", { className: "key-value" }, [
        el("h2", { className: "region-title", text: title }),
        el("p", { className: "region-subtitle", text: subtitle }),
      ]),
    ]),
    body,
  ]);
}

function renderSkeleton(rows) {
  return el(
    "div",
    { className: "skeleton", attrs: { "aria-hidden": "true" } },
    Array.from({ length: rows }, (_, index) =>
      el("div", { className: `skeleton-bar ${index % 3 === 0 ? "short" : index % 3 === 1 ? "medium" : "long"}` }),
    ),
  );
}

function renderEmpty(message, checkedFact) {
  return el("div", { className: "empty-state" }, [
    el("p", { text: message }),
    el("p", { className: "muted", text: checkedFact }),
  ]);
}

function renderError(problem) {
  const description = errorDescription(problem);
  return el("div", { className: "error-state" }, [
    el("p", { text: description.title }),
    el("p", { className: "muted", text: description.message }),
    el("div", { className: "inline-button-row" }, [
      el("button", {
        className: "secondary",
        attrs: { type: "button" },
        text: "重试快照",
        onclick: () => void refreshOperationalTruth(),
      }),
      description.clearCursor
          ? el("button", {
            className: "ghost",
            attrs: { type: "button" },
            text: "清除游标",
            onclick: () => {
              state.cursor = "";
              void loadExecutions();
              restartStream();
              render();
            },
          })
        : null,
    ]),
  ]);
}

function renderBatchTable(batches, { condensed }) {
  const table = el("table", {}, [
    el("thead", {}, [
      el("tr", {}, [
        el("th", { attrs: { scope: "col" }, text: "批次" }),
        el("th", { attrs: { scope: "col" }, text: "策略 / 交易对" }),
        el("th", { attrs: { scope: "col" }, text: "状态" }),
        el("th", { attrs: { scope: "col" }, text: "恢复" }),
        el("th", { attrs: { scope: "col" }, text: "序号" }),
        el("th", { attrs: { scope: "col" }, text: "更新时间" }),
        condensed ? null : el("th", { attrs: { scope: "col" }, text: "阶段" }),
        el("th", { attrs: { scope: "col" }, text: "检查" }),
      ]),
    ]),
    el(
      "tbody",
      {},
      batches.map((batch) => {
        const selected = state.route.batch === batch.batch_id;
        return el("tr", {
          attrs: { "data-selected": selected ? "true" : "false" },
        }, [
          el("td", {}, [
            el("button", {
              className: "row-button mono visually-contained",
              attrs: {
                type: "button",
                title: batch.batch_id,
                "data-batch-id": batch.batch_id,
                "data-batch-action": "id",
              },
              text: shortId(batch.batch_id),
              onclick: () => {
                state.drawerReturnBatch = batch.batch_id;
                state.drawerReturnAction = "id";
                navigate({ view: "executions", batch: batch.batch_id });
              },
            }),
          ]),
          el("td", {}, [
            el("div", { className: "mono", text: batch.strategy }),
            el("div", { className: "muted mono", text: batch.symbol }),
          ]),
          el("td", {}, [buildBatchStateTag(batch.state)]),
          el("td", {}, [buildRecoveryTag(batch.recovery)]),
          el("td", { className: "mono" }, [
            el("div", { text: `${batch.first_sequence} -> ${batch.last_sequence}` }),
          ]),
          el("td", {}, [
            el("div", { className: "mono", text: formatDateTime(batch.updated_at) }),
            batch.status_summary ? el("div", { className: "muted", text: batch.status_summary }) : null,
          ]),
          condensed
            ? null
            : el("td", {}, [
                el("div", { className: "tag-row" }, (batch.phases || []).map((phase) => buildTag(humanizeToken(phase), tagToneForPhase(phase)))),
              ]),
          el("td", {}, [
            el("button", {
              className: "row-link",
              attrs: {
                type: "button",
                "data-batch-id": batch.batch_id,
                "data-batch-action": "detail",
              },
              text: selected ? "已选择" : "打开详情",
              onclick: () => {
                state.drawerReturnBatch = batch.batch_id;
                state.drawerReturnAction = "detail";
                navigate({ view: "executions", batch: batch.batch_id });
              },
            }),
          ]),
        ]);
      }),
    ),
  ]);
  return el("div", { className: "table-wrap" }, [table]);
}

function renderNoticeTable(page) {
  return el("div", { className: "table-wrap" }, [
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "序号" }),
          el("th", { attrs: { scope: "col" }, text: "类型" }),
          el("th", { attrs: { scope: "col" }, text: "聚合对象" }),
          el("th", { attrs: { scope: "col" }, text: "记录时间" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        page.events.map((event) =>
          el("tr", {}, [
            el("td", { className: "mono", text: String(event.sequence) }),
            el("td", { text: humanizeToken(event.kind) }),
            el("td", {}, [
              el("div", { className: "mono", text: event.aggregate_kind }),
              el("div", { className: "muted mono", text: shortId(event.aggregate_id) }),
            ]),
            el("td", { className: "mono", text: formatDateTime(event.recorded_at) }),
          ]),
        ),
      ),
    ]),
  ]);
}

function renderAdapterMatrix(adapters) {
  const facet = state.route.facet;
  const columns = [
    ["public_data", "public-data", "公共数据"],
    ["testnet_protocol", "testnet-protocol", "测试网协议"],
    ["authenticated", "authenticated", "鉴权访问"],
    ["reconcile", "reconcile", "对账"],
    ["live", "live", "实盘"],
  ];
  return el("div", { className: "table-wrap" }, [
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "适配器" }),
          ...columns.map(([, , label]) => el("th", { attrs: { scope: "col" }, text: label })),
        ]),
      ]),
      el(
        "tbody",
        {},
        adapters.map((adapter) =>
          el("tr", {}, [
            el("td", {}, [
              el("div", { className: "mono", text: adapter.id }),
              el("div", { className: "muted", text: adapter.name }),
            ]),
            ...columns.map(([field, facetId]) =>
              renderAdapterCell(adapter[field], facet !== "all" && facet === facetId),
            ),
          ]),
        ),
      ),
    ]),
  ]);
}

function renderAdapterCell(cell, highlighted) {
  if (!cell) {
    return el("td", {}, [
      buildTag("不可用", "neutral"),
      el("div", { className: "muted", text: "这个能力面尚未发布支持证据。" }),
    ]);
  }
  return el("td", {}, [
    el("div", { className: "tag-row" }, [
      buildTag(humanizeToken(cell.level), tagToneForSupport(cell.level)),
      highlighted ? buildTag("当前筛选", "info") : null,
    ]),
    el("div", { className: "muted" }, [
      `${cell.blockers.length} 个阻塞项`,
    ]),
    el("div", { className: "muted" }, [
      `${cell.evidence.length} 条证据`,
    ]),
  ]);
}

function renderCapabilityTable(capabilities) {
  return el("div", { className: "table-wrap" }, [
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { attrs: { scope: "col" }, text: "能力" }),
          el("th", { attrs: { scope: "col" }, text: "领域" }),
          el("th", { attrs: { scope: "col" }, text: "级别" }),
          el("th", { attrs: { scope: "col" }, text: "范围" }),
          el("th", { attrs: { scope: "col" }, text: "说明" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        capabilities.map((capability) =>
          el("tr", {}, [
            el("td", {}, [
              el("div", { className: "mono", text: capability.id }),
              el("div", { className: "muted" }, [
                `${capability.evidence.length} 条证据 / ${capability.blockers.length} 个阻塞项`,
              ]),
            ]),
            el("td", { text: humanizeToken(capability.area) }),
            el("td", {}, [buildTag(humanizeToken(capability.level), tagToneForCapability(capability.level))]),
            el("td", {}, [
              el("div", { className: "mono", text: capability.scope.access }),
              el("div", { className: "muted mono", text: capability.scope.environments.join(", ") }),
            ]),
            el("td", {}, [
              el("div", { text: capability.summary }),
              capability.blockers[0]
                ? el("div", { className: "muted", text: `阻塞项：${capability.blockers[0]}` })
                : null,
            ]),
          ]),
        ),
      ),
    ]),
  ]);
}

function renderSelect(label, options, value, onChange) {
  return el("label", {}, [
    el("span", { className: "compact-label", text: label }),
    el(
      "select",
      {
        attrs: { value },
        onchange: (event) => onChange(event.target.value),
      },
      options.map(([optionValue, optionLabel]) =>
        el("option", {
          attrs: {
            value: optionValue,
            selected: optionValue === value ? "true" : null,
          },
          text: optionLabel,
        }),
      ),
    ),
  ]);
}

function filteredBatches() {
  const batches = [...(state.executions?.operator?.batches || [])].sort(
    (left, right) => Date.parse(right.updated_at) - Date.parse(left.updated_at),
  );
  return batches.filter((batch) => matchesBatchFilter(batch));
}

function filteredCapabilities() {
  const capabilities = state.capabilities?.capabilities || [];
  return capabilities.filter((capability) =>
    state.route.area === "all" ? true : capability.area === state.route.area,
  );
}

function capabilityRowsFor(area) {
  return (state.capabilities?.capabilities || []).filter((capability) => capability.area === area);
}

function renderStrategiesView() {
  const capabilities = capabilityRowsFor("strategy");
  const taskKinds = uniqueValues((state.tasks?.tasks || []).map((task) => task.kind));
  const batchStrategies = uniqueValues(
    (state.executions?.operator?.batches || []).map((batch) => batch.strategy),
  );
  const latestAlert = latestAlertOccurrence();
  const latestScanner = state.scanner?.latest || null;
  const latestMonitor = state.monitor?.latest || null;
  return el("section", { className: "view-stack" }, [
    renderRegion({
      title: "策略运行面",
      subtitle:
        "\u8fd9\u91cc\u540c\u65f6\u5c55\u793a\u5f53\u524d\u7b56\u7565\u8bc1\u636e\u4e0e\u53d7\u4fe1 Paper submit \u63a7\u5236\uff1b\u53ea\u5141\u8bb8 Grid / Arbitrage \u7684 paper task lifecycle\uff0c\u4e0d\u5f00\u653e live\u3001reconcile \u6216\u4e0b\u5355\u63a7\u5236\u3002",
      body: el("div", { className: "detail-grid" }, [
        detailStat("\u76d1\u63a7", latestMonitor ? humanizeToken(latestMonitor.state) : "\u6682\u4e0d\u53ef\u7528"),
        detailStat("Scanner \u884c\u6570", latestScanner?.rows?.length || 0),
        detailStat("\u9884\u8b66\u4e8b\u4ef6", visibleAlertOccurrences(state.alerts).length),
        detailStat("\u4efb\u52a1\u7c7b\u578b", taskKinds.length),
        detailStat("\u6279\u6b21\u7b56\u7565", batchStrategies.length),
        detailStat("\u7b56\u7565\u80fd\u529b", capabilities.length),
      ]),
    }),
    renderRegion({
      title: "Paper 任务控制",
      subtitle:
        "\u6240\u6709\u52a8\u4f5c\u90fd\u901a\u8fc7 /api/v1/submit \u53d1\u9001 SubmitEnvelope v1\uff0c\u56fa\u5b9a principal_id=local-paper-operator\uff0crole=paper_operator\uff0crisk_confirmation=paper_only\u3002",
      body: el("div", { className: "evidence-grid" }, [
        renderStrategyTaskControl("arbitrage"),
        renderStrategyTaskControl("grid"),
      ]),
    }),
    renderRegion({
      title: "只读策略证据",
      subtitle:
        "\u6bcf\u4e2a\u533a\u57df\u53ea\u53cd\u6620\u5f53\u524d journal \u5feb\u7167\u4e2d\u6700\u540e\u4e00\u6761\u6709\u754c\u4e8b\u5b9e\uff1b\u6570\u636e\u7f3a\u5931\u65f6\u4fdd\u6301\u5173\u95ed\u3002",
      body: el("div", { className: "evidence-grid" }, [
        renderSubregion("\u5957\u5229\u76d1\u63a7", el("div", { className: "detail-grid" }, [
          detailStat("\u6295\u5f71", humanizeToken(state.monitor?.projection_status || "unavailable")),
          detailStat(
            "\u4ea4\u6613\u5bf9",
            latestMonitor
              ? `${latestMonitor.left.exchange}/${latestMonitor.left.symbol} -> ${latestMonitor.right.exchange}/${latestMonitor.right.symbol}`
              : "--",
          ),
          detailStat("\u8bb0\u5f55\u65f6\u95f4", latestMonitor ? formatDateTime(latestMonitor.recorded_at) : "--"),
          detailStat("\u5e8f\u53f7", latestMonitor?.monitor_sequence ?? "--"),
        ])),
        renderSubregion("\u865a\u62df\u7f51\u683c Scanner", el("div", { className: "detail-grid" }, [
          detailStat("\u6295\u5f71", humanizeToken(state.scanner?.projection_status || "unavailable")),
          detailStat("\u8fd0\u884c ID", latestScanner?.run_id || "--"),
          detailStat(
            "\u9996\u4f4d\u6807\u7684",
            latestScanner?.rows?.[0] ? scannerMarketLabel(latestScanner.rows[0]) : "--",
          ),
          detailStat("\u8bb0\u5f55\u65f6\u95f4", latestScanner ? formatDateTime(latestScanner.recorded_at) : "--"),
        ])),
        renderSubregion("\u4ef7\u683c\u9884\u8b66", el("div", { className: "detail-grid" }, [
          detailStat("\u6295\u5f71", alertProjectionLabel(state.alerts)),
          detailStat("\u6700\u65b0\u5e8f\u53f7", latestAlert?.alert_sequence ?? "--"),
          detailStat("\u5f85\u786e\u8ba4\u6295\u9012", countPendingAlertDeliveries(state.alerts)),
          detailStat("\u8bb0\u5f55\u65f6\u95f4", latestAlert ? formatDateTime(latestAlert.recorded_at) : "--"),
        ])),
        renderSubregion("\u6267\u884c\u8d26\u672c", el("div", { className: "detail-grid" }, [
          detailStat("\u6279\u6b21", state.executions?.operator?.batches?.length || 0),
          detailStat("\u7b56\u7565", batchStrategies.length),
          detailStat("\u9700\u8981\u6062\u590d", state.system?.recovery_required_count ?? "--"),
          detailStat("\u6295\u5f71", humanizeToken(state.system?.projection_status || "unavailable")),
        ])),
      ]),
    }),
    renderRegion({
      title: "策略能力账本",
      subtitle:
        "\u80fd\u529b\u9879\u6765\u81ea CLI \u4e0e HTTP \u5171\u7528\u7684\u540c\u4e00\u4efd\u6709\u754c\u6e05\u5355\u3002",
      body:
        state.loads.capabilities === "error" && !state.capabilities
          ? renderError(state.errors.capabilities)
          : state.capabilities
          ? capabilities.length > 0
            ? renderCapabilityTable(capabilities)
            : renderEmpty(
                "\u5f53\u524d\u80fd\u529b\u6e05\u5355\u4e2d\u6ca1\u6709\u7b56\u7565\u80fd\u529b\u9879\u3002",
                "\u5df2\u68c0\u67e5 /api/v1/capabilities \u4e2d area=strategy \u7684\u8bb0\u5f55\u3002",
              )
          : renderSkeleton(6),
    }),
  ]);
}

function renderStrategyTaskControl(formKey) {
  const definition = STRATEGY_CONTROL_DEFS[formKey];
  const form = state.strategySubmit[formKey];
  const latestTask = latestTaskForKind(definition.taskKind);
  const authBlocked = state.authRequired && !state.authToken;
  const readbackBlocked =
    state.loads.tasks !== "ready" ||
    state.tasks?.projection_status !== "complete";
  const actionDisabled =
    authBlocked ||
    readbackBlocked ||
    form.inFlight ||
    form.lockedByOutcomeUnknown;
  const fieldDisabled = form.inFlight;
  return renderSubregion(definition.title, el("div", { className: "view-stack" }, [
    el("p", { className: "muted", text: definition.summary }),
    latestTask
      ? el("div", { className: "detail-grid" }, [
          detailStat("\u6700\u8fd1 task_id", latestTask.task_id || "--"),
          detailStat("\u6301\u4e45\u9636\u6bb5", humanizeToken(latestTask.phase)),
          detailStat("\u6062\u590d\u5224\u65ad", humanizeToken(latestTask.recovery)),
          detailStat("\u66f4\u65b0\u65f6\u95f4", formatDateTime(latestTask.updated_at)),
        ])
      : renderEmpty(
          "\u5c1a\u672a\u89c2\u5bdf\u5230\u8fd9\u7c7b Paper \u4efb\u52a1\u7684 durable \u5feb\u7167\u3002",
          "\u5982\u9700 start / stop / cancel\uff0c\u8bf7\u663e\u5f0f\u586b\u5199 task_id\u3001strategy_id \u548c strategy_revision\u3002",
        ),
    el("div", { className: "support-grid" }, [
      renderStrategyTextField(
        "\u4efb\u52a1 ID / task_id",
        form.taskId,
        formKey === "grid" ? "paper-grid-btc-usdt" : "paper-arbitrage-btc-usdt",
        fieldDisabled,
        (value) => updateStrategySubmitField(formKey, "taskId", value),
      ),
      renderStrategyTextField(
        "\u7b56\u7565 ID / strategy_id",
        form.strategyId,
        definition.defaultStrategyId,
        fieldDisabled,
        (value) => updateStrategySubmitField(formKey, "strategyId", value),
      ),
      renderStrategyTextField(
        "\u7248\u672c / strategy_revision",
        form.strategyRevision,
        DEFAULT_STRATEGY_REVISION,
        fieldDisabled,
        (value) => updateStrategySubmitField(formKey, "strategyRevision", value),
      ),
    ]),
    el("label", { className: "confirm-row" }, [
      el("input", {
        attrs: {
          type: "checkbox",
          checked: form.confirmed ? "checked" : null,
        },
        onchange: (event) => {
          form.confirmed = event.target.checked;
          render();
        },
      }),
      el("span", {
        text:
          "\u6211\u786e\u8ba4\u8fd9\u662f paper_only \u6307\u4ee4\uff0c\u53ea\u64cd\u4f5c Paper \u4efb\u52a1\uff0c\u4e0d\u542f\u7528\u5b9e\u76d8\u6216\u5bf9\u8d26\u91ca\u653e\u3002",
      }),
    ]),
    el("p", {
      className: "field-help",
      text: form.lockedByOutcomeUnknown
        ? "\u5f53\u524d\u8868\u5355\u56e0 outcome_unknown \u88ab\u9501\u5b9a\uff1b\u8bf7\u5148\u6838\u5bf9 /api/v1/tasks\uff0c\u6216\u8005\u660e\u786e\u4fee\u6539 task_id / strategy_id / strategy_revision \u540e\u518d\u751f\u6210\u65b0 ID\u3002"
        : authBlocked
        ? "\u5f53\u524d submit \u9700\u8981 Bearer\uff1b\u4ee4\u724c\u53ea\u4fdd\u5b58\u5728 state.authToken \u7684\u9875\u9762\u5185\u5b58\uff0c\u4e0d\u5199\u5165 DOM \u6216\u4efb\u4f55\u6d4f\u89c8\u5668\u6301\u4e45\u5316\u5b58\u50a8\u3002"
        : readbackBlocked
        ? "\u4efb\u52a1\u6295\u5f71\u5c1a\u672a\u5c31\u7eea\u6216\u5df2\u964d\u7ea7\uff1b\u5728 /api/v1/tasks \u6062\u590d complete \u4e4b\u524d\u7981\u6b62\u63d0\u4ea4\u3002"
        : "首次动作会生成 UUID command_id 和幂等键；仅在提交中、网络结果未知或 durable receipt 为 outcome_unknown 时，未修改的同动作重试才复用原身份。已拒绝 / 已写入属于闭合结果，再次点击会生成新指令。",
    }),
    el("div", { className: "inline-button-row" }, [
      renderStrategyActionButton(formKey, "start", definition.startLabel, actionDisabled),
      renderStrategyActionButton(formKey, "stop", "\u505c\u6b62\u4efb\u52a1", actionDisabled),
      renderStrategyActionButton(formKey, "cancel", "\u53d6\u6d88\u4efb\u52a1", actionDisabled),
    ]),
    renderStrategySubmitResult(form),
  ]));
}

function renderStrategyTextField(label, value, placeholder, disabled, onChange) {
  return el("label", {}, [
    el("span", { className: "compact-label", text: label }),
    el("input", {
      attrs: {
        type: "text",
        value,
        placeholder,
        autocomplete: "off",
        spellcheck: "false",
        disabled: disabled ? "true" : null,
      },
      oninput: (event) => {
        onChange(event.target.value);
      },
    }),
  ]);
}

function renderStrategyActionButton(formKey, action, label, disabled) {
  const form = state.strategySubmit[formKey];
  const active = form.inFlight && form.pendingAction === action;
  const lockedToAnotherAction =
    Boolean(form.pendingSubmission) &&
    !form.lockedByOutcomeUnknown &&
    form.pendingSubmission.action !== action;
  return el("button", {
    className: action === "stop" ? "secondary" : action === "cancel" ? "ghost" : "",
    attrs: {
      type: "button",
      disabled: disabled || lockedToAnotherAction ? "true" : null,
      "aria-busy": active ? "true" : null,
    },
    text: active ? "\u63d0\u4ea4\u4e2d\u2026" : label,
    onclick: () => {
      void submitStrategyCommand(formKey, action);
    },
  });
}

function renderStrategySubmitResult(form) {
  if (form.lastError) {
    return el("div", { className: "error-state" }, [
      el("div", { className: "tag-row" }, [
        buildTag(
          form.lastAction ? submitCommandLabel(form.lastAction) : "\u53d7\u4fe1 submit",
          "danger",
        ),
        buildTag(form.lastError.code || "invalid_input", "warning"),
      ]),
      el("p", { text: form.lastError.message }),
      form.pendingSubmission
        ? el("div", { className: "detail-grid" }, [
            detailStat(
              "command_id",
              form.pendingSubmission.envelope.command_id || "--",
            ),
            detailStat(
              "idempotency_key",
              form.pendingSubmission.envelope.idempotency_key || "--",
            ),
            detailStat(
              "task_id",
              form.pendingSubmission.envelope.target_task_id || "--",
            ),
            detailStat(
              "动作",
              submitCommandLabel(form.pendingSubmission.commandKind),
            ),
          ])
        : null,
      form.pendingSubmission
        ? el("p", {
            className: "muted",
            text:
              "\u5f53\u524d\u9519\u8bef\u4ecd\u4fdd\u7559\u540c\u4e00\u4e2a pending envelope\uff1b\u53ea\u8981 task_id / strategy_id / strategy_revision \u4e0d\u53d8\uff0c\u4e0b\u4e00\u6b21\u540c\u52a8\u4f5c\u4f1a\u590d\u7528\u76f8\u540c\u7684 command_id \u548c idempotency_key\u3002",
          })
        : null,
      form.lastError.status
        ? el("p", {
            className: "muted mono",
            text: `HTTP ${form.lastError.status}`,
          })
        : null,
    ]);
  }
  if (form.lastReceipt) {
    return el("div", { className: "view-stack" }, [
      el("div", { className: "tag-row" }, [
        buildTag(submitCommandLabel(form.lastAction), "info"),
        buildTag(
          humanizeToken(form.lastReceipt.status),
          submitReceiptTone(form.lastReceipt.status),
        ),
      ]),
      el("div", { className: "detail-grid" }, [
        detailStat("command_id", form.lastReceipt.command_id || "--"),
        detailStat("task_id", form.lastReceipt.target_task_id || "--"),
        detailStat(
          "journal_projection",
          form.lastReceipt.journal_projection || "--",
        ),
        detailStat("source", form.lastReceipt.source || "--"),
        detailStat(
          "status",
          humanizeToken(form.lastReceipt.status || "--"),
        ),
      ]),
      form.lastReceipt.status === "outcome_unknown"
        ? el("p", {
            className: "muted",
            text:
              "\u201coutcome_unknown\u201d \u8868\u793a submit \u5df2\u88ab durable journal \u63a5\u7eb3\uff0c\u4f46\u7ed3\u679c\u4ecd\u9700\u56de\u5230\u4efb\u52a1\u4e0e\u6267\u884c\u8d26\u672c\u4e2d\u4eba\u5de5\u6838\u5bf9\u3002",
          })
        : null,
      form.lockedByOutcomeUnknown
        ? el("p", {
            className: "muted",
            text:
              "\u8be5\u8868\u5355\u76ee\u524d\u4e0d\u4f1a\u751f\u6210\u65b0 mutation\uff1b\u8bf7\u5148\u6838\u5bf9 /api/v1/tasks\uff0c\u6216\u4fee\u6539 task_id / strategy_id / strategy_revision \u540e\u518d\u83b7\u53d6\u65b0 IDs\u3002",
          })
        : null,
    ]);
  }
  return renderEmpty(
    "\u5c1a\u672a\u63d0\u4ea4\u53d7\u4fe1 Paper \u6307\u4ee4\u3002",
    "\u53ea\u5141\u8bb8 start_paper_grid\u3001start_paper_arbitrage\u3001stop_task \u548c cancel_task \u8fd9\u56db\u79cd lifecycle \u52a8\u4f5c\u3002",
  );
}

function attentionBatches() {
  return filteredBatches().filter((batch) =>
    batch.recovery !== "none" ||
    ["conflict", "partial", "failed", "outcome_unknown"].includes(batch.state),
  );
}

function attentionTasks() {
  return (state.tasks?.tasks || [])
    .slice()
    .filter(
      (task) =>
        task.recovery === "investigate" ||
        Boolean(task.failure) ||
        task.phase === "failed",
    )
    .sort((left, right) => Date.parse(right.updated_at) - Date.parse(left.updated_at));
}

function uniqueValues(values) {
  return [...new Set(values.filter(Boolean))];
}

function latestAlertOccurrence() {
  const occurrences = visibleAlertOccurrences(state.alerts);
  return occurrences.length > 0 ? occurrences[occurrences.length - 1] : null;
}

function latestTaskUpdateAt() {
  const tasks = (state.tasks?.tasks || []).slice().sort(
    (left, right) => Date.parse(right.updated_at) - Date.parse(left.updated_at),
  );
  return tasks[0] ? formatDateTime(tasks[0].updated_at) : "--";
}

function latestTaskSequence() {
  const tasks = state.tasks?.tasks || [];
  if (tasks.length === 0) {
    return "--";
  }
  return tasks.reduce(
    (latest, task) => (task.last_sequence > latest ? task.last_sequence : latest),
    0,
  );
}

function selectedBatch() {
  return (state.executions?.operator?.batches || []).find((batch) => batch.batch_id === state.route.batch) || null;
}

function matchesBatchFilter(batch) {
  switch (state.route.batchState) {
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

function collectBands() {
  const bands = [];
  const projectionStatus = state.system?.projection_status;
  if (projectionStatus === "windowed") {
    bands.push({
      title: "窗口化投影",
      tag: "窗口化",
      tone: "warning",
      message:
        "有界读取模型保留了未解决批次，并可能淘汰较早的已完成批次，以维持最近运行窗口。",
    });
  }
  if (projectionStatus === "degraded") {
    bands.push({
      title: "降级投影",
      tag: "降级",
      tone: "danger",
      message:
        "读取模型只接受安全的部分事实；无效或不完整的持久记录不会被提升为健康状态。",
    });
  }
  if (state.monitor && state.monitor.projection_status !== "complete") {
    bands.push({
      title: "监控投影已降级",
      tag: "停止展示",
      tone: "danger",
      message:
        "最后一个有效监控结果已停止展示；无效事件或不完整尾记录修复前，不把旧机会提升为可信状态。",
    });
  }
  if (state.tasks && state.tasks.projection_status !== "complete") {
    bands.push({
      title: "任务投影已降级",
      tag: "最后有效事实",
      tone: "danger",
      message:
        "无效任务生命周期事实不会覆盖最后一个通过校验的状态；修复 journal 前，所有任务 liveness 都需要人工核对。",
    });
  }
  const taskInvestigationCount = (state.tasks?.tasks || []).filter(
    (task) => task.recovery === "investigate",
  ).length;
  if (taskInvestigationCount > 0) {
    bands.push({
      title: "任务存活性未验证",
      tag: "历史事实 / 不自动重放",
      tone: "warning",
      message: `${taskInvestigationCount} 个任务的最后持久阶段尚未形成可安全闭合的正常终态。页面不会把 running 解释为当前进程存活，也不会在重启后自动重连数据源。`,
    });
  }
  if (state.loads.tasks === "error" && state.tasks) {
    bands.push({
      title: "任务快照刷新失败",
      tag: "保留旧快照",
      tone: "warning",
      message:
        "最后一个通过生命周期校验的任务快照仍然可见；重新读取成功前，不把其中的阶段或源健康解释为当前状态。",
    });
  }
  if (state.scanner && state.scanner.projection_status !== "complete") {
    bands.push({
      title: "scanner 投影已降级",
      tag: "保留最后有效历史排行",
      tone: "danger",
      message:
        "无效排行事实或不完整尾记录不会覆盖最后一次通过严格校验的离线结果；该结果不代表当前行情或进程存活性。",
    });
  }
  if (state.loads.scanner === "error" && state.scanner) {
    bands.push({
      title: "scanner 快照刷新失败",
      tag: "保留旧快照",
      tone: "warning",
      message:
        "最后一个通过校验的离线排行仍然可见；重新读取成功前，不把它解释为当前市场排名。",
    });
  }
  const alertProjectionStatus = state.alerts?.projection_status;
  if (alertProjectionStatus === "windowed" && isTrustedAlertProjection(state.alerts)) {
    bands.push({
      title: "预警投影已窗口化",
      tag: "可信 / 已截断",
      tone: "warning",
      message: `预警事实仍通过完整性校验；当前只展示最近 ${
        visibleAlertOccurrences(state.alerts).length
      } 条 occurrence，更早记录已被有界淘汰。`,
    });
  } else if (state.alerts && !isTrustedAlertProjection(state.alerts)) {
    bands.push({
      title: "预警投影已降级",
      tag: "停止展示",
      tone: "danger",
      message:
        "无效预警事件或不完整尾记录修复前，所有 occurrence 与最近预警都停止展示。",
    });
  }
  if (
    state.loads.alerts === "error" &&
    state.alerts &&
    isTrustedAlertProjection(state.alerts)
  ) {
    bands.push({
      title: "预警快照刷新失败",
      tag: "保留旧快照",
      tone: "warning",
      message:
        "最后一个通过完整性校验的预警快照仍然可见；重新读取成功前，不把它解释为最新状态。",
    });
  }
  const pendingAlertDeliveries = countPendingAlertDeliveries(state.alerts);
  if (pendingAlertDeliveries > 0) {
    bands.push({
      title: "存在未决通知记录",
      tag: "历史事实 / 不保证重放",
      tone: "warning",
      message: `冻结 journal 中有 ${pendingAlertDeliveries} 条 adapter 记录最后停在 pending。它可能源于进程中断或终态持久化失败；恢复默认不重放，本页不会把它解释为仍在排队。`,
    });
  }
  if (isStale()) {
    bands.push({
      title: "操作事件流已断开",
      tag: "断开",
      tone: "warning",
      message:
        "最后一个良好快照仍然可见，但操作通知通道当前断开；这不代表监控行情仍然新鲜。",
      action: {
        label: "重试快照",
        onClick: () => void refreshOperationalTruth(),
      },
    });
  }
  const authProblem =
    state.errors.system?.code === "authentication_required" ||
    state.errors.monitor?.code === "authentication_required" ||
    state.errors.alerts?.code === "authentication_required" ||
    state.errors.tasks?.code === "authentication_required" ||
    state.errors.capabilities?.code === "authentication_required" ||
    state.errors.executions?.code === "authentication_required";
  if (authProblem) {
    bands.push({
      title: "需要认证",
      tag: "不可用",
      tone: "neutral",
      message:
        "这个回环会话需要有效的 Bearer 令牌。请在风险脊柱中绑定到内存；浏览器不会持久化该令牌。",
    });
  }
  if (state.errors.executions && !state.executions) {
    bands.push({
      title: "执行账本不可用",
      tag: "错误",
      tone: "danger",
      message: errorDescription(state.errors.executions).message,
      action: {
        label: state.errors.executions.code === "invalid_cursor" || state.errors.executions.code === "cursor_expired"
          ? "清除游标"
          : "重试快照",
        onClick: () => {
          if (state.errors.executions.code === "invalid_cursor" || state.errors.executions.code === "cursor_expired") {
            state.cursor = "";
            void loadExecutions();
            restartStream();
            render();
          } else {
            void refreshOperationalTruth();
          }
        },
        ghost: state.errors.executions.code !== "invalid_cursor" && state.errors.executions.code !== "cursor_expired",
      },
    });
  }
  return bands;
}

function summarizeCapabilityLevels(capabilities) {
  const counts = {
    available: 0,
    "read-only": 0,
    "paper-once": 0,
    "validate-only": 0,
    "contract-only": 0,
    unavailable: 0,
  };
  for (const capability of capabilities) {
    counts[capability.level] += 1;
  }
  return counts;
}

function metaRow(label, value) {
  return el("div", { className: "spine-meta-row" }, [
    el("span", { className: "compact-label", text: label }),
    el("span", {
      className: looksMonospaced(value) ? "mono align-right" : "align-right",
      text: value,
    }),
  ]);
}

function detailStat(label, value) {
  return el("div", { className: "detail-block" }, [
    el("div", { className: "compact-label", text: label }),
    el("div", {
      className: typeof value === "string" && looksMonospaced(value) ? "metric-value mono" : "metric-value",
      text: String(value),
    }),
  ]);
}

function buildTag(text, tone) {
  return el("span", {
    className: "status-tag",
    attrs: { "data-tone": tone || "neutral" },
    text,
  });
}

function buildBatchStateTag(stateValue) {
  return buildTag(humanizeToken(stateValue), tagToneForBatch(stateValue));
}

function buildRecoveryTag(recovery) {
  return buildTag(humanizeToken(recovery), tagToneForRecovery(recovery));
}

function tagToneForBatch(stateValue) {
  switch (stateValue) {
    case "completed":
      return "success";
    case "partial":
    case "incomplete":
    case "outcome_unknown":
      return "warning";
    case "failed":
    case "conflict":
      return "danger";
    default:
      return "neutral";
  }
}

function tagToneForRecovery(recovery) {
  switch (recovery) {
    case "none":
      return "success";
    case "reconcile_required":
      return "warning";
    case "investigate":
      return "danger";
    default:
      return "neutral";
  }
}

function tagToneForPhase(phase) {
  switch (phase) {
    case "completed":
      return "success";
    case "partial":
    case "incomplete":
      return "warning";
    case "failed":
      return "danger";
    default:
      return "neutral";
  }
}

function tagToneForSupport(level) {
  switch (level) {
    case "implemented":
      return "success";
    case "protocol-only":
    case "request-only":
    case "config-only":
      return "warning";
    case "unavailable":
      return "neutral";
    case "not-applicable":
      return "ghost";
    default:
      return "info";
  }
}

function tagToneForCapability(level) {
  switch (level) {
    case "available":
      return "success";
    case "read-only":
    case "paper-once":
    case "validate-only":
    case "contract-only":
      return "warning";
    case "unavailable":
      return "neutral";
    default:
      return "info";
  }
}

function projectionToneForLabel(label) {
  switch (label) {
    case "complete":
    case "完整":
      return "success";
    case "windowed":
    case "窗口化":
      return "warning";
    case "degraded":
    case "降级":
      return "danger";
    case "unavailable":
    case "not_available":
    case "不可用":
      return "neutral";
    default:
      return "info";
  }
}

function humanizeToken(value) {
  const token = String(value || "");
  return TOKEN_LABELS.get(token) || token
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function shortId(value) {
  if (!value) {
    return "--";
  }
  return value.length > 12 ? `${value.slice(0, 8)}...${value.slice(-4)}` : value;
}

function routeHref(patch) {
  const next = { ...state.route, ...patch };
  const params = new URLSearchParams();
  if (next.area !== "all") {
    params.set("area", next.area);
  }
  if (next.facet !== "all") {
    params.set("facet", next.facet);
  }
  if (next.batchState !== "all") {
    params.set("status", next.batchState);
  }
  if (next.batch) {
    params.set("batch", next.batch);
  }
  const nextPath = `/${next.view}`;
  return `${nextPath}${params.size ? `?${params.toString()}` : ""}`;
}

function formatDateTime(value) {
  const date = typeof value === "number" ? new Date(value) : new Date(String(value));
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function formatOptionalDate(value) {
  return value ? formatDateTime(value) : "--";
}

function formatOptionalNumber(value) {
  return typeof value === "number" ? String(value) : "--";
}

function eventStreamLabel() {
  if (state.stream.connected) {
    return "已连接 / 仅通知";
  }
  if (isStale()) {
    return "断开 / 正在重试";
  }
  if (state.lastSnapshotAt) {
    return "未连接 / 仅快照";
  }
  return "正在连接";
}

function compactEventStreamLabel() {
  if (state.stream.connected) {
    return "已连接";
  }
  if (isStale()) {
    return "断开";
  }
  if (state.lastSnapshotAt) {
    return "仅快照";
  }
  return "连接中";
}

function eventStreamTone() {
  if (state.stream.connected) {
    return "success";
  }
  if (isStale()) {
    return "warning";
  }
  return "neutral";
}

function isStale() {
  return Boolean(
    state.lastSnapshotAt &&
      !state.stream.connected &&
      Date.now() - (state.stream.lastEventAt || state.lastSnapshotAt) > STALE_AFTER_MS,
  );
}

function accessDescriptor() {
  if (state.authRequired) {
    return state.authToken ? "回环 + Bearer" : "回环 / 需要 Bearer";
  }
  return state.authToken ? "回环 + 已绑定 Bearer" : "开放回环";
}

function errorDescription(problem) {
  switch (problem?.code) {
    case "invalid_cursor":
      return {
        title: "当前游标不适用于这个日志。",
        message: "清除游标，然后重新获取有界快照。",
        clearCursor: true,
      };
    case "cursor_expired":
      return {
        title: "游标已不再匹配当前日志代次。",
        message: "清除游标，并从当前持久日志头恢复。",
        clearCursor: true,
      };
    case "journal_unavailable":
      return {
        title: "日志暂时不可用。",
        message: "保留现有只读视图，待有界来源恢复后重试快照。",
      };
    case "read_limit_exceeded":
      return {
        title: "有界读取模型达到资源限制。",
        message: "当前无法在这个只读界面中安全表达该来源。",
      };
    case "journal_invalid":
      return {
        title: "持久日志未通过完整性校验。",
        message: "在修复日志来源前，应将所有可见事实视为可疑。",
      };
    case "authentication_required":
      return {
        title: "需要有效的 Bearer 令牌。",
        message: "请从风险脊柱将令牌绑定到内存；浏览器不会持久化它。",
      };
    case "network_error":
    case "stream_error":
      return {
        title: "网络路径未能正常完成。",
        message: "最后一个良好快照仍然可见；请重试有界快照与事件流。",
      };
    default:
      return {
        title: "请求无法安全完成。",
        message: "请重试有界快照；原始适配器或日志文本会被刻意隐藏。",
      };
  }
}

function hasAnyData() {
  return Boolean(
      state.system ||
      state.monitor ||
      state.alerts ||
      state.tasks ||
      state.scanner ||
      state.capabilities ||
      state.executions,
  );
}

function looksMonospaced(value) {
  return typeof value === "string" && /[0-9a-f]{4,}|->|,|:|T/.test(value);
}

async function copyValue(value, message) {
  if (!value) {
    return;
  }
  try {
    await navigator.clipboard.writeText(value);
    announce(message);
  } catch {
    announce("剪贴板当前不可用。");
  }
}

function announce(message) {
  dom.announcer.textContent = "";
  window.setTimeout(() => {
    dom.announcer.textContent = message;
  }, 20);
}

function delay(ms, signal) {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timeout = window.setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timeout);
        resolve();
      },
      { once: true },
    );
  });
}

function el(tag, options = {}, children = []) {
  const node = document.createElement(tag);
  if (options.className) {
    node.className = options.className;
  }
  if (options.text !== undefined) {
    node.textContent = options.text;
  }
  if (options.attrs) {
    for (const [key, value] of Object.entries(options.attrs)) {
      if (value === null || value === undefined || value === false) {
        continue;
      }
      node.setAttribute(key, String(value));
    }
  }
  if (options.onclick) {
    node.addEventListener("click", options.onclick);
  }
  if (options.onchange) {
    node.addEventListener("change", options.onchange);
  }
  if (options.oninput) {
    node.addEventListener("input", options.oninput);
  }
  if (options.onsubmit) {
    node.addEventListener("submit", options.onsubmit);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) {
      continue;
    }
    node.append(child);
  }
  return node;
}

class ApiProblem extends Error {
  constructor(code, message, status) {
    super(message);
    this.name = "ApiProblem";
    this.code = code;
    this.status = status;
  }
}
