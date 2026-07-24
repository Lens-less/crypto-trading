const VIEW_IDS = new Set(["overview", "executions", "integrations"]);
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
const TOKEN_LABELS = new Map([
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
]) {
  TOKEN_LABELS.set(token, label);
}

const STREAM_RETRY_BASE_MS = 1500;
const STREAM_RETRY_MAX_MS = 10000;
const STALE_AFTER_MS = 15000;
const REFETCH_DEBOUNCE_MS = 180;

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
  loads: {
    system: "idle",
    monitor: "idle",
    capabilities: "idle",
    executions: "idle",
  },
  errors: {
    system: null,
    monitor: null,
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
  if (state.refreshTimer) {
    window.clearTimeout(state.refreshTimer);
    state.refreshTimer = 0;
  }
  state.cursor = "";
  state.system = null;
  state.monitor = null;
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
    capabilities: "idle",
    executions: "idle",
  };
  state.errors = {
    system: null,
    monitor: null,
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
    await Promise.all([loadSystem(), loadMonitor(), loadExecutions()]);
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

async function fetchJson(url) {
  const headers = {
    Accept: "application/json",
  };
  if (state.authToken) {
    headers.Authorization = `Bearer ${state.authToken}`;
  }
  const response = await fetch(url, {
    method: "GET",
    cache: "no-store",
    headers,
  });
  let payload = null;
  if (response.headers.get("content-type")?.includes("application/json")) {
    payload = await response.json();
  }
  if (!response.ok) {
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

function render() {
  state.renderedStale = isStale();
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
    ["overview", "总览"],
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
    overview: "跨域读取模型与风险优先的运行事实。",
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
  } else if (state.route.view === "executions") {
    content.push(renderExecutionsView());
  } else {
    content.push(renderIntegrationsView());
  }
  dom.main.replaceChildren(...content);
}

function renderOverviewView() {
  const leftColumn = el("div", { className: "view-stack" }, [
    renderRibbon(),
    renderMonitorRegion(),
    renderExecutionsSummaryRegion(),
    renderRecentNoticesRegion(),
  ]);
  const rightColumn = el("div", { className: "view-stack" }, [
    renderProjectionRegion(),
    renderCapabilityPulseRegion(),
  ]);
  return el("section", { className: "overview-grid" }, [leftColumn, rightColumn]);
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
    state.system || state.monitor || state.capabilities || state.executions,
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
