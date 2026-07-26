/**
 * SSE 通知通道客户端(GET /api/v1/events)。
 *
 * 语义红线:这是「已连接 · 仅通知」的变更通知通道,不是实时行情。
 * `operation_page` 事件 payload-free(只含不透明游标与变更通知),收到后
 * 一律由 React Query 重新拉取 REST 端点(tickflow 模式)。
 *
 * 双传输路径:
 * - 无 bearer token:原生 EventSource(浏览器自带解析与页尾 id 语义);
 *   我们自管重连,恢复游标经 `?cursor=` 查询参数回传(与后端
 *   resolve_resume_cursor 对齐;游标不透明、非凭据,且不进入浏览器历史)。
 * - 有 bearer token:EventSource 无法携带 Authorization 头,改用 fetch
 *   ReadableStream + 手写 SSE 行协议解析器,重连时带 Last-Event-ID 头。
 *
 * 重连策略:指数退避(base 1s、cap 30s、半区间抖动),连接成功即重置;
 * 连续失败计数通过 onStateChange 暴露给 UI(≥3 次由上层降级文案)。
 * 401 属于终止性失败:通知 onUnauthorized 并永久关闭本连接,由上层
 * (会话代际)决定何时以新凭据重建。
 */
import { apiErrorEnvelopeSchema } from "./api-types";
import { getBearerToken } from "./api";

export const SSE_EVENTS_URL = "/api/v1/events";
export const SSE_BACKOFF_BASE_MS = 1_000;
export const SSE_BACKOFF_CAP_MS = 30_000;
/**
 * 兜底:连续失败达到该次数仍带着恢复游标时,丢弃游标以全新流重试。
 * EventSource 路径读不到 HTTP 错误码,后端在建连时对失效游标直接回
 * 400/410(而非 stream_error 帧),没有这个阀门会带着坏游标永久循环。
 */
export const SSE_CURSOR_DROP_FAILURES = 3;

/* ------------------------------------------------------------ 行协议解析器 */

/** 一个完整 SSE 帧(空行分帧后派发)。 */
export interface SseFrame {
  /** 事件名;未指定时按规范回落为 "message"。 */
  event: string;
  /** 多行 data 以 "\n" 连接后的完整字符串。 */
  data: string;
  /** 分帧完成时生效的 last event id(含此前帧记住的 id)。 */
  id: string | null;
}

/**
 * 增量式 text/event-stream 行协议解析器。
 *
 * 规则(WHATWG SSE):
 * - 行终止符 \r\n、\r、\n 等价;跨 chunk 的半行会被缓冲;
 * - 空行分帧:data 缓冲非空才派发,事件名缺省为 "message";
 * - `:` 开头是注释(后端 15s keep-alive),忽略;
 * - `data:` 多行累积,以 "\n" 连接;
 * - `id:` 立即更新 last event id(含 NUL 的值按规范忽略),跨帧记忆;
 * - 字段值紧跟冒号后的单个空格被剥除。
 */
export class SseStreamParser {
  #buffer = "";
  #dataLines: string[] = [];
  #eventType = "";
  #lastEventId: string | null = null;

  /** 最近一次 `id:` 字段生效的值;跨帧保持。 */
  lastEventId(): string | null {
    return this.#lastEventId;
  }

  /** 喂入一段文本,返回本次新完成的帧(可能为空)。 */
  feed(chunk: string): SseFrame[] {
    this.#buffer += chunk;
    const frames: SseFrame[] = [];
    for (;;) {
      const line = this.#nextLine();
      if (line === null) {
        break;
      }
      const frame = this.#processLine(line);
      if (frame !== null) {
        frames.push(frame);
      }
    }
    return frames;
  }

  #nextLine(): string | null {
    const buffer = this.#buffer;
    for (let index = 0; index < buffer.length; index += 1) {
      const character = buffer[index];
      if (character === "\n") {
        this.#buffer = buffer.slice(index + 1);
        return buffer.slice(0, index);
      }
      if (character === "\r") {
        if (index + 1 === buffer.length) {
          // 可能是被 chunk 边界拆开的 \r\n,等待更多字节。
          return null;
        }
        this.#buffer = buffer.slice(buffer[index + 1] === "\n" ? index + 2 : index + 1);
        return buffer.slice(0, index);
      }
    }
    return null;
  }

  #processLine(line: string): SseFrame | null {
    if (line === "") {
      return this.#dispatch();
    }
    if (line.startsWith(":")) {
      return null;
    }
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    switch (field) {
      case "event":
        this.#eventType = value;
        break;
      case "data":
        this.#dataLines.push(value);
        break;
      case "id":
        if (!value.includes("\0")) {
          this.#lastEventId = value;
        }
        break;
      default:
        // retry 与未知字段:通知通道自管退避,不消费。
        break;
    }
    return null;
  }

  #dispatch(): SseFrame | null {
    const hasData = this.#dataLines.length > 0;
    const frame: SseFrame | null = hasData
      ? {
          event: this.#eventType === "" ? "message" : this.#eventType,
          data: this.#dataLines.join("\n"),
          id: this.#lastEventId,
        }
      : null;
    this.#dataLines = [];
    this.#eventType = "";
    return frame;
  }
}

/* ------------------------------------------------------------------- 退避 */

/**
 * 第 consecutiveFailures 次连续失败后的重连延迟(毫秒)。
 * 指数上限 min(cap, base * 2^(n-1)),取 [上限/2, 上限) 的半区间抖动。
 */
export function backoffDelayMs(
  consecutiveFailures: number,
  random: () => number = Math.random,
): number {
  const attempt = Math.max(1, consecutiveFailures);
  const exponent = Math.min(attempt - 1, 30);
  const ceiling = Math.min(SSE_BACKOFF_CAP_MS, SSE_BACKOFF_BASE_MS * 2 ** exponent);
  const half = ceiling / 2;
  return Math.floor(half + random() * half);
}

/* ------------------------------------------------------------------- 连接 */

export type SseStatus = "connecting" | "open" | "reconnecting" | "closed";

export interface SseConnectionState {
  status: SseStatus;
  /** 自上次成功连接以来的连续失败次数;成功即归零。 */
  consecutiveFailures: number;
}

/** stream_error 事件体中的稳定错误标识(如 invalid_cursor / cursor_expired)。 */
export interface StreamErrorBody {
  code: string;
  message: string;
}

export interface OperationEventsHandlers {
  /** 收到 operation_page:page 为已解析 JSON,lastEventId 为当前恢复游标。 */
  onPage?: (page: unknown, lastEventId: string | null) => void;
  /** 收到终止性 stream_error;随后传输会断开并走常规重连。 */
  onStreamError?: (error: StreamErrorBody) => void;
  onStateChange?: (state: SseConnectionState) => void;
  /** fetch 路径收到 401:连接永久关闭,由上层处理会话作废。 */
  onUnauthorized?: () => void;
}

/** EventSource 消息事件的最小结构(便于测试注入)。 */
export interface SseMessageEventLike {
  data: string;
  lastEventId: string;
}

export interface EventSourceLike {
  addEventListener(type: string, listener: (event: SseMessageEventLike) => void): void;
  close(): void;
}

export interface OperationEventsOptions {
  url?: string;
  /** 每次(重)连读取一次;默认读内存 bearer token。 */
  getToken?: () => string | null;
  fetchImpl?: typeof fetch;
  createEventSource?: (url: string) => EventSourceLike;
  random?: () => number;
}

export interface SseConnection {
  /** 永久关闭;不再触发任何回调。 */
  close(): void;
  /** 立即断开当前传输并重连(不清失败计数;游标按当前值恢复)。 */
  restart(): void;
  /** 丢弃本地恢复游标;下次连接以全新流开始。 */
  clearLastEventId(): void;
  getLastEventId(): string | null;
  getState(): SseConnectionState;
}

function defaultCreateEventSource(url: string): EventSourceLike {
  return new EventSource(url) as unknown as EventSourceLike;
}

function parseJson(data: string): unknown {
  try {
    return JSON.parse(data) as unknown;
  } catch {
    return undefined;
  }
}

/**
 * 建立通知通道连接并自动维护重连;返回句柄供上层控制生命周期。
 * onStateChange 在创建时即同步收到一次 "connecting"。
 */
export function connectOperationEvents(
  handlers: OperationEventsHandlers,
  options: OperationEventsOptions = {},
): SseConnection {
  const url = options.url ?? SSE_EVENTS_URL;
  const getToken = options.getToken ?? getBearerToken;
  const fetchImpl =
    options.fetchImpl ??
    ((input: RequestInfo | URL, init?: RequestInit) => fetch(input, init));
  const random = options.random ?? Math.random;

  let lastEventId: string | null = null;
  let consecutiveFailures = 0;
  let status: SseStatus = "connecting";
  let closed = false;
  /** 单调递增的传输代际;旧传输的回调一律按代际丢弃。 */
  let epoch = 0;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  let activeSource: EventSourceLike | null = null;
  let activeAbort: AbortController | null = null;

  function notifyState(next: SseStatus): void {
    status = next;
    handlers.onStateChange?.({ status, consecutiveFailures });
  }

  function rememberEventId(id: string | null): void {
    if (id !== null && id !== "") {
      lastEventId = id;
    }
  }

  function dispatchFrame(event: string, data: string, id: string | null): void {
    rememberEventId(id);
    if (event === "operation_page") {
      const page = parseJson(data);
      if (page !== undefined) {
        handlers.onPage?.(page, lastEventId);
      }
      return;
    }
    if (event === "stream_error") {
      const envelope = apiErrorEnvelopeSchema.safeParse(parseJson(data));
      handlers.onStreamError?.(
        envelope.success
          ? {
              code: envelope.data.error.code,
              message: envelope.data.error.message,
            }
          : { code: "internal_error", message: "事件流已中断" },
      );
    }
    // 其余事件名(含 message)不消费:通知通道只认识以上两种。
  }

  function teardownTransport(): void {
    if (retryTimer !== null) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    const source = activeSource;
    activeSource = null;
    source?.close();
    const abort = activeAbort;
    activeAbort = null;
    abort?.abort();
  }

  function isStale(myEpoch: number): boolean {
    return closed || myEpoch !== epoch;
  }

  function handleOpen(myEpoch: number): void {
    if (isStale(myEpoch)) {
      return;
    }
    consecutiveFailures = 0;
    notifyState("open");
  }

  function handleFailure(myEpoch: number): void {
    if (isStale(myEpoch)) {
      return;
    }
    // 立刻作废当前传输代际,防止同一次故障被重复计数。
    epoch += 1;
    teardownTransport();
    consecutiveFailures += 1;
    if (consecutiveFailures >= SSE_CURSOR_DROP_FAILURES && lastEventId !== null) {
      lastEventId = null;
    }
    notifyState("reconnecting");
    retryTimer = setTimeout(() => {
      retryTimer = null;
      openTransport();
    }, backoffDelayMs(consecutiveFailures, random));
  }

  function handleUnauthorized(): void {
    if (closed) {
      return;
    }
    closed = true;
    epoch += 1;
    teardownTransport();
    handlers.onUnauthorized?.();
    notifyState("closed");
  }

  function eventsUrlWithCursor(): string {
    if (lastEventId === null || lastEventId === "") {
      return url;
    }
    const separator = url.includes("?") ? "&" : "?";
    return `${url}${separator}cursor=${encodeURIComponent(lastEventId)}`;
  }

  function openEventSourceTransport(myEpoch: number): void {
    let source: EventSourceLike;
    try {
      source = (options.createEventSource ?? defaultCreateEventSource)(
        eventsUrlWithCursor(),
      );
    } catch {
      handleFailure(myEpoch);
      return;
    }
    activeSource = source;
    source.addEventListener("open", () => handleOpen(myEpoch));
    source.addEventListener("error", () => handleFailure(myEpoch));
    source.addEventListener("operation_page", (event) => {
      if (isStale(myEpoch)) {
        return;
      }
      dispatchFrame(
        "operation_page",
        event.data,
        event.lastEventId === "" ? null : event.lastEventId,
      );
    });
    source.addEventListener("stream_error", (event) => {
      if (isStale(myEpoch)) {
        return;
      }
      dispatchFrame(
        "stream_error",
        event.data,
        event.lastEventId === "" ? null : event.lastEventId,
      );
    });
  }

  async function runFetchTransport(
    myEpoch: number,
    token: string | null,
  ): Promise<void> {
    const controller = new AbortController();
    activeAbort = controller;
    const headers = new Headers({ accept: "text/event-stream" });
    if (token !== null) {
      headers.set("authorization", `Bearer ${token}`);
    }
    if (lastEventId !== null && lastEventId !== "") {
      headers.set("last-event-id", lastEventId);
    }

    let response: Response;
    try {
      response = await fetchImpl(url, {
        method: "GET",
        headers,
        signal: controller.signal,
        credentials: "omit",
        cache: "no-store",
      });
    } catch {
      handleFailure(myEpoch);
      return;
    }
    if (isStale(myEpoch)) {
      return;
    }
    if (response.status === 401) {
      handleUnauthorized();
      return;
    }
    if (!response.ok || response.body === null) {
      await handleRejectedResponse(myEpoch, response);
      return;
    }

    handleOpen(myEpoch);
    const parser = new SseStreamParser();
    const decoder = new TextDecoder();
    const reader = response.body.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (isStale(myEpoch)) {
          return;
        }
        if (done) {
          break;
        }
        for (const frame of parser.feed(decoder.decode(value, { stream: true }))) {
          dispatchFrame(frame.event, frame.data, frame.id);
          if (isStale(myEpoch)) {
            // onStreamError 回调可能已 restart()/close()。
            return;
          }
        }
      }
    } catch {
      // 读取中断与服务端断流统一走 failure。
    }
    handleFailure(myEpoch);
  }

  /**
   * 建连即被拒(非 2xx)。后端对失效游标在建连时回 400 invalid_cursor /
   * 410 cursor_expired(不进入流):丢弃本地游标并走 onStreamError,
   * 让上层与流中 stream_error 走同一条失效协议;其余错误按普通失败退避。
   */
  async function handleRejectedResponse(
    myEpoch: number,
    response: Response,
  ): Promise<void> {
    let cursorError: StreamErrorBody | null = null;
    try {
      const envelope = apiErrorEnvelopeSchema.safeParse(await response.json());
      if (
        envelope.success &&
        (envelope.data.error.code === "invalid_cursor" ||
          envelope.data.error.code === "cursor_expired")
      ) {
        cursorError = {
          code: envelope.data.error.code,
          message: envelope.data.error.message,
        };
      }
    } catch {
      // 非 JSON 错误体:按普通失败处理。
    }
    if (isStale(myEpoch)) {
      return;
    }
    if (cursorError !== null) {
      lastEventId = null;
      handlers.onStreamError?.(cursorError);
      if (isStale(myEpoch)) {
        // onStreamError 回调可能已 restart()/close()。
        return;
      }
    }
    handleFailure(myEpoch);
  }

  function openTransport(): void {
    if (closed) {
      return;
    }
    epoch += 1;
    const myEpoch = epoch;
    const token = getToken();
    const eventSourceAvailable =
      options.createEventSource !== undefined ||
      typeof EventSource !== "undefined";
    if (token !== null || !eventSourceAvailable) {
      void runFetchTransport(myEpoch, token);
    } else {
      openEventSourceTransport(myEpoch);
    }
  }

  const connection: SseConnection = {
    close(): void {
      if (closed) {
        return;
      }
      closed = true;
      epoch += 1;
      teardownTransport();
      status = "closed";
    },
    restart(): void {
      if (closed) {
        return;
      }
      epoch += 1;
      teardownTransport();
      notifyState("connecting");
      // openTransport 自增代际,上面的自增确保旧传输回调立即失效。
      openTransport();
    },
    clearLastEventId(): void {
      lastEventId = null;
    },
    getLastEventId(): string | null {
      return lastEventId;
    },
    getState(): SseConnectionState {
      return { status, consecutiveFailures };
    },
  };

  notifyState("connecting");
  openTransport();
  return connection;
}
