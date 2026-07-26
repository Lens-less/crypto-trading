import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SseStreamParser,
  backoffDelayMs,
  connectOperationEvents,
  type EventSourceLike,
  type SseConnectionState,
  type SseMessageEventLike,
} from "./sse";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

/* ------------------------------------------------------------ 行协议解析器 */

describe("SseStreamParser 分帧", () => {
  it("完整帧:event + 多行 data + id,空行派发", () => {
    const parser = new SseStreamParser();
    const frames = parser.feed(
      "event: operation_page\nid: cursor-1\ndata: line-a\ndata: line-b\n\n",
    );
    expect(frames).toEqual([
      { event: "operation_page", data: "line-a\nline-b", id: "cursor-1" },
    ]);
  });

  it("跨 chunk 的半行会被缓冲,直到行终止符到齐", () => {
    const parser = new SseStreamParser();
    expect(parser.feed("event: operation_page\nda")).toEqual([]);
    expect(parser.feed('ta: {"x":1}\n')).toEqual([]);
    const frames = parser.feed("\n");
    expect(frames).toEqual([
      { event: "operation_page", data: '{"x":1}', id: null },
    ]);
  });

  it("CRLF 与被 chunk 边界拆开的 \\r\\n 等价于 \\n", () => {
    const parser = new SseStreamParser();
    expect(parser.feed("data: a\r")).toEqual([]);
    const frames = parser.feed("\n\r\n");
    expect(frames).toEqual([{ event: "message", data: "a", id: null }]);
  });

  it("注释行(keep-alive)被忽略,不产生帧", () => {
    const parser = new SseStreamParser();
    expect(parser.feed(": keep-alive\n\n: keep-alive\n")).toEqual([]);
  });

  it("id 跨帧记忆:后续无 id 的帧沿用最近的 id", () => {
    const parser = new SseStreamParser();
    const first = parser.feed("id: cursor-7\ndata: one\n\n");
    expect(first[0]?.id).toBe("cursor-7");
    const second = parser.feed("data: two\n\n");
    expect(second[0]?.id).toBe("cursor-7");
    expect(parser.lastEventId()).toBe("cursor-7");
  });

  it("含 NUL 的 id 按规范忽略", () => {
    const parser = new SseStreamParser();
    const frames = parser.feed("id: bad\0id\ndata: x\n\n");
    expect(frames[0]?.id).toBeNull();
    expect(parser.lastEventId()).toBeNull();
  });

  it("只有 event 没有 data 的空行不派发,且事件名被重置", () => {
    const parser = new SseStreamParser();
    expect(parser.feed("event: stream_error\n\n")).toEqual([]);
    const frames = parser.feed("data: x\n\n");
    expect(frames).toEqual([{ event: "message", data: "x", id: null }]);
  });

  it("冒号后单个空格被剥除,无空格与无冒号也合法", () => {
    const parser = new SseStreamParser();
    const frames = parser.feed("data:no-space\ndata\ndata:  two-spaces\n\n");
    expect(frames[0]?.data).toBe("no-space\n\n two-spaces");
  });

  it("一次 feed 可产出多帧", () => {
    const parser = new SseStreamParser();
    const frames = parser.feed("data: 1\n\ndata: 2\n\n");
    expect(frames.map((frame) => frame.data)).toEqual(["1", "2"]);
  });
});

/* ------------------------------------------------------------------- 退避 */

describe("backoffDelayMs", () => {
  it("指数上限序列:1s,2s,4s,8s,16s,30s,30s(random=1 取上限)", () => {
    const delays = [1, 2, 3, 4, 5, 6, 7].map((n) => backoffDelayMs(n, () => 1));
    expect(delays).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000]);
  });

  it("半区间抖动:random=0 时取上限的一半", () => {
    const delays = [1, 2, 3, 4, 5, 6, 7].map((n) => backoffDelayMs(n, () => 0));
    expect(delays).toEqual([500, 1000, 2000, 4000, 8000, 15000, 15000]);
  });
});

/* --------------------------------------------------------- fetch 传输路径 */

interface StreamHandle {
  response: Response;
  push(text: string): void;
  end(): void;
}

function streamResponse(): StreamHandle {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
  });
  const encoder = new TextEncoder();
  const response = {
    ok: true,
    status: 200,
    body: stream,
    headers: new Headers({ "content-type": "text/event-stream" }),
  } as unknown as Response;
  return {
    response,
    push: (text) => controller.enqueue(encoder.encode(text)),
    end: () => controller.close(),
  };
}

function trackStates(): {
  states: SseConnectionState[];
  onStateChange: (state: SseConnectionState) => void;
  latest: () => SseConnectionState;
} {
  const states: SseConnectionState[] = [];
  return {
    states,
    onStateChange: (state) => states.push(state),
    latest: () => {
      const last = states[states.length - 1];
      if (last === undefined) {
        throw new Error("no state recorded");
      }
      return last;
    },
  };
}

describe("connectOperationEvents(fetch 路径,有 token)", () => {
  it("携带 Authorization/Accept 头,解析 operation_page 并记住 id", async () => {
    const stream = streamResponse();
    const fetchImpl = vi.fn().mockResolvedValue(stream.response);
    const onPage = vi.fn();
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onPage, onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl },
    );

    await vi.waitFor(() => expect(tracker.latest().status).toBe("open"));
    const headers = fetchImpl.mock.calls[0]?.[1]?.headers as Headers;
    expect(headers.get("authorization")).toBe("Bearer token-1");
    expect(headers.get("accept")).toBe("text/event-stream");
    expect(headers.get("last-event-id")).toBeNull();

    stream.push(
      'event: operation_page\nid: cursor-9\ndata: {"schema_version":1,"events":[]}\n\n',
    );
    await vi.waitFor(() => expect(onPage).toHaveBeenCalledTimes(1));
    expect(onPage).toHaveBeenCalledWith(
      { schema_version: 1, events: [] },
      "cursor-9",
    );
    expect(connection.getLastEventId()).toBe("cursor-9");
    connection.close();
  });

  it("解析 stream_error 错误封套并回调稳定 code", async () => {
    const stream = streamResponse();
    const fetchImpl = vi.fn().mockResolvedValue(stream.response);
    const onStreamError = vi.fn();
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onStreamError, onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl },
    );
    await vi.waitFor(() => expect(tracker.latest().status).toBe("open"));

    stream.push(
      'event: stream_error\ndata: {"schema_version":1,"error":{"code":"cursor_expired","message":"gone"}}\n\n',
    );
    await vi.waitFor(() => expect(onStreamError).toHaveBeenCalledTimes(1));
    expect(onStreamError).toHaveBeenCalledWith({
      code: "cursor_expired",
      message: "gone",
    });
    connection.close();
  });

  it("断流后按退避重连,重连带 Last-Event-ID,成功后失败计数归零", async () => {
    vi.useFakeTimers();
    const first = streamResponse();
    const second = streamResponse();
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(first.response)
      .mockResolvedValueOnce(second.response);
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl, random: () => 1 },
    );

    await vi.advanceTimersByTimeAsync(0);
    expect(tracker.latest().status).toBe("open");
    first.push("event: operation_page\nid: cursor-3\ndata: {}\n\n");
    await vi.advanceTimersByTimeAsync(0);

    first.end(); // 服务端断流
    await vi.advanceTimersByTimeAsync(0);
    expect(tracker.latest()).toEqual({
      status: "reconnecting",
      consecutiveFailures: 1,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);

    // base 1s:999ms 时尚未重连,1000ms 触发
    await vi.advanceTimersByTimeAsync(999);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    const headers = fetchImpl.mock.calls[1]?.[1]?.headers as Headers;
    expect(headers.get("last-event-id")).toBe("cursor-3");

    await vi.advanceTimersByTimeAsync(0);
    expect(tracker.latest()).toEqual({ status: "open", consecutiveFailures: 0 });
    connection.close();
  });

  it("连续失败按指数退避累计计数", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn().mockRejectedValue(new TypeError("refused"));
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl, random: () => 1 },
    );

    await vi.advanceTimersByTimeAsync(0);
    expect(tracker.latest().consecutiveFailures).toBe(1);
    await vi.advanceTimersByTimeAsync(1000); // 第 2 次尝试
    expect(tracker.latest().consecutiveFailures).toBe(2);
    await vi.advanceTimersByTimeAsync(2000); // 第 3 次尝试
    expect(tracker.latest().consecutiveFailures).toBe(3);
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    // 尚未到 4s 不重试
    await vi.advanceTimersByTimeAsync(3999);
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    connection.close();
  });

  it("401 触发 onUnauthorized、连接永久关闭且不再重试", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 401 }));
    const onUnauthorized = vi.fn();
    const tracker = trackStates();
    connectOperationEvents(
      { onUnauthorized, onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl },
    );

    await vi.advanceTimersByTimeAsync(0);
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    expect(tracker.latest().status).toBe("closed");
    await vi.advanceTimersByTimeAsync(120_000);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("clearLastEventId + restart 后以全新流立即重连(无游标头)", async () => {
    const first = streamResponse();
    const second = streamResponse();
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(first.response)
      .mockResolvedValueOnce(second.response);
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl },
    );
    await vi.waitFor(() => expect(tracker.latest().status).toBe("open"));
    first.push("event: operation_page\nid: cursor-5\ndata: {}\n\n");
    await vi.waitFor(() => expect(connection.getLastEventId()).toBe("cursor-5"));

    connection.clearLastEventId();
    connection.restart();
    await vi.waitFor(() => expect(fetchImpl).toHaveBeenCalledTimes(2));
    const headers = fetchImpl.mock.calls[1]?.[1]?.headers as Headers;
    expect(headers.get("last-event-id")).toBeNull();
    connection.close();
  });

  it("建连即被 400 invalid_cursor 拒绝:丢弃游标、回调 onStreamError、重连不带游标", async () => {
    vi.useFakeTimers();
    const first = streamResponse();
    const rejected = new Response(
      '{"schema_version":1,"error":{"code":"invalid_cursor","message":"bad"}}',
      { status: 400, headers: { "content-type": "application/json" } },
    );
    const third = streamResponse();
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(first.response)
      .mockResolvedValueOnce(rejected)
      .mockResolvedValueOnce(third.response);
    const onStreamError = vi.fn();
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onStreamError, onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl, random: () => 1 },
    );

    await vi.advanceTimersByTimeAsync(0);
    first.push("event: operation_page\nid: cursor-8\ndata: {}\n\n");
    await vi.advanceTimersByTimeAsync(0);
    first.end();
    await vi.advanceTimersByTimeAsync(1000); // 重连,带失效游标 → 400
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(0);
    expect(onStreamError).toHaveBeenCalledWith({
      code: "invalid_cursor",
      message: "bad",
    });
    expect(connection.getLastEventId()).toBeNull();

    await vi.advanceTimersByTimeAsync(2000); // 第三次:全新流
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    const headers = fetchImpl.mock.calls[2]?.[1]?.headers as Headers;
    expect(headers.get("last-event-id")).toBeNull();
    connection.close();
  });

  it("close 后旧传输的帧不再派发", async () => {
    const stream = streamResponse();
    const fetchImpl = vi.fn().mockResolvedValue(stream.response);
    const onPage = vi.fn();
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onPage, onStateChange: tracker.onStateChange },
      { getToken: () => "token-1", fetchImpl },
    );
    await vi.waitFor(() => expect(tracker.latest().status).toBe("open"));
    connection.close();
    stream.push("event: operation_page\ndata: {}\n\n");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(onPage).not.toHaveBeenCalled();
  });
});

/* --------------------------------------------------- EventSource 传输路径 */

class FakeEventSource implements EventSourceLike {
  static instances: FakeEventSource[] = [];
  readonly url: string;
  closed = false;
  #listeners = new Map<string, Array<(event: SseMessageEventLike) => void>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(
    type: string,
    listener: (event: SseMessageEventLike) => void,
  ): void {
    const existing = this.#listeners.get(type) ?? [];
    existing.push(listener);
    this.#listeners.set(type, existing);
  }

  close(): void {
    this.closed = true;
  }

  emit(type: string, event: Partial<SseMessageEventLike> = {}): void {
    for (const listener of this.#listeners.get(type) ?? []) {
      listener({ data: event.data ?? "", lastEventId: event.lastEventId ?? "" });
    }
  }
}

describe("connectOperationEvents(EventSource 路径,无 token)", () => {
  afterEach(() => {
    FakeEventSource.instances = [];
  });

  it("open→派发 operation_page,错误后重连并以 ?cursor= 恢复", async () => {
    vi.useFakeTimers();
    const onPage = vi.fn();
    const tracker = trackStates();
    const connection = connectOperationEvents(
      { onPage, onStateChange: tracker.onStateChange },
      {
        getToken: () => null,
        createEventSource: (url) => new FakeEventSource(url),
        random: () => 1,
      },
    );

    const source = FakeEventSource.instances[0];
    expect(source?.url).toBe("/api/v1/events");
    source?.emit("open");
    expect(tracker.latest().status).toBe("open");
    source?.emit("operation_page", {
      data: '{"schema_version":1}',
      lastEventId: "cursor-2",
    });
    expect(onPage).toHaveBeenCalledWith({ schema_version: 1 }, "cursor-2");

    source?.emit("error");
    expect(source?.closed).toBe(true);
    expect(tracker.latest()).toEqual({
      status: "reconnecting",
      consecutiveFailures: 1,
    });

    await vi.advanceTimersByTimeAsync(1000);
    const second = FakeEventSource.instances[1];
    expect(second?.url).toBe("/api/v1/events?cursor=cursor-2");
    second?.emit("open");
    expect(tracker.latest()).toEqual({ status: "open", consecutiveFailures: 0 });
    connection.close();
    expect(second?.closed).toBe(true);
  });

  it("连续失败达到阈值后丢弃游标,后续重试为全新流(兜底阀门)", async () => {
    vi.useFakeTimers();
    const connection = connectOperationEvents(
      {},
      {
        getToken: () => null,
        createEventSource: (url) => new FakeEventSource(url),
        random: () => 1,
      },
    );
    const first = FakeEventSource.instances[0];
    first?.emit("open");
    first?.emit("operation_page", { data: "{}", lastEventId: "cursor-x" });

    first?.emit("error"); // 失败 1:游标保留
    await vi.advanceTimersByTimeAsync(1000);
    expect(FakeEventSource.instances[1]?.url).toBe(
      "/api/v1/events?cursor=cursor-x",
    );
    FakeEventSource.instances[1]?.emit("error"); // 失败 2:游标保留
    await vi.advanceTimersByTimeAsync(2000);
    expect(FakeEventSource.instances[2]?.url).toBe(
      "/api/v1/events?cursor=cursor-x",
    );
    FakeEventSource.instances[2]?.emit("error"); // 失败 3:丢弃游标
    await vi.advanceTimersByTimeAsync(4000);
    expect(FakeEventSource.instances[3]?.url).toBe("/api/v1/events");
    connection.close();
  });

  it("stream_error 事件走 onStreamError 回调", () => {
    const onStreamError = vi.fn();
    const connection = connectOperationEvents(
      { onStreamError },
      {
        getToken: () => null,
        createEventSource: (url) => new FakeEventSource(url),
      },
    );
    const source = FakeEventSource.instances[0];
    source?.emit("open");
    source?.emit("stream_error", {
      data: '{"schema_version":1,"error":{"code":"invalid_cursor","message":"bad"}}',
    });
    expect(onStreamError).toHaveBeenCalledWith({
      code: "invalid_cursor",
      message: "bad",
    });
    connection.close();
  });
});
