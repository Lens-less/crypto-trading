import { act, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiRequestError, getBearerToken, setBearerToken } from "./api";
import type {
  OperationEventsHandlers,
  SseConnection,
  connectOperationEvents,
} from "./sse";
import {
  DEGRADED_FAILURE_THRESHOLD,
  changeBearerToken,
  currentSessionGeneration,
  handleUnauthorizedError,
  invalidateSession,
  resetSessionStateForTests,
  useOperationEvents,
} from "./useOperationEvents";

interface CapturedConnection {
  handlers: OperationEventsHandlers;
  connection: {
    close: ReturnType<typeof vi.fn>;
    restart: ReturnType<typeof vi.fn>;
    clearLastEventId: ReturnType<typeof vi.fn>;
    getLastEventId: ReturnType<typeof vi.fn>;
    getState: ReturnType<typeof vi.fn>;
  };
}

function makeConnectImpl(): {
  captured: CapturedConnection[];
  connectImpl: typeof connectOperationEvents;
} {
  const captured: CapturedConnection[] = [];
  const connectImpl: typeof connectOperationEvents = (handlers) => {
    const connection = {
      close: vi.fn(),
      restart: vi.fn(),
      clearLastEventId: vi.fn(),
      getLastEventId: vi.fn(() => null),
      getState: vi.fn(() => ({
        status: "connecting" as const,
        consecutiveFailures: 0,
      })),
    };
    captured.push({ handlers, connection });
    handlers.onStateChange?.({ status: "connecting", consecutiveFailures: 0 });
    return connection as unknown as SseConnection;
  };
  return { captured, connectImpl };
}

function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
}

function wrapperFor(queryClient: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

function renderEvents(queryClient: QueryClient) {
  const { captured, connectImpl } = makeConnectImpl();
  const rendered = renderHook(() => useOperationEvents({ connectImpl }), {
    wrapper: wrapperFor(queryClient),
  });
  return { captured, ...rendered };
}

function lastCaptured(captured: CapturedConnection[]): CapturedConnection {
  const last = captured[captured.length - 1];
  if (last === undefined) {
    throw new Error("no connection captured");
  }
  return last;
}

beforeEach(() => {
  resetSessionStateForTests();
  setBearerToken(null);
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("operation_page → React Query 失效信号(tickflow)", () => {
  it("收到 operation_page 时按 queryKeys 失效全部受保护端点", () => {
    const queryClient = createQueryClient();
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { captured, unmount } = renderEvents(queryClient);

    act(() => {
      lastCaptured(captured).handlers.onPage?.({ schema_version: 1 }, "c-1");
    });

    const keys = invalidateSpy.mock.calls.map((call) => call[0]?.queryKey);
    expect(keys).toEqual([
      ["system"],
      ["capabilities"],
      ["monitor"],
      ["tasks"],
      ["risk"],
      ["settings"],
      ["executions"],
    ]);
    unmount();
  });
});

describe("cursor 失效协议", () => {
  it.each(["invalid_cursor", "cursor_expired"])(
    "stream_error %s → 丢弃游标、整体失效、全新流重连",
    (code) => {
      const queryClient = createQueryClient();
      const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
      const { captured, unmount } = renderEvents(queryClient);
      const { handlers, connection } = lastCaptured(captured);

      act(() => {
        handlers.onStreamError?.({ code, message: "cursor gone" });
      });
      expect(connection.clearLastEventId).toHaveBeenCalledTimes(1);
      expect(invalidateSpy).toHaveBeenCalled();
      expect(connection.restart).toHaveBeenCalledTimes(1);
      unmount();
    },
  );

  it("其他 stream_error(如 journal_unavailable)不清游标不重启", () => {
    const queryClient = createQueryClient();
    const { captured, unmount } = renderEvents(queryClient);
    const { handlers, connection } = lastCaptured(captured);

    act(() => {
      handlers.onStreamError?.({
        code: "journal_unavailable",
        message: "unavailable",
      });
    });
    expect(connection.clearLastEventId).not.toHaveBeenCalled();
    expect(connection.restart).not.toHaveBeenCalled();
    unmount();
  });
});

describe("连接状态机:connected | reconnecting | degraded", () => {
  it("open → connected;失败 < 阈值 → reconnecting;≥ 阈值 → degraded", () => {
    const queryClient = createQueryClient();
    const { captured, result, unmount } = renderEvents(queryClient);
    const { handlers } = lastCaptured(captured);

    act(() => {
      handlers.onStateChange?.({ status: "open", consecutiveFailures: 0 });
    });
    expect(result.current.status).toBe("connected");

    act(() => {
      handlers.onStateChange?.({
        status: "reconnecting",
        consecutiveFailures: DEGRADED_FAILURE_THRESHOLD - 1,
      });
    });
    expect(result.current.status).toBe("reconnecting");

    act(() => {
      handlers.onStateChange?.({
        status: "reconnecting",
        consecutiveFailures: DEGRADED_FAILURE_THRESHOLD,
      });
    });
    expect(result.current.status).toBe("degraded");
    expect(result.current.consecutiveFailures).toBe(DEGRADED_FAILURE_THRESHOLD);
    unmount();
  });
});

describe("认证代际(session generation)", () => {
  it("401(流)→ 清缓存 + generation++ + 断流并降级", () => {
    const queryClient = createQueryClient();
    const clearSpy = vi.spyOn(queryClient, "clear");
    const { captured, result, unmount } = renderEvents(queryClient);
    const { handlers, connection } = lastCaptured(captured);
    const generationBefore = currentSessionGeneration();

    act(() => {
      handlers.onUnauthorized?.();
    });
    expect(clearSpy).toHaveBeenCalledTimes(1);
    expect(currentSessionGeneration()).toBe(generationBefore + 1);
    expect(connection.close).toHaveBeenCalled();
    expect(result.current.status).toBe("degraded");
    unmount();
  });

  it("旧 generation 的回调一律丢弃(防陈旧响应写入)", () => {
    const queryClient = createQueryClient();
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { captured, unmount } = renderEvents(queryClient);
    const stale = lastCaptured(captured);

    act(() => {
      stale.handlers.onUnauthorized?.(); // generation++
    });
    invalidateSpy.mockClear();

    act(() => {
      stale.handlers.onPage?.({ schema_version: 1 }, "old-cursor");
      stale.handlers.onStreamError?.({ code: "cursor_expired", message: "x" });
      stale.handlers.onStateChange?.({ status: "open", consecutiveFailures: 0 });
    });
    expect(invalidateSpy).not.toHaveBeenCalled();
    expect(stale.connection.restart).not.toHaveBeenCalled();
    unmount();
  });

  it("同一失效凭据下的重复 401 只清一次缓存(幂等,防 401 风暴)", () => {
    const queryClient = createQueryClient();
    const clearSpy = vi.spyOn(queryClient, "clear");
    invalidateSession(queryClient);
    invalidateSession(queryClient);
    expect(clearSpy).toHaveBeenCalledTimes(1);
  });

  it("bearer token 变更 → 断旧流、清缓存、新 generation 重连", () => {
    const queryClient = createQueryClient();
    const clearSpy = vi.spyOn(queryClient, "clear");
    const { captured, unmount } = renderEvents(queryClient);
    const first = lastCaptured(captured);
    const generationBefore = currentSessionGeneration();

    act(() => {
      changeBearerToken(queryClient, "token-0123456789abcdef0123456789abcdef");
    });
    expect(getBearerToken()).toBe("token-0123456789abcdef0123456789abcdef");
    expect(clearSpy).toHaveBeenCalledTimes(1);
    expect(currentSessionGeneration()).toBe(generationBefore + 1);
    expect(first.connection.close).toHaveBeenCalled();
    expect(captured).toHaveLength(2); // 新代际重建了连接
    unmount();
  });

  it("401 降级后,凭据变更可恢复通道(authHalted 复位)", () => {
    const queryClient = createQueryClient();
    const clearSpy = vi.spyOn(queryClient, "clear");
    const { captured, result, unmount } = renderEvents(queryClient);

    act(() => {
      lastCaptured(captured).handlers.onUnauthorized?.();
    });
    expect(result.current.status).toBe("degraded");
    expect(captured).toHaveLength(1);

    act(() => {
      changeBearerToken(queryClient, "token-0123456789abcdef0123456789abcdef");
    });
    expect(captured).toHaveLength(2);
    expect(clearSpy).toHaveBeenCalledTimes(2);

    // 新连接进入 open 后状态恢复 connected
    act(() => {
      lastCaptured(captured).handlers.onStateChange?.({
        status: "open",
        consecutiveFailures: 0,
      });
    });
    expect(result.current.status).toBe("connected");
    unmount();
  });

  it("卸载时关闭连接", () => {
    const queryClient = createQueryClient();
    const { captured, unmount } = renderEvents(queryClient);
    unmount();
    expect(lastCaptured(captured).connection.close).toHaveBeenCalled();
  });
});

describe("handleUnauthorizedError(queryCache/mutationCache onError)", () => {
  it("401 错误触发会话作废,其余错误不动缓存", () => {
    const queryClient = createQueryClient();
    const clearSpy = vi.spyOn(queryClient, "clear");

    handleUnauthorizedError(new Error("plain"), queryClient);
    handleUnauthorizedError(
      new ApiRequestError("network", "网络请求失败"),
      queryClient,
    );
    expect(clearSpy).not.toHaveBeenCalled();

    const generationBefore = currentSessionGeneration();
    handleUnauthorizedError(
      new ApiRequestError("unauthorized", "需要认证", { status: 401 }),
      queryClient,
    );
    expect(clearSpy).toHaveBeenCalledTimes(1);
    expect(currentSessionGeneration()).toBe(generationBefore + 1);
  });
});
