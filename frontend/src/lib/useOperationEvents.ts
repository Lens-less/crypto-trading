/**
 * 通知通道 → React Query 失效协议(tickflow 模式)。
 *
 * - SSE 只做「已连接 / 仅通知」的失效信号:收到 operation_page 一律
 *   invalidateQueries,数据永远由 REST 端点重新拉取,UI 不消费事件 payload。
 * - 游标失效(invalid_cursor / cursor_expired):丢弃本地 Last-Event-ID、
 *   整体失效、以全新流重连。
 * - 认证代际(session generation):401(流或请求)→ 断流 + 清空 React
 *   Query 缓存 + generation++;旧 generation 的回调一律丢弃,防止陈旧
 *   响应写回新会话。凭据变更(settings 页写入)同样推进代际并重建流。
 * - 连接状态机(对 UI):connected | reconnecting | degraded,连续失败
 *   ≥ 3 次才降级,避免网络抖动刷屏。
 */
import { useEffect, useRef, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { asApiError, setBearerToken } from "./api";
import { queryKeys } from "./queryKeys";
import {
  connectOperationEvents,
  type OperationEventsOptions,
  type SseConnection,
  type SseConnectionState,
} from "./sse";

/** 连续失败达到该阈值才把通道标为「通知不可用」。 */
export const DEGRADED_FAILURE_THRESHOLD = 3;

export type NotificationChannelStatus = "connected" | "reconnecting" | "degraded";

export interface NotificationChannelState {
  status: NotificationChannelStatus;
  consecutiveFailures: number;
}

/* -------------------------------------------------------------- 会话代际 */

export type SessionChangeReason = "unauthorized" | "credentials-changed";

let sessionGeneration = 0;
/** 401 处理后置位:旧凭据下的重复 401 不再反复清缓存(防 401 风暴)。 */
let authHalted = false;
const sessionListeners = new Set<(reason: SessionChangeReason) => void>();

export function currentSessionGeneration(): number {
  return sessionGeneration;
}

export function subscribeSessionChanges(
  listener: (reason: SessionChangeReason) => void,
): () => void {
  sessionListeners.add(listener);
  return () => {
    sessionListeners.delete(listener);
  };
}

function bumpSession(reason: SessionChangeReason): void {
  sessionGeneration += 1;
  for (const listener of [...sessionListeners]) {
    listener(reason);
  }
}

/**
 * 401 会话作废:清空 React Query 缓存并推进代际。
 * 幂等:同一失效凭据下只执行一次,直到 changeBearerToken 换新凭据。
 */
export function invalidateSession(queryClient: QueryClient): void {
  if (authHalted) {
    return;
  }
  authHalted = true;
  queryClient.clear();
  bumpSession("unauthorized");
}

/**
 * 写入新 bearer token(仅内存)并开启新会话代际:清缓存、重建通知流。
 */
export function changeBearerToken(
  queryClient: QueryClient,
  token: string | null,
): void {
  setBearerToken(token);
  authHalted = false;
  queryClient.clear();
  bumpSession("credentials-changed");
}

/**
 * 全局 401 拦截:装在 QueryClient 的 queryCache/mutationCache onError
 * (main.tsx 处组装)。非 401 错误不做任何事。
 */
export function handleUnauthorizedError(
  error: unknown,
  queryClient: QueryClient,
): void {
  if (asApiError(error)?.kind === "unauthorized") {
    invalidateSession(queryClient);
  }
}

/** 仅供测试:重置模块级会话状态。 */
export function resetSessionStateForTests(): void {
  sessionGeneration = 0;
  authHalted = false;
  sessionListeners.clear();
}

/* -------------------------------------------------------------- 失效映射 */

/** 全部受保护端点的 query key;executions 用前缀匹配覆盖所有游标分页。 */
const OPERATION_QUERY_KEYS: ReadonlyArray<readonly unknown[]> = [
  queryKeys.system,
  queryKeys.capabilities,
  queryKeys.monitor,
  queryKeys.alerts,
  queryKeys.tasks,
  queryKeys.scanner,
  queryKeys.risk,
  queryKeys.settings,
  ["executions"],
];

export function invalidateOperationQueries(queryClient: QueryClient): void {
  for (const queryKey of OPERATION_QUERY_KEYS) {
    void queryClient.invalidateQueries({ queryKey });
  }
}

/* ------------------------------------------------------------------ hook */

function mapStatus(sse: SseConnectionState): NotificationChannelStatus {
  if (sse.status === "open") {
    return "connected";
  }
  if (sse.status === "closed") {
    return "degraded";
  }
  return sse.consecutiveFailures >= DEGRADED_FAILURE_THRESHOLD
    ? "degraded"
    : "reconnecting";
}

export interface UseOperationEventsOptions {
  /** 测试注入:替代 connectOperationEvents。 */
  connectImpl?: typeof connectOperationEvents;
  /** 透传给 SSE 客户端(测试注入 fetch/EventSource/random)。 */
  sseOptions?: OperationEventsOptions;
}

/**
 * 维护通知通道生命周期并返回三态连接状态。
 * 必须在 QueryClientProvider 内使用;整个应用挂载一次(AppShell)。
 */
export function useOperationEvents(
  options: UseOperationEventsOptions = {},
): NotificationChannelState {
  const queryClient = useQueryClient();
  const [state, setState] = useState<NotificationChannelState>({
    status: "reconnecting",
    consecutiveFailures: 0,
  });
  const optionsRef = useRef(options);
  optionsRef.current = options;

  useEffect(() => {
    let disposed = false;
    let connection: SseConnection | null = null;

    const connect = (): void => {
      connection?.close();
      const generation = currentSessionGeneration();
      const stale = (): boolean =>
        disposed || generation !== currentSessionGeneration();
      const fresh = (optionsRef.current.connectImpl ?? connectOperationEvents)(
        {
          onPage: () => {
            if (stale()) {
              return;
            }
            invalidateOperationQueries(queryClient);
          },
          onStreamError: (error) => {
            if (stale()) {
              return;
            }
            if (
              error.code === "invalid_cursor" ||
              error.code === "cursor_expired"
            ) {
              // 游标已失效:丢弃本地游标、整体失效,以全新流重连。
              fresh.clearLastEventId();
              invalidateOperationQueries(queryClient);
              fresh.restart();
            }
          },
          onStateChange: (sse) => {
            if (stale()) {
              return;
            }
            setState({
              status: mapStatus(sse),
              consecutiveFailures: sse.consecutiveFailures,
            });
          },
          onUnauthorized: () => {
            if (stale()) {
              return;
            }
            invalidateSession(queryClient);
          },
        },
        optionsRef.current.sseOptions,
      );
      connection = fresh;
    };

    connect();
    const unsubscribe = subscribeSessionChanges((reason) => {
      if (disposed) {
        return;
      }
      if (reason === "credentials-changed") {
        connect();
        return;
      }
      // unauthorized:断流并保持「通知不可用」,直到凭据变更。
      connection?.close();
      connection = null;
      setState({ status: "degraded", consecutiveFailures: 0 });
    });

    return () => {
      disposed = true;
      unsubscribe();
      connection?.close();
    };
  }, [queryClient]);

  return state;
}
