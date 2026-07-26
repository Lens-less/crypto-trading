import { act, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it } from "vitest";
import type {
  OperationEventsHandlers,
  SseConnection,
  connectOperationEvents,
} from "../lib/sse";
import { resetSessionStateForTests } from "../lib/useOperationEvents";
import {
  NotificationChannelBadge,
  notificationChannelDisplay,
} from "./NotificationChannelBadge";

beforeEach(() => {
  resetSessionStateForTests();
});

describe("notificationChannelDisplay 三态文案", () => {
  it("文案严格三态,且不出现「实时/新鲜」字样", () => {
    expect(notificationChannelDisplay("connected")).toEqual({
      label: "已连接 · 仅通知",
      tone: "ok",
    });
    expect(notificationChannelDisplay("reconnecting")).toEqual({
      label: "重连中",
      tone: "warning",
    });
    expect(notificationChannelDisplay("degraded")).toEqual({
      label: "通知不可用",
      tone: "danger",
    });
    for (const status of ["connected", "reconnecting", "degraded"] as const) {
      const { label } = notificationChannelDisplay(status);
      expect(label).not.toContain("实时");
      expect(label).not.toContain("新鲜");
    }
  });
});

describe("NotificationChannelBadge", () => {
  it("随连接状态渲染对应徽标文案", () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    let handlers: OperationEventsHandlers | null = null;
    const connectImpl: typeof connectOperationEvents = (h) => {
      handlers = h;
      h.onStateChange?.({ status: "connecting", consecutiveFailures: 0 });
      return {
        close: () => {},
        restart: () => {},
        clearLastEventId: () => {},
        getLastEventId: () => null,
        getState: () => ({
          status: "connecting" as const,
          consecutiveFailures: 0,
        }),
      } as SseConnection;
    };

    render(
      <QueryClientProvider client={queryClient}>
        <NotificationChannelBadge events={{ connectImpl }} />
      </QueryClientProvider>,
    );
    expect(screen.getByText("通知通道")).toBeDefined();
    expect(screen.getByText("重连中")).toBeDefined();

    act(() => {
      handlers?.onStateChange?.({ status: "open", consecutiveFailures: 0 });
    });
    expect(screen.getByText("已连接 · 仅通知")).toBeDefined();

    act(() => {
      handlers?.onStateChange?.({
        status: "reconnecting",
        consecutiveFailures: 3,
      });
    });
    expect(screen.getByText("通知不可用")).toBeDefined();
  });
});
