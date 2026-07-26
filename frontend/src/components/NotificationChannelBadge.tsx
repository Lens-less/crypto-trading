import { StatusPill, type StatusPillProps } from "./StatusPill";
import {
  useOperationEvents,
  type NotificationChannelStatus,
  type UseOperationEventsOptions,
} from "../lib/useOperationEvents";

/**
 * 通知通道三态文案。语义红线:SSE 只是变更通知通道,文案不得出现
 * 「实时 / 新鲜」等行情语义;状态 = 文字 + 安全语义色,颜色不单独承载含义。
 */
export function notificationChannelDisplay(status: NotificationChannelStatus): {
  label: string;
  tone: NonNullable<StatusPillProps["tone"]>;
} {
  switch (status) {
    case "connected":
      return { label: "已连接 · 仅通知", tone: "ok" };
    case "reconnecting":
      return { label: "重连中", tone: "warning" };
    case "degraded":
      return { label: "通知不可用", tone: "danger" };
  }
}

export interface NotificationChannelBadgeProps {
  /** 测试注入:透传给 useOperationEvents。 */
  events?: UseOperationEventsOptions;
}

/** 权限脊柱上的通知通道徽标;整个应用只挂载一处(AppShell)。 */
export function NotificationChannelBadge({
  events,
}: NotificationChannelBadgeProps) {
  const state = useOperationEvents(events);
  const display = notificationChannelDisplay(state.status);
  return (
    <div
      className="border-t border-border px-5 py-3"
      data-testid="notification-channel"
    >
      <p className="text-xs text-muted-foreground">通知通道</p>
      <div className="mt-1.5">
        <StatusPill label={display.label} tone={display.tone} />
      </div>
    </div>
  );
}
