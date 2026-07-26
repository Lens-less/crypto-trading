import type { ReactNode } from "react";
import { cn } from "../lib/cn";

export interface DataCardProps {
  /** 卡片标题(必填:每张卡都要说明自己呈现的是哪份事实)。 */
  title: string;
  /** 数据来源或语义说明(如 API 路径、投影性质)。 */
  subtitle?: string;
  /** 标题右侧的状态区(StatusPill 等)。 */
  headerExtra?: ReactNode;
  children: ReactNode;
  className?: string;
}

/** 内容区标准卡片:--card 表面 + --border 边线,标题 + 可选副标题。 */
export function DataCard({
  title,
  subtitle,
  headerExtra,
  children,
  className,
}: DataCardProps) {
  return (
    <section className={cn("rounded-lg border border-border bg-card p-5", className)}>
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-medium text-card-foreground">{title}</h2>
          {subtitle !== undefined && (
            <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p>
          )}
        </div>
        {headerExtra !== undefined && <div className="shrink-0">{headerExtra}</div>}
      </header>
      <div className="mt-3 space-y-2">{children}</div>
    </section>
  );
}

/** 卡片内的「标签 - 值」事实行;值默认等宽 tabular。 */
export function FactRow({
  label,
  value,
  numeric = true,
}: {
  label: string;
  value: ReactNode;
  numeric?: boolean;
}) {
  return (
    <div className="flex items-baseline justify-between gap-4">
      <span className="shrink-0 text-xs text-muted-foreground">{label}</span>
      <span
        className={cn(
          "min-w-0 break-all text-right text-sm",
          numeric && "numeric",
        )}
      >
        {value}
      </span>
    </div>
  );
}
