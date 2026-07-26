import type { ReactNode } from "react";
import { cn } from "../lib/cn";
import { StatusPill } from "./StatusPill";
import type { BannerDescriptor } from "../lib/banners";

const TONE_CLASSES: Record<BannerDescriptor["tone"], string> = {
  warning: "border-safe-warning/50 bg-safe-warning/10",
  danger: "border-safe-danger/50 bg-safe-danger/10",
  neutral: "border-safe-neutral/50 bg-safe-neutral/10",
};

export interface DegradedBannerProps {
  banner: BannerDescriptor;
  /** 可选恢复动作(如「清除游标」「重试快照」)。 */
  action?: ReactNode;
}

/**
 * 一等状态横幅:窗口化 / 降级 / 未决等必须固定显示在受影响区域上方,
 * 状态 = 文字(title + tag)+ 安全色,颜色永不单独承载含义。
 * danger 用 role="alert",其余 role="status",读屏可感知。
 */
export function DegradedBanner({ banner, action }: DegradedBannerProps) {
  return (
    <div
      role={banner.tone === "danger" ? "alert" : "status"}
      data-banner-key={banner.key}
      className={cn(
        "rounded-lg border px-4 py-3",
        TONE_CLASSES[banner.tone],
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm font-medium">{banner.title}</span>
        <StatusPill tone={banner.tone === "neutral" ? "neutral" : banner.tone} label={banner.tag} />
      </div>
      <p className="mt-1 text-xs text-muted-foreground">{banner.message}</p>
      {action !== undefined && <div className="mt-2">{action}</div>}
    </div>
  );
}
