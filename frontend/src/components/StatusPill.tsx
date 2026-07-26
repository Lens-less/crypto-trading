import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "../lib/cn";

/**
 * 安全语义状态标签:状态 = 文字 + 颜色,颜色永不单独承载含义。
 * tone 只使用安全语义色(--safe-*),与方向语义(--up/--down)严格分离。
 */
const statusPill = cva(
  "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium",
  {
    variants: {
      tone: {
        ok: "border-safe-ok/40 bg-safe-ok/10 text-safe-ok",
        warning: "border-safe-warning/40 bg-safe-warning/10 text-safe-warning",
        danger: "border-safe-danger/40 bg-safe-danger/10 text-safe-danger",
        neutral: "border-safe-neutral/40 bg-safe-neutral/10 text-safe-neutral",
      },
    },
    defaultVariants: {
      tone: "neutral",
    },
  },
);

export interface StatusPillProps extends VariantProps<typeof statusPill> {
  /** 必填:状态必须有文字,不允许纯色点。 */
  label: string;
  className?: string;
}

export function StatusPill({ label, tone, className }: StatusPillProps) {
  return <span className={cn(statusPill({ tone }), className)}>{label}</span>;
}
