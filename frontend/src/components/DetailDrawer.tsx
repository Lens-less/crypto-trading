import { useEffect, useRef, type ReactNode } from "react";

export interface DetailDrawerProps {
  /** 无障碍标题(role=dialog 的可见名称)。 */
  title: string;
  /** 标题下方的等宽标识符(如 batch_id 完整值)。 */
  identifier?: string;
  onClose: () => void;
  children: ReactNode;
  headerExtra?: ReactNode;
}

/**
 * 右侧详情抽屉(执行批次等)。
 *
 * 焦点管理契约:
 * - 打开时把焦点移到「关闭」按钮;
 * - Escape 关闭;
 * - 卸载时把焦点还给打开前聚焦的元素(触发行),
 *   通过保存元素引用实现,绝不把 id 插值进选择器。
 */
export function DetailDrawer({
  title,
  identifier,
  onClose,
  children,
  headerExtra,
}: DetailDrawerProps) {
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const restoreFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    restoreFocusRef.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    closeButtonRef.current?.focus();
    return () => {
      restoreFocusRef.current?.focus();
    };
  }, []);

  return (
    <aside
      role="dialog"
      aria-modal="false"
      aria-labelledby="detail-drawer-title"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          onClose();
        }
      }}
      className="flex h-full w-full flex-col rounded-lg border border-border bg-card xl:w-[26rem]"
    >
      <header className="border-b border-border p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <h2 id="detail-drawer-title" className="text-sm font-medium">
              {title}
            </h2>
            {identifier !== undefined && (
              <p className="numeric mt-1 break-all text-xs text-muted-foreground">
                {identifier}
              </p>
            )}
          </div>
          <button
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
            className="min-h-10 shrink-0 rounded-md border border-border px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
          >
            关闭详情(Esc)
          </button>
        </div>
        {headerExtra !== undefined && <div className="mt-2">{headerExtra}</div>}
      </header>
      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-4">{children}</div>
    </aside>
  );
}
