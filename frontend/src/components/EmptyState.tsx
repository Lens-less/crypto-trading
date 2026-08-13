/**
 * 一等空态:说明缺的是什么、查过哪个事实来源。
 * 空数据不是错误,也不伪装成加载中;缺失事实永不被解释为健康。
 */
export function EmptyState({
  message,
  checkedFact,
}: {
  message: string;
  /** 已核对的事实来源说明(如「已检查 /api/v1/tasks 返回的 tasks」)。 */
  checkedFact: string;
}) {
  return (
    <div className="rounded-md border border-dashed border-border px-4 py-6 text-center">
      <p className="text-sm">{message}</p>
      <p className="mt-1 text-xs text-muted-foreground">{checkedFact}</p>
    </div>
  );
}

/** 加载骨架:与最终行几何一致,只用表面色阶,不做闪光动画。 */
export function SkeletonRows({ rows }: { rows: number }) {
  return (
    <div aria-hidden="true" className="space-y-2">
      {Array.from({ length: rows }, (_, index) => (
        <div
          key={index}
          className={
            index % 3 === 0
              ? "h-4 w-1/3 rounded bg-muted"
              : index % 3 === 1
                ? "h-4 w-2/3 rounded bg-muted"
                : "h-4 w-full rounded bg-muted"
          }
        />
      ))}
    </div>
  );
}
