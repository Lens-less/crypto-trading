import { humanizeToken } from "../lib/labels";
import type { CursorPagerState } from "../lib/cursorPager";
import { hasMoreInSnapshot } from "../lib/cursorPager";

export interface CursorPagerProps {
  state: CursorPagerState;
  loading: boolean;
  onLoadMore: () => void;
}

/**
 * 「加载更多」游标分页控件:沿 next_cursor 前进;
 * boundary=snapshot_end 时显式说明已到快照末尾,而不是隐藏按钮装作没有分页。
 * 不透明游标只存在于内存 state,永不进入 URL。
 */
export function CursorPager({ state, loading, onLoadMore }: CursorPagerProps) {
  const more = hasMoreInSnapshot(state);
  return (
    <div className="flex flex-wrap items-center gap-3">
      <button
        type="button"
        disabled={!more || loading}
        onClick={onLoadMore}
        className="min-h-10 rounded-md border border-border px-4 py-2 text-sm transition-colors enabled:hover:bg-muted disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
      >
        {loading ? "正在加载…" : "加载更多"}
      </button>
      <span className="text-xs text-muted-foreground">
        {state.boundary === null
          ? "尚未读取通知页"
          : more
            ? "同一快照内还有更多通知页"
            : `已到边界:${humanizeToken(state.boundary)}`}
      </span>
      {state.noticesTruncated && (
        <span className="text-xs text-safe-warning">
          窗口化:更早的通知已被有界淘汰
        </span>
      )}
    </div>
  );
}
