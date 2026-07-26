/**
 * 执行页事件通知的游标分页 reducer(纯函数,可测)。
 *
 * 语义:
 * - /api/v1/executions?cursor= 的 changes 是「游标之后」的通知页;
 *   「加载更多」沿 next_cursor 前进,boundary=page_limit 时快照内还有下一页;
 * - 通知按 sequence 去重、升序保留,并有界截断(只留最新一段),
 *   与后端「有界读取」的姿态一致;
 * - journal_id 变化意味着日志代次更替:旧通知全部作废,从新页重建;
 * - 游标只存在于本 reducer 状态(内存),永不进入 URL 或持久化存储。
 */
import type { ControlPlaneEventNotice, ControlPlaneEventsPage } from "./api-types";

/** UI 有界保留的通知条数上限。 */
export const MAX_RETAINED_NOTICES = 512;

export interface CursorPagerState {
  journalId: string | null;
  notices: ControlPlaneEventNotice[];
  /** 下一次「加载更多」使用的不透明游标;仅存内存。 */
  nextCursor: string | null;
  boundary: ControlPlaneEventsPage["boundary"]["kind"] | null;
  /** UI 侧因有界保留而丢弃过更早通知。 */
  noticesTruncated: boolean;
}

export type CursorPagerAction =
  | { type: "apply_page"; page: ControlPlaneEventsPage }
  | { type: "reset" };

export const initialCursorPagerState: CursorPagerState = {
  journalId: null,
  notices: [],
  nextCursor: null,
  boundary: null,
  noticesTruncated: false,
};

/** boundary=page_limit 表示同一快照内还有下一页可立即加载。 */
export function hasMoreInSnapshot(state: CursorPagerState): boolean {
  return state.boundary === "page_limit";
}

export function cursorPagerReducer(
  state: CursorPagerState,
  action: CursorPagerAction,
): CursorPagerState {
  switch (action.type) {
    case "reset":
      return initialCursorPagerState;
    case "apply_page": {
      const page = action.page;
      // 日志代次更替:旧通知与旧游标全部作废。
      const generationChanged =
        state.journalId !== null && state.journalId !== page.journal_id;
      const base = generationChanged ? [] : state.notices;

      const merged = new Map<number, ControlPlaneEventNotice>();
      for (const notice of base) {
        merged.set(notice.sequence, notice);
      }
      for (const notice of page.events) {
        merged.set(notice.sequence, notice);
      }
      let notices = [...merged.values()].sort(
        (left, right) => left.sequence - right.sequence,
      );
      let noticesTruncated = generationChanged ? false : state.noticesTruncated;
      if (notices.length > MAX_RETAINED_NOTICES) {
        notices = notices.slice(notices.length - MAX_RETAINED_NOTICES);
        noticesTruncated = true;
      }
      return {
        journalId: page.journal_id,
        notices,
        nextCursor: page.next_cursor,
        boundary: page.boundary.kind,
        noticesTruncated,
      };
    }
  }
}
