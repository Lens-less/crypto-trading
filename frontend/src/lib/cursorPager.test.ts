import { describe, expect, it } from "vitest";
import type { ControlPlaneEventNotice, ControlPlaneEventsPage } from "./api-types";
import {
  cursorPagerReducer,
  hasMoreInSnapshot,
  initialCursorPagerState,
  MAX_RETAINED_NOTICES,
} from "./cursorPager";

function notice(sequence: number): ControlPlaneEventNotice {
  return {
    sequence,
    event_id: `event-${sequence}`,
    recorded_at: "2026-07-26T00:00:00Z",
    kind: "execution_planned",
    aggregate_kind: "execution_batch",
    aggregate_id: "00000000-0000-0000-0000-000000000007",
    producer: "legacy_jsonl",
  };
}

function page(
  sequences: number[],
  options: Partial<Pick<ControlPlaneEventsPage, "journal_id" | "next_cursor">> & {
    boundary?: ControlPlaneEventsPage["boundary"]["kind"];
  } = {},
): ControlPlaneEventsPage {
  return {
    schema_version: 1,
    journal_id: options.journal_id ?? "journal-a",
    events: sequences.map(notice),
    next_cursor: options.next_cursor ?? "cursor-1",
    boundary: { kind: options.boundary ?? "snapshot_end" },
  };
}

describe("cursorPagerReducer", () => {
  it("应用首页:保留通知、游标与边界", () => {
    const state = cursorPagerReducer(initialCursorPagerState, {
      type: "apply_page",
      page: page([1, 2, 3], { boundary: "page_limit", next_cursor: "c1" }),
    });
    expect(state.notices.map((entry) => entry.sequence)).toEqual([1, 2, 3]);
    expect(state.nextCursor).toBe("c1");
    expect(hasMoreInSnapshot(state)).toBe(true);
  });

  it("追加下一页:按 sequence 去重并升序合并", () => {
    let state = cursorPagerReducer(initialCursorPagerState, {
      type: "apply_page",
      page: page([1, 2, 3], { boundary: "page_limit", next_cursor: "c1" }),
    });
    state = cursorPagerReducer(state, {
      type: "apply_page",
      page: page([3, 4, 5], { boundary: "snapshot_end", next_cursor: "c2" }),
    });
    expect(state.notices.map((entry) => entry.sequence)).toEqual([1, 2, 3, 4, 5]);
    expect(state.nextCursor).toBe("c2");
    expect(hasMoreInSnapshot(state)).toBe(false);
  });

  it("journal_id 变化(日志代次更替):旧通知全部作废,从新页重建", () => {
    let state = cursorPagerReducer(initialCursorPagerState, {
      type: "apply_page",
      page: page([1, 2, 3]),
    });
    state = cursorPagerReducer(state, {
      type: "apply_page",
      page: page([10, 11], { journal_id: "journal-b", next_cursor: "fresh" }),
    });
    expect(state.journalId).toBe("journal-b");
    expect(state.notices.map((entry) => entry.sequence)).toEqual([10, 11]);
    expect(state.nextCursor).toBe("fresh");
  });

  it("有界保留:超过上限只留最新一段并标记窗口化", () => {
    const sequences = Array.from({ length: MAX_RETAINED_NOTICES + 10 }, (_, i) => i + 1);
    const state = cursorPagerReducer(initialCursorPagerState, {
      type: "apply_page",
      page: page(sequences),
    });
    expect(state.notices).toHaveLength(MAX_RETAINED_NOTICES);
    expect(state.notices.at(0)?.sequence).toBe(11);
    expect(state.notices.at(-1)?.sequence).toBe(MAX_RETAINED_NOTICES + 10);
    expect(state.noticesTruncated).toBe(true);
  });

  it("reset(游标失效协议):回到初始状态,丢弃游标", () => {
    let state = cursorPagerReducer(initialCursorPagerState, {
      type: "apply_page",
      page: page([1, 2]),
    });
    state = cursorPagerReducer(state, { type: "reset" });
    expect(state).toEqual(initialCursorPagerState);
    expect(state.nextCursor).toBeNull();
  });
});
