import type { ControlPlaneEventNotice } from "../api-types";
import { formatDateTime } from "../format";
import type { ColumnDef } from "./types";

/** 事件通知列定义(payload-free:只有元数据,没有业务载荷)。 */
export const noticeColumns: ColumnDef<ControlPlaneEventNotice>[] = [
  {
    id: "sequence",
    header: "序号",
    numeric: true,
    cell: (notice) => <span className="numeric">{notice.sequence}</span>,
  },
  {
    id: "kind",
    header: "类型",
    cell: (notice) => <span className="numeric text-xs">{notice.kind}</span>,
  },
  {
    id: "aggregate",
    header: "聚合对象",
    cell: (notice) => (
      <span className="block">
        <span className="numeric block text-xs">{notice.aggregate_kind}</span>
        <span
          className="numeric block text-xs text-muted-foreground"
          title={notice.aggregate_id}
        >
          {notice.aggregate_id.slice(0, 8)}…
        </span>
      </span>
    ),
  },
  {
    id: "recorded_at",
    header: "记录时间",
    numeric: true,
    cell: (notice) => (
      <span className="numeric">{formatDateTime(notice.recorded_at)}</span>
    ),
  },
];
