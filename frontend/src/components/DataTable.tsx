import { cn } from "../lib/cn";
import type { ColumnDef } from "../lib/columns/types";

export interface DataTableProps<T> {
  columns: ColumnDef<T>[];
  rows: readonly T[];
  rowKey: (row: T) => string;
  /** 横向滚动容器的无障碍名称(如「执行账本,可横向滚动」)。 */
  ariaLabel: string;
  /** 表格最小宽度(如 "56rem"):宽内容在容器内滚动,页面不横向滚动。 */
  minWidth?: string;
  onRowClick?: (row: T) => void;
  selectedRowKey?: string | null;
}

/**
 * 数据驱动表格:列来自 ColumnDef<T>[](lib/columns/),
 * 数字列右对齐 + 等宽 tabular;宽表放入自身 overflow-x-auto 容器。
 */
export function DataTable<T>({
  columns,
  rows,
  rowKey,
  ariaLabel,
  minWidth,
  onRowClick,
  selectedRowKey,
}: DataTableProps<T>) {
  return (
    <div
      role="region"
      tabIndex={0}
      aria-label={ariaLabel}
      className="overflow-x-auto rounded-md border border-border focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
    >
      <table
        className="w-full border-collapse text-sm"
        style={minWidth !== undefined ? { minWidth } : undefined}
      >
        <thead>
          <tr className="border-b border-border bg-muted/40">
            {columns.map((column) => (
              <th
                key={column.id}
                scope="col"
                className={cn(
                  "whitespace-nowrap px-3 py-2 text-xs font-medium text-muted-foreground",
                  column.numeric === true ? "text-right" : "text-left",
                )}
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const key = rowKey(row);
            const selected = selectedRowKey === key;
            return (
              <tr
                key={key}
                data-selected={selected ? "true" : "false"}
                onClick={onRowClick === undefined ? undefined : () => onRowClick(row)}
                className={cn(
                  "border-b border-border last:border-b-0",
                  onRowClick !== undefined && "cursor-pointer transition-colors hover:bg-muted/50",
                  selected && "bg-muted/60",
                )}
              >
                {columns.map((column) => (
                  <td
                    key={column.id}
                    className={cn(
                      "px-3 py-2 align-top",
                      column.numeric === true && "text-right",
                    )}
                  >
                    {column.cell(row)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
