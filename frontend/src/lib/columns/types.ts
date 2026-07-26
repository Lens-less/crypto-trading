import type { ReactNode } from "react";

/**
 * 数据驱动列定义:表格从列数组渲染,为后续列自定义(拖拽排序、
 * 显隐配置)预留接口 —— 届时只需操作 ColumnDef<T>[],不改表格组件。
 */
export interface ColumnDef<T> {
  /** 稳定列 id(未来列自定义的持久化键)。 */
  id: string;
  /** 表头文字。 */
  header: string;
  /** 数字列:右对齐 + 等宽 tabular。 */
  numeric?: boolean;
  /** 单元格渲染(纯投影,不做副作用)。 */
  cell: (row: T) => ReactNode;
}
