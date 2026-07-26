/**
 * 数据格式化:时间戳、标识符。全部落在等宽 tabular(.numeric)语境下使用。
 */
const TIME_FORMAT = new Intl.DateTimeFormat("zh-CN", {
  hour12: false,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

/** ISO 时间戳 → 本地可读时间;缺失/无效值显式呈现为 "--"。 */
export function formatDateTime(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  return TIME_FORMAT.format(date);
}

/** 长标识符截断展示(完整值经由 title 或详情面板提供)。 */
export function shortId(value: string): string {
  return value.length > 8 ? `${value.slice(0, 8)}…` : value;
}

/** 可空计数的一等呈现。 */
export function formatOptionalNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "--" : String(value);
}
