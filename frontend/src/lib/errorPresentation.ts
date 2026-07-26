/**
 * 归一化 API 错误 → 脱敏、可辨别的一等状态文案。
 * 只依赖 ApiRequestError 的 kind/code 与后端稳定文案,
 * 永不透传原始异常文本、路径或 payload。
 */
import { asApiError } from "./api";

export interface ErrorPresentation {
  label: string;
  tone: "warning" | "danger" | "neutral";
  detail: string;
  /** 游标类错误:恢复动作是清除游标而不是原样重试。 */
  cursorInvalidated: boolean;
}

export function errorPresentation(error: unknown): ErrorPresentation {
  const apiError = asApiError(error);
  const code = apiError?.code ?? null;
  if (code === "invalid_cursor") {
    return {
      label: "游标无效",
      tone: "warning",
      detail: "当前游标不适用于这个日志;清除游标后重新获取有界快照",
      cursorInvalidated: true,
    };
  }
  switch (apiError?.kind) {
    case "cursor_expired":
      return {
        label: "游标已过期",
        tone: "warning",
        detail: "游标已不再匹配当前日志代次;清除游标并从当前持久日志头恢复",
        cursorInvalidated: true,
      };
    case "unauthorized":
      return {
        label: "需要令牌",
        tone: "warning",
        detail: "后端要求 bearer 认证(HTTP 401),令牌只保存在内存中",
        cursorInvalidated: false,
      };
    case "unavailable":
      return {
        label: "暂不可用",
        tone: "warning",
        detail:
          code === "read_limit_exceeded"
            ? "有界读取模型达到资源限制,当前无法安全表达该来源"
            : "operation journal 暂时不可读(HTTP 503),保留现有只读视图",
        cursorInvalidated: false,
      };
    case "rate_limited":
      return {
        label: "限流",
        tone: "warning",
        detail: "已达到本地 API 请求上限,稍后自动重试",
        cursorInvalidated: false,
      };
    case "network":
      return {
        label: "无法连接",
        tone: "danger",
        detail: "无法连接本机后端,后端可能未运行",
        cursorInvalidated: false,
      };
    case "invalid_body":
      return {
        label: "响应异常",
        tone: "danger",
        detail: "响应不符合已知 schema_version",
        cursorInvalidated: false,
      };
    case "server":
      return {
        label: "错误",
        tone: "danger",
        detail:
          code === "journal_invalid"
            ? "持久日志未通过完整性校验;修复日志来源前,所有可见事实都应视为可疑"
            : "请求无法安全完成;原始适配器或日志文本被刻意隐藏",
        cursorInvalidated: false,
      };
    default:
      return {
        label: "错误",
        tone: "danger",
        detail: code ?? "请求无法安全完成;原始适配器或日志文本被刻意隐藏",
        cursorInvalidated: false,
      };
  }
}
