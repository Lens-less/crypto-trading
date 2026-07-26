/**
 * 单一 fetch wrapper:所有 REST 请求经由 request<T>() 出入。
 *
 * 职责:
 * - 统一携带 Accept 与(仅存内存的)bearer token;
 * - 识别 401(kind = "unauthorized")并保留 WWW-Authenticate 语义;
 * - 把后端错误封套 `{ schema_version, error: { code, message } }`、
 *   网络错误、非法响应体归一为 ApiRequestError;
 * - 可选 zod 窄校验(schema_version 与关键判别字段)。
 *
 * 不可协商安全语义:
 * - bearer token 只存内存(模块级变量),永不写入任何持久化存储;
 * - 错误信息脱敏:对外只暴露稳定 code 与后端给定 message,
 *   不透传底层异常文本、路径或 payload。
 */
import { apiErrorEnvelopeSchema } from "./api-types";

export type ApiErrorKind =
  | "unauthorized"
  | "rate_limited"
  | "bad_request"
  | "not_found"
  | "cursor_expired"
  | "unavailable"
  | "server"
  | "network"
  | "invalid_body";

export class ApiRequestError extends Error {
  readonly kind: ApiErrorKind;
  /** HTTP 状态码;网络层失败时为 null。 */
  readonly status: number | null;
  /** 后端错误封套中的稳定错误码,如 "authentication_required"。 */
  readonly code: string | null;
  /** 429 时来自 Retry-After 的整秒退避。 */
  readonly retryAfterSeconds: number | null;

  constructor(
    kind: ApiErrorKind,
    message: string,
    options: {
      status?: number | null;
      code?: string | null;
      retryAfterSeconds?: number | null;
    } = {},
  ) {
    super(message);
    this.name = "ApiRequestError";
    this.kind = kind;
    this.status = options.status ?? null;
    this.code = options.code ?? null;
    this.retryAfterSeconds = options.retryAfterSeconds ?? null;
  }
}

/** 判别 helper:React Query 的 error 是 unknown。 */
export function asApiError(error: unknown): ApiRequestError | null {
  return error instanceof ApiRequestError ? error : null;
}

/* --------------------------------------------------- bearer token(仅内存) */

let inMemoryBearerToken: string | null = null;

export function setBearerToken(token: string | null): void {
  inMemoryBearerToken = token !== null && token.length > 0 ? token : null;
}

export function getBearerToken(): string | null {
  return inMemoryBearerToken;
}

/* ------------------------------------------------------------------ request */

interface NarrowSchema {
  safeParse(data: unknown): { success: boolean };
}

export interface RequestOptions {
  /** zod 窄校验;失败归一为 kind = "invalid_body"。 */
  schema?: NarrowSchema;
  signal?: AbortSignal;
  /** 测试注入口;默认 globalThis.fetch。 */
  fetchImpl?: typeof fetch;
}

function kindForStatus(status: number, code: string | null): ApiErrorKind {
  if (status === 401) {
    return "unauthorized";
  }
  if (status === 429) {
    return "rate_limited";
  }
  if (status === 404) {
    return "not_found";
  }
  if (status === 410 || code === "cursor_expired") {
    return "cursor_expired";
  }
  if (status === 503) {
    return "unavailable";
  }
  if (status >= 500) {
    return "server";
  }
  return "bad_request";
}

function parseRetryAfter(headers: Headers): number | null {
  const raw = headers.get("retry-after");
  if (raw === null) {
    return null;
  }
  const seconds = Number.parseInt(raw, 10);
  return Number.isFinite(seconds) && seconds >= 0 ? seconds : null;
}

async function normalizedHttpError(response: Response): Promise<ApiRequestError> {
  let code: string | null = null;
  let message: string | null = null;
  try {
    const body: unknown = await response.json();
    const envelope = apiErrorEnvelopeSchema.safeParse(body);
    if (envelope.success) {
      code = envelope.data.error.code;
      message = envelope.data.error.message;
    }
  } catch {
    // 无法解析的错误体不进入用户可见文案(脱敏)。
  }
  const kind = kindForStatus(response.status, code);
  return new ApiRequestError(kind, message ?? `请求失败(HTTP ${response.status})`, {
    status: response.status,
    code,
    retryAfterSeconds: parseRetryAfter(response.headers),
  });
}

/**
 * 发起一次 API GET 请求并归一所有失败路径。
 *
 * 成功时返回 JSON 响应体(可选经过 zod 窄校验),失败一律抛出
 * ApiRequestError,调用方只依赖 kind/code,不解析自由文本。
 */
export async function request<T>(
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const headers = new Headers({ accept: "application/json" });
  const token = getBearerToken();
  if (token !== null) {
    headers.set("authorization", `Bearer ${token}`);
  }

  let response: Response;
  try {
    response = await fetchImpl(path, {
      method: "GET",
      headers,
      signal: options.signal ?? null,
      credentials: "omit",
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw error;
    }
    // 不透传底层网络异常文本(脱敏)。
    throw new ApiRequestError("network", "网络请求失败,后端可能未运行");
  }

  if (!response.ok) {
    throw await normalizedHttpError(response);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new ApiRequestError("invalid_body", "响应不是有效的 JSON", {
      status: response.status,
    });
  }

  if (options.schema && !options.schema.safeParse(body).success) {
    throw new ApiRequestError("invalid_body", "响应不符合已知 schema", {
      status: response.status,
    });
  }
  return body as T;
}
