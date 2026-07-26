import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import {
  ApiRequestError,
  asApiError,
  getBearerToken,
  request,
  setBearerToken,
} from "./api";

function jsonResponse(
  body: unknown,
  init: { status?: number; headers?: Record<string, string> } = {},
): Response {
  return new Response(JSON.stringify(body), {
    status: init.status ?? 200,
    headers: { "content-type": "application/json", ...init.headers },
  });
}

function errorEnvelope(code: string, message: string): unknown {
  return { schema_version: 1, error: { code, message } };
}

async function expectApiError(
  promise: Promise<unknown>,
): Promise<ApiRequestError> {
  try {
    await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(ApiRequestError);
    return error as ApiRequestError;
  }
  throw new Error("expected request to reject");
}

afterEach(() => {
  setBearerToken(null);
  vi.restoreAllMocks();
});

describe("request 错误归一", () => {
  it("401 归一为 unauthorized 并保留稳定错误码", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          errorEnvelope(
            "authentication_required",
            "valid bearer authentication is required",
          ),
          { status: 401 },
        ),
      );
    const error = await expectApiError(
      request("/api/v1/system", { fetchImpl }),
    );
    expect(error.kind).toBe("unauthorized");
    expect(error.status).toBe(401);
    expect(error.code).toBe("authentication_required");
  });

  it("429 归一为 rate_limited 并解析 Retry-After", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse(errorEnvelope("rate_limited", "limit reached"), {
        status: 429,
        headers: { "retry-after": "17" },
      }),
    );
    const error = await expectApiError(request("/api/v1/health", { fetchImpl }));
    expect(error.kind).toBe("rate_limited");
    expect(error.retryAfterSeconds).toBe(17);
  });

  it("503 journal_unavailable 归一为 unavailable", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse(
        errorEnvelope(
          "journal_unavailable",
          "the operation journal is temporarily unavailable",
        ),
        { status: 503 },
      ),
    );
    const error = await expectApiError(request("/api/v1/system", { fetchImpl }));
    expect(error.kind).toBe("unavailable");
    expect(error.code).toBe("journal_unavailable");
  });

  it("410 cursor_expired 归一为 cursor_expired", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(errorEnvelope("cursor_expired", "expired"), {
          status: 410,
        }),
      );
    const error = await expectApiError(
      request("/api/v1/executions", { fetchImpl }),
    );
    expect(error.kind).toBe("cursor_expired");
  });

  it("错误体不可解析时仍归一,且不透传底层文本", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response("<html>stack trace...</html>", {
        status: 500,
        headers: { "content-type": "text/html" },
      }),
    );
    const error = await expectApiError(request("/api/v1/system", { fetchImpl }));
    expect(error.kind).toBe("server");
    expect(error.code).toBeNull();
    expect(error.message).not.toContain("stack trace");
  });

  it("网络失败归一为 network,不透传异常细节", async () => {
    const fetchImpl = vi
      .fn()
      .mockRejectedValue(new TypeError("fetch failed: ECONNREFUSED 127.0.0.1"));
    const error = await expectApiError(request("/api/v1/health", { fetchImpl }));
    expect(error.kind).toBe("network");
    expect(error.status).toBeNull();
    expect(error.message).not.toContain("ECONNREFUSED");
  });

  it("非 JSON 成功响应归一为 invalid_body", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response("not-json", {
        status: 200,
        headers: { "content-type": "text/plain" },
      }),
    );
    const error = await expectApiError(request("/api/v1/health", { fetchImpl }));
    expect(error.kind).toBe("invalid_body");
  });

  it("zod 窄校验失败归一为 invalid_body", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(jsonResponse({ schema_version: 99 }));
    const schema = z.looseObject({ schema_version: z.literal(1) });
    const error = await expectApiError(
      request("/api/v1/health", { fetchImpl, schema }),
    );
    expect(error.kind).toBe("invalid_body");
  });
});

describe("request 成功路径与认证头", () => {
  it("返回解析后的 JSON", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(jsonResponse({ schema_version: 1, status: "ready" }));
    const body = await request<{ schema_version: number; status: string }>(
      "/api/v1/health",
      { fetchImpl },
    );
    expect(body.status).toBe("ready");
  });

  it("设置内存 token 后携带 Authorization: Bearer", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(jsonResponse({ schema_version: 1 }));
    setBearerToken("token-0123456789abcdef0123456789abcdef");
    await request("/api/v1/system", { fetchImpl });

    const headers = fetchImpl.mock.calls[0]?.[1]?.headers as Headers;
    expect(headers.get("authorization")).toBe(
      "Bearer token-0123456789abcdef0123456789abcdef",
    );
  });

  it("token 只存内存,不写入 localStorage", () => {
    setBearerToken("token-0123456789abcdef0123456789abcdef");
    expect(getBearerToken()).toBe("token-0123456789abcdef0123456789abcdef");
    expect(window.localStorage.length).toBe(0);
    setBearerToken(null);
    expect(getBearerToken()).toBeNull();
  });
});

describe("asApiError", () => {
  it("识别 ApiRequestError,其他值返回 null", () => {
    const apiError = new ApiRequestError("network", "网络请求失败");
    expect(asApiError(apiError)).toBe(apiError);
    expect(asApiError(new Error("plain"))).toBeNull();
    expect(asApiError("string")).toBeNull();
  });
});
