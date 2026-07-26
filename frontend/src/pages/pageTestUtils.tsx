/**
 * 页面测试工具:QueryClientProvider 包装 + 按 URL 分发的 fetch stub。
 * 仅供 *.test.tsx 使用,不进入生产 bundle(无生产模块引用它)。
 */
import type { ReactElement } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, type RenderResult } from "@testing-library/react";

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export type RouteMap = Record<string, () => Response>;

/** 按路径前缀分发的 fetch stub;未声明的路径返回 404 错误封套。 */
export function routedFetch(routes: RouteMap): typeof fetch {
  return async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const path = url.split("?")[0] ?? url;
    const handler = routes[path];
    if (handler !== undefined) {
      return handler();
    }
    return jsonResponse(
      { schema_version: 1, error: { code: "not_found", message: "resource not found" } },
      404,
    );
  };
}

export function renderWithQueryClient(element: ReactElement): {
  view: RenderResult;
  queryClient: QueryClient;
} {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, staleTime: Infinity },
    },
  });
  const view = render(
    <QueryClientProvider client={queryClient}>{element}</QueryClientProvider>,
  );
  return { view, queryClient };
}
