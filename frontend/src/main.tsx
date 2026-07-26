import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import {
  MutationCache,
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { RouterProvider } from "react-router-dom";
import { router } from "./app/router";
import { handleUnauthorizedError } from "./lib/useOperationEvents";
import "./styles/index.css";

// 401 全局拦截:任何请求/流收到 401 → 清缓存 + 会话代际++(旧回调丢弃)。
const queryClient: QueryClient = new QueryClient({
  queryCache: new QueryCache({
    onError: (error) => handleUnauthorizedError(error, queryClient),
  }),
  mutationCache: new MutationCache({
    onError: (error) => handleUnauthorizedError(error, queryClient),
  }),
  defaultOptions: {
    queries: {
      // 数据是 journal 投影,不是外部行情;失败时明确显示状态而非无限重试。
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 5_000,
    },
  },
});

const container = document.getElementById("root");
if (container === null) {
  throw new Error("missing #root element");
}

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
