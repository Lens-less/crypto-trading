import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./AppShell";

/**
 * 9 条路由,除总览外均为懒加载占位骨架。
 * 页面模块导出 `Component`,由 React Router 的 route.lazy 消费。
 */
export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/overview" replace /> },
      { path: "overview", lazy: () => import("../pages/overview") },
      { path: "scanner", lazy: () => import("../pages/scanner") },
      { path: "alerts", lazy: () => import("../pages/alerts") },
      { path: "executions", lazy: () => import("../pages/executions") },
      { path: "integrations", lazy: () => import("../pages/integrations") },
      { path: "strategies", lazy: () => import("../pages/strategies") },
      { path: "risk", lazy: () => import("../pages/risk") },
      { path: "replay", lazy: () => import("../pages/replay") },
      { path: "settings", lazy: () => import("../pages/settings") },
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
