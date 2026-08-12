import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./AppShell";

/**
 * The shell redirects `/` to `/overview` and lazy-loads each page module.
 * Every route module exports `Component` for React Router's `route.lazy`.
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
