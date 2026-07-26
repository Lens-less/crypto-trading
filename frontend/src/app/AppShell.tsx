import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { NavLink, Outlet } from "react-router-dom";
import { request } from "../lib/api";
import {
  capabilityManifestSchema,
  type CapabilityManifest,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { cn } from "../lib/cn";
import { NotificationChannelBadge } from "../components/NotificationChannelBadge";
import {
  applyTheme,
  resolveTheme,
  toggleTheme,
  watchTheme,
  type Theme,
} from "../lib/theme";

const NAV_ITEMS: ReadonlyArray<{ to: string; label: string }> = [
  { to: "/overview", label: "总览" },
  { to: "/scanner", label: "扫描" },
  { to: "/alerts", label: "预警" },
  { to: "/executions", label: "执行" },
  { to: "/integrations", label: "集成" },
  { to: "/strategies", label: "策略" },
  { to: "/risk", label: "风险" },
  { to: "/replay", label: "回放" },
  { to: "/settings", label: "设置" },
];

/**
 * 权限脊柱第一区:3 秒内可辨 PAPER / LIVE CLOSED。
 * 权限态只从 /api/v1/capabilities 推导,浏览器永不构造 live 权限。
 */
function AuthoritySection() {
  const capabilities = useQuery({
    queryKey: queryKeys.capabilities,
    queryFn: ({ signal }) =>
      request<CapabilityManifest>("/api/v1/capabilities", {
        schema: capabilityManifestSchema,
        signal,
      }),
    staleTime: 60_000,
    retry: false,
  });

  if (capabilities.isPending) {
    return (
      <div className="border-b border-border px-5 py-6">
        <p className="text-xs text-safe-neutral">权限态:正在加载</p>
      </div>
    );
  }
  if (capabilities.isError) {
    return (
      <div className="border-b border-border px-5 py-6">
        <p className="text-xs text-safe-warning">权限态:暂不可用</p>
        <p className="mt-1 text-xs text-muted-foreground">
          无法读取 /api/v1/capabilities
        </p>
      </div>
    );
  }

  const manifest = capabilities.data;
  const paperAvailable =
    manifest.release_stage === "paper-only" ||
    manifest.capabilities.some(
      (capability) =>
        capability.scope.access === "paper-trading" &&
        capability.level !== "unavailable",
    );
  return (
    <div className="border-b border-border px-5 py-6" data-testid="authority">
      {paperAvailable ? (
        <p className="numeric text-3xl font-semibold tracking-widest text-foreground">
          PAPER
        </p>
      ) : (
        <p className="text-sm text-safe-warning">Paper 权限:暂不可用</p>
      )}
      {manifest.live_trading_enabled ? (
        <p className="mt-2 text-xs text-safe-warning">
          live 授权由后端声明,本界面仍为只读
        </p>
      ) : (
        <p className="numeric mt-2 text-sm tracking-widest text-safe-neutral">
          LIVE CLOSED
        </p>
      )}
    </div>
  );
}

function ThemeToggle() {
  const [theme, setThemeState] = useState<Theme>(() => resolveTheme());

  // 仅挂载时同步一次并订阅;后续变化由 watchTheme 回调驱动。
  useEffect(() => {
    applyTheme(resolveTheme());
    return watchTheme(setThemeState);
  }, []);

  return (
    <button
      type="button"
      onClick={() => setThemeState(toggleTheme())}
      className="w-full rounded-md border border-border px-3 py-2 text-left text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
    >
      主题:{theme === "dark" ? "深色" : "浅色"}(点击切换)
    </button>
  );
}

/** 左侧固定权限脊柱 + 内容区。 */
export function AppShell() {
  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <aside className="flex w-56 shrink-0 flex-col border-r border-border bg-card">
        <AuthoritySection />
        <nav aria-label="主导航" className="flex-1 overflow-y-auto py-2">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                cn(
                  "block border-l-2 px-5 py-2.5 text-sm transition-colors",
                  isActive
                    ? "border-primary bg-muted text-foreground"
                    : "border-transparent text-muted-foreground hover:bg-muted hover:text-foreground",
                )
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <NotificationChannelBadge />
        <div className="border-t border-border p-3">
          <ThemeToggle />
        </div>
      </aside>
      <main className="min-w-0 flex-1 overflow-x-hidden p-6 lg:p-8">
        <Outlet />
      </main>
    </div>
  );
}
