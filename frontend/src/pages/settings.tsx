import { useEffect, useState, type FormEvent } from "react";
import {
  useQuery,
  useQueryClient,
  type UseQueryResult,
} from "@tanstack/react-query";
import { getBearerToken, request } from "../lib/api";
import {
  settingsResponseSchema,
  systemResponseSchema,
  type SettingsResponse,
  type SystemResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { credentialLabel, humanizeToken } from "../lib/labels";
import {
  changeBearerToken,
  subscribeSessionChanges,
} from "../lib/useOperationEvents";
import {
  applyTheme,
  resolveTheme,
  toggleTheme,
  watchTheme,
  type Theme,
} from "../lib/theme";
import { DataCard, FactRow } from "../components/DataCard";
import { EmptyState } from "../components/EmptyState";
import { StatusPill } from "../components/StatusPill";
import { DataAsOf, QueryStateBody } from "../components/QueryStateBody";

/**
 * Bearer 令牌输入:值只进入内存(lib/api.ts 模块变量),
 * 应用后立即清空输入框,永不回显、永不写入任何持久化存储。
 * 应用即推进会话代际:清缓存并重建通知流(changeBearerToken)。
 */
function BearerTokenCard() {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState("");
  const [bound, setBound] = useState(() => getBearerToken() !== null);

  useEffect(
    () =>
      subscribeSessionChanges(() => {
        setBound(getBearerToken() !== null);
      }),
    [],
  );

  const apply = (event: FormEvent): void => {
    event.preventDefault();
    changeBearerToken(queryClient, draft === "" ? null : draft);
    setDraft("");
    setBound(getBearerToken() !== null);
  };

  return (
    <DataCard
      title="Bearer 令牌"
      subtitle="令牌只保存在页面内存,不写入 DOM 或任何浏览器持久化存储"
    >
      <div className="flex flex-wrap items-center gap-2">
        <StatusPill
          tone={bound ? "ok" : "neutral"}
          label={bound ? "Bearer 绑定:仅页内存" : "未绑定"}
        />
      </div>
      <form onSubmit={apply} className="space-y-2">
        <label className="block">
          <span className="text-xs text-muted-foreground">
            bearer token(32–512 字节)
          </span>
          <input
            type="password"
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            autoComplete="off"
            spellCheck={false}
            className="numeric mt-1 min-h-10 w-full rounded-md border border-border bg-card px-3 py-2 text-sm"
            aria-label="bearer token"
          />
        </label>
        <div className="flex flex-wrap gap-2">
          <button
            type="submit"
            className="min-h-10 rounded-md border border-border bg-primary/10 px-3 py-1.5 text-sm text-primary transition-colors hover:bg-primary/20 focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
          >
            应用令牌并重建流
          </button>
          <button
            type="button"
            onClick={() => {
              changeBearerToken(queryClient, null);
              setDraft("");
              setBound(false);
            }}
            className="min-h-10 rounded-md border border-border px-3 py-1.5 text-sm transition-colors hover:bg-muted focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
          >
            清除令牌
          </button>
        </div>
        <p className="text-xs text-muted-foreground">
          应用后会清空受保护缓存、推进会话代际并以新凭据重建通知流;输入框随即清空,值不再可见。
        </p>
      </form>
    </DataCard>
  );
}

function ThemeCard() {
  const [theme, setThemeState] = useState<Theme>(() => resolveTheme());
  useEffect(() => {
    applyTheme(resolveTheme());
    return watchTheme(setThemeState);
  }, []);
  return (
    <DataCard title="主题" subtitle="唯一允许的持久化键:ct-theme(仅外观偏好)">
      <button
        type="button"
        onClick={() => setThemeState(toggleTheme())}
        className="min-h-10 rounded-md border border-border px-3 py-1.5 text-sm transition-colors hover:bg-muted focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
      >
        当前:{theme === "dark" ? "深色" : "浅色"}(点击切换)
      </button>
    </DataCard>
  );
}

function RuntimeSettingsCard({
  settings,
}: {
  settings: UseQueryResult<SettingsResponse>;
}) {
  return (
    <DataCard
      title="运行设置投影"
      subtitle="/api/v1/settings · 路径、日志去向、限流与凭据配置状态;不包含秘钥或令牌内容"
    >
      <QueryStateBody query={settings} skeletonRows={8}>
        {(data) => (
          <div className="space-y-2">
            <FactRow label="数据目录" value={data.data_directory ?? "未配置"} />
            <FactRow label="journal 路径" value={data.journal_path ?? "未配置"} />
            <FactRow label="日志输出" value={humanizeToken(data.log_sink)} />
            <FactRow
              label="通知证据"
              value={humanizeToken(data.notification_evidence)}
            />
            <FactRow
              label="Web Bearer / Testnet / Mainnet"
              numeric={false}
              value={
                <span className="text-xs">
                  {credentialLabel(data.credentials.web_bearer)} /{" "}
                  {credentialLabel(data.credentials.binance_testnet)} /{" "}
                  {credentialLabel(data.credentials.mainnet)}
                </span>
              }
            />
            <FactRow
              label="Paper principal"
              value={data.paper_principal_id ?? "未启用写入"}
            />
            <FactRow
              label="Paper profiles"
              value={String(data.paper_profiles.length)}
            />
            <FactRow label="schema_version" value={String(data.schema_version)} />
            <DataAsOf updatedAt={settings.dataUpdatedAt} />
          </div>
        )}
      </QueryStateBody>
    </DataCard>
  );
}

function RequestLimitCard({
  settings,
}: {
  settings: UseQueryResult<SettingsResponse>;
}) {
  return (
    <DataCard
      title="请求限流"
      subtitle="本地 API 的有界请求预算;达到上限后返回 429 并携带 Retry-After"
    >
      <QueryStateBody query={settings} skeletonRows={3}>
        {(data) => (
          <div className="space-y-2">
            <FactRow
              label="请求上限"
              value={`${data.request_limit.maximum_requests} 次 / ${data.request_limit.window_seconds}s`}
            />
            <p className="text-xs text-muted-foreground">
              限流窗口由后端强制;界面收到 429 时按 Retry-After
              退避重试,不会绕过预算。就绪探针也受同一预算约束。
            </p>
          </div>
        )}
      </QueryStateBody>
    </DataCard>
  );
}

export function Component() {
  const settings = useQuery({
    queryKey: queryKeys.settings,
    queryFn: ({ signal }) =>
      request<SettingsResponse>("/api/v1/settings", {
        schema: settingsResponseSchema,
        signal,
      }),
    refetchInterval: 60_000,
  });
  const system = useQuery({
    queryKey: queryKeys.system,
    queryFn: ({ signal }) =>
      request<SystemResponse>("/api/v1/system", {
        schema: systemResponseSchema,
        signal,
      }),
    refetchInterval: 30_000,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">设置</h1>
        <p className="text-sm text-muted-foreground">
          访问与外壳(/api/v1/settings);只展示可安全投影的运行元数据,凭证值始终保持隐藏
        </p>
      </header>

      <div className="grid gap-4 lg:grid-cols-2">
        <DataCard
          title="访问与外壳"
          subtitle="/api/v1/system · 只读边界与认证要求由后端声明"
        >
          <QueryStateBody query={system} skeletonRows={5}>
            {(data) => (
              <div className="space-y-2">
                <FactRow label="产品版本" value={data.product_version} />
                <FactRow label="发布阶段" value={data.release_stage} />
                <FactRow label="访问范围" value={data.access_scope} />
                <FactRow
                  label="需要认证"
                  value={data.authentication_required ? "是" : "否"}
                />
                <FactRow label="令牌持久化" value="仅页内存" />
                <DataAsOf updatedAt={system.dataUpdatedAt} />
              </div>
            )}
          </QueryStateBody>
        </DataCard>

        <BearerTokenCard />
        <RuntimeSettingsCard settings={settings} />
        <div className="space-y-4">
          <RequestLimitCard settings={settings} />
          <ThemeCard />
        </div>
      </div>

      <DataCard
        title="Paper profiles"
        subtitle="受信 settings 声明的回放档案;不会从表单默认值推断后端所有权"
      >
        <QueryStateBody query={settings} skeletonRows={3}>
          {(data) =>
            data.paper_profiles.length === 0 ? (
              <EmptyState
                message="当前运行实例没有配置 Paper profile。"
                checkedFact="已检查 /api/v1/settings 的 paper_profiles。"
              />
            ) : (
              <ul className="space-y-2">
                {data.paper_profiles.map((profile) => (
                  <li
                    key={profile.task_id}
                    className="rounded-md border border-border px-3 py-2"
                  >
                    <div className="flex flex-wrap items-center gap-1.5">
                      <StatusPill tone="neutral" label={humanizeToken(profile.kind)} />
                      <span className="numeric text-xs">{profile.task_id}</span>
                    </div>
                    <p className="numeric mt-1 text-xs text-muted-foreground">
                      {profile.strategy_id} · {profile.strategy_revision}
                    </p>
                    <p className="numeric mt-0.5 break-all text-xs text-muted-foreground">
                      {profile.configuration_files.join(", ")} · {profile.replay_file}
                    </p>
                  </li>
                ))}
              </ul>
            )
          }
        </QueryStateBody>
      </DataCard>
    </section>
  );
}
