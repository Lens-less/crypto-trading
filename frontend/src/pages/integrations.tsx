import { useQuery } from "@tanstack/react-query";
import { request } from "../lib/api";
import {
  capabilityManifestSchema,
  settingsResponseSchema,
  type AdapterFacetSupport,
  type AdapterSupport,
  type AdapterSupportLevel,
  type Capability,
  type CapabilityManifest,
  type SettingsResponse,
} from "../lib/api-types";
import { queryKeys } from "../lib/queryKeys";
import { credentialLabel, humanizeToken } from "../lib/labels";
import type { ColumnDef } from "../lib/columns/types";
import { DataCard, FactRow } from "../components/DataCard";
import { DataTable } from "../components/DataTable";
import { EmptyState } from "../components/EmptyState";
import { StatusPill, type StatusPillProps } from "../components/StatusPill";
import { DataAsOf, QueryStateBody } from "../components/QueryStateBody";

type Tone = NonNullable<StatusPillProps["tone"]>;

/** 不可用/不适用保持中性且明确(不是危险,也绝不是健康)。 */
export function toneForAdapterLevel(level: AdapterSupportLevel): Tone {
  switch (level) {
    case "implemented":
      return "ok";
    case "protocol-only":
    case "request-only":
    case "config-only":
      return "warning";
    default:
      return "neutral";
  }
}

const ADAPTER_FACETS: ReadonlyArray<{
  id: keyof Pick<
    AdapterSupport,
    "public_data" | "testnet_protocol" | "authenticated" | "reconcile" | "live"
  >;
  label: string;
}> = [
  { id: "public_data", label: "公共数据" },
  { id: "testnet_protocol", label: "测试网协议" },
  { id: "authenticated", label: "鉴权访问" },
  { id: "reconcile", label: "对账" },
  { id: "live", label: "实盘" },
];

function FacetCell({ facet }: { facet: AdapterFacetSupport }) {
  return (
    <span className="block space-y-1">
      <StatusPill
        tone={toneForAdapterLevel(facet.level)}
        label={humanizeToken(facet.level)}
      />
      <span className="numeric block text-xs text-muted-foreground">
        {facet.evidence.length} 证据 / {facet.blockers.length} 阻塞
      </span>
    </span>
  );
}

const adapterColumns: ColumnDef<AdapterSupport>[] = [
  {
    id: "adapter",
    header: "适配器",
    cell: (adapter) => (
      <span className="block">
        <span className="block font-medium">{adapter.name}</span>
        <span className="numeric block text-xs text-muted-foreground">
          {adapter.id}
        </span>
      </span>
    ),
  },
  ...ADAPTER_FACETS.map(
    (facet): ColumnDef<AdapterSupport> => ({
      id: facet.id,
      header: facet.label,
      cell: (adapter) => <FacetCell facet={adapter[facet.id]} />,
    }),
  ),
];

/** 能力账本行:<details> 折叠展开 evidence 文件名与阻塞项。 */
function CapabilityRow({ capability }: { capability: Capability }) {
  return (
    <li className="rounded-md border border-border">
      <details>
        <summary className="flex min-h-10 cursor-pointer flex-wrap items-center gap-2 px-3 py-2 text-sm marker:text-muted-foreground">
          <span className="numeric min-w-0 break-all font-medium">
            {capability.id}
          </span>
          <StatusPill
            tone={
              capability.level === "unavailable"
                ? "neutral"
                : capability.level === "available"
                  ? "ok"
                  : "warning"
            }
            label={humanizeToken(capability.level)}
          />
          <span className="numeric text-xs text-muted-foreground">
            {capability.scope.access}
          </span>
          <span className="numeric text-xs text-muted-foreground">
            {capability.scope.environments.join(", ")}
          </span>
        </summary>
        <div className="space-y-2 border-t border-border px-3 py-2">
          <p className="text-xs">{capability.summary}</p>
          <div>
            <p className="text-xs font-medium text-muted-foreground">
              阻塞项({capability.blockers.length})
            </p>
            {capability.blockers.length === 0 ? (
              <p className="text-xs text-muted-foreground">无</p>
            ) : (
              <ul className="mt-0.5 list-inside list-disc space-y-0.5">
                {capability.blockers.map((blocker) => (
                  <li key={blocker} className="text-xs">
                    {blocker}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <div>
            <p className="text-xs font-medium text-muted-foreground">
              证据文件({capability.evidence.length})
            </p>
            {capability.evidence.length === 0 ? (
              <p className="text-xs text-muted-foreground">无</p>
            ) : (
              <ul className="mt-0.5 space-y-0.5">
                {capability.evidence.map((evidence) => (
                  <li key={evidence} className="numeric break-all text-xs">
                    {evidence}
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </details>
    </li>
  );
}

export function Component() {
  const capabilities = useQuery({
    queryKey: queryKeys.capabilities,
    queryFn: ({ signal }) =>
      request<CapabilityManifest>("/api/v1/capabilities", {
        schema: capabilityManifestSchema,
        signal,
      }),
    refetchInterval: 60_000,
  });
  const settings = useQuery({
    queryKey: queryKeys.settings,
    queryFn: ({ signal }) =>
      request<SettingsResponse>("/api/v1/settings", {
        schema: settingsResponseSchema,
        signal,
      }),
    refetchInterval: 60_000,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">集成</h1>
        <p className="text-sm text-muted-foreground">
          适配器支持矩阵与能力账本(/api/v1/capabilities);展示证据强度,不改变任何运行权限
        </p>
      </header>

      <DataCard
        title="适配器支持矩阵"
        subtitle="每个单元格展示支持强度、证据与阻塞数量;不可用保持中性且明确"
      >
        <QueryStateBody query={capabilities} skeletonRows={6}>
          {(manifest) =>
            manifest.adapters.length === 0 ? (
              <EmptyState
                message="能力清单没有声明任何交易所适配器。"
                checkedFact="已检查 /api/v1/capabilities 的 adapters。"
              />
            ) : (
              <DataTable
                columns={adapterColumns}
                rows={manifest.adapters}
                rowKey={(adapter) => adapter.id}
                ariaLabel="适配器支持矩阵,可横向滚动"
                minWidth="56rem"
              />
            )
          }
        </QueryStateBody>
      </DataCard>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <DataCard
          title="能力账本"
          subtitle="精确能力 ID、访问范围与阻塞项;展开行可见 evidence 文件名列表"
        >
          <QueryStateBody query={capabilities} skeletonRows={8}>
            {(manifest) =>
              manifest.capabilities.length === 0 ? (
                <EmptyState
                  message="能力清单为空。"
                  checkedFact="已检查 /api/v1/capabilities 的 capabilities。"
                />
              ) : (
                <>
                  <ul className="space-y-2">
                    {manifest.capabilities.map((capability) => (
                      <CapabilityRow key={capability.id} capability={capability} />
                    ))}
                  </ul>
                  <DataAsOf updatedAt={capabilities.dataUpdatedAt} />
                </>
              )
            }
          </QueryStateBody>
        </DataCard>

        <DataCard
          title="凭据配置状态"
          subtitle="/api/v1/settings · 只表示配置完整性,绝不显示明文或令牌值"
        >
          <QueryStateBody query={settings} skeletonRows={4}>
            {(data) => (
              <div className="space-y-2">
                <FactRow
                  label="Web Bearer"
                  numeric={false}
                  value={
                    <StatusPill
                      tone={
                        data.credentials.web_bearer === "configured"
                          ? "ok"
                          : "neutral"
                      }
                      label={credentialLabel(data.credentials.web_bearer)}
                    />
                  }
                />
                <FactRow
                  label="Binance Testnet"
                  numeric={false}
                  value={
                    <StatusPill
                      tone={
                        data.credentials.binance_testnet === "configured"
                          ? "ok"
                          : data.credentials.binance_testnet === "partial"
                            ? "warning"
                            : "neutral"
                      }
                      label={credentialLabel(data.credentials.binance_testnet)}
                    />
                  }
                />
                <FactRow
                  label="Mainnet"
                  numeric={false}
                  value={
                    <StatusPill
                      tone="neutral"
                      label={credentialLabel(data.credentials.mainnet)}
                    />
                  }
                />
                <p className="text-xs text-muted-foreground">
                  状态只表示配置完整性,不返回值;mainnet 凭据在 paper-only
                  阶段不被接受。
                </p>
                <DataAsOf updatedAt={settings.dataUpdatedAt} />
              </div>
            )}
          </QueryStateBody>
        </DataCard>
      </div>
    </section>
  );
}
