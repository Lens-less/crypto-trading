import type { ReactNode } from "react";
import type { UseQueryResult } from "@tanstack/react-query";
import { errorPresentation } from "../lib/errorPresentation";
import { formatDateTime } from "../lib/format";
import { FactRow } from "./DataCard";
import { DegradedBanner } from "./DegradedBanner";
import { SkeletonRows } from "./EmptyState";
import { StatusPill } from "./StatusPill";

/**
 * 卡片体的一等状态外壳:loading(骨架)/ error(脱敏文案)在这里收敛,
 * 刷新失败但留有旧快照 → 「保留旧快照」横幅;成功后再渲染事实。
 */
export function QueryStateBody<T>({
  query,
  skeletonRows,
  children,
}: {
  query: UseQueryResult<T>;
  skeletonRows: number;
  children: (data: T) => ReactNode;
}) {
  if (query.isPending) {
    return <SkeletonRows rows={skeletonRows} />;
  }
  if (query.isError && query.data === undefined) {
    const presentation = errorPresentation(query.error);
    return (
      <div className="space-y-2">
        <StatusPill tone={presentation.tone} label={presentation.label} />
        <p className="text-xs text-muted-foreground">{presentation.detail}</p>
      </div>
    );
  }
  if (query.data === undefined) {
    return <SkeletonRows rows={skeletonRows} />;
  }
  return (
    <>
      {query.isError && (
        <DegradedBanner
          banner={{
            key: "stale-snapshot",
            tone: "warning",
            title: "快照刷新失败",
            tag: "保留旧快照",
            message:
              "最后一个通过校验的快照仍然可见;重新读取成功前,不把它解释为最新状态。",
          }}
        />
      )}
      {children(query.data)}
    </>
  );
}

/** 「数据截至」= 本页读取投影的时间,永不代表外部行情时间。 */
export function DataAsOf({ updatedAt }: { updatedAt: number }) {
  return (
    <FactRow
      label="数据截至"
      value={updatedAt === 0 ? "--" : formatDateTime(updatedAt)}
    />
  );
}

/** 一等「投影未提供」状态:后端没有该字段时显式陈述,不造假数据。 */
export function NotProjectedFact({ label }: { label: string }) {
  return (
    <FactRow
      label={label}
      numeric={false}
      value={<span className="text-xs text-safe-neutral">当前投影未提供</span>}
    />
  );
}
