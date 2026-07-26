/**
 * 降级 / 窗口化 / 未决横幅的出现条件(纯函数,可测)。
 * 横幅是一等状态:窗口化与降级不藏在 tooltip 后面,
 * 固定显示在受影响区域上方,状态 = 文字 + 安全色。
 */
import type {
  ArbitrageMonitorReadModel,
  OperatorReadModel,
  PriceAlertReadModel,
  ReadOnlyTaskReadModel,
} from "./api-types";
import {
  countPendingAlertDeliveries,
  isTrustedAlertProjection,
  visibleAlertOccurrences,
} from "./alerts";
import { MAX_ALERT_OCCURRENCES } from "./api-types";

export interface BannerDescriptor {
  key: string;
  tone: "warning" | "danger" | "neutral";
  title: string;
  tag: string;
  message: string;
}

/** 执行账本(operator 投影)横幅:窗口化 / 降级 / 截断。 */
export function executionBanners(
  operator: OperatorReadModel | null | undefined,
): BannerDescriptor[] {
  if (!operator) {
    return [];
  }
  const banners: BannerDescriptor[] = [];
  if (operator.projection_status === "windowed") {
    banners.push({
      key: "execution-windowed",
      tone: "warning",
      title: "窗口化投影",
      tag: "窗口化",
      message:
        "有界读取模型保留了未解决批次,并可能淘汰较早的已完成批次,以维持最近运行窗口。",
    });
  }
  if (operator.projection_status === "degraded") {
    banners.push({
      key: "execution-degraded",
      tone: "danger",
      title: "降级投影",
      tag: "降级",
      message:
        "读取模型只接受安全的部分事实;无效或不完整的持久记录不会被提升为健康状态。",
    });
  }
  if (operator.batches_truncated) {
    banners.push({
      key: "execution-batches-truncated",
      tone: "warning",
      title: "批次列表已截断",
      tag: "部分投影",
      message:
        "更早的已完结批次已被有界淘汰;本表只是最近运行窗口,不是完整历史账本。",
    });
  }
  if (operator.warnings_truncated) {
    banners.push({
      key: "execution-warnings-truncated",
      tone: "warning",
      title: "警告列表已截断",
      tag: "部分投影",
      message: "投影警告数量超出有界上限,更早的警告已被淘汰。",
    });
  }
  return banners;
}

/** 预警投影横幅:窗口化(可信) / 降级(停止展示) / 刷新失败 / 未决投递。 */
export function alertBanners(
  model: PriceAlertReadModel | null | undefined,
  options: { refreshFailed?: boolean } = {},
): BannerDescriptor[] {
  if (!model) {
    return [];
  }
  const banners: BannerDescriptor[] = [];
  const trusted = isTrustedAlertProjection(model);
  if (model.projection_status === "windowed" && trusted) {
    banners.push({
      key: "alert-windowed",
      tone: "warning",
      title: "预警投影已窗口化",
      tag: "可信 / 已截断",
      message: `预警事实仍通过完整性校验;当前只展示最近 ${
        visibleAlertOccurrences(model).length
      } 条 occurrence(read model 窗口上限 ${MAX_ALERT_OCCURRENCES} 条),更早记录已被有界淘汰。`,
    });
  } else if (!trusted) {
    banners.push({
      key: "alert-degraded",
      tone: "danger",
      title: "预警投影已降级",
      tag: "停止展示",
      message:
        "无效预警事件或不完整尾记录修复前,所有 occurrence 与最近预警都停止展示;界面不会把不可信结果提升成可操作事实。",
    });
  }
  if (options.refreshFailed === true && trusted) {
    banners.push({
      key: "alert-refresh-failed",
      tone: "warning",
      title: "预警快照刷新失败",
      tag: "保留旧快照",
      message:
        "最后一个通过完整性校验的预警快照仍然可见;重新读取成功前,不把它解释为最新状态。",
    });
  }
  const pendingDeliveries = countPendingAlertDeliveries(model);
  if (pendingDeliveries > 0) {
    banners.push({
      key: "alert-pending-deliveries",
      tone: "warning",
      title: "存在未决通知记录",
      tag: "历史事实 / 不保证重放",
      message: `冻结 journal 中有 ${pendingDeliveries} 条 adapter 记录最后停在 pending。它可能源于进程中断或终态持久化失败;恢复默认不重放,本页不会把它解释为仍在排队。`,
    });
  }
  return banners;
}

/** 监控投影横幅:非 complete 一律停止展示最后结果。 */
export function monitorBanner(
  model: ArbitrageMonitorReadModel | null | undefined,
): BannerDescriptor | null {
  if (!model || model.projection_status === "complete") {
    return null;
  }
  return {
    key: "monitor-degraded",
    tone: "danger",
    title: "监控投影已降级",
    tag: "停止展示",
    message:
      "最后一个有效监控结果已停止展示;无效事件或不完整尾记录修复前,不把旧机会提升为可信状态。",
  };
}

/** 任务投影横幅:降级 + 存活性未验证。 */
export function taskBanners(
  model: ReadOnlyTaskReadModel | null | undefined,
): BannerDescriptor[] {
  if (!model) {
    return [];
  }
  const banners: BannerDescriptor[] = [];
  if (model.projection_status !== "complete") {
    banners.push({
      key: "task-degraded",
      tone: "danger",
      title: "任务投影已降级",
      tag: "最后有效事实",
      message:
        "无效任务生命周期事实不会覆盖最后一个通过校验的状态;修复 journal 前,所有任务 liveness 都需要人工核对。",
    });
  }
  const investigateCount = model.tasks.filter(
    (task) => task.recovery === "investigate",
  ).length;
  if (investigateCount > 0) {
    banners.push({
      key: "task-liveness",
      tone: "warning",
      title: "任务存活性未验证",
      tag: "历史事实 / 不自动重放",
      message: `${investigateCount} 个任务的最后持久阶段尚未形成可安全闭合的正常终态。页面不会把 running 解释为当前进程存活,也不会在重启后自动重连数据源。`,
    });
  }
  return banners;
}
