import { describe, expect, it } from "vitest";
import type {
  ArbitrageMonitorReadModel,
  OperatorReadModel,
  ReadOnlyTaskReadModel,
  ReadOnlyTaskView,
} from "./api-types";
import { executionBanners, monitorBanner, taskBanners } from "./banners";

function operator(overrides: Partial<OperatorReadModel> = {}): OperatorReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    head_sequence: 10,
    head_event_id: "event-10",
    projection_status: "complete",
    batches: [],
    batches_truncated: false,
    warnings: [],
    warnings_truncated: false,
    ...overrides,
  };
}

describe("executionBanners(降级横幅出现条件)", () => {
  it("complete 且未截断:没有横幅", () => {
    expect(executionBanners(operator())).toEqual([]);
  });

  it("windowed:出现窗口化横幅(warning)", () => {
    const banners = executionBanners(operator({ projection_status: "windowed" }));
    expect(banners.map((banner) => banner.key)).toContain("execution-windowed");
    expect(banners.find((banner) => banner.key === "execution-windowed")?.tone).toBe(
      "warning",
    );
  });

  it("degraded:出现降级横幅(danger)", () => {
    const banners = executionBanners(operator({ projection_status: "degraded" }));
    const degraded = banners.find((banner) => banner.key === "execution-degraded");
    expect(degraded?.tone).toBe("danger");
    expect(degraded?.title).toBe("降级投影");
  });

  it("批次 / 警告截断:出现部分投影横幅", () => {
    const banners = executionBanners(
      operator({ batches_truncated: true, warnings_truncated: true }),
    );
    expect(banners.map((banner) => banner.key)).toEqual([
      "execution-batches-truncated",
      "execution-warnings-truncated",
    ]);
  });

  it("没有数据时不产生横幅(不伪装状态)", () => {
    expect(executionBanners(undefined)).toEqual([]);
  });
});

describe("monitorBanner", () => {
  const monitor = (
    status: ArbitrageMonitorReadModel["projection_status"],
  ): ArbitrageMonitorReadModel => ({
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 10,
    projection_status: status,
    latest: null,
    invalid_event_count: 0,
  });

  it("complete:无横幅;非 complete:停止展示横幅", () => {
    expect(monitorBanner(monitor("complete"))).toBeNull();
    const banner = monitorBanner(monitor("degraded"));
    expect(banner?.tone).toBe("danger");
    expect(banner?.tag).toBe("停止展示");
  });
});

describe("taskBanners", () => {
  const task = (recovery: ReadOnlyTaskView["recovery"]): ReadOnlyTaskView => ({
    task_id: "task-1",
    kind: "arbitrage_monitor",
    first_sequence: 1,
    last_sequence: 5,
    registered_at: "2026-07-26T00:00:00Z",
    updated_at: "2026-07-26T00:00:00Z",
    phase: "running",
    recovery,
    processed_event_count: 4,
    sources: [],
    exit: null,
    failure: null,
  });
  const tasks = (
    status: ReadOnlyTaskReadModel["projection_status"],
    entries: ReadOnlyTaskView[],
  ): ReadOnlyTaskReadModel => ({
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 10,
    projection_status: status,
    tasks: entries,
    invalid_event_count: 0,
  });

  it("investigate 任务触发「任务存活性未验证」横幅", () => {
    const banners = taskBanners(tasks("complete", [task("investigate")]));
    expect(banners.map((banner) => banner.key)).toEqual(["task-liveness"]);
    expect(banners.at(0)?.message).toContain("不会把 running 解释为当前进程存活");
  });

  it("降级投影触发 danger 横幅", () => {
    const banners = taskBanners(tasks("degraded", [task("none")]));
    expect(banners.at(0)?.key).toBe("task-degraded");
    expect(banners.at(0)?.tone).toBe("danger");
  });
});
