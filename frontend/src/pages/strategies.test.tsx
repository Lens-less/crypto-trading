// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { setBearerToken } from "../lib/api";
import { resetSessionStateForTests } from "../lib/useOperationEvents";
import type {
  ReadOnlyTaskReadModel,
  ReadOnlyTaskView,
  SettingsResponse,
  SubmitEnvelope,
  SubmitStatus,
} from "../lib/api-types";
import { jsonResponse, renderWithQueryClient } from "./pageTestUtils";
import { Component as StrategiesPage } from "./strategies";

function task(overrides: Partial<ReadOnlyTaskView> = {}): ReadOnlyTaskView {
  return {
    task_id: "paper-grid-btc",
    kind: "grid_paper",
    first_sequence: 1,
    last_sequence: 6,
    registered_at: "2026-07-25T00:00:00Z",
    updated_at: "2026-07-26T00:00:00Z",
    phase: "running",
    recovery: "none",
    processed_event_count: 6,
    sources: [
      { source_id: "grid-owner", event_sequence: 6, phase: "running", health: "healthy" },
    ],
    exit: null,
    failure: null,
    ...overrides,
  };
}

function tasksModel(
  tasks: ReadOnlyTaskView[],
  projection: ReadOnlyTaskReadModel["projection_status"] = "complete",
): ReadOnlyTaskReadModel {
  return {
    schema_version: 1,
    journal_id: "journal-a",
    journal_head_sequence: 9,
    projection_status: projection,
    tasks,
    invalid_event_count: 0,
  };
}

function settingsModel(paperPrincipalId: string | null): SettingsResponse {
  return {
    schema_version: 1,
    data_directory: "data",
    journal_path: "data/journal.jsonl",
    log_sink: "stdout_stderr",
    notification_evidence: "journal_projection",
    credentials: {
      web_bearer: "configured",
      binance_testnet: "not_configured",
      mainnet: "not_accepted",
    },
    paper_principal_id: paperPrincipalId,
    paper_profiles:
      paperPrincipalId === null
        ? []
        : [
            {
              kind: "grid",
              task_id: "paper-grid-btc",
              strategy_id: "paper-grid",
              strategy_revision: "2026-07-25",
              configuration_files: ["config/grid/paper-once-btc.yaml"],
              replay_file: "fixtures/m4-grid-paper-replay.jsonl",
            },
          ],
    request_limit: { maximum_requests: 240, window_seconds: 60 },
  };
}

interface SubmitStubOptions {
  status: SubmitStatus;
  httpStatus?: number;
}

function stubStrategies(
  tasks: ReadOnlyTaskReadModel,
  settings: SettingsResponse,
  submit?: SubmitStubOptions,
): { submitCalls: SubmitEnvelope[] } {
  const submitCalls: SubmitEnvelope[] = [];
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const path = url.split("?")[0] ?? url;
    if (path === "/api/v1/tasks") {
      return jsonResponse(tasks);
    }
    if (path === "/api/v1/settings") {
      return jsonResponse(settings);
    }
    if (path === "/api/v1/submit") {
      expect(init?.method).toBe("POST");
      const envelope = JSON.parse(String(init?.body)) as SubmitEnvelope;
      submitCalls.push(envelope);
      if (submit === undefined) {
        return jsonResponse(
          { schema_version: 1, error: { code: "not_found", message: "resource not found" } },
          404,
        );
      }
      return jsonResponse(
        {
          schema_version: 1,
          command_id: envelope.command_id,
          target_task_id: envelope.target_task_id,
          status: submit.status,
          journal_projection: "submit_command_v1",
          source: "durable_journal",
        },
        submit.httpStatus ?? (submit.status === "outcome_unknown" ? 202 : 200),
      );
    }
    return jsonResponse(
      { schema_version: 1, error: { code: "not_found", message: "resource not found" } },
      404,
    );
  });
  return { submitCalls };
}

beforeEach(() => {
  resetSessionStateForTests();
  setBearerToken(null);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("/strategies 任务明细", () => {
  it("任务投影明细表:双源健康、事件计数与恢复判断列", async () => {
    stubStrategies(tasksModel([task()]), settingsModel(null));
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(
        screen.getByRole("region", { name: "只读连续任务明细,可横向滚动" }),
      ).toBeTruthy();
    });
    expect(screen.getByText("grid-owner #6")).toBeTruthy();
    expect(screen.getByText("健康")).toBeTruthy();
    expect(screen.getByText(/6 个事件/)).toBeTruthy();
    expect(screen.getByText("持久终态已闭合")).toBeTruthy();
    expect(screen.getByText("最后记录:运行中")).toBeTruthy();
  });

  it("任务投影降级:danger 横幅 + 存活性语义保留", async () => {
    stubStrategies(tasksModel([task()], "degraded"), settingsModel(null));
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toContain("任务投影已降级");
    });
  });
});

describe("/strategies 写能力门控", () => {
  it("settings 未发布 paper_principal_id → 不渲染写控件,一等只读状态", async () => {
    stubStrategies(tasksModel([task()]), settingsModel(null));
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(
        screen.getByText("后端未启用 Paper 写路径,本页保持只读。"),
      ).toBeTruthy();
    });
    expect(screen.queryByRole("button", { name: "启动网格" })).toBeNull();
    expect(screen.queryByRole("button", { name: "停止任务" })).toBeNull();
  });

  it("写能力开启 → 渲染 grid / arbitrage 双表单与固定身份说明", async () => {
    stubStrategies(tasksModel([task()]), settingsModel("local-paper-operator"));
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "启动网格" })).toBeTruthy();
    });
    expect(screen.getByRole("button", { name: "启动套利" })).toBeTruthy();
    expect(screen.getByText("local-paper-operator")).toBeTruthy();
    expect(screen.getByText(/role=paper_operator,risk_confirmation=paper_only/)).toBeTruthy();
  });

  it("tasks 投影不是 complete → 提交按钮禁用并说明读回被降级", async () => {
    stubStrategies(
      tasksModel([task()], "windowed"),
      settingsModel("local-paper-operator"),
    );
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "启动网格" })).toBeTruthy();
    });
    const start = screen.getByRole("button", { name: "启动网格" }) as HTMLButtonElement;
    expect(start.disabled).toBe(true);
    expect(
      screen.getAllByText(/在 \/api\/v1\/tasks 恢复 complete 之前禁止提交/).length,
    ).toBeGreaterThan(0);
  });

  it.each(["stopped", "failed"] as const)(
    "目标任务 phase=%s → stop/cancel 禁用,但 restart 保留",
    async (phase) => {
      stubStrategies(
        tasksModel([task({ phase })]),
        settingsModel("local-paper-operator"),
      );
      renderWithQueryClient(<StrategiesPage />);
      await waitFor(() => {
        expect(screen.getByRole("button", { name: "启动网格" })).toBeTruthy();
      });
      const start = screen.getByRole("button", {
        name: "启动网格",
      }) as HTMLButtonElement;
      const stop = screen.getAllByRole("button", {
        name: "停止任务",
      })[0] as HTMLButtonElement;
      const cancel = screen.getAllByRole("button", {
        name: "取消任务",
      })[0] as HTMLButtonElement;
      expect(start.disabled).toBe(false);
      expect(stop.disabled).toBe(true);
      expect(cancel.disabled).toBe(true);
      expect(
        screen.getByText(/目标任务已处于持久终态;停止与取消已禁用/),
      ).toBeTruthy();
    },
  );
});

describe("/strategies 提交流程", () => {
  async function renderReady(submit?: SubmitStubOptions) {
    const stub = stubStrategies(
      tasksModel([task()]),
      settingsModel("local-paper-operator"),
      submit,
    );
    renderWithQueryClient(<StrategiesPage />);
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "启动网格" })).toBeTruthy();
    });
    return stub;
  }

  function confirmPaperOnly(index = 0): void {
    const checkbox = screen.getAllByRole("checkbox")[index] as HTMLInputElement;
    fireEvent.click(checkbox);
  }

  it("start:回执 applied 展示 command_id / 幂等键均可见,并给出生效判定", async () => {
    const { submitCalls } = await renderReady({ status: "applied" });
    confirmPaperOnly();
    fireEvent.click(screen.getByRole("button", { name: "启动网格" }));
    await waitFor(() => {
      expect(screen.getByText("已写入")).toBeTruthy();
    });
    expect(submitCalls.length).toBe(1);
    const envelope = submitCalls[0]!;
    expect(envelope.schema_version).toBe(1);
    expect(envelope.permission).toEqual({
      principal_id: "local-paper-operator",
      role: "paper_operator",
    });
    expect(envelope.risk_confirmation).toBe("paper_only");
    expect(envelope.idempotency_key).toBe(
      `paper-start_paper_grid-${envelope.command_id}`,
    );
    // 回执与生效判定(任务指纹未变化 → 未观察到变化)。
    expect(screen.getByText(envelope.command_id)).toBeTruthy();
    expect(screen.getByTestId("effect-assessment").textContent).toContain(
      "未观察到变化",
    );
  });

  it("stop:必须二次确认,确认文案含任务身份;确认后才发送 stop_task", async () => {
    const { submitCalls } = await renderReady({ status: "applied" });
    confirmPaperOnly();
    fireEvent.click(screen.getAllByRole("button", { name: "停止任务" })[0]!);
    // 尚未提交:出现含任务身份的确认对话。
    expect(submitCalls.length).toBe(0);
    const dialog = screen.getByRole("alertdialog", { name: "确认任务干预" });
    expect(dialog.textContent).toContain("paper-grid-btc");
    expect(dialog.textContent).toContain("停止任务");
    fireEvent.click(screen.getByRole("button", { name: "确认停止" }));
    await waitFor(() => {
      expect(submitCalls.length).toBe(1);
    });
    expect(submitCalls[0]!.command).toEqual({ kind: "stop_task" });
  });

  it("二次确认可返回:不发送任何请求", async () => {
    const { submitCalls } = await renderReady({ status: "applied" });
    confirmPaperOnly();
    fireEvent.click(screen.getAllByRole("button", { name: "取消任务" })[0]!);
    fireEvent.click(screen.getByRole("button", { name: "返回" }));
    expect(submitCalls.length).toBe(0);
    expect(screen.queryByRole("alertdialog")).toBeNull();
  });

  it("outcome_unknown:表单锁定、按钮禁用,横幅要求先核对 /api/v1/tasks", async () => {
    await renderReady({ status: "outcome_unknown" });
    confirmPaperOnly();
    fireEvent.click(screen.getByRole("button", { name: "启动网格" }));
    await waitFor(() => {
      expect(screen.getByText("提交结果不明,表单已锁定")).toBeTruthy();
    });
    const start = screen.getByRole("button", { name: "启动网格" }) as HTMLButtonElement;
    expect(start.disabled).toBe(true);
    expect(
      screen.getAllByText(/请先核对 \/api\/v1\/tasks/).length,
    ).toBeGreaterThan(0);
  });

  it("未勾选 paper_only 确认 → paper_confirmation_required,不发请求", async () => {
    const { submitCalls } = await renderReady({ status: "applied" });
    fireEvent.click(screen.getByRole("button", { name: "启动网格" }));
    await waitFor(() => {
      expect(screen.getByText("paper_confirmation_required")).toBeTruthy();
    });
    expect(submitCalls.length).toBe(0);
  });

  it("写路径 404(兜底):保留 envelope 并陈述写路径未启用", async () => {
    const { submitCalls } = await renderReady(undefined);
    confirmPaperOnly();
    fireEvent.click(screen.getByRole("button", { name: "启动网格" }));
    await waitFor(() => {
      expect(screen.getByText("submit_route_unavailable")).toBeTruthy();
    });
    expect(submitCalls.length).toBe(1);
    expect(screen.getByText(/写路径未启用,已保留原 envelope/)).toBeTruthy();
    // pending envelope 身份仍可见(幂等重试基础)。
    expect(screen.getByText(/复用相同的 command_id 和/)).toBeTruthy();
  });
});
