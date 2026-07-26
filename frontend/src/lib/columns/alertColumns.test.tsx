// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import type { AlertOccurrenceView } from "../api-types";
import { ChangePercent, alertColumns } from "./alertColumns";

function occurrence(overrides: Partial<AlertOccurrenceView> = {}): AlertOccurrenceView {
  return {
    source_sequence: 1,
    event_id: "event-1",
    alert_sequence: 1,
    recorded_at: "2026-07-26T00:00:00Z",
    exchange: "binance",
    symbol: "BTCUSDT",
    market_type: "spot",
    kind: "volatility_up",
    price: "50000",
    change_percent: null,
    acknowledged_at: null,
    deliveries: [],
    ...overrides,
  };
}

function renderCell(columnId: string, row: AlertOccurrenceView) {
  const column = alertColumns.find((entry) => entry.id === columnId);
  if (column === undefined) {
    throw new Error(`missing column ${columnId}`);
  }
  return render(<div>{column.cell(row)}</div>);
}

describe("alert severity → 安全色映射(文字 + 颜色同现)", () => {
  it("投递失败:danger 色类与「投递失败」文字同时出现", () => {
    renderCell(
      "kind",
      occurrence({
        deliveries: [
          { adapter_id: "local", status: "failed", failure: "rejected", updated_at: "t" },
        ],
      }),
    );
    const pill = screen.getByText("投递失败");
    expect(pill.className).toContain("text-safe-danger");
  });

  it("待确认:warning 色类与「待确认」文字同时出现", () => {
    renderCell("kind", occurrence());
    const pill = screen.getByText("待确认");
    expect(pill.className).toContain("text-safe-warning");
  });

  it("已确认:ok 色类与「已确认」文字同时出现", () => {
    renderCell("kind", occurrence({ acknowledged_at: "2026-07-26T00:01:00Z" }));
    const pill = screen.getByText("已确认");
    expect(pill.className).toContain("text-safe-ok");
  });

  it("投递状态标签同时携带 adapter 文字与安全色(pending → warning)", () => {
    renderCell(
      "deliveries",
      occurrence({
        deliveries: [
          { adapter_id: "local", status: "pending", failure: null, updated_at: "t" },
        ],
      }),
    );
    const pill = screen.getByText("local: 最后记录:未决");
    expect(pill.className).toContain("text-safe-warning");
  });
});

describe("ChangePercent 方向呈现", () => {
  it("正向:方向绿恒伴随 + 符号(方向色与安全色分离)", () => {
    render(<ChangePercent value="3.2" />);
    const value = screen.getByText("+3.2%");
    expect(value.className).toContain("text-up");
    expect(value.className).not.toContain("safe-");
  });

  it("负向:方向红恒伴随 − 符号", () => {
    render(<ChangePercent value="-1.5" />);
    const value = screen.getByText("−1.5%");
    expect(value.className).toContain("text-down");
  });

  it("缺失值显式呈现为 --", () => {
    render(<ChangePercent value={null} />);
    expect(screen.getByText("--")).toBeTruthy();
  });
});
