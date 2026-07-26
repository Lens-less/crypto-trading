// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { DegradedBanner } from "./DegradedBanner";

describe("DegradedBanner", () => {
  it("danger 横幅用 role=alert,标题、标签与说明同现(状态 = 文字 + 颜色)", () => {
    render(
      <DegradedBanner
        banner={{
          key: "alert-degraded",
          tone: "danger",
          title: "预警投影已降级",
          tag: "停止展示",
          message: "所有 occurrence 与最近预警都停止展示。",
        }}
      />,
    );
    const banner = screen.getByRole("alert");
    expect(banner.textContent).toContain("预警投影已降级");
    expect(banner.textContent).toContain("停止展示");
    expect(banner.textContent).toContain("所有 occurrence 与最近预警都停止展示。");
    // 安全色必须与文字同现:tag pill 携带 safe-danger 色类,且有文字。
    const tag = screen.getByText("停止展示");
    expect(tag.className).toContain("text-safe-danger");
  });

  it("warning 横幅用 role=status,tag 用 safe-warning 色", () => {
    render(
      <DegradedBanner
        banner={{
          key: "alert-windowed",
          tone: "warning",
          title: "预警投影已窗口化",
          tag: "可信 / 已截断",
          message: "更早记录已被有界淘汰。",
        }}
      />,
    );
    const banner = screen.getByRole("status");
    expect(banner.textContent).toContain("预警投影已窗口化");
    const tag = screen.getByText("可信 / 已截断");
    expect(tag.className).toContain("text-safe-warning");
  });
});
