// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { PlaceholderPage } from "./PlaceholderPage";

describe("PlaceholderPage", () => {
  it("渲染标题与「待接入」一等状态文字", () => {
    render(
      <PlaceholderPage
        title="扫描"
        description="虚拟网格扫描读模型"
        endpoint="/api/v1/scanner"
      />,
    );
    expect(screen.getByRole("heading", { name: "扫描" })).toBeTruthy();
    expect(screen.getByText("待接入")).toBeTruthy();
    expect(screen.getByText("/api/v1/scanner")).toBeTruthy();
  });
});
