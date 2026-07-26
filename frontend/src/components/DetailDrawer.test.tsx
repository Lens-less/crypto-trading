// @vitest-environment jsdom
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { DetailDrawer } from "./DetailDrawer";

// vitest 未开启 globals,@testing-library/react 不会自动 cleanup。
afterEach(cleanup);

function Harness() {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button type="button" onClick={() => setOpen(true)}>
        打开详情
      </button>
      {open && (
        <DetailDrawer
          title="执行详情"
          identifier="batch-1"
          onClose={() => setOpen(false)}
        >
          <p>内容</p>
        </DetailDrawer>
      )}
    </div>
  );
}

describe("DetailDrawer 焦点管理", () => {
  it("打开时焦点移到关闭按钮", () => {
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "打开详情" });
    trigger.focus();
    fireEvent.click(trigger);
    const close = screen.getByRole("button", { name: "关闭详情(Esc)" });
    expect(document.activeElement).toBe(close);
    expect(screen.getByRole("dialog")).toBeTruthy();
  });

  it("Escape 关闭抽屉,焦点还给触发元素", () => {
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "打开详情" });
    trigger.focus();
    fireEvent.click(trigger);
    const dialog = screen.getByRole("dialog");
    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(trigger);
  });

  it("onClose 由关闭按钮触发", () => {
    const onClose = vi.fn();
    render(
      <DetailDrawer title="详情" onClose={onClose}>
        <p>内容</p>
      </DetailDrawer>,
    );
    fireEvent.click(screen.getByRole("button", { name: "关闭详情(Esc)" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
