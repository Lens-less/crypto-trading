// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  THEME_STORAGE_KEY,
  applyTheme,
  readStoredTheme,
  resolveTheme,
  setTheme,
  systemTheme,
  toggleTheme,
  watchTheme,
} from "./theme";

function stubMatchMedia(lightMatches: boolean): void {
  vi.stubGlobal(
    "matchMedia",
    vi.fn().mockImplementation((query: string) => ({
      matches: query.includes("light") && lightMatches,
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  );
}

beforeEach(() => {
  window.localStorage.clear();
  document.documentElement.classList.remove("light");
  stubMatchMedia(false);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("storage 键约束", () => {
  it("持久化键必须是白名单里的 ct-theme", () => {
    expect(THEME_STORAGE_KEY).toBe("ct-theme");
  });

  it("setTheme 只写入 ct-theme 一个键", () => {
    setTheme("light");
    expect(window.localStorage.length).toBe(1);
    expect(window.localStorage.getItem("ct-theme")).toBe("light");
  });
});

describe("resolveTheme", () => {
  it("无持久化值且系统偏好深色时,默认深色", () => {
    expect(resolveTheme()).toBe("dark");
  });

  it("无持久化值时以 prefers-color-scheme 兜底", () => {
    stubMatchMedia(true);
    expect(systemTheme()).toBe("light");
    expect(resolveTheme()).toBe("light");
  });

  it("持久化值优先于系统偏好", () => {
    stubMatchMedia(true);
    window.localStorage.setItem(THEME_STORAGE_KEY, "dark");
    expect(resolveTheme()).toBe("dark");
  });

  it("非法持久化值按未设置处理", () => {
    window.localStorage.setItem(THEME_STORAGE_KEY, "neon");
    expect(readStoredTheme()).toBeNull();
    expect(resolveTheme()).toBe("dark");
  });
});

describe("applyTheme / setTheme / toggleTheme", () => {
  it("浅色加 .light 类,深色移除", () => {
    applyTheme("light");
    expect(document.documentElement.classList.contains("light")).toBe(true);
    applyTheme("dark");
    expect(document.documentElement.classList.contains("light")).toBe(false);
  });

  it("setTheme 同时持久化并应用", () => {
    setTheme("light");
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("light");
    expect(document.documentElement.classList.contains("light")).toBe(true);
  });

  it("toggleTheme 在深浅之间往返", () => {
    expect(toggleTheme()).toBe("light");
    expect(toggleTheme()).toBe("dark");
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe("dark");
  });
});

describe("watchTheme(跨标签同步)", () => {
  it("storage 事件携带新主题时应用并回调", () => {
    const onChange = vi.fn();
    const unsubscribe = watchTheme(onChange);

    window.dispatchEvent(
      new StorageEvent("storage", {
        key: THEME_STORAGE_KEY,
        newValue: "light",
      }),
    );

    expect(onChange).toHaveBeenCalledWith("light");
    expect(document.documentElement.classList.contains("light")).toBe(true);
    unsubscribe();
  });

  it("其他键的 storage 事件被忽略", () => {
    const onChange = vi.fn();
    const unsubscribe = watchTheme(onChange);

    window.dispatchEvent(
      new StorageEvent("storage", { key: "other-key", newValue: "light" }),
    );

    expect(onChange).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("取消订阅后不再回调", () => {
    const onChange = vi.fn();
    const unsubscribe = watchTheme(onChange);
    unsubscribe();

    window.dispatchEvent(
      new StorageEvent("storage", {
        key: THEME_STORAGE_KEY,
        newValue: "light",
      }),
    );

    expect(onChange).not.toHaveBeenCalled();
  });

  it("清空持久化值(newValue 为 null)时回退系统偏好", () => {
    const onChange = vi.fn();
    const unsubscribe = watchTheme(onChange);

    window.dispatchEvent(
      new StorageEvent("storage", { key: THEME_STORAGE_KEY, newValue: null }),
    );

    expect(onChange).toHaveBeenCalledWith("dark");
    unsubscribe();
  });
});
