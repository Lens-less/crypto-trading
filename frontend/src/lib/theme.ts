/**
 * 主题状态管理。
 *
 * 约束(不可协商安全语义):
 * - localStorage 的 storage 白名单里只有这个键(`ct-theme`);
 *   bearer token、游标等一律不落任何持久化存储。
 * - storage 事件用于跨标签同步。
 * - 无持久化值时以 prefers-color-scheme 兜底,默认深色。
 */

export const THEME_STORAGE_KEY = "ct-theme";

export type Theme = "dark" | "light";

function safeStorage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function parseTheme(value: unknown): Theme | null {
  return value === "dark" || value === "light" ? value : null;
}

/** 读取持久化主题;非法或不可用时返回 null。 */
export function readStoredTheme(): Theme | null {
  const storage = safeStorage();
  if (!storage) {
    return null;
  }
  try {
    return parseTheme(storage.getItem(THEME_STORAGE_KEY));
  } catch {
    return null;
  }
}

/** 系统偏好兜底;matchMedia 不可用时默认深色。 */
export function systemTheme(): Theme {
  if (
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-color-scheme: light)").matches
  ) {
    return "light";
  }
  return "dark";
}

/** 当前应当生效的主题:持久化值优先,其次系统偏好。 */
export function resolveTheme(): Theme {
  return readStoredTheme() ?? systemTheme();
}

/** 将主题应用到 <html>(深色为默认,浅色加 .light 类)。 */
export function applyTheme(theme: Theme): void {
  document.documentElement.classList.toggle("light", theme === "light");
}

/** 持久化并应用主题。 */
export function setTheme(theme: Theme): void {
  const storage = safeStorage();
  if (storage) {
    try {
      storage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // 存储失败时仍然应用到当前标签。
    }
  }
  applyTheme(theme);
}

/** 在深浅之间切换,返回新主题。 */
export function toggleTheme(): Theme {
  const next: Theme = resolveTheme() === "dark" ? "light" : "dark";
  setTheme(next);
  return next;
}

/**
 * 订阅主题变化:
 * - 其他标签页写入 `ct-theme`(storage 事件)时同步并回调;
 * - 未持久化主题时,系统 prefers-color-scheme 变化也会同步。
 * 返回取消订阅函数。
 */
export function watchTheme(onChange: (theme: Theme) => void): () => void {
  const onStorage = (event: StorageEvent): void => {
    if (event.key !== null && event.key !== THEME_STORAGE_KEY) {
      return;
    }
    const theme = parseTheme(event.newValue) ?? systemTheme();
    applyTheme(theme);
    onChange(theme);
  };
  window.addEventListener("storage", onStorage);

  let media: MediaQueryList | null = null;
  const onMedia = (): void => {
    if (readStoredTheme() !== null) {
      return;
    }
    const theme = systemTheme();
    applyTheme(theme);
    onChange(theme);
  };
  if (typeof window.matchMedia === "function") {
    media = window.matchMedia("(prefers-color-scheme: light)");
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", onMedia);
    }
  }

  return () => {
    window.removeEventListener("storage", onStorage);
    if (media && typeof media.removeEventListener === "function") {
      media.removeEventListener("change", onMedia);
    }
  };
}
