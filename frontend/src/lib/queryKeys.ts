/**
 * 集中管理 React Query 的 query key,避免各页面手写字符串漂移。
 * 游标只出现在内存 query key 中,永不进入 URL 历史或持久化存储。
 */
export const queryKeys = {
  health: ["health"] as const,
  system: ["system"] as const,
  capabilities: ["capabilities"] as const,
  monitor: ["monitor"] as const,
  tasks: ["tasks"] as const,
  risk: ["risk"] as const,
  settings: ["settings"] as const,
  executions: (cursor: string | null = null) => ["executions", cursor] as const,
} as const;
