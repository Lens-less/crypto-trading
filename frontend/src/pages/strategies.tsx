import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="策略"
      description="Paper 任务读模型(网格 / 套利任务与最后记录状态)"
      endpoint="/api/v1/tasks"
    />
  );
}
