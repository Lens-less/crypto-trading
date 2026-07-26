import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="回放"
      description="套利监控读模型(必须显示 recorded_at 与 market generation)"
      endpoint="/api/v1/monitor"
    />
  );
}
