import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="预警"
      description="价格预警读模型(触发历史与投递结果)"
      endpoint="/api/v1/alerts"
    />
  );
}
