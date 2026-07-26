import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="风险"
      description="Paper 账户读模型(余额、敞口与风控边界)"
      endpoint="/api/v1/risk"
    />
  );
}
