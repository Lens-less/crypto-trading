import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="设置"
      description="运行时部署元数据(凭据配置状态、请求限流、Paper 档案)"
      endpoint="/api/v1/settings"
    />
  );
}
