import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="集成"
      description="交易所适配器支持矩阵(capability manifest 的 adapters)"
      endpoint="/api/v1/capabilities"
    />
  );
}
