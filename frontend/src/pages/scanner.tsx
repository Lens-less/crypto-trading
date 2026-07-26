import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="扫描"
      description="虚拟网格扫描读模型(候选与投影状态)"
      endpoint="/api/v1/scanner"
    />
  );
}
