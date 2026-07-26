import { PlaceholderPage } from "../components/PlaceholderPage";

export function Component() {
  return (
    <PlaceholderPage
      title="执行"
      description="执行批次账本与游标式变更页(游标只存内存)"
      endpoint="/api/v1/executions?cursor="
    />
  );
}
