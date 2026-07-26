import { StatusPill } from "./StatusPill";

export interface PlaceholderPageProps {
  /** 页面中文标题。 */
  title: string;
  /** 该页未来将消费的后端读模型说明。 */
  description: string;
  /** 计划接入的 API 路径(展示为等宽文本)。 */
  endpoint: string;
}

/**
 * 占位骨架页:明确说明「待接入」是一等状态,
 * 不伪装成加载中,也不伪装成空数据。
 */
export function PlaceholderPage({
  title,
  description,
  endpoint,
}: PlaceholderPageProps) {
  return (
    <section className="mx-auto max-w-5xl space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="text-sm text-muted-foreground">{description}</p>
      </header>
      <div className="rounded-lg border border-border bg-card p-6">
        <div className="flex items-center gap-3">
          <StatusPill tone="neutral" label="待接入" />
          <span className="text-sm text-muted-foreground">
            此页面尚未接入后端读模型
          </span>
        </div>
        <p className="mt-4 text-sm text-muted-foreground">
          计划数据源:<code className="numeric text-xs">{endpoint}</code>
        </p>
      </div>
    </section>
  );
}
