import { StatusPill, type StatusPillProps } from "../../components/StatusPill";
import type { ReadOnlyTaskView } from "../api-types";
import { formatDateTime } from "../format";
import { humanizeToken } from "../labels";
import type { ColumnDef } from "./types";

type Tone = NonNullable<StatusPillProps["tone"]>;

/** 任务色调(沿承旧 taskTone):失败 → danger;需关注 → warning;已停止 → ok。 */
export function toneForTask(task: ReadOnlyTaskView): Tone {
  if (task.failure !== null || task.phase === "failed") {
    return "danger";
  }
  if (task.recovery !== "none" || task.exit === "shutdown_timed_out") {
    return "warning";
  }
  if (task.phase === "stopped") {
    return "ok";
  }
  return "neutral";
}

function toneForSourceHealth(health: string): Tone {
  if (health === "healthy") {
    return "ok";
  }
  if (health === "degraded") {
    return "warning";
  }
  return "neutral";
}

/**
 * 只读任务明细列(双源健康、事件计数、恢复判断);
 * running / stopping 只代表 journal 最后记录,不证明进程仍存活。
 */
export const taskColumns: ColumnDef<ReadOnlyTaskView>[] = [
  {
    id: "identity",
    header: "任务 / 最后事实",
    cell: (task) => (
      <span className="block">
        <span className="numeric block">{task.task_id}</span>
        <span className="numeric block text-xs text-muted-foreground">
          #{task.last_sequence} · {formatDateTime(task.updated_at)}
        </span>
        <span className="block text-xs text-muted-foreground">
          {humanizeToken(task.kind)} · {task.processed_event_count} 个事件
        </span>
      </span>
    ),
  },
  {
    id: "phase",
    header: "阶段",
    cell: (task) => (
      <span className="block space-y-1">
        <StatusPill tone={toneForTask(task)} label={humanizeToken(task.phase)} />
        {task.exit !== null && (
          <span className="block text-xs text-muted-foreground">
            {humanizeToken(task.exit)}
          </span>
        )}
        {task.failure !== null && (
          <span className="block text-xs text-muted-foreground">
            {humanizeToken(task.failure)}
          </span>
        )}
      </span>
    ),
  },
  {
    id: "sources",
    header: "数据源",
    cell: (task) =>
      task.sources.length === 0 ? (
        <span className="text-xs text-muted-foreground">--</span>
      ) : (
        <span className="block space-y-1">
          {task.sources.map((source) => (
            <span
              key={source.source_id}
              className="flex flex-wrap items-center gap-1.5"
            >
              <span className="numeric text-xs">
                {source.source_id} #{source.event_sequence}
              </span>
              <StatusPill
                tone={toneForSourceHealth(source.health)}
                label={humanizeToken(source.health)}
              />
            </span>
          ))}
        </span>
      ),
  },
  {
    id: "recovery",
    header: "恢复判断",
    cell: (task) => (
      <span className="block space-y-1">
        <StatusPill
          tone={task.recovery === "none" ? "ok" : "warning"}
          label={humanizeToken(task.recovery)}
        />
        <span className="block text-xs text-muted-foreground">
          {task.recovery === "none" ? "持久终态已闭合" : "仅有历史事实;需核对进程"}
        </span>
      </span>
    ),
  },
  {
    id: "sequences",
    header: "序号范围",
    numeric: true,
    cell: (task) => (
      <span className="numeric block text-xs">
        {task.first_sequence} – {task.last_sequence}
      </span>
    ),
  },
  {
    id: "registered",
    header: "登记时间",
    numeric: true,
    cell: (task) => (
      <span className="numeric text-xs">{formatDateTime(task.registered_at)}</span>
    ),
  },
];
