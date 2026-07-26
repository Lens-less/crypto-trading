import { StatusPill, type StatusPillProps } from "../../components/StatusPill";
import type {
  ExecutionBatchState,
  ExecutionBatchView,
  ExecutionPhase,
  RecoveryDirective,
} from "../api-types";
import { formatDateTime, shortId } from "../format";
import { humanizeToken } from "../labels";
import type { ColumnDef } from "./types";

type Tone = NonNullable<StatusPillProps["tone"]>;

/** 批次状态 → 安全色(沿承旧 tagToneForBatch)。 */
export function toneForBatchState(state: ExecutionBatchState): Tone {
  switch (state) {
    case "completed":
      return "ok";
    case "partial":
    case "incomplete":
    case "outcome_unknown":
      return "warning";
    case "failed":
    case "conflict":
      return "danger";
    default:
      return "neutral";
  }
}

/** 恢复指令 → 安全色(沿承旧 tagToneForRecovery)。 */
export function toneForRecovery(recovery: RecoveryDirective): Tone {
  switch (recovery) {
    case "none":
      return "ok";
    case "reconcile_required":
      return "warning";
    case "investigate":
      return "danger";
    default:
      return "neutral";
  }
}

/** 执行阶段 → 安全色(沿承旧 tagToneForPhase)。 */
export function toneForPhase(phase: ExecutionPhase): Tone {
  switch (phase) {
    case "completed":
      return "ok";
    case "partial":
    case "incomplete":
      return "warning";
    case "failed":
      return "danger";
    default:
      return "neutral";
  }
}

/** 执行账本列定义(全宽表)。 */
export const executionColumns: ColumnDef<ExecutionBatchView>[] = [
  {
    id: "batch",
    header: "批次",
    cell: (batch) => (
      <span className="numeric" title={batch.batch_id}>
        {shortId(batch.batch_id)}
      </span>
    ),
  },
  {
    id: "strategy",
    header: "策略 / 交易对",
    cell: (batch) => (
      <span className="block">
        <span className="numeric block">{batch.strategy}</span>
        <span className="numeric block text-muted-foreground">{batch.symbol}</span>
      </span>
    ),
  },
  {
    id: "state",
    header: "状态",
    cell: (batch) => (
      <StatusPill tone={toneForBatchState(batch.state)} label={humanizeToken(batch.state)} />
    ),
  },
  {
    id: "recovery",
    header: "恢复",
    cell: (batch) => (
      <StatusPill tone={toneForRecovery(batch.recovery)} label={humanizeToken(batch.recovery)} />
    ),
  },
  {
    id: "sequence",
    header: "序号",
    numeric: true,
    cell: (batch) => (
      <span className="numeric">
        {batch.first_sequence} → {batch.last_sequence}
      </span>
    ),
  },
  {
    id: "updated",
    header: "更新时间",
    numeric: true,
    cell: (batch) => (
      <span className="block">
        <span className="numeric block">{formatDateTime(batch.updated_at)}</span>
        {batch.status_summary !== "" && (
          <span className="block text-xs text-muted-foreground">
            {batch.status_summary}
          </span>
        )}
      </span>
    ),
  },
  {
    id: "phases",
    header: "阶段",
    cell: (batch) => (
      <span className="flex flex-wrap gap-1">
        {batch.phases.map((phase, index) => (
          <StatusPill
            key={`${phase}-${index}`}
            tone={toneForPhase(phase)}
            label={humanizeToken(phase)}
          />
        ))}
      </span>
    ),
  },
];
