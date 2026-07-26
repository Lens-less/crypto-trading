/**
 * 手写的最小 API 类型 + zod 窄校验。
 *
 * 后端事实来源:rust/crates/web/src/api.rs 与 rust/crates/runtime/src/capability.rs。
 * - /api/v1/health、/api/v1/system 顶层 `schema_version: 1`(API_SCHEMA_VERSION);
 * - /api/v1/capabilities 返回 CapabilityManifest,`schema_version: 2`
 *   (CAPABILITY_SCHEMA_VERSION),注意与 API 版本不同;
 * - 错误响应统一为 `{ schema_version, error: { code, message } }`。
 *
 * zod 只做窄校验(schema_version 与关键判别字段),用 looseObject 允许
 * 后端新增字段而不破坏前端。完整形态由手写 TS 类型描述。
 */
import { z } from "zod";

export const API_SCHEMA_VERSION = 1;
export const CAPABILITY_SCHEMA_VERSION = 2;

/* ---------------------------------------------------------------- 错误封套 */

export interface ApiErrorBody {
  code: string;
  message: string;
}

export interface ApiErrorEnvelope {
  schema_version: number;
  error: ApiErrorBody;
}

export const apiErrorEnvelopeSchema = z.looseObject({
  schema_version: z.number(),
  error: z.looseObject({
    code: z.string(),
    message: z.string(),
  }),
});

/* -------------------------------------------------------- GET /api/v1/health */

export interface HealthResponse {
  schema_version: typeof API_SCHEMA_VERSION;
  status: "ready";
  live_trading_enabled: boolean;
}

export const healthResponseSchema = z.looseObject({
  schema_version: z.literal(API_SCHEMA_VERSION),
  status: z.literal("ready"),
  live_trading_enabled: z.boolean(),
});

/* -------------------------------------------------------- GET /api/v1/system */

export type ProjectionStatus = "complete" | "windowed" | "degraded";
export type ReleaseStage = "paper-only";
export type OperationalSignal = "not_available";

export interface ProjectionTruncation {
  batches: boolean;
  warnings: boolean;
}

export interface SystemResponse {
  schema_version: typeof API_SCHEMA_VERSION;
  product_version: string;
  release_stage: ReleaseStage;
  live_trading_enabled: boolean;
  access_scope: "loopback";
  authentication_required: boolean;
  projection_status: ProjectionStatus;
  journal_id: string;
  head_sequence: number | null;
  execution_batch_count: number;
  recovery_required_count: number;
  conflict_count: number;
  warning_count: number;
  truncation: ProjectionTruncation;
  kill_switch: OperationalSignal;
  market_data_freshness: OperationalSignal;
  adapter_health: OperationalSignal;
}

export const systemResponseSchema = z.looseObject({
  schema_version: z.literal(API_SCHEMA_VERSION),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  journal_id: z.string(),
  live_trading_enabled: z.boolean(),
  head_sequence: z.number().nullable(),
});

/* -------------------------------------------------- GET /api/v1/capabilities */

export type CapabilityLevel =
  | "available"
  | "read-only"
  | "paper-once"
  | "validate-only"
  | "contract-only"
  | "unavailable";

export type CapabilityAccess =
  | "local"
  | "market-data"
  | "paper-trading"
  | "testnet-trading"
  | "mainnet-trading";

export interface CapabilityScope {
  environments: string[];
  access: CapabilityAccess;
}

export interface Capability {
  id: string;
  area: string;
  level: CapabilityLevel;
  scope: CapabilityScope;
  summary: string;
  blockers: string[];
  evidence: string[];
}

export interface CapabilityManifest {
  schema_version: typeof CAPABILITY_SCHEMA_VERSION;
  product_version: string;
  release_stage: ReleaseStage;
  live_trading_enabled: boolean;
  adapters: unknown[];
  capabilities: Capability[];
}

export const capabilityManifestSchema = z.looseObject({
  schema_version: z.literal(CAPABILITY_SCHEMA_VERSION),
  release_stage: z.literal("paper-only"),
  live_trading_enabled: z.boolean(),
});

/* ------------------------------------------------------------- 共享投影原语 */

export type MarketType = "spot" | "perpetual";

/**
 * Journal 页边界(runtime::JournalPageBoundary,serde tag = "kind")。
 * partial_tail 额外携带 offset/bytes,前端只消费 kind 判别。
 */
export type BoundaryKind = "snapshot_end" | "page_limit" | "partial_tail";

export interface JournalPageBoundary {
  kind: BoundaryKind;
  offset?: number;
  bytes?: number;
}

export const READ_MODEL_SCHEMA_VERSION = 1;

/* ------------------------------------------------------- GET /api/v1/monitor */

export type MonitorProjectionState =
  | "waiting"
  | "no_opportunity"
  | "opportunity"
  | "analysis_rejected";

export type MonitorFreshnessState = "missing" | "fresh" | "stale" | "future";

export type MonitorContinuityState =
  | "missing"
  | "continuous"
  | "gap"
  | "duplicate"
  | "out_of_order"
  | "duplicate_timestamp"
  | "out_of_order_timestamp"
  | "out_of_order_receipt"
  | "source_gap"
  | "unavailable";

export interface MonitorLegView {
  exchange: string;
  symbol: string;
  market_type: MarketType;
}

export type ArbitrageMonitorProjection =
  | {
      type: "waiting";
      instrument: MonitorLegView;
      freshness: MonitorFreshnessState;
      continuity: MonitorContinuityState;
    }
  | {
      type: "no_opportunity" | "opportunity";
      buy_exchange: string;
      sell_exchange: string;
      buy_price: string;
      sell_price: string;
      absolute_spread: string;
      spread_percent: string;
      threshold_percent: string;
    }
  | { type: "analysis_rejected"; failure: string };

export interface ArbitrageMonitorView {
  source_sequence: number;
  event_id: string;
  /** journal 持久化时间;监控数据是历史投影,永不暗示实时行情。 */
  recorded_at: string;
  monitor_sequence: number;
  market_generation: number;
  symbol: string;
  state: MonitorProjectionState;
  left: MonitorLegView;
  right: MonitorLegView;
  projection: ArbitrageMonitorProjection;
}

export interface ArbitrageMonitorReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  journal_head_sequence: number | null;
  projection_status: ProjectionStatus;
  latest: ArbitrageMonitorView | null;
  invalid_event_count: number;
}

export const arbitrageMonitorReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  latest: z.nullable(z.looseObject({ recorded_at: z.string() })),
  invalid_event_count: z.number(),
});

/* -------------------------------------------------------- GET /api/v1/alerts */

/** 后端有界窗口:read model 最多保留 256 条 occurrence。 */
export const MAX_ALERT_OCCURRENCES = 256;

export type AlertOccurrenceKind =
  | "volatility_up"
  | "volatility_down"
  | "upper_limit"
  | "lower_limit";

export type AlertDeliveryStatus =
  | "pending"
  | "dropped"
  | "succeeded"
  | "failed"
  | "timed_out";

export type AlertDeliveryFailure =
  | "backpressure"
  | "adapter_closed"
  | "device_unavailable"
  | "rejected"
  | "worker_failed"
  | "timeout";

export interface AlertDeliveryView {
  adapter_id: string;
  status: AlertDeliveryStatus;
  failure: AlertDeliveryFailure | null;
  updated_at: string;
}

export interface AlertOccurrenceView {
  source_sequence: number;
  event_id: string;
  alert_sequence: number;
  recorded_at: string;
  exchange: string;
  symbol: string;
  market_type: MarketType;
  kind: AlertOccurrenceKind;
  price: string;
  change_percent: string | null;
  acknowledged_at: string | null;
  deliveries: AlertDeliveryView[];
}

export interface PriceAlertReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  journal_head_sequence: number | null;
  boundary: JournalPageBoundary;
  projection_status: ProjectionStatus;
  occurrences: AlertOccurrenceView[];
  occurrences_truncated: boolean;
  invalid_event_count: number;
}

export const priceAlertReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  boundary: z.looseObject({ kind: z.string() }),
  occurrences: z.array(z.looseObject({ alert_sequence: z.number() })),
  occurrences_truncated: z.boolean(),
  invalid_event_count: z.number(),
});

/* --------------------------------------------------------- GET /api/v1/tasks */

export type ReadOnlyTaskKind =
  | "arbitrage_monitor"
  | "arbitrage_paper"
  | "grid_paper"
  | "price_alert"
  | "scanner";

export type ReadOnlyTaskPhase =
  | "registered"
  | "running"
  | "stopping"
  | "stopped"
  | "failed";

export type ReadOnlyTaskRecovery = "none" | "investigate";

export interface ReadOnlyTaskSourceView {
  source_id: string;
  event_sequence: number;
  phase: string;
  health: string;
}

export interface ReadOnlyTaskView {
  task_id: string;
  kind: ReadOnlyTaskKind;
  first_sequence: number;
  last_sequence: number;
  registered_at: string;
  updated_at: string;
  phase: ReadOnlyTaskPhase;
  recovery: ReadOnlyTaskRecovery;
  processed_event_count: number;
  sources: ReadOnlyTaskSourceView[];
  exit: string | null;
  failure: string | null;
}

export interface ReadOnlyTaskReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  journal_head_sequence: number | null;
  projection_status: ProjectionStatus;
  tasks: ReadOnlyTaskView[];
  invalid_event_count: number;
}

export const readOnlyTaskReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  tasks: z.array(z.looseObject({ task_id: z.string(), phase: z.string() })),
});

/* ---------------------------------------------- GET /api/v1/executions?cursor= */

export type ExecutionBatchState =
  | "outcome_unknown"
  | "completed"
  | "partial"
  | "incomplete"
  | "failed"
  | "conflict";

export type RecoveryDirective = "none" | "reconcile_required" | "investigate";

export type ExecutionPhase =
  | "planned"
  | "completed"
  | "partial"
  | "incomplete"
  | "failed";

export type ReadModelWarningCode =
  | "conflicting_duplicate"
  | "duplicate_ignored"
  | "invalid_execution_event"
  | "metadata_conflict"
  | "orphan_outcome"
  | "out_of_order_planned"
  | "partial_tail"
  | "resolved_batch_evicted"
  | "terminal_conflict"
  | "timestamp_regressed";

export interface ReadModelWarning {
  code: ReadModelWarningCode;
  sequence: number | null;
  event_id: string | null;
  batch_id: string | null;
  detail: string;
}

export interface ExecutionBatchView {
  batch_id: string;
  strategy: string;
  symbol: string;
  first_sequence: number;
  last_sequence: number;
  first_seen_at: string;
  updated_at: string;
  planned_at: string | null;
  outcome_at: string | null;
  state: ExecutionBatchState;
  recovery: RecoveryDirective;
  status_summary: string;
  leg_count: number | null;
  receipt_count: number | null;
  expected_receipt_count: number | null;
  failed_index: number | null;
  unattempted_count: number | null;
  reconciliation_observation_count: number | null;
  reconciliation_error_count: number | null;
  failure_recorded: boolean;
  phases: ExecutionPhase[];
}

export interface OperatorReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  head_sequence: number | null;
  head_event_id: string | null;
  projection_status: ProjectionStatus;
  batches: ExecutionBatchView[];
  batches_truncated: boolean;
  warnings: ReadModelWarning[];
  warnings_truncated: boolean;
}

/** 完整有界执行投影 + 携带游标的变更水位(changes)。 */
export interface ExecutionsResponse {
  schema_version: typeof API_SCHEMA_VERSION;
  operator: OperatorReadModel;
  changes: ControlPlaneEventsPage;
}

export const executionsResponseSchema = z.looseObject({
  schema_version: z.literal(API_SCHEMA_VERSION),
  operator: z.looseObject({
    schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
    journal_id: z.string(),
    projection_status: z.enum(["complete", "windowed", "degraded"]),
    batches: z.array(z.looseObject({ batch_id: z.string() })),
    batches_truncated: z.boolean(),
    warnings_truncated: z.boolean(),
  }),
  changes: z.looseObject({
    journal_id: z.string(),
    events: z.array(z.looseObject({ sequence: z.number() })),
    next_cursor: z.string().nullable(),
    boundary: z.looseObject({ kind: z.string() }),
  }),
});

/* ------------------------------------------------------ GET /api/v1/events */

/**
 * SSE `operation_page` 事件体(ControlPlaneEventsPage,schema_version = 1,
 * CONTROL_PLANE_EVENTS_SCHEMA_VERSION)。payload-free 变更通知:事件只说明
 * 「哪个聚合变了」,不携带业务数据;数据一律由 REST 端点重新拉取。
 */
export const CONTROL_PLANE_EVENTS_SCHEMA_VERSION = 1;

export interface ControlPlaneEventNotice {
  sequence: number;
  event_id: string;
  recorded_at: string;
  kind: string;
  aggregate_kind: string;
  aggregate_id: string;
  producer: string;
}

export interface ControlPlaneEventsPage {
  schema_version: typeof CONTROL_PLANE_EVENTS_SCHEMA_VERSION;
  journal_id: string;
  events: ControlPlaneEventNotice[];
  /** 不透明恢复游标;只存内存,重连时经 Last-Event-ID/`?cursor=` 回传。 */
  next_cursor: string | null;
  boundary: { kind: "snapshot_end" | "page_limit" | "partial_tail" };
}

export const controlPlaneEventsPageSchema = z.looseObject({
  schema_version: z.literal(CONTROL_PLANE_EVENTS_SCHEMA_VERSION),
  journal_id: z.string(),
  events: z.array(z.looseObject({ sequence: z.number(), kind: z.string() })),
  next_cursor: z.string().nullable(),
});
