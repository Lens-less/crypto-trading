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
export type OperationalSignal =
  | "normal"
  | "engaged"
  | "degraded"
  | "not_available";

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
  kill_switch: z.enum(["normal", "engaged", "degraded", "not_available"]),
  market_data_freshness: z.enum([
    "normal",
    "engaged",
    "degraded",
    "not_available",
  ]),
  adapter_health: z.enum(["normal", "engaged", "degraded", "not_available"]),
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

/** 适配器支持矩阵的一个证据单元(runtime::AdapterFacetSupport)。 */
export type AdapterSupportLevel =
  | "implemented"
  | "protocol-only"
  | "request-only"
  | "config-only"
  | "unavailable"
  | "not-applicable";

export interface AdapterFacetSupport {
  level: AdapterSupportLevel;
  blockers: string[];
  evidence: string[];
}

export type AdapterFacetId =
  | "public_data"
  | "testnet_protocol"
  | "authenticated"
  | "reconcile"
  | "live";

/** 一个交易所适配器行(runtime::AdapterSupport,五个能力面)。 */
export interface AdapterSupport {
  id: string;
  name: string;
  public_data: AdapterFacetSupport;
  testnet_protocol: AdapterFacetSupport;
  authenticated: AdapterFacetSupport;
  reconcile: AdapterFacetSupport;
  live: AdapterFacetSupport;
}

export interface CapabilityManifest {
  schema_version: typeof CAPABILITY_SCHEMA_VERSION;
  product_version: string;
  release_stage: ReleaseStage;
  live_trading_enabled: boolean;
  adapters: AdapterSupport[];
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
  | "scanner"
  | "volume_maker";

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

/* ------------------------------------------------------ GET /api/v1/scanner */

/** benchmark 是展示优先级,不是评分加成。 */
export type ScannerPriority = "benchmark" | "standard";

/** 评级是估算证据,不是安全状态;呈现时用中性/强调色而非安全色。 */
export type ScannerRatingGrade = "s" | "a" | "b" | "c" | "d";

/**
 * heuristic = 新版记录显式声明的启发式估算;
 * unknown = 旧版 v1 记录未声明类型,后端按固定公式恢复,仍然不是可交易收益。
 */
export type ScannerAprEstimateKind = "heuristic" | "unknown";

export interface ScannerAprEstimateAssumptions {
  order_notional_usdc: string;
  round_trip_fee_percent: string;
}

export interface ScannerInstrumentView {
  exchange: string;
  symbol: string;
  market_type: MarketType;
}

/** 十进制数值保持后端给定的规范字符串,前端不做浮点转换。 */
export interface VirtualGridScanRowView {
  rank: number;
  priority: ScannerPriority;
  instrument: ScannerInstrumentView;
  started_at: string;
  last_observed_at: string;
  observation_count: number;
  last_observation_sequence: number;
  current_price: string;
  lower_price: string;
  upper_price: string;
  pending_buy_price: string;
  pending_sell_price: string;
  grid_width_percent: string;
  grid_interval_percent: string;
  grid_count: number;
  running_seconds: number;
  buy_crosses: number;
  sell_crosses: number;
  total_crosses: number;
  complete_cycles: number;
  recent_five_minute_cycles: number;
  cycles_per_hour: string;
  estimated_apr: string;
  estimated_apr_kind: ScannerAprEstimateKind;
  volume_24h_usdc: string;
  price_change_24h_percent: string | null;
  rating_grade: ScannerRatingGrade;
  rating_score: string;
}

export interface VirtualGridScanView {
  source_sequence: number;
  event_id: string;
  recorded_at: string;
  run_id: string;
  ranking_policy: string;
  apr_window_seconds: number;
  estimated_apr_kind: ScannerAprEstimateKind;
  estimated_apr_assumptions: ScannerAprEstimateAssumptions;
  min_complete_cycles: number;
  row_limit: number;
  candidate_count: number;
  eligible_count: number;
  filtered_by_cycles_count: number;
  truncated: boolean;
  rows: VirtualGridScanRowView[];
}

export interface VirtualGridScannerReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  journal_head_sequence: number | null;
  projection_status: ProjectionStatus;
  latest: VirtualGridScanView | null;
  invalid_event_count: number;
}

export const virtualGridScannerReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  latest: z.nullable(
    z.looseObject({
      run_id: z.string(),
      estimated_apr_kind: z.enum(["heuristic", "unknown"]),
      estimated_apr_assumptions: z.looseObject({
        order_notional_usdc: z.string(),
        round_trip_fee_percent: z.string(),
      }),
      rows: z.array(
        z.looseObject({
          rank: z.number(),
          estimated_apr_kind: z.enum(["heuristic", "unknown"]),
        }),
      ),
    }),
  ),
  invalid_event_count: z.number(),
});

/* --------------------------------------------------------- GET /api/v1/risk */

/** Paper 预留阶段(runtime::PaperReservationPhase)。 */
export type PaperReservationPhase =
  | "pending"
  | "uncertain"
  | "committed"
  | "released";

export type PaperReconciliationOutcome = "released" | "failed";

export interface PaperCostModel {
  version: number;
  fee_bps: number;
  funding_buffer_bps: number;
  slippage_bps: number;
}

export interface PaperReservationLeg {
  index: number;
  exchange: string;
  symbol: string;
  market_type: MarketType;
  side: string;
  /** Money 序列化为规范十进制字符串。 */
  reserved_notional: string;
}

export interface PaperReconciliationRecord {
  outcome: PaperReconciliationOutcome;
  /** 对账证明细节(前端只展示 outcome 与证据序号,不解释 digest)。 */
  proof: unknown;
  evidence_sequence: number;
}

export interface PaperReservationView {
  reservation_id: string;
  task_id: string;
  idempotency_key: string;
  batch_id: string;
  cost_model: PaperCostModel;
  legs: PaperReservationLeg[];
  reserved_exposure: string;
  held_exposure: string;
  phase: PaperReservationPhase;
  first_sequence: number;
  last_sequence: number;
  reconciliation: PaperReconciliationRecord | null;
}

export interface PaperAccountSnapshot {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  projection_status: ProjectionStatus;
  invalid_event_count: number;
  account_id: string;
  initial_available: string;
  available: string;
  pending_reserved: string;
  uncertain_reserved: string;
  committed_exposure: string;
  reservations: PaperReservationView[];
}

export interface PaperAccountReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  projection_status: ProjectionStatus;
  invalid_event_count: number;
  accounts: PaperAccountSnapshot[];
}

export const paperAccountReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  invalid_event_count: z.number(),
  accounts: z.array(
    z.looseObject({ account_id: z.string(), available: z.string() }),
  ),
});

/** 一条 open position 时钟(runtime::AccountRiskOpenPositionView)。 */
export interface AccountRiskOpenPositionView {
  task_id: string;
  symbol: string;
  opened_at: string;
}

/**
 * 每个 scope 的持久账户风控状态(runtime::AccountRiskStateView)。
 * kill_switch_engaged 是闩锁事实:一旦为 true,读侧永不呈现「可解除」。
 */
export interface AccountRiskStateView {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  scope_id: string;
  paused: boolean;
  pause_reason: string | null;
  kill_switch_engaged: boolean;
  kill_switch_reason: string | null;
  /** daily_trade_count 所属的 UTC 交易日(YYYY-MM-DD)。 */
  trade_date_utc: string | null;
  daily_trade_count: number;
  open_positions: AccountRiskOpenPositionView[];
  admitted_count: number;
  rejected_count: number;
  last_rejection: string | null;
  last_recorded_at: string | null;
}

export interface AccountRiskReadModel {
  schema_version: typeof READ_MODEL_SCHEMA_VERSION;
  journal_id: string;
  projection_status: ProjectionStatus;
  invalid_event_count: number;
  scopes: AccountRiskStateView[];
}

export const accountRiskReadModelSchema = z.looseObject({
  schema_version: z.literal(READ_MODEL_SCHEMA_VERSION),
  journal_id: z.string(),
  projection_status: z.enum(["complete", "windowed", "degraded"]),
  invalid_event_count: z.number(),
  scopes: z.array(
    z.looseObject({
      scope_id: z.string(),
      paused: z.boolean(),
      pause_reason: z.string().nullable(),
      kill_switch_engaged: z.boolean(),
      kill_switch_reason: z.string().nullable(),
      trade_date_utc: z.string().nullable(),
      daily_trade_count: z.number(),
      open_positions: z.array(
        z.looseObject({
          task_id: z.string(),
          symbol: z.string(),
          opened_at: z.string(),
        }),
      ),
      admitted_count: z.number(),
      rejected_count: z.number(),
      last_rejection: z.string().nullable(),
      last_recorded_at: z.string().nullable(),
    }),
  ),
});

/**
 * GET /api/v1/risk 组合响应(web::RiskResponse):journal-backed paper
 * 账户敞口 + 持久账户级风控状态(pause / kill switch / UTC 日计数)。
 */
export interface RiskResponse {
  schema_version: typeof API_SCHEMA_VERSION;
  paper_accounts: PaperAccountReadModel;
  account_risk: AccountRiskReadModel;
}

export const riskResponseSchema = z.looseObject({
  schema_version: z.literal(API_SCHEMA_VERSION),
  paper_accounts: paperAccountReadModelSchema,
  account_risk: accountRiskReadModelSchema,
});

/* ----------------------------------------------------- GET /api/v1/settings */

export const SETTINGS_SCHEMA_VERSION = 1;

/** 凭据只投影配置完整性,永不返回值。 */
export type CredentialConfiguration =
  | "configured"
  | "partial"
  | "not_configured"
  | "not_accepted"
  | "not_projected";

export type PaperProfileKind = "grid" | "arbitrage";

export interface PaperProfileSettings {
  kind: PaperProfileKind;
  task_id: string;
  strategy_id: string;
  strategy_revision: string;
  configuration_files: string[];
  replay_file: string;
}

export interface CredentialSettings {
  web_bearer: CredentialConfiguration;
  binance_testnet: CredentialConfiguration;
  mainnet: CredentialConfiguration;
}

export interface RequestLimitSettings {
  maximum_requests: number;
  window_seconds: number;
}

export interface SettingsResponse {
  schema_version: typeof SETTINGS_SCHEMA_VERSION;
  data_directory: string | null;
  journal_path: string | null;
  log_sink: "stdout_stderr";
  notification_evidence: "journal_projection";
  credentials: CredentialSettings;
  /**
   * 写路径探测:组合根仅在 --enable-paper-writes 生效时发布该字段
   * (web-app/src/lib.rs),浏览器据此决定是否渲染写控件。
   */
  paper_principal_id: string | null;
  paper_profiles: PaperProfileSettings[];
  request_limit: RequestLimitSettings;
}

export const settingsResponseSchema = z.looseObject({
  schema_version: z.literal(SETTINGS_SCHEMA_VERSION),
  credentials: z.looseObject({
    web_bearer: z.string(),
    binance_testnet: z.string(),
    mainnet: z.string(),
  }),
  paper_principal_id: z.string().nullable(),
  paper_profiles: z.array(z.looseObject({ task_id: z.string() })),
  request_limit: z.looseObject({
    maximum_requests: z.number(),
    window_seconds: z.number(),
  }),
});

/* ---------------------------------------------------- POST /api/v1/submit */

export const SUBMIT_SCHEMA_VERSION = 1;

/** 浏览器唯一允许构造的角色;Reconciler 面永不在 UI 出现。 */
export type SubmitRole = "paper_operator";
export type SubmitRiskConfirmation = "paper_only";

export type SubmitCommandKind =
  | "start_paper_arbitrage"
  | "start_paper_grid"
  | "stop_task"
  | "cancel_task";

export type SubmitCommand =
  | {
      kind: "start_paper_arbitrage" | "start_paper_grid";
      strategy_id: string;
      strategy_revision: string;
    }
  | { kind: "stop_task" | "cancel_task" };

export interface SubmitPermission {
  principal_id: string;
  role: SubmitRole;
}

export interface SubmitEnvelope {
  schema_version: typeof SUBMIT_SCHEMA_VERSION;
  command_id: string;
  idempotency_key: string;
  target_task_id: string;
  permission: SubmitPermission;
  risk_confirmation: SubmitRiskConfirmation;
  command: SubmitCommand;
}

export type SubmitStatus = "applied" | "rejected" | "outcome_unknown";

export const SUBMIT_JOURNAL_PROJECTION = "submit_command_v1";
export const SUBMIT_JOURNAL_SOURCE = "durable_journal";

export interface SubmitReceipt {
  schema_version: typeof SUBMIT_SCHEMA_VERSION;
  command_id: string;
  target_task_id: string;
  status: SubmitStatus;
  journal_projection: string;
  source: string;
}

export const submitReceiptSchema = z.looseObject({
  schema_version: z.literal(SUBMIT_SCHEMA_VERSION),
  command_id: z.string(),
  target_task_id: z.string(),
  status: z.enum(["applied", "rejected", "outcome_unknown"]),
  journal_projection: z.string(),
  source: z.string(),
});
