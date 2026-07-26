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
