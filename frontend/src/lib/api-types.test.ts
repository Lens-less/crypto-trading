import { describe, expect, it } from "vitest";
import {
  apiErrorEnvelopeSchema,
  capabilityManifestSchema,
  healthResponseSchema,
  systemResponseSchema,
  virtualGridScannerReadModelSchema,
} from "./api-types";

describe("healthResponseSchema", () => {
  const valid = { schema_version: 1, status: "ready", live_trading_enabled: false };

  it("接受后端 /health 形态", () => {
    expect(healthResponseSchema.safeParse(valid).success).toBe(true);
  });

  it("允许后端新增未知字段(前向兼容)", () => {
    expect(
      healthResponseSchema.safeParse({ ...valid, future_field: 1 }).success,
    ).toBe(true);
  });

  it("拒绝错误的 schema_version", () => {
    expect(
      healthResponseSchema.safeParse({ ...valid, schema_version: 2 }).success,
    ).toBe(false);
  });

  it("拒绝未知 status 判别值", () => {
    expect(
      healthResponseSchema.safeParse({ ...valid, status: "starting" }).success,
    ).toBe(false);
  });
});

describe("systemResponseSchema", () => {
  const valid = {
    schema_version: 1,
    projection_status: "complete",
    journal_id: "journal-8f3a",
    live_trading_enabled: false,
    head_sequence: 42,
    kill_switch: "normal",
    market_data_freshness: "not_available",
    adapter_health: "degraded",
  };

  it("接受最小 /system 形态(窄校验)", () => {
    expect(systemResponseSchema.safeParse(valid).success).toBe(true);
  });

  it("接受空 journal 的 head_sequence: null", () => {
    expect(
      systemResponseSchema.safeParse({ ...valid, head_sequence: null }).success,
    ).toBe(true);
  });

  it("接受三种一等投影状态", () => {
    for (const status of ["complete", "windowed", "degraded"]) {
      expect(
        systemResponseSchema.safeParse({ ...valid, projection_status: status })
          .success,
      ).toBe(true);
    }
  });

  it("拒绝未知投影状态", () => {
    expect(
      systemResponseSchema.safeParse({ ...valid, projection_status: "fresh" })
        .success,
    ).toBe(false);
  });

  it("拒绝缺失 journal_id", () => {
    const { journal_id: _journalId, ...withoutJournal } = valid;
    expect(systemResponseSchema.safeParse(withoutJournal).success).toBe(false);
  });

  it("接受受控 operational signals", () => {
    for (const signal of ["normal", "engaged", "degraded", "not_available"]) {
      expect(
        systemResponseSchema.safeParse({
          ...valid,
          kill_switch: signal,
          market_data_freshness: signal,
          adapter_health: signal,
        }).success,
      ).toBe(true);
    }
  });

  it("拒绝未知 operational signal", () => {
    expect(
      systemResponseSchema.safeParse({
        ...valid,
        adapter_health: "healthy",
      }).success,
    ).toBe(false);
  });
});

describe("capabilityManifestSchema", () => {
  const valid = {
    schema_version: 2,
    release_stage: "paper-only",
    live_trading_enabled: false,
  };

  it("接受 capability manifest(注意 schema_version 是 2)", () => {
    expect(capabilityManifestSchema.safeParse(valid).success).toBe(true);
  });

  it("拒绝 API 的 schema_version 1(两个版本号不同源)", () => {
    expect(
      capabilityManifestSchema.safeParse({ ...valid, schema_version: 1 })
        .success,
    ).toBe(false);
  });

  it("拒绝未知 release_stage 判别值", () => {
    expect(
      capabilityManifestSchema.safeParse({ ...valid, release_stage: "live" })
        .success,
    ).toBe(false);
  });
});

describe("virtualGridScannerReadModelSchema", () => {
  const valid = {
    schema_version: 1,
    journal_id: "journal-scanner",
    projection_status: "complete",
    latest: {
      run_id: "run-1",
      estimated_apr_kind: "heuristic",
      estimated_apr_assumptions: {
        order_notional_usdc: "100",
        round_trip_fee_percent: "0.2",
      },
      rows: [{ rank: 1, estimated_apr_kind: "heuristic" }],
    },
    invalid_event_count: 0,
  };

  it("要求 scanner 明示启发式 APR 及其假设", () => {
    expect(virtualGridScannerReadModelSchema.safeParse(valid).success).toBe(true);
    const { estimated_apr_assumptions: _assumptions, ...latest } = valid.latest;
    expect(
      virtualGridScannerReadModelSchema.safeParse({ ...valid, latest }).success,
    ).toBe(false);
  });

  it("兼容旧版 v1 恢复出的 unknown APR 类型，但仍要求显式假设", () => {
    expect(
      virtualGridScannerReadModelSchema.safeParse({
        ...valid,
        latest: {
          ...valid.latest,
          estimated_apr_kind: "unknown",
          rows: [{ rank: 1, estimated_apr_kind: "unknown" }],
        },
      }).success,
    ).toBe(true);
  });
});

describe("apiErrorEnvelopeSchema", () => {
  it("接受后端错误封套", () => {
    const envelope = {
      schema_version: 1,
      error: { code: "authentication_required", message: "..." },
    };
    expect(apiErrorEnvelopeSchema.safeParse(envelope).success).toBe(true);
  });

  it("拒绝缺失 error.code 的载荷", () => {
    expect(
      apiErrorEnvelopeSchema.safeParse({
        schema_version: 1,
        error: { message: "..." },
      }).success,
    ).toBe(false);
  });
});
