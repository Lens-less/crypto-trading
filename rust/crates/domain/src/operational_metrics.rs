//! Process-local, secret-free operational metrics.
//!
//! The trading layers deliberately share this tiny atomic registry instead of
//! accepting free-form metric names or labels. That keeps telemetry bounded
//! and prevents account identifiers, symbols, request URLs, or credentials
//! from becoming metric dimensions.

use std::{
    fmt::Write as _,
    sync::{
        OnceLock,
        atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// Fixed stream identities accepted by the operational registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationalStreamKind {
    Market,
    UserData,
}

/// Fixed lifecycle phases for the single authoritative execution owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationalOwnerPhase {
    Booting = 0,
    Reconciling = 1,
    AwaitingStreams = 2,
    ReadyUnarmed = 3,
    CampaignRunning = 4,
    Degraded = 5,
    RecoveryRequired = 6,
    KilledClean = 7,
}

impl OperationalOwnerPhase {
    const ALL: [(Self, &'static str); 8] = [
        (Self::Booting, "booting"),
        (Self::Reconciling, "reconciling"),
        (Self::AwaitingStreams, "awaiting_streams"),
        (Self::ReadyUnarmed, "ready_unarmed"),
        (Self::CampaignRunning, "campaign_running"),
        (Self::Degraded, "degraded"),
        (Self::RecoveryRequired, "recovery_required"),
        (Self::KilledClean, "killed_clean"),
    ];

    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Reconciling,
            2 => Self::AwaitingStreams,
            3 => Self::ReadyUnarmed,
            4 => Self::CampaignRunning,
            5 => Self::Degraded,
            6 => Self::RecoveryRequired,
            7 => Self::KilledClean,
            _ => Self::Booting,
        }
    }
}

/// Sanitized REST response facts retained for budgeting and alerting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationalRestObservation {
    pub latency_micros: u64,
    pub status: u16,
    pub used_weight: Option<u64>,
    pub order_count: Option<u64>,
    pub retry_after_unix_seconds: Option<u64>,
}

/// Snapshot of one stream's bounded counters and watermarks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperationalStreamMetricsSnapshot {
    pub generation: u64,
    pub last_frame_unix_seconds: u64,
    pub reconnect_total: u64,
    pub gap_total: u64,
    pub queue_drop_total: u64,
}

/// Snapshot of bounded REST telemetry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperationalRestMetricsSnapshot {
    pub request_total: u64,
    pub transport_error_total: u64,
    pub status_2xx_total: u64,
    pub status_4xx_total: u64,
    pub status_5xx_total: u64,
    pub status_429_total: u64,
    pub last_latency_micros: u64,
    pub max_latency_micros: u64,
    pub used_weight: u64,
    pub order_count: u64,
    pub retry_after_unix_seconds: u64,
    pub clock_skew_milliseconds: i64,
}

/// Snapshot of append latency and failure counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperationalJournalMetricsSnapshot {
    pub append_total: u64,
    pub append_failure_total: u64,
    pub last_append_latency_micros: u64,
    pub max_append_latency_micros: u64,
}

/// Coherent-enough process snapshot for telemetry export.
///
/// Atomic fields can advance during collection. Prometheus counters are
/// individually monotonic; consumers must not treat one scrape as a database
/// transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationalMetricsSnapshot {
    pub process_started_at_unix_seconds: u64,
    pub market_stream: OperationalStreamMetricsSnapshot,
    pub user_data_stream: OperationalStreamMetricsSnapshot,
    pub rest: OperationalRestMetricsSnapshot,
    pub journal: OperationalJournalMetricsSnapshot,
    pub owner_phase: OperationalOwnerPhase,
    pub owner_recovery_required_total: u64,
}

#[derive(Default)]
struct StreamMetrics {
    generation: AtomicU64,
    last_frame_unix_seconds: AtomicU64,
    reconnect_total: AtomicU64,
    gap_total: AtomicU64,
    queue_drop_total: AtomicU64,
}

impl StreamMetrics {
    fn snapshot(&self) -> OperationalStreamMetricsSnapshot {
        OperationalStreamMetricsSnapshot {
            generation: self.generation.load(Ordering::Relaxed),
            last_frame_unix_seconds: self.last_frame_unix_seconds.load(Ordering::Relaxed),
            reconnect_total: self.reconnect_total.load(Ordering::Relaxed),
            gap_total: self.gap_total.load(Ordering::Relaxed),
            queue_drop_total: self.queue_drop_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct RestMetrics {
    request_total: AtomicU64,
    transport_error_total: AtomicU64,
    status_2xx_total: AtomicU64,
    status_4xx_total: AtomicU64,
    status_5xx_total: AtomicU64,
    status_429_total: AtomicU64,
    last_latency_micros: AtomicU64,
    max_latency_micros: AtomicU64,
    used_weight: AtomicU64,
    order_count: AtomicU64,
    retry_after_unix_seconds: AtomicU64,
    clock_skew_milliseconds: AtomicI64,
}

impl RestMetrics {
    fn snapshot(&self) -> OperationalRestMetricsSnapshot {
        OperationalRestMetricsSnapshot {
            request_total: self.request_total.load(Ordering::Relaxed),
            transport_error_total: self.transport_error_total.load(Ordering::Relaxed),
            status_2xx_total: self.status_2xx_total.load(Ordering::Relaxed),
            status_4xx_total: self.status_4xx_total.load(Ordering::Relaxed),
            status_5xx_total: self.status_5xx_total.load(Ordering::Relaxed),
            status_429_total: self.status_429_total.load(Ordering::Relaxed),
            last_latency_micros: self.last_latency_micros.load(Ordering::Relaxed),
            max_latency_micros: self.max_latency_micros.load(Ordering::Relaxed),
            used_weight: self.used_weight.load(Ordering::Relaxed),
            order_count: self.order_count.load(Ordering::Relaxed),
            retry_after_unix_seconds: self.retry_after_unix_seconds.load(Ordering::Relaxed),
            clock_skew_milliseconds: self.clock_skew_milliseconds.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct JournalMetrics {
    append_total: AtomicU64,
    append_failure_total: AtomicU64,
    last_append_latency_micros: AtomicU64,
    max_append_latency_micros: AtomicU64,
}

impl JournalMetrics {
    fn snapshot(&self) -> OperationalJournalMetricsSnapshot {
        OperationalJournalMetricsSnapshot {
            append_total: self.append_total.load(Ordering::Relaxed),
            append_failure_total: self.append_failure_total.load(Ordering::Relaxed),
            last_append_latency_micros: self.last_append_latency_micros.load(Ordering::Relaxed),
            max_append_latency_micros: self.max_append_latency_micros.load(Ordering::Relaxed),
        }
    }
}

struct OperationalMetrics {
    process_started_at_unix_seconds: u64,
    market_stream: StreamMetrics,
    user_data_stream: StreamMetrics,
    rest: RestMetrics,
    journal: JournalMetrics,
    owner_phase: AtomicU8,
    owner_recovery_required_total: AtomicU64,
}

impl OperationalMetrics {
    fn new() -> Self {
        Self {
            process_started_at_unix_seconds: unix_seconds_now(),
            market_stream: StreamMetrics::default(),
            user_data_stream: StreamMetrics::default(),
            rest: RestMetrics::default(),
            journal: JournalMetrics::default(),
            owner_phase: AtomicU8::new(OperationalOwnerPhase::Booting as u8),
            owner_recovery_required_total: AtomicU64::new(0),
        }
    }

    const fn stream(&self, kind: OperationalStreamKind) -> &StreamMetrics {
        match kind {
            OperationalStreamKind::Market => &self.market_stream,
            OperationalStreamKind::UserData => &self.user_data_stream,
        }
    }
}

static OPERATIONAL_METRICS: OnceLock<OperationalMetrics> = OnceLock::new();

fn metrics() -> &'static OperationalMetrics {
    OPERATIONAL_METRICS.get_or_init(OperationalMetrics::new)
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Records a successfully decoded inbound stream frame.
pub fn record_stream_frame(
    kind: OperationalStreamKind,
    generation: u64,
    observed_at_unix_seconds: u64,
) {
    let stream = metrics().stream(kind);
    stream.generation.fetch_max(generation, Ordering::Relaxed);
    stream
        .last_frame_unix_seconds
        .fetch_max(observed_at_unix_seconds, Ordering::Relaxed);
}

/// Records one reconnect attempt for a fixed stream kind.
pub fn record_stream_reconnect(kind: OperationalStreamKind) {
    metrics()
        .stream(kind)
        .reconnect_total
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one detected continuity gap for a fixed stream kind.
pub fn record_stream_gap(kind: OperationalStreamKind) {
    metrics()
        .stream(kind)
        .gap_total
        .fetch_add(1, Ordering::Relaxed);
}

/// Records bounded-queue loss without exposing the dropped payload.
pub fn record_stream_queue_drop(kind: OperationalStreamKind, count: u64) {
    metrics()
        .stream(kind)
        .queue_drop_total
        .fetch_add(count, Ordering::Relaxed);
}

/// Records one sanitized REST response and its rate-limit watermarks.
pub fn record_operational_rest_response(observation: OperationalRestObservation) {
    let rest = &metrics().rest;
    rest.request_total.fetch_add(1, Ordering::Relaxed);
    match observation.status {
        200..=299 => {
            rest.status_2xx_total.fetch_add(1, Ordering::Relaxed);
        }
        400..=499 => {
            rest.status_4xx_total.fetch_add(1, Ordering::Relaxed);
        }
        500..=599 => {
            rest.status_5xx_total.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    if observation.status == 429 {
        rest.status_429_total.fetch_add(1, Ordering::Relaxed);
    }
    rest.last_latency_micros
        .store(observation.latency_micros, Ordering::Relaxed);
    rest.max_latency_micros
        .fetch_max(observation.latency_micros, Ordering::Relaxed);
    if let Some(used_weight) = observation.used_weight {
        rest.used_weight.store(used_weight, Ordering::Relaxed);
    }
    if let Some(order_count) = observation.order_count {
        rest.order_count.store(order_count, Ordering::Relaxed);
    }
    if let Some(retry_after) = observation.retry_after_unix_seconds {
        rest.retry_after_unix_seconds
            .fetch_max(retry_after, Ordering::Relaxed);
    }
}

/// Records a transport failure without retaining a URL or remote error text.
pub fn record_operational_rest_transport_error(latency_micros: u64) {
    let rest = &metrics().rest;
    rest.request_total.fetch_add(1, Ordering::Relaxed);
    rest.transport_error_total.fetch_add(1, Ordering::Relaxed);
    rest.last_latency_micros
        .store(latency_micros, Ordering::Relaxed);
    rest.max_latency_micros
        .fetch_max(latency_micros, Ordering::Relaxed);
}

/// Records the signed-request clock correction without an account dimension.
pub fn record_operational_clock_skew_milliseconds(clock_skew_milliseconds: i64) {
    metrics()
        .rest
        .clock_skew_milliseconds
        .store(clock_skew_milliseconds, Ordering::Relaxed);
}

/// Records one durable journal append result.
pub fn record_journal_append(latency_micros: u64, succeeded: bool) {
    let journal = &metrics().journal;
    journal.append_total.fetch_add(1, Ordering::Relaxed);
    if !succeeded {
        journal.append_failure_total.fetch_add(1, Ordering::Relaxed);
    }
    journal
        .last_append_latency_micros
        .store(latency_micros, Ordering::Relaxed);
    journal
        .max_append_latency_micros
        .fetch_max(latency_micros, Ordering::Relaxed);
}

/// Updates the single-owner lifecycle gauge.
pub fn set_operational_owner_phase(phase: OperationalOwnerPhase) {
    let registry = metrics();
    let previous = registry.owner_phase.swap(phase as u8, Ordering::Relaxed);
    if phase == OperationalOwnerPhase::RecoveryRequired
        && OperationalOwnerPhase::from_u8(previous) != OperationalOwnerPhase::RecoveryRequired
    {
        registry
            .owner_recovery_required_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Returns a bounded, secret-free snapshot of process metrics.
#[must_use]
pub fn operational_metrics_snapshot() -> OperationalMetricsSnapshot {
    let registry = metrics();
    OperationalMetricsSnapshot {
        process_started_at_unix_seconds: registry.process_started_at_unix_seconds,
        market_stream: registry.market_stream.snapshot(),
        user_data_stream: registry.user_data_stream.snapshot(),
        rest: registry.rest.snapshot(),
        journal: registry.journal.snapshot(),
        owner_phase: OperationalOwnerPhase::from_u8(registry.owner_phase.load(Ordering::Relaxed)),
        owner_recovery_required_total: registry
            .owner_recovery_required_total
            .load(Ordering::Relaxed),
    }
}

/// Renders the fixed registry in Prometheus text exposition format.
#[must_use]
pub fn render_prometheus_metrics(
    snapshot: &OperationalMetricsSnapshot,
    now_unix_seconds: u64,
) -> String {
    let mut output = String::with_capacity(4_096);
    output.push_str("# TYPE crypto_trading_process_up gauge\ncrypto_trading_process_up 1\n");
    let _ = writeln!(
        output,
        "# TYPE crypto_trading_process_start_time_seconds gauge\ncrypto_trading_process_start_time_seconds {}",
        snapshot.process_started_at_unix_seconds
    );
    render_stream_metrics(
        &mut output,
        "market",
        snapshot.market_stream,
        now_unix_seconds,
    );
    render_stream_metrics(
        &mut output,
        "user_data",
        snapshot.user_data_stream,
        now_unix_seconds,
    );
    let rest = snapshot.rest;
    let _ = writeln!(
        output,
        "# TYPE crypto_trading_rest_request_total counter\ncrypto_trading_rest_request_total {}",
        rest.request_total
    );
    let _ = writeln!(
        output,
        "crypto_trading_rest_transport_error_total {}",
        rest.transport_error_total
    );
    for (class, count) in [
        ("2xx", rest.status_2xx_total),
        ("4xx", rest.status_4xx_total),
        ("5xx", rest.status_5xx_total),
        ("429", rest.status_429_total),
    ] {
        let _ = writeln!(
            output,
            "crypto_trading_rest_status_total{{class=\"{class}\"}} {count}"
        );
    }
    let _ = writeln!(
        output,
        "crypto_trading_rest_last_latency_microseconds {}\ncrypto_trading_rest_max_latency_microseconds {}\ncrypto_trading_binance_used_weight {}\ncrypto_trading_binance_order_count {}\ncrypto_trading_rest_retry_after_timestamp_seconds {}\ncrypto_trading_clock_skew_milliseconds {}",
        rest.last_latency_micros,
        rest.max_latency_micros,
        rest.used_weight,
        rest.order_count,
        rest.retry_after_unix_seconds,
        rest.clock_skew_milliseconds
    );
    let journal = snapshot.journal;
    let _ = writeln!(
        output,
        "# TYPE crypto_trading_journal_append_total counter\ncrypto_trading_journal_append_total {}\ncrypto_trading_journal_append_failure_total {}\ncrypto_trading_journal_last_append_latency_microseconds {}\ncrypto_trading_journal_max_append_latency_microseconds {}",
        journal.append_total,
        journal.append_failure_total,
        journal.last_append_latency_micros,
        journal.max_append_latency_micros
    );
    output.push_str("# TYPE crypto_trading_owner_phase gauge\n");
    for (phase, label) in OperationalOwnerPhase::ALL {
        let value = u8::from(snapshot.owner_phase == phase);
        let _ = writeln!(
            output,
            "crypto_trading_owner_phase{{phase=\"{label}\"}} {value}"
        );
    }
    let _ = writeln!(
        output,
        "# TYPE crypto_trading_owner_recovery_required_total counter\ncrypto_trading_owner_recovery_required_total {}",
        snapshot.owner_recovery_required_total
    );
    output
}

fn render_stream_metrics(
    output: &mut String,
    label: &str,
    stream: OperationalStreamMetricsSnapshot,
    now_unix_seconds: u64,
) {
    let observed = u8::from(stream.last_frame_unix_seconds != 0);
    let age = if observed == 0 {
        0
    } else {
        now_unix_seconds.saturating_sub(stream.last_frame_unix_seconds)
    };
    let _ = writeln!(
        output,
        "crypto_trading_stream_observed{{stream=\"{label}\"}} {observed}\ncrypto_trading_stream_generation{{stream=\"{label}\"}} {}\ncrypto_trading_stream_last_frame_timestamp_seconds{{stream=\"{label}\"}} {}\ncrypto_trading_stream_age_seconds{{stream=\"{label}\"}} {age}\ncrypto_trading_stream_reconnect_total{{stream=\"{label}\"}} {}\ncrypto_trading_stream_gap_total{{stream=\"{label}\"}} {}\ncrypto_trading_stream_queue_drop_total{{stream=\"{label}\"}} {}",
        stream.generation,
        stream.last_frame_unix_seconds,
        stream.reconnect_total,
        stream.gap_total,
        stream.queue_drop_total
    );
}
