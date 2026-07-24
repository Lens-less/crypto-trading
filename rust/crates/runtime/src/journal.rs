use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub const OPERATION_EVENT_SCHEMA_VERSION: u16 = 1;
pub const JOURNAL_CURSOR_SCHEMA_VERSION: u16 = 1;
/// Maximum JSON body size; the JSONL delimiter brings a record to at most 1 MiB.
pub const MAX_OPERATION_EVENT_BYTES: usize = 1_048_575;
pub const MAX_OPERATION_EVENT_PAYLOAD_BYTES: usize = 1_000_000;
pub const MAX_JOURNAL_CURSOR_BYTES: usize = 160;

const MAX_EVENT_LABEL_BYTES: usize = 128;
const INTEGRITY_ALGORITHM: &str = "fnv1a64";
const CURSOR_FAMILY: &str = "ctc";
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stable aggregate identity carried by every operation event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AggregateRef {
    kind: String,
    id: Uuid,
}

impl AggregateRef {
    /// Creates a validated aggregate reference.
    ///
    /// # Errors
    ///
    /// Returns [`EventContractError`] when `kind` is not a bounded event label
    /// or `id` is nil.
    pub fn new(kind: impl Into<String>, id: Uuid) -> Result<Self, EventContractError> {
        let aggregate = Self {
            kind: kind.into(),
            id,
        };
        aggregate.validate()?;
        Ok(aggregate)
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    fn validate(&self) -> Result<(), EventContractError> {
        validate_label("aggregate kind", &self.kind)?;
        validate_non_nil_id("aggregate id", self.id)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateRefWire {
    kind: String,
    id: Uuid,
}

impl<'de> Deserialize<'de> for AggregateRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AggregateRefWire::deserialize(deserializer)?;
        Self::new(wire.kind, wire.id).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct EventIntegrity {
    algorithm: &'static str,
    checksum: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventIntegrityWire {
    algorithm: String,
    checksum: String,
}

/// Versioned journal envelope consumed by trusted journal/control-plane adapters.
///
/// `sequence` is the only ordering authority. `recorded_at` is descriptive and
/// must never be used to repair or reorder a stream. The integrity checksum
/// detects accidental mutation; it is not a signature and conveys no trust.
/// Operator transports must project or redact payloads instead of serializing
/// this internal envelope directly.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperationEventEnvelope {
    schema_version: u16,
    journal_id: Uuid,
    sequence: u64,
    event_id: Uuid,
    recorded_at: DateTime<Utc>,
    kind: String,
    aggregate: AggregateRef,
    producer: String,
    payload: Value,
    integrity: EventIntegrity,
}

impl OperationEventEnvelope {
    /// Creates and checksums one v1 operation event.
    ///
    /// # Errors
    ///
    /// Returns [`EventContractError`] for invalid identifiers, labels, sequence
    /// values, serialization failures, or resource-budget violations.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        journal_id: Uuid,
        sequence: u64,
        event_id: Uuid,
        recorded_at: DateTime<Utc>,
        kind: impl Into<String>,
        aggregate: AggregateRef,
        producer: impl Into<String>,
        payload: Value,
    ) -> Result<Self, EventContractError> {
        let mut event = Self {
            schema_version: OPERATION_EVENT_SCHEMA_VERSION,
            journal_id,
            sequence,
            event_id,
            recorded_at,
            kind: kind.into(),
            aggregate,
            producer: producer.into(),
            payload,
            integrity: EventIntegrity {
                algorithm: INTEGRITY_ALGORITHM,
                checksum: String::new(),
            },
        };
        event.validate_fields()?;
        event.integrity.checksum = event.expected_checksum()?;
        event.validate_size()?;
        Ok(event)
    }

    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    #[must_use]
    pub const fn recorded_at(&self) -> DateTime<Utc> {
        self.recorded_at
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn aggregate(&self) -> &AggregateRef {
        &self.aggregate
    }

    #[must_use]
    pub fn producer(&self) -> &str {
        &self.producer
    }

    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }

    #[must_use]
    pub const fn integrity_algorithm(&self) -> &'static str {
        self.integrity.algorithm
    }

    #[must_use]
    pub fn integrity_checksum(&self) -> &str {
        &self.integrity.checksum
    }

    /// Revalidates size, identifiers, labels, and the corruption checksum.
    ///
    /// # Errors
    ///
    /// Returns [`EventContractError`] when the envelope has drifted from its
    /// versioned contract.
    pub fn validate(&self) -> Result<(), EventContractError> {
        self.validate_fields()?;
        if self.integrity.algorithm != INTEGRITY_ALGORITHM {
            return Err(EventContractError::UnsupportedIntegrityAlgorithm);
        }
        let expected = self.expected_checksum()?;
        if self.integrity.checksum != expected {
            return Err(EventContractError::IntegrityMismatch);
        }
        self.validate_size()
    }

    fn validate_fields(&self) -> Result<(), EventContractError> {
        if self.schema_version != OPERATION_EVENT_SCHEMA_VERSION {
            return Err(EventContractError::UnsupportedSchema(self.schema_version));
        }
        validate_non_nil_id("journal id", self.journal_id)?;
        validate_non_nil_id("event id", self.event_id)?;
        if self.sequence == 0 {
            return Err(EventContractError::InvalidSequence);
        }
        validate_label("event kind", &self.kind)?;
        self.aggregate.validate()?;
        validate_label("event producer", &self.producer)?;
        let payload_bytes = serde_json::to_vec(&self.payload)?.len();
        if payload_bytes > MAX_OPERATION_EVENT_PAYLOAD_BYTES {
            return Err(EventContractError::PayloadTooLarge {
                bytes: payload_bytes,
                limit: MAX_OPERATION_EVENT_PAYLOAD_BYTES,
            });
        }
        Ok(())
    }

    fn validate_size(&self) -> Result<(), EventContractError> {
        let bytes = serde_json::to_vec(self)?.len();
        if bytes > MAX_OPERATION_EVENT_BYTES {
            return Err(EventContractError::EnvelopeTooLarge {
                bytes,
                limit: MAX_OPERATION_EVENT_BYTES,
            });
        }
        Ok(())
    }

    fn expected_checksum(&self) -> Result<String, EventContractError> {
        let input = EventChecksumInput {
            schema_version: self.schema_version,
            journal_id: self.journal_id,
            sequence: self.sequence,
            event_id: self.event_id,
            recorded_at: self.recorded_at,
            kind: &self.kind,
            aggregate: &self.aggregate,
            producer: &self.producer,
            payload: &self.payload,
        };
        Ok(checksum_hex(&serde_json::to_vec(&input)?))
    }
}

#[derive(Serialize)]
struct EventChecksumInput<'a> {
    schema_version: u16,
    journal_id: Uuid,
    sequence: u64,
    event_id: Uuid,
    recorded_at: DateTime<Utc>,
    kind: &'a str,
    aggregate: &'a AggregateRef,
    producer: &'a str,
    payload: &'a Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationEventWire {
    schema_version: u16,
    journal_id: Uuid,
    sequence: u64,
    event_id: Uuid,
    recorded_at: DateTime<Utc>,
    kind: String,
    aggregate: AggregateRef,
    producer: String,
    payload: Value,
    integrity: EventIntegrityWire,
}

impl<'de> Deserialize<'de> for OperationEventEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = OperationEventWire::deserialize(deserializer)?;
        if wire.integrity.algorithm != INTEGRITY_ALGORITHM {
            return Err(D::Error::custom(
                EventContractError::UnsupportedIntegrityAlgorithm,
            ));
        }
        let event = Self {
            schema_version: wire.schema_version,
            journal_id: wire.journal_id,
            sequence: wire.sequence,
            event_id: wire.event_id,
            recorded_at: wire.recorded_at,
            kind: wire.kind,
            aggregate: wire.aggregate,
            producer: wire.producer,
            payload: wire.payload,
            integrity: EventIntegrity {
                algorithm: INTEGRITY_ALGORITHM,
                checksum: wire.integrity.checksum,
            },
        };
        event.validate().map_err(D::Error::custom)?;
        Ok(event)
    }
}

/// Opaque resume token for operation-journal consumers.
///
/// The textual representation is a transport contract, not a client API:
/// callers must store and return it unchanged. It intentionally contains no
/// filesystem path, inode, or wall-clock value. Its FNV checksum detects
/// accidental mutation only; it is forgeable, grants no authority, and never
/// replaces a journal reader's boundary verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursor {
    journal_id: Uuid,
    after_sequence: u64,
    next_offset: u64,
    last_event_id: Uuid,
}

impl JournalCursor {
    /// Creates a cursor immediately after an event at `next_offset`.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError`] when the event or offset cannot describe a
    /// resumable position.
    pub fn after_event(
        event: &OperationEventEnvelope,
        next_offset: u64,
    ) -> Result<Self, CursorError> {
        if next_offset == 0 {
            return Err(CursorError::InvalidValue("next offset must be positive"));
        }
        Ok(Self {
            journal_id: event.journal_id,
            after_sequence: event.sequence,
            next_offset,
            last_event_id: event.event_id,
        })
    }

    /// Parses and validates an opaque cursor token.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError`] for malformed, oversized, unsupported, or
    /// checksum-mismatched values.
    pub fn decode(encoded: &str) -> Result<Self, CursorError> {
        if encoded.len() > MAX_JOURNAL_CURSOR_BYTES {
            return Err(CursorError::TooLong {
                bytes: encoded.len(),
                limit: MAX_JOURNAL_CURSOR_BYTES,
            });
        }
        let (body, supplied_checksum) = encoded
            .rsplit_once('.')
            .ok_or(CursorError::Malformed("missing checksum"))?;
        if supplied_checksum.len() != 16
            || !supplied_checksum
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CursorError::Malformed("checksum must be 16 hex digits"));
        }
        if checksum_hex(body.as_bytes()) != supplied_checksum.to_ascii_lowercase() {
            return Err(CursorError::ChecksumMismatch);
        }

        let parts = body.split('.').collect::<Vec<_>>();
        let [
            family,
            version,
            journal_id,
            sequence,
            next_offset,
            last_event_id,
        ] = parts.as_slice()
        else {
            return Err(CursorError::Malformed("unexpected cursor field count"));
        };
        if *family != CURSOR_FAMILY {
            return Err(CursorError::Malformed("unknown cursor family"));
        }
        let version = version
            .parse::<u16>()
            .map_err(|_| CursorError::Malformed("invalid cursor version"))?;
        if version != JOURNAL_CURSOR_SCHEMA_VERSION {
            return Err(CursorError::UnsupportedVersion(version));
        }

        let cursor = Self {
            journal_id: Uuid::parse_str(journal_id)
                .map_err(|_| CursorError::Malformed("invalid journal id"))?,
            after_sequence: parse_hex_u64(sequence, "invalid sequence")?,
            next_offset: parse_hex_u64(next_offset, "invalid next offset")?,
            last_event_id: Uuid::parse_str(last_event_id)
                .map_err(|_| CursorError::Malformed("invalid event id"))?,
        };
        cursor.validate_values()?;
        Ok(cursor)
    }

    /// Preflights a cursor against a generation and current file boundary.
    ///
    /// This does not prove that `last_event_id` is still anchored at the
    /// advertised offset. A journal reader must verify that anchor before it
    /// advances or falls back to a bounded scan.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError::Expired`] when the source was replaced,
    /// truncated, or has not yet reached the cursor offset.
    pub fn validate_source_bounds(
        &self,
        journal_id: Uuid,
        file_len: u64,
    ) -> Result<(), CursorError> {
        if self.journal_id != journal_id || self.next_offset > file_len {
            return Err(CursorError::Expired);
        }
        Ok(())
    }

    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub const fn after_sequence(&self) -> u64 {
        self.after_sequence
    }

    #[must_use]
    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    #[must_use]
    pub const fn last_event_id(&self) -> Uuid {
        self.last_event_id
    }

    fn encode(&self) -> String {
        let body = format!(
            "{CURSOR_FAMILY}.{JOURNAL_CURSOR_SCHEMA_VERSION}.{}.{:016x}.{:016x}.{}",
            self.journal_id.simple(),
            self.after_sequence,
            self.next_offset,
            self.last_event_id.simple()
        );
        format!("{body}.{}", checksum_hex(body.as_bytes()))
    }

    fn validate_values(&self) -> Result<(), CursorError> {
        if self.journal_id.is_nil() {
            return Err(CursorError::InvalidValue("journal id must not be nil"));
        }
        if self.after_sequence == 0 {
            return Err(CursorError::InvalidValue("after sequence must be positive"));
        }
        if self.next_offset == 0 {
            return Err(CursorError::InvalidValue("next offset must be positive"));
        }
        if self.last_event_id.is_nil() {
            return Err(CursorError::InvalidValue("event id must not be nil"));
        }
        Ok(())
    }
}

impl fmt::Display for JournalCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.encode())
    }
}

impl FromStr for JournalCursor {
    type Err = CursorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::decode(value)
    }
}

impl Serialize for JournalCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.encode())
    }
}

impl<'de> Deserialize<'de> for JournalCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::decode(&encoded).map_err(D::Error::custom)
    }
}

#[derive(Debug, Error)]
pub enum EventContractError {
    #[error("unsupported operation event schema version {0}")]
    UnsupportedSchema(u16),
    #[error("{field} must not be nil")]
    NilId { field: &'static str },
    #[error("operation event sequence must be positive")]
    InvalidSequence,
    #[error("{field} must be 1..={MAX_EVENT_LABEL_BYTES} bytes of lowercase ASCII label text")]
    InvalidLabel { field: &'static str },
    #[error("operation event payload has {bytes} bytes; maximum is {limit}")]
    PayloadTooLarge { bytes: usize, limit: usize },
    #[error("operation event envelope has {bytes} bytes; maximum is {limit}")]
    EnvelopeTooLarge { bytes: usize, limit: usize },
    #[error("unsupported operation event integrity algorithm")]
    UnsupportedIntegrityAlgorithm,
    #[error("operation event integrity checksum does not match its content")]
    IntegrityMismatch,
    #[error("failed to serialize operation event contract: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("journal cursor has {bytes} bytes; maximum is {limit}")]
    TooLong { bytes: usize, limit: usize },
    #[error("malformed journal cursor: {0}")]
    Malformed(&'static str),
    #[error("unsupported journal cursor version {0}")]
    UnsupportedVersion(u16),
    #[error("journal cursor checksum does not match its content")]
    ChecksumMismatch,
    #[error("invalid journal cursor: {0}")]
    InvalidValue(&'static str),
    #[error(
        "journal cursor expired because its source generation or file boundary no longer matches"
    )]
    Expired,
}

fn validate_non_nil_id(field: &'static str, id: Uuid) -> Result<(), EventContractError> {
    if id.is_nil() {
        return Err(EventContractError::NilId { field });
    }
    Ok(())
}

fn validate_label(field: &'static str, value: &str) -> Result<(), EventContractError> {
    let valid_length = !value.is_empty() && value.len() <= MAX_EVENT_LABEL_BYTES;
    let valid_characters = value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });
    if !valid_length || !valid_characters {
        return Err(EventContractError::InvalidLabel { field });
    }
    Ok(())
}

fn parse_hex_u64(value: &str, message: &'static str) -> Result<u64, CursorError> {
    if value.len() != 16 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CursorError::Malformed(message));
    }
    u64::from_str_radix(value, 16).map_err(|_| CursorError::Malformed(message))
}

fn checksum_hex(bytes: &[u8]) -> String {
    format!("{:016x}", fnv1a64(bytes))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV1A64_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV1A64_PRIME)
    })
}
