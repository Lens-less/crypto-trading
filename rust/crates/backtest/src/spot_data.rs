use std::{collections::HashSet, path::Path, str::FromStr};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketType, Price, Symbol};
use rust_decimal::Decimal;

use crate::{BacktestError, sha256::sha256};

const OFFICIAL_SPOT_PREFIX: &str = "https://data.binance.vision/data/spot/";
const PARSER_VERSION: &str = "binance-spot-kline-v1";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    /// Parses a canonical SHA-256 digest.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidChecksumFormat`] unless `value` is
    /// exactly 64 hexadecimal characters.
    pub fn new(value: &str) -> Result<Self, BacktestError> {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BacktestError::InvalidChecksumFormat);
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    /// Computes a SHA-256 digest over exact bytes without external I/O.
    #[must_use]
    pub fn from_bytes(value: &[u8]) -> Self {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let bytes = sha256(value);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(DIGITS[usize::from(byte >> 4)]));
            output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
        }
        Self(output)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampUnit {
    Milliseconds,
    Microseconds,
}

impl TimestampUnit {
    fn tick_micros(self) -> i64 {
        match self {
            Self::Milliseconds => 1_000,
            Self::Microseconds => 1,
        }
    }

    fn parse_timestamp(self, value: i64) -> Result<DateTime<Utc>, BacktestError> {
        match self {
            Self::Milliseconds => Utc
                .timestamp_millis_opt(value)
                .single()
                .ok_or(BacktestError::InvalidBarSequence),
            Self::Microseconds => {
                let seconds = value.div_euclid(1_000_000);
                let micros = u32::try_from(value.rem_euclid(1_000_000))
                    .map_err(|_| BacktestError::InvalidBarSequence)?;
                Utc.timestamp_opt(seconds, micros * 1_000)
                    .single()
                    .ok_or(BacktestError::InvalidBarSequence)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetManifest {
    pub source_url: String,
    pub retrieved_at: DateTime<Utc>,
    pub venue: String,
    pub product: MarketType,
    pub symbol: Symbol,
    pub interval_micros: i64,
    pub timezone: String,
    pub timestamp_unit: TimestampUnit,
    pub archive_sha256: Sha256Digest,
    pub content_sha256: Sha256Digest,
    pub parser_version: String,
    pub expected_first_open: DateTime<Utc>,
    pub expected_last_close: DateTime<Utc>,
    pub expected_bar_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotBar {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub trade_count: u64,
}

impl SpotBar {
    /// Builds one closed Spot bar after validating its price and volume shape.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidBarSequence`] for an inverted timestamp,
    /// inconsistent OHLC values, or negative volume.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        open_time: DateTime<Utc>,
        close_time: DateTime<Utc>,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Decimal,
        quote_volume: Decimal,
        trade_count: u64,
    ) -> Result<Self, BacktestError> {
        if close_time < open_time
            || high < low
            || high < open
            || high < close
            || low > open
            || low > close
            || volume.is_sign_negative()
            || quote_volume.is_sign_negative()
        {
            return Err(BacktestError::InvalidBarSequence);
        }

        Ok(Self {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotKlineDataset {
    manifests: Vec<DatasetManifest>,
    bars: Vec<SpotBar>,
}

impl SpotKlineDataset {
    /// Parses a checksum-matched, frozen Binance Spot kline CSV payload.
    ///
    /// # Errors
    ///
    /// Returns a typed error when provenance is unsupported, checksum evidence
    /// differs, a row is malformed, timestamps are ambiguous or discontinuous,
    /// the expected range is incomplete, or the terminal bar was not closed at
    /// `sealed_at`.
    pub fn parse_csv(
        manifest: DatasetManifest,
        csv: &str,
        archive_sha256: &Sha256Digest,
        sealed_at: DateTime<Utc>,
    ) -> Result<Self, BacktestError> {
        validate_manifest(&manifest)?;
        if archive_sha256 != &manifest.archive_sha256
            || Sha256Digest::from_bytes(csv.as_bytes()) != manifest.content_sha256
        {
            return Err(BacktestError::ChecksumMismatch);
        }

        let mut bars = Vec::new();
        let mut previous_open: Option<DateTime<Utc>> = None;
        let interval = Duration::microseconds(manifest.interval_micros);
        let close_offset = manifest
            .interval_micros
            .checked_sub(manifest.timestamp_unit.tick_micros())
            .ok_or(BacktestError::InvalidBarSequence)?;

        for line in csv.lines().filter(|line| !line.trim().is_empty()) {
            let fields = line.trim_end_matches('\r').split(',').collect::<Vec<_>>();
            if fields.len() != 12 {
                return Err(BacktestError::InvalidBarSequence);
            }

            let open_time = manifest
                .timestamp_unit
                .parse_timestamp(parse_i64(fields[0])?)?;
            let close_time = manifest
                .timestamp_unit
                .parse_timestamp(parse_i64(fields[6])?)?;
            let open = parse_price(fields[1])?;
            let high = parse_price(fields[2])?;
            let low = parse_price(fields[3])?;
            let close = parse_price(fields[4])?;
            let volume = parse_decimal(fields[5])?;
            let quote_volume = parse_decimal(fields[7])?;
            let trade_count = parse_u64(fields[8])?;
            let _ = parse_decimal(fields[9])?;
            let _ = parse_decimal(fields[10])?;
            let _ = parse_decimal(fields[11])?;

            let expected_close = open_time
                .checked_add_signed(Duration::microseconds(close_offset))
                .ok_or(BacktestError::InvalidBarSequence)?;
            if close_time != expected_close {
                return Err(BacktestError::InvalidBarSequence);
            }

            if let Some(previous_open) = previous_open {
                let expected_open = previous_open
                    .checked_add_signed(interval)
                    .ok_or(BacktestError::InvalidBarSequence)?;
                if open_time != expected_open {
                    return Err(BacktestError::InvalidBarSequence);
                }
            }

            bars.push(SpotBar::new(
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count,
            )?);
            previous_open = Some(open_time);
        }

        if bars.len() != manifest.expected_bar_count {
            return Err(BacktestError::IncompleteDataset {
                expected: manifest.expected_bar_count,
                actual: bars.len(),
            });
        }

        let Some(first_bar) = bars.first() else {
            return Err(BacktestError::InvalidBarSequence);
        };
        let Some(last_bar) = bars.last() else {
            return Err(BacktestError::InvalidBarSequence);
        };

        if first_bar.open_time != manifest.expected_first_open
            || last_bar.close_time != manifest.expected_last_close
        {
            return Err(BacktestError::InvalidBarSequence);
        }
        if last_bar.close_time >= sealed_at {
            return Err(BacktestError::StillOpenBar);
        }

        Ok(Self {
            manifests: vec![manifest],
            bars,
        })
    }

    /// Returns the first archive manifest for legacy single-archive callers.
    ///
    /// # Panics
    ///
    /// Panics only if an internal constructor violated the invariant that
    /// verified spot datasets retain at least one manifest.
    pub fn manifest(&self) -> &DatasetManifest {
        self.manifests
            .first()
            .expect("spot datasets always retain at least one manifest")
    }

    /// Returns the ordered archive manifests that back this verified dataset.
    pub fn manifests(&self) -> &[DatasetManifest] {
        &self.manifests
    }

    /// Merges verified spot datasets while preserving caller-supplied archive order.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidBarSequence`] when the input is empty,
    /// manifest contracts differ, source URLs repeat, or any boundary between
    /// component datasets is non-contiguous.
    pub fn merge_verified(datasets: Vec<SpotKlineDataset>) -> Result<Self, BacktestError> {
        let mut datasets = datasets.into_iter();
        let Some(first_dataset) = datasets.next() else {
            return Err(BacktestError::InvalidBarSequence);
        };

        let merge_root = first_dataset.manifest().clone();
        let interval = Duration::microseconds(merge_root.interval_micros);

        let mut manifests = Vec::new();
        let mut bars = Vec::new();
        let mut seen_source_urls = HashSet::new();
        let mut previous_open: Option<DateTime<Utc>> = None;

        extend_verified_dataset(
            first_dataset,
            &merge_root,
            interval,
            &mut seen_source_urls,
            &mut previous_open,
            &mut manifests,
            &mut bars,
        )?;

        for dataset in datasets {
            extend_verified_dataset(
                dataset,
                &merge_root,
                interval,
                &mut seen_source_urls,
                &mut previous_open,
                &mut manifests,
                &mut bars,
            )?;
        }

        Ok(Self { manifests, bars })
    }

    pub fn bars(&self) -> &[SpotBar] {
        &self.bars
    }
}

fn extend_verified_dataset(
    dataset: SpotKlineDataset,
    merge_root: &DatasetManifest,
    interval: Duration,
    seen_source_urls: &mut HashSet<String>,
    previous_open: &mut Option<DateTime<Utc>>,
    manifests: &mut Vec<DatasetManifest>,
    bars: &mut Vec<SpotBar>,
) -> Result<(), BacktestError> {
    let SpotKlineDataset {
        manifests: dataset_manifests,
        bars: dataset_bars,
    } = dataset;

    let first_bar = dataset_bars
        .first()
        .ok_or(BacktestError::InvalidBarSequence)?;
    let last_bar = dataset_bars
        .last()
        .ok_or(BacktestError::InvalidBarSequence)?;

    if let Some(previous_open_time) = previous_open {
        let expected_open = previous_open_time
            .checked_add_signed(interval)
            .ok_or(BacktestError::InvalidBarSequence)?;
        if first_bar.open_time != expected_open {
            return Err(BacktestError::InvalidBarSequence);
        }
    }

    validate_bar_chain(&dataset_bars, interval)?;

    for manifest in &dataset_manifests {
        validate_manifest(manifest)?;
        if !same_merge_contract(merge_root, manifest)
            || !seen_source_urls.insert(manifest.source_url.clone())
        {
            return Err(BacktestError::InvalidBarSequence);
        }
    }

    *previous_open = Some(last_bar.open_time);
    manifests.extend(dataset_manifests);
    bars.extend(dataset_bars);
    Ok(())
}

fn same_merge_contract(left: &DatasetManifest, right: &DatasetManifest) -> bool {
    left.venue.eq_ignore_ascii_case(&right.venue)
        && left.product == right.product
        && left.symbol == right.symbol
        && left.interval_micros == right.interval_micros
        && left.timezone == right.timezone
        && left.parser_version == right.parser_version
}

fn validate_bar_chain(bars: &[SpotBar], interval: Duration) -> Result<(), BacktestError> {
    let mut previous_open: Option<DateTime<Utc>> = None;
    for bar in bars {
        if let Some(previous_open_time) = previous_open {
            let expected_open = previous_open_time
                .checked_add_signed(interval)
                .ok_or(BacktestError::InvalidBarSequence)?;
            if bar.open_time != expected_open {
                return Err(BacktestError::InvalidBarSequence);
            }
        }
        previous_open = Some(bar.open_time);
    }
    Ok(())
}

fn validate_manifest(manifest: &DatasetManifest) -> Result<(), BacktestError> {
    if manifest.product != MarketType::Spot {
        return Err(BacktestError::UnsupportedDerivativesMarginModel);
    }

    if !manifest.venue.eq_ignore_ascii_case("binance")
        || manifest.timezone != "UTC"
        || manifest.parser_version != PARSER_VERSION
        || manifest.interval_micros <= 0
        || manifest.interval_micros % manifest.timestamp_unit.tick_micros() != 0
        || !manifest.source_url.starts_with(OFFICIAL_SPOT_PREFIX)
        || !Path::new(&manifest.source_url)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        || !manifest
            .source_url
            .contains(&format!("/klines/{}/", manifest.symbol.as_str()))
    {
        return Err(BacktestError::InvalidBarSequence);
    }

    Ok(())
}

fn parse_decimal(value: &str) -> Result<Decimal, BacktestError> {
    Decimal::from_str(value).map_err(|_| BacktestError::InvalidBarSequence)
}

fn parse_price(value: &str) -> Result<Price, BacktestError> {
    Price::new(parse_decimal(value)?).map_err(|_| BacktestError::InvalidBarSequence)
}

fn parse_i64(value: &str) -> Result<i64, BacktestError> {
    value
        .parse::<i64>()
        .map_err(|_| BacktestError::InvalidBarSequence)
}

fn parse_u64(value: &str) -> Result<u64, BacktestError> {
    value
        .parse::<u64>()
        .map_err(|_| BacktestError::InvalidBarSequence)
}
