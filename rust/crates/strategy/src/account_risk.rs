//! Pure account-level risk policy.
//!
//! This module ports the admission semantics of the legacy Python
//! `GlobalRiskController` (single-symbol and global exposure caps checked
//! before opening, total-balance thresholds for warning and forced close,
//! daily trade caps with UTC midnight reset, symbol deny/high-risk lists, and
//! pause/kill-switch gating) into a deterministic, I/O-free policy. A stateful
//! runtime authority owns durable facts and feeds observations into this
//! policy; every limit is optional and disabled by default.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::Money;
use rust_decimal::Decimal;

use crate::StrategyError;

/// Hard bound for configured symbol lists.
pub const MAX_ACCOUNT_RISK_SYMBOLS: usize = 256;
/// Hard bound for the optional maximum position duration.
pub const MAX_ACCOUNT_RISK_POSITION_DURATION_SECONDS: i64 = 30 * 24 * 60 * 60;
/// Hard bound for open-position observations fed into timeout evaluation.
pub const MAX_ACCOUNT_RISK_OPEN_POSITIONS: usize = 1_024;

const MAX_SYMBOL_BYTES: usize = 64;
const MAX_REASON_BYTES: usize = 128;

/// Optional account-level limits; `None` disables the corresponding check.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountRiskLimits {
    pub max_symbol_exposure: Option<Money>,
    pub max_total_exposure: Option<Money>,
    pub min_balance_warning: Option<Money>,
    pub min_balance_close: Option<Money>,
    pub max_position_duration: Option<Duration>,
    pub max_daily_trades: Option<u32>,
    pub disabled_symbols: BTreeSet<String>,
    pub high_risk_symbols: BTreeSet<String>,
}

/// Deterministic account-risk admission policy over validated limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskPolicy {
    limits: AccountRiskLimits,
}

/// One admission candidate plus the account observations it is judged against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskInput {
    /// Uppercased instrument identity of the candidate entry.
    pub candidate_symbol: String,
    /// Gross additional exposure the candidate would reserve.
    pub candidate_notional: Money,
    /// Current non-released exposure already attributed to the symbol.
    pub symbol_exposure: Money,
    /// Current non-released exposure across every symbol and account.
    pub total_exposure: Money,
    /// Total account balance (total, not merely available, mirroring the
    /// legacy controller's use of total balances).
    pub total_balance: Money,
    /// Admitted trades observed so far in the current UTC day.
    pub daily_trade_count: u32,
    /// Durable pause reason when admissions are suspended.
    pub paused_reason: Option<String>,
    /// Durable kill-switch reason when all new admissions are refused.
    pub kill_switch_reason: Option<String>,
}

/// Non-fatal annotations attached to an admitted candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskWarning {
    LowBalance { total_balance: Money, limit: Money },
    HighRiskSymbol { symbol: String },
}

impl AccountRiskWarning {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::LowBalance { .. } => "low_balance",
            Self::HighRiskSymbol { .. } => "high_risk_symbol",
        }
    }
}

/// Deterministic refusal reasons, ordered by evaluation priority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskRejection {
    KillSwitchEngaged {
        reason: String,
    },
    Paused {
        reason: String,
    },
    SymbolDisabled {
        symbol: String,
    },
    BalanceBelowCloseThreshold {
        total_balance: Money,
        limit: Money,
    },
    DailyTradeLimitReached {
        count: u32,
        limit: u32,
    },
    SymbolExposureExceeded {
        symbol: String,
        projected: Money,
        limit: Money,
    },
    TotalExposureExceeded {
        projected: Money,
        limit: Money,
    },
    InvalidCandidate,
    ArithmeticOverflow,
}

impl AccountRiskRejection {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::KillSwitchEngaged { .. } => "kill_switch_engaged",
            Self::Paused { .. } => "paused",
            Self::SymbolDisabled { .. } => "symbol_disabled",
            Self::BalanceBelowCloseThreshold { .. } => "balance_below_close_threshold",
            Self::DailyTradeLimitReached { .. } => "daily_trade_limit_reached",
            Self::SymbolExposureExceeded { .. } => "symbol_exposure_exceeded",
            Self::TotalExposureExceeded { .. } => "total_exposure_exceeded",
            Self::InvalidCandidate => "invalid_candidate",
            Self::ArithmeticOverflow => "arithmetic_overflow",
        }
    }
}

/// Outcome of one pure admission evaluation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskDecision {
    Admitted { warnings: Vec<AccountRiskWarning> },
    Rejected(AccountRiskRejection),
}

/// One open owner-level position observation for timeout evaluation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskOpenPosition {
    pub task_id: String,
    pub symbol: String,
    pub opened_at: DateTime<Utc>,
}

/// One position that exceeded the configured maximum holding duration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskTimeout {
    pub task_id: String,
    pub symbol: String,
    pub opened_at: DateTime<Utc>,
}

impl AccountRiskPolicy {
    /// Validates the optional limits and constructs the pure policy.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for non-positive limits, a
    /// warning threshold below the close threshold, an out-of-bound duration,
    /// a zero daily-trade cap, or unsafe/unbounded symbol lists.
    pub fn new(limits: AccountRiskLimits) -> Result<Self, StrategyError> {
        for (label, value) in [
            ("maximum symbol exposure", limits.max_symbol_exposure),
            ("maximum total exposure", limits.max_total_exposure),
            ("minimum balance warning", limits.min_balance_warning),
            ("minimum balance close", limits.min_balance_close),
        ] {
            if value.is_some_and(|value| value.as_decimal() <= Decimal::ZERO) {
                let _ = label;
                return Err(StrategyError::InvalidConfig(
                    "account risk limits must be positive when configured",
                ));
            }
        }
        if let (Some(warning), Some(close)) = (limits.min_balance_warning, limits.min_balance_close)
            && warning < close
        {
            return Err(StrategyError::InvalidConfig(
                "balance warning threshold must not be below the close threshold",
            ));
        }
        if let Some(duration) = limits.max_position_duration
            && (duration <= Duration::zero()
                || duration > Duration::seconds(MAX_ACCOUNT_RISK_POSITION_DURATION_SECONDS))
        {
            return Err(StrategyError::InvalidConfig(
                "maximum position duration is outside the supported bound",
            ));
        }
        if limits.max_daily_trades == Some(0) {
            return Err(StrategyError::InvalidConfig(
                "maximum daily trades must be positive when configured",
            ));
        }
        for symbols in [&limits.disabled_symbols, &limits.high_risk_symbols] {
            if symbols.len() > MAX_ACCOUNT_RISK_SYMBOLS {
                return Err(StrategyError::InvalidConfig(
                    "account risk symbol list exceeds the supported bound",
                ));
            }
            for symbol in symbols {
                if !safe_symbol(symbol) {
                    return Err(StrategyError::InvalidConfig(
                        "account risk symbol list entry is unsafe",
                    ));
                }
            }
        }
        Ok(Self { limits })
    }

    #[must_use]
    pub const fn limits(&self) -> &AccountRiskLimits {
        &self.limits
    }

    /// Whether the candidate symbol carries the high-risk annotation.
    #[must_use]
    pub fn is_high_risk_symbol(&self, symbol: &str) -> bool {
        self.limits
            .high_risk_symbols
            .contains(&symbol.to_ascii_uppercase())
    }

    /// Evaluates one admission candidate against the account observations.
    ///
    /// The function is total: unrepresentable arithmetic and malformed
    /// candidates become deterministic rejections instead of panics.
    #[must_use]
    pub fn evaluate(&self, input: &AccountRiskInput) -> AccountRiskDecision {
        if let Some(reason) = &input.kill_switch_reason {
            return AccountRiskDecision::Rejected(AccountRiskRejection::KillSwitchEngaged {
                reason: bounded_reason(reason),
            });
        }
        if let Some(reason) = &input.paused_reason {
            return AccountRiskDecision::Rejected(AccountRiskRejection::Paused {
                reason: bounded_reason(reason),
            });
        }
        let symbol = input.candidate_symbol.to_ascii_uppercase();
        if !safe_symbol(&symbol) || input.candidate_notional <= Money::default() {
            return AccountRiskDecision::Rejected(AccountRiskRejection::InvalidCandidate);
        }
        if self.limits.disabled_symbols.contains(&symbol) {
            return AccountRiskDecision::Rejected(AccountRiskRejection::SymbolDisabled { symbol });
        }
        if let Some(limit) = self.limits.min_balance_close
            && input.total_balance < limit
        {
            return AccountRiskDecision::Rejected(
                AccountRiskRejection::BalanceBelowCloseThreshold {
                    total_balance: input.total_balance,
                    limit,
                },
            );
        }
        if let Some(limit) = self.limits.max_daily_trades
            && input.daily_trade_count >= limit
        {
            return AccountRiskDecision::Rejected(AccountRiskRejection::DailyTradeLimitReached {
                count: input.daily_trade_count,
                limit,
            });
        }
        if let Some(limit) = self.limits.max_symbol_exposure {
            let Some(projected) = checked_add(input.symbol_exposure, input.candidate_notional)
            else {
                return AccountRiskDecision::Rejected(AccountRiskRejection::ArithmeticOverflow);
            };
            if projected > limit {
                return AccountRiskDecision::Rejected(
                    AccountRiskRejection::SymbolExposureExceeded {
                        symbol,
                        projected,
                        limit,
                    },
                );
            }
        }
        if let Some(limit) = self.limits.max_total_exposure {
            let Some(projected) = checked_add(input.total_exposure, input.candidate_notional)
            else {
                return AccountRiskDecision::Rejected(AccountRiskRejection::ArithmeticOverflow);
            };
            if projected > limit {
                return AccountRiskDecision::Rejected(
                    AccountRiskRejection::TotalExposureExceeded { projected, limit },
                );
            }
        }
        let mut warnings = Vec::new();
        if let Some(limit) = self.limits.min_balance_warning
            && input.total_balance < limit
        {
            warnings.push(AccountRiskWarning::LowBalance {
                total_balance: input.total_balance,
                limit,
            });
        }
        if self.limits.high_risk_symbols.contains(&symbol) {
            warnings.push(AccountRiskWarning::HighRiskSymbol { symbol });
        }
        AccountRiskDecision::Admitted { warnings }
    }

    /// Returns every observed open position held longer than the configured
    /// maximum duration; an unset duration limit reports nothing.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when the observation set
    /// exceeds the supported bound.
    pub fn expired_positions(
        &self,
        positions: &[AccountRiskOpenPosition],
        now: DateTime<Utc>,
    ) -> Result<Vec<AccountRiskTimeout>, StrategyError> {
        if positions.len() > MAX_ACCOUNT_RISK_OPEN_POSITIONS {
            return Err(StrategyError::InvalidConfig(
                "open position observations exceed the supported bound",
            ));
        }
        let Some(limit) = self.limits.max_position_duration else {
            return Ok(Vec::new());
        };
        Ok(positions
            .iter()
            .filter(|position| now.signed_duration_since(position.opened_at) > limit)
            .map(|position| AccountRiskTimeout {
                task_id: position.task_id.clone(),
                symbol: position.symbol.clone(),
                opened_at: position.opened_at,
            })
            .collect())
    }
}

impl TryFrom<&crypto_trading_config::AccountRiskConfig> for AccountRiskPolicy {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::AccountRiskConfig) -> Result<Self, Self::Error> {
        let duration = config
            .max_position_duration_seconds
            .map(|seconds| {
                i64::try_from(seconds).map(Duration::seconds).map_err(|_| {
                    StrategyError::InvalidConfig(
                        "maximum position duration is outside the supported bound",
                    )
                })
            })
            .transpose()?;
        Self::new(AccountRiskLimits {
            max_symbol_exposure: config.max_symbol_exposure.map(Money::new),
            max_total_exposure: config.max_total_exposure.map(Money::new),
            min_balance_warning: config.min_balance_warning.map(Money::new),
            min_balance_close: config.min_balance_close_position.map(Money::new),
            max_position_duration: duration,
            max_daily_trades: config.max_daily_trades,
            disabled_symbols: normalized_symbols(&config.disabled_symbols),
            high_risk_symbols: normalized_symbols(&config.high_risk_symbols),
        })
    }
}

fn normalized_symbols(symbols: &[String]) -> BTreeSet<String> {
    symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect()
}

fn checked_add(left: Money, right: Money) -> Option<Money> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
}

fn safe_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= MAX_SYMBOL_BYTES
        && symbol.trim() == symbol
        && !symbol.chars().any(char::is_control)
}

fn bounded_reason(reason: &str) -> String {
    let mut bounded = String::with_capacity(reason.len().min(MAX_REASON_BYTES));
    for character in reason.chars() {
        let safe = if character.is_control() {
            ' '
        } else {
            character
        };
        if bounded.len() + safe.len_utf8() > MAX_REASON_BYTES {
            break;
        }
        bounded.push(safe);
    }
    bounded
}
