//! Pure grid-protection state machines ported from the frozen Python grid
//! subsystem (`archive/python-legacy/core/services/grid/`).
//!
//! Five independent policies observe one price/position/equity snapshot and
//! emit a single [`GridDirective`]. [`GridProtectionMachine`] arbitrates them
//! with the legacy priority: stop-loss > capital protection > price lock >
//! take profit > scalping (`grid_config.py:196`). No policy performs I/O; the
//! runtime owns journaling and order actions.

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{Price, Quantity, Side};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::{AprCalculator, GridDirection, StrategyError};

const MAX_PROTECTION_LEVELS: u32 = 10_000;
const MAX_ESCAPE_TIMEOUT_SECONDS: u64 = 31_536_000;
const MAX_TRIGGER_PERCENT: u32 = 100;

/// Why a protection directive fired, bounded to the legacy trigger set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridProtectionReason {
    /// Adverse escape persisted and realtime APR met the threshold
    /// (`stop_loss_monitor.py:334-345`).
    StopLossAprRecovered,
    /// Adverse escape persisted and realtime APR missed the threshold
    /// (`stop_loss_monitor.py:334-350`).
    StopLossAprBelowThreshold,
    /// Collateral recovered to the protected principal
    /// (`capital_protection_manager.py:136-174`).
    CapitalRecovered,
    /// Price escaped favourably beyond the lock threshold
    /// (`price_lock_manager.py:44-97`).
    PriceLockActive,
    /// Equity profit rate reached the take-profit target
    /// (`take_profit_manager.py:79-119`).
    TakeProfitTarget,
    /// Grid progress crossed the scalping trigger
    /// (`scalping_manager.py:82-144`).
    ScalpingActive,
}

impl GridProtectionReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StopLossAprRecovered => "stop_loss_apr_recovered",
            Self::StopLossAprBelowThreshold => "stop_loss_apr_below_threshold",
            Self::CapitalRecovered => "capital_recovered",
            Self::PriceLockActive => "price_lock_active",
            Self::TakeProfitTarget => "take_profit_target",
            Self::ScalpingActive => "scalping_active",
        }
    }
}

/// One arbitration outcome the runtime must translate into durable facts and
/// paper actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridDirective {
    /// No protection fired; process the observation normally.
    Continue,
    /// Stop admitting new entry orders but keep the position and the task.
    FreezeEntries { reason: GridProtectionReason },
    /// Place one reduce-only take-profit order for the full tracked position.
    Scalp {
        reason: GridProtectionReason,
        side: Side,
        quantity: Quantity,
        take_profit_price: Price,
    },
    /// Flatten, cancel, rebuild the grid at the current price, and re-base the
    /// protected principal.
    ResetGrid { reason: GridProtectionReason },
    /// Flatten, cancel, and stop the task.
    ExitAll { reason: GridProtectionReason },
}

impl GridDirective {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::FreezeEntries { .. } => "freeze_entries",
            Self::Scalp { .. } => "scalp",
            Self::ResetGrid { .. } => "reset_grid",
            Self::ExitAll { .. } => "exit_all",
        }
    }

    #[must_use]
    pub const fn reason(&self) -> Option<GridProtectionReason> {
        match self {
            Self::Continue => None,
            Self::FreezeEntries { reason }
            | Self::Scalp { reason, .. }
            | Self::ResetGrid { reason }
            | Self::ExitAll { reason } => Some(*reason),
        }
    }
}

/// One pure observation of the market and the tracked paper account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridProtectionObservation {
    pub price: Price,
    pub observed_at: DateTime<Utc>,
    /// Signed net position quantity; positive means long exposure.
    pub position_quantity: Decimal,
    /// Account equity observed now, in quote currency.
    pub current_collateral: Decimal,
    /// Completed grid cycles per hour feeding the stop-loss APR gate.
    pub cycles_per_hour: Decimal,
}

/// Validated fixed-grid geometry shared by every protection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridProtectionGeometry {
    direction: GridDirection,
    lower: Decimal,
    upper: Decimal,
    level_count: u32,
    interval: Decimal,
}

impl GridProtectionGeometry {
    /// Builds the shared geometry for one bounded grid.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for an empty or inverted range
    /// or a level count outside the business limit.
    pub fn new(
        direction: GridDirection,
        lower: Price,
        upper: Price,
        level_count: u32,
    ) -> Result<Self, StrategyError> {
        let lower = lower.as_decimal();
        let upper = upper.as_decimal();
        if lower >= upper {
            return Err(StrategyError::InvalidConfig(
                "grid protection lower price must be below upper price",
            ));
        }
        if level_count == 0 || level_count > MAX_PROTECTION_LEVELS {
            return Err(StrategyError::InvalidConfig(
                "grid protection level count is outside the business limit",
            ));
        }
        let span = checked(upper.checked_sub(lower), "grid protection range")?;
        let interval = checked(
            span.checked_div(Decimal::from(level_count)),
            "grid protection interval",
        )?;
        if interval <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "grid protection interval must be positive",
            ));
        }
        Ok(Self {
            direction,
            lower,
            upper,
            level_count,
            interval,
        })
    }

    pub const fn direction(&self) -> GridDirection {
        self.direction
    }

    /// Rounded level index for a price, clamped to `1..=level_count`
    /// (`grid_config.py:316-346`).
    fn level_index(&self, price: Decimal) -> Result<u32, StrategyError> {
        let offset = match self.direction {
            GridDirection::Long => price.checked_sub(self.lower),
            GridDirection::Short => self.upper.checked_sub(price),
        };
        let steps = checked(offset, "grid protection level offset")?
            .checked_div(self.interval)
            .ok_or(StrategyError::InvalidFinancialValue(
                "grid protection level steps",
            ))?
            .round();
        let index = steps
            .to_i64()
            .ok_or(StrategyError::InvalidFinancialValue(
                "grid protection level index",
            ))?
            .saturating_add(1);
        let clamped = index.clamp(1, i64::from(self.level_count));
        u32::try_from(clamped)
            .map_err(|_| StrategyError::InvalidFinancialValue("grid protection level index"))
    }

    /// Conservative (floored, minimum 1) level index
    /// (`grid_config.py:634-668`).
    fn conservative_index(&self, price: Decimal) -> Result<u32, StrategyError> {
        let offset = match self.direction {
            GridDirection::Long => price.checked_sub(self.lower),
            GridDirection::Short => self.upper.checked_sub(price),
        };
        let steps = checked(offset, "grid protection cost offset")?
            .checked_div(self.interval)
            .and_then(|steps| steps.checked_add(Decimal::ONE))
            .ok_or(StrategyError::InvalidFinancialValue(
                "grid protection cost steps",
            ))?
            .floor();
        let index = steps.to_i64().ok_or(StrategyError::InvalidFinancialValue(
            "grid protection cost index",
        ))?;
        u32::try_from(index.max(1))
            .map_err(|_| StrategyError::InvalidFinancialValue("grid protection cost index"))
    }

    /// Exact level price for a one-based index (`grid_config.py:293-314`).
    fn level_price(&self, index: u32) -> Result<Decimal, StrategyError> {
        let offset = checked(
            self.interval
                .checked_mul(Decimal::from(index.saturating_sub(1))),
            "grid protection level price offset",
        )?;
        checked(
            match self.direction {
                GridDirection::Long => self.lower.checked_add(offset),
                GridDirection::Short => self.upper.checked_sub(offset),
            },
            "grid protection level price",
        )
    }

    /// Trigger level for a progress percent, identical for both directions
    /// (`grid_config.py:603-632` and `grid_config.py:674-703`).
    fn trigger_level(&self, percent: u32) -> Result<u32, StrategyError> {
        let offset = checked(
            Decimal::from(self.level_count)
                .checked_mul(Decimal::from(percent))
                .and_then(|value| value.checked_div(Decimal::ONE_HUNDRED)),
            "grid protection trigger offset",
        )?
        .floor()
        .to_u32()
        .ok_or(StrategyError::InvalidFinancialValue(
            "grid protection trigger offset",
        ))?;
        Ok(self.level_count.saturating_sub(offset).max(1))
    }

    /// Full-range width as a percent of the midpoint, feeding
    /// [`AprCalculator::annualized`].
    fn width_percent(&self) -> Result<Decimal, StrategyError> {
        let midpoint = checked(
            self.lower
                .checked_add(self.upper)
                .and_then(|sum| sum.checked_div(Decimal::TWO)),
            "grid protection midpoint",
        )?;
        checked(
            self.upper
                .checked_sub(self.lower)
                .and_then(|span| span.checked_div(midpoint))
                .and_then(|ratio| ratio.checked_mul(Decimal::ONE_HUNDRED)),
            "grid protection width percent",
        )
    }

    fn interval_percent(&self) -> Result<Decimal, StrategyError> {
        checked(
            self.width_percent()?
                .checked_div(Decimal::from(self.level_count)),
            "grid protection interval percent",
        )
    }
}

/// Scalping trigger/take-profit configuration
/// (`grid_config.py:143-146`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalpingPolicyConfig {
    trigger_percent: u32,
    take_profit_levels: u32,
}

impl ScalpingPolicyConfig {
    /// Validates the scalping trigger progress and take-profit distance.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a trigger outside
    /// `1..=100` or a zero take-profit distance.
    pub const fn new(trigger_percent: u32, take_profit_levels: u32) -> Result<Self, StrategyError> {
        if trigger_percent == 0 || trigger_percent > MAX_TRIGGER_PERCENT {
            return Err(StrategyError::InvalidConfig(
                "scalping trigger percent must be within 1..=100",
            ));
        }
        if take_profit_levels == 0 {
            return Err(StrategyError::InvalidConfig(
                "scalping take-profit levels must be positive",
            ));
        }
        Ok(Self {
            trigger_percent,
            take_profit_levels,
        })
    }
}

/// Pure scalping state machine (`scalping_manager.py:82-380`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalpingPolicy {
    config: ScalpingPolicyConfig,
    active: bool,
}

impl ScalpingPolicy {
    #[must_use]
    pub const fn new(config: ScalpingPolicyConfig) -> Self {
        Self {
            config,
            active: false,
        }
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Evaluates one observation against the scalping trigger and, while
    /// active, derives the breakeven-plus-N take-profit order
    /// (`scalping_manager.py:233-344`).
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when a derived price or quantity leaves the
    /// financial domain.
    pub fn evaluate(
        &mut self,
        geometry: &GridProtectionGeometry,
        observation: &GridProtectionObservation,
        initial_capital: Decimal,
    ) -> Result<GridDirective, StrategyError> {
        let price = observation.price.as_decimal();
        let index = geometry.level_index(price)?;
        let trigger = geometry.trigger_level(self.config.trigger_percent)?;
        if self.active {
            // Exit when price recovers above the trigger level
            // (`scalping_manager.py:146-188`).
            if index > trigger {
                self.active = false;
                return Ok(GridDirective::Continue);
            }
        } else if index <= trigger {
            // Trigger when grid progress reaches the configured percent
            // (`scalping_manager.py:123-144`).
            self.active = true;
        } else {
            return Ok(GridDirective::Continue);
        }

        let position = observation.position_quantity;
        if position.is_zero() || initial_capital <= Decimal::ZERO {
            // Active without a position or principal keeps observing
            // (`scalping_manager.py:253-260`).
            return Ok(GridDirective::Continue);
        }
        let magnitude = position.abs();
        let realized_pnl = checked(
            observation.current_collateral.checked_sub(initial_capital),
            "scalping realized pnl",
        )?;
        // Breakeven move that recovers the realized loss
        // (`scalping_manager.py:263-288`).
        let required_move = checked(
            realized_pnl.checked_div(magnitude).map(|value| -value),
            "scalping breakeven move",
        )?;
        let (breakeven, side) = match geometry.direction {
            GridDirection::Long => (
                checked(price.checked_add(required_move), "scalping breakeven price")?,
                Side::Sell,
            ),
            GridDirection::Short => (
                checked(price.checked_sub(required_move), "scalping breakeven price")?,
                Side::Buy,
            ),
        };
        let breakeven_index = geometry.conservative_index(breakeven)?;
        // Clamp the take-profit level to the grid range
        // (`scalping_manager.py:296-340`).
        let take_profit_index = match geometry.direction {
            GridDirection::Long => {
                if breakeven_index > geometry.level_count {
                    geometry.level_count
                } else {
                    breakeven_index
                        .saturating_add(self.config.take_profit_levels)
                        .min(geometry.level_count)
                }
            }
            GridDirection::Short => breakeven_index
                .saturating_sub(self.config.take_profit_levels)
                .max(1),
        };
        let take_profit_price = Price::new(geometry.level_price(take_profit_index)?)
            .map_err(|_| StrategyError::InvalidFinancialValue("scalping take-profit price"))?;
        let quantity = Quantity::new(magnitude)
            .map_err(|_| StrategyError::InvalidFinancialValue("scalping reduce quantity"))?;
        Ok(GridDirective::Scalp {
            reason: GridProtectionReason::ScalpingActive,
            side,
            quantity,
            take_profit_price,
        })
    }

    fn reset(&mut self) {
        self.active = false;
    }
}

/// Capital protection trigger configuration (`grid_config.py:160-162`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapitalProtectionPolicyConfig {
    trigger_percent: u32,
}

impl CapitalProtectionPolicyConfig {
    /// Validates the capital protection trigger progress.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a trigger outside
    /// `1..=100`.
    pub const fn new(trigger_percent: u32) -> Result<Self, StrategyError> {
        if trigger_percent == 0 || trigger_percent > MAX_TRIGGER_PERCENT {
            return Err(StrategyError::InvalidConfig(
                "capital protection trigger percent must be within 1..=100",
            ));
        }
        Ok(Self { trigger_percent })
    }
}

/// Pure capital protection state machine
/// (`capital_protection_manager.py:89-174`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapitalProtectionPolicy {
    config: CapitalProtectionPolicyConfig,
    active: bool,
}

impl CapitalProtectionPolicy {
    #[must_use]
    pub const fn new(config: CapitalProtectionPolicyConfig) -> Self {
        Self {
            config,
            active: false,
        }
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Activates on the trigger level and resets the grid once collateral is
    /// back within $0.01 of the protected principal
    /// (`capital_protection_manager.py:104-174`).
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when a derived index or profit value leaves
    /// the financial domain.
    pub fn evaluate(
        &mut self,
        geometry: &GridProtectionGeometry,
        observation: &GridProtectionObservation,
        initial_capital: Decimal,
    ) -> Result<GridDirective, StrategyError> {
        if !self.active {
            let index = geometry.level_index(observation.price.as_decimal())?;
            if index <= geometry.trigger_level(self.config.trigger_percent)? {
                self.active = true;
            } else {
                return Ok(GridDirective::Continue);
            }
        }
        if initial_capital <= Decimal::ZERO {
            return Ok(GridDirective::Continue);
        }
        let profit_loss = checked(
            observation.current_collateral.checked_sub(initial_capital),
            "capital protection profit",
        )?;
        // $0.01 recovery tolerance (`capital_protection_manager.py:154-157`).
        let tolerance = Decimal::new(1, 2);
        if profit_loss >= -tolerance {
            self.active = false;
            return Ok(GridDirective::ResetGrid {
                reason: GridProtectionReason::CapitalRecovered,
            });
        }
        Ok(GridDirective::Continue)
    }

    fn reset(&mut self) {
        self.active = false;
    }
}

/// Take-profit threshold configuration (`grid_config.py:164-166`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakeProfitPolicyConfig {
    profit_rate: Decimal,
}

impl TakeProfitPolicyConfig {
    /// Validates the take-profit rate expressed as a fraction (0.01 = 1%).
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a non-positive or
    /// implausibly large rate.
    pub fn new(profit_rate: Decimal) -> Result<Self, StrategyError> {
        if profit_rate <= Decimal::ZERO || profit_rate > Decimal::TEN {
            return Err(StrategyError::InvalidConfig(
                "take-profit rate must be a positive fraction of at most 10",
            ));
        }
        Ok(Self { profit_rate })
    }
}

/// Pure take-profit state machine (`take_profit_manager.py:79-119`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakeProfitPolicy {
    config: TakeProfitPolicyConfig,
}

impl TakeProfitPolicy {
    #[must_use]
    pub const fn new(config: TakeProfitPolicyConfig) -> Self {
        Self { config }
    }

    /// Resets the grid once the equity profit rate meets the threshold
    /// (`take_profit_manager.py:101-119`).
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when the derived profit rate leaves the
    /// financial domain.
    pub fn evaluate(
        &mut self,
        observation: &GridProtectionObservation,
        initial_capital: Decimal,
    ) -> Result<GridDirective, StrategyError> {
        if initial_capital <= Decimal::ZERO {
            return Ok(GridDirective::Continue);
        }
        let profit_rate = checked(
            observation
                .current_collateral
                .checked_sub(initial_capital)
                .and_then(|profit| profit.checked_div(initial_capital)),
            "take-profit rate",
        )?;
        if profit_rate >= self.config.profit_rate {
            return Ok(GridDirective::ResetGrid {
                reason: GridProtectionReason::TakeProfitTarget,
            });
        }
        Ok(GridDirective::Continue)
    }
}

/// Price-lock threshold configuration (`grid_config.py:168-176`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLockPolicyConfig {
    threshold: Price,
}

impl PriceLockPolicyConfig {
    #[must_use]
    pub const fn new(threshold: Price) -> Self {
        Self { threshold }
    }
}

/// Pure price-lock state machine (`price_lock_manager.py:44-142`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLockPolicy {
    config: PriceLockPolicyConfig,
    locked: bool,
}

impl PriceLockPolicy {
    #[must_use]
    pub const fn new(config: PriceLockPolicyConfig) -> Self {
        Self {
            config,
            locked: false,
        }
    }

    #[must_use]
    pub const fn is_locked(&self) -> bool {
        self.locked
    }

    /// Freezes entries while price escapes favourably beyond the threshold and
    /// unlocks once price returns inside the grid range
    /// (`price_lock_manager.py:44-142`).
    #[allow(clippy::missing_errors_doc)]
    pub fn evaluate(
        &mut self,
        geometry: &GridProtectionGeometry,
        observation: &GridProtectionObservation,
    ) -> Result<GridDirective, StrategyError> {
        let price = observation.price.as_decimal();
        if self.locked {
            // Unlock when price returns inside the grid range
            // (`price_lock_manager.py:110-142`).
            if price >= geometry.lower && price <= geometry.upper {
                self.locked = false;
                return Ok(GridDirective::Continue);
            }
            return Ok(GridDirective::FreezeEntries {
                reason: GridProtectionReason::PriceLockActive,
            });
        }
        // Lock only on a favourable escape that also reaches the threshold
        // (`price_lock_manager.py:59-97`).
        let threshold = self.config.threshold.as_decimal();
        let lock = match geometry.direction {
            GridDirection::Long => price > geometry.upper && price >= threshold,
            GridDirection::Short => price < geometry.lower && price <= threshold,
        };
        if lock {
            self.locked = true;
            return Ok(GridDirective::FreezeEntries {
                reason: GridProtectionReason::PriceLockActive,
            });
        }
        Ok(GridDirective::Continue)
    }

    const fn reset(&mut self) {
        self.locked = false;
    }
}

/// Stop-loss trigger configuration (`grid_config.py:178-197`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopLossPolicyConfig {
    trigger_percent: Decimal,
    escape_timeout_seconds: u64,
    apr_threshold_percent: Decimal,
}

impl StopLossPolicyConfig {
    /// Validates the stop-loss trigger geometry, escape timeout, and APR gate.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a trigger outside
    /// `(0, 100]`, a zero or unrepresentable timeout, or a negative APR
    /// threshold.
    pub fn new(
        trigger_percent: Decimal,
        escape_timeout_seconds: u64,
        apr_threshold_percent: Decimal,
    ) -> Result<Self, StrategyError> {
        if trigger_percent <= Decimal::ZERO || trigger_percent > Decimal::ONE_HUNDRED {
            return Err(StrategyError::InvalidConfig(
                "stop-loss trigger percent must be within (0, 100]",
            ));
        }
        if escape_timeout_seconds == 0 || escape_timeout_seconds > MAX_ESCAPE_TIMEOUT_SECONDS {
            return Err(StrategyError::InvalidConfig(
                "stop-loss escape timeout must be within 1..=31536000 seconds",
            ));
        }
        if apr_threshold_percent < Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "stop-loss APR threshold must not be negative",
            ));
        }
        Ok(Self {
            trigger_percent,
            escape_timeout_seconds,
            apr_threshold_percent,
        })
    }
}

/// Pure stop-loss state machine (`stop_loss_monitor.py:126-345`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopLossPolicy {
    config: StopLossPolicyConfig,
    escape_started_at: Option<DateTime<Utc>>,
}

impl StopLossPolicy {
    #[must_use]
    pub const fn new(config: StopLossPolicyConfig) -> Self {
        Self {
            config,
            escape_started_at: None,
        }
    }

    #[must_use]
    pub const fn is_escaped(&self) -> bool {
        self.escape_started_at.is_some()
    }

    /// Tracks adverse escape duration and, on timeout, gates reset-vs-exit on
    /// the realtime APR (`stop_loss_monitor.py:126-184` and `:334-350`).
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for an unrepresentable timeout or an APR
    /// input outside the financial domain.
    pub fn evaluate(
        &mut self,
        geometry: &GridProtectionGeometry,
        observation: &GridProtectionObservation,
    ) -> Result<GridDirective, StrategyError> {
        let price = observation.price.as_decimal();
        // Trigger price sits `trigger_percent` of the grid height into the
        // adverse direction (`stop_loss_monitor.py:209-272`).
        let range = checked(
            geometry.upper.checked_sub(geometry.lower),
            "stop-loss grid range",
        )?;
        let distance = checked(
            range
                .checked_mul(self.config.trigger_percent)
                .and_then(|value| value.checked_div(Decimal::ONE_HUNDRED)),
            "stop-loss trigger distance",
        )?;
        let adverse = match geometry.direction {
            GridDirection::Long => {
                let trigger = checked(
                    geometry.upper.checked_sub(distance),
                    "stop-loss trigger price",
                )?;
                price <= trigger
            }
            GridDirection::Short => {
                let trigger = checked(
                    geometry.lower.checked_add(distance),
                    "stop-loss trigger price",
                )?;
                price >= trigger
            }
        };
        if !adverse {
            // Price recovered before the timeout (`stop_loss_monitor.py:157-165`).
            self.escape_started_at = None;
            return Ok(GridDirective::Continue);
        }
        let started_at = *self
            .escape_started_at
            .get_or_insert(observation.observed_at);
        let timeout = i64::try_from(self.config.escape_timeout_seconds).map_err(|_| {
            StrategyError::InvalidConfig("stop-loss escape timeout is unrepresentable")
        })?;
        if observation.observed_at.signed_duration_since(started_at) < Duration::seconds(timeout) {
            return Ok(GridDirective::Continue);
        }
        // Realtime APR gate reuses the shared calculator
        // (`stop_loss_monitor.py:312-345`, `grid_config.py:193-196`).
        let apr = AprCalculator::annualized(
            geometry.interval_percent()?,
            geometry.width_percent()?,
            observation.cycles_per_hour,
        )?;
        self.escape_started_at = None;
        if apr >= self.config.apr_threshold_percent {
            Ok(GridDirective::ResetGrid {
                reason: GridProtectionReason::StopLossAprRecovered,
            })
        } else {
            Ok(GridDirective::ExitAll {
                reason: GridProtectionReason::StopLossAprBelowThreshold,
            })
        }
    }

    const fn reset(&mut self) {
        self.escape_started_at = None;
    }
}

/// Optional per-policy configuration consumed by the combined machine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GridProtectionPolicies {
    pub stop_loss: Option<StopLossPolicyConfig>,
    pub capital_protection: Option<CapitalProtectionPolicyConfig>,
    pub price_lock: Option<PriceLockPolicyConfig>,
    pub take_profit: Option<TakeProfitPolicyConfig>,
    pub scalping: Option<ScalpingPolicyConfig>,
}

/// Combined arbitration machine with the legacy priority order: stop-loss >
/// capital protection > price lock > take profit > scalping
/// (`grid_config.py:196`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridProtectionMachine {
    geometry: GridProtectionGeometry,
    capital_baseline: Option<Decimal>,
    stop_loss: Option<StopLossPolicy>,
    capital_protection: Option<CapitalProtectionPolicy>,
    price_lock: Option<PriceLockPolicy>,
    take_profit: Option<TakeProfitPolicy>,
    scalping: Option<ScalpingPolicy>,
}

impl GridProtectionMachine {
    /// Builds one arbitration machine over the enabled policies.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when no policy is enabled.
    pub fn new(
        geometry: GridProtectionGeometry,
        policies: GridProtectionPolicies,
    ) -> Result<Self, StrategyError> {
        if policies.stop_loss.is_none()
            && policies.capital_protection.is_none()
            && policies.price_lock.is_none()
            && policies.take_profit.is_none()
            && policies.scalping.is_none()
        {
            return Err(StrategyError::InvalidConfig(
                "grid protection machine requires at least one enabled policy",
            ));
        }
        Ok(Self {
            geometry,
            capital_baseline: None,
            stop_loss: policies.stop_loss.map(StopLossPolicy::new),
            capital_protection: policies
                .capital_protection
                .map(CapitalProtectionPolicy::new),
            price_lock: policies.price_lock.map(PriceLockPolicy::new),
            take_profit: policies.take_profit.map(TakeProfitPolicy::new),
            scalping: policies.scalping.map(ScalpingPolicy::new),
        })
    }

    pub const fn geometry(&self) -> &GridProtectionGeometry {
        &self.geometry
    }

    /// Evaluates every enabled policy and returns the highest-priority
    /// non-`Continue` directive. A `ResetGrid` outcome re-bases the protected
    /// principal to the observed collateral and clears transient policy state,
    /// mirroring the legacy re-initialization on grid reset.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when any policy input leaves the financial
    /// domain.
    pub fn observe(
        &mut self,
        observation: &GridProtectionObservation,
    ) -> Result<GridDirective, StrategyError> {
        let baseline = *self
            .capital_baseline
            .get_or_insert(observation.current_collateral);
        let stop_loss = self
            .stop_loss
            .as_mut()
            .map(|policy| policy.evaluate(&self.geometry, observation))
            .transpose()?;
        let capital_protection = self
            .capital_protection
            .as_mut()
            .map(|policy| policy.evaluate(&self.geometry, observation, baseline))
            .transpose()?;
        let price_lock = self
            .price_lock
            .as_mut()
            .map(|policy| policy.evaluate(&self.geometry, observation))
            .transpose()?;
        let take_profit = self
            .take_profit
            .as_mut()
            .map(|policy| policy.evaluate(observation, baseline))
            .transpose()?;
        let scalping = self
            .scalping
            .as_mut()
            .map(|policy| policy.evaluate(&self.geometry, observation, baseline))
            .transpose()?;

        let selected = [
            stop_loss,
            capital_protection,
            price_lock,
            take_profit,
            scalping,
        ]
        .into_iter()
        .flatten()
        .find(|directive| *directive != GridDirective::Continue)
        .unwrap_or(GridDirective::Continue);

        if matches!(selected, GridDirective::ResetGrid { .. }) {
            self.capital_baseline = Some(observation.current_collateral);
            self.reset_transient_state();
        }
        Ok(selected)
    }

    fn reset_transient_state(&mut self) {
        if let Some(policy) = self.stop_loss.as_mut() {
            policy.reset();
        }
        if let Some(policy) = self.capital_protection.as_mut() {
            policy.reset();
        }
        if let Some(policy) = self.price_lock.as_mut() {
            policy.reset();
        }
        if let Some(policy) = self.scalping.as_mut() {
            policy.reset();
        }
    }
}

fn checked(value: Option<Decimal>, name: &'static str) -> Result<Decimal, StrategyError> {
    value.ok_or(StrategyError::InvalidFinancialValue(name))
}
