use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{Price, Symbol};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::StrategyError;

const MAX_VIRTUAL_GRID_LEVELS: u32 = 10_000;
const MAX_VIRTUAL_GRID_CYCLE_EVENTS: usize = 100_000;
const MAX_APR_WINDOW_SECONDS: i64 = 366 * 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualGridConfig {
    pub symbol: Symbol,
    pub initial_price: Price,
    /// Total grid width. A value of 10 means 5% below and 5% above.
    pub grid_width_percent: Decimal,
    pub grid_interval_percent: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridFill {
    Buy,
    Sell,
}

/// A deterministic two-sided paper grid used for volatility scoring.
#[derive(Debug, Clone)]
pub struct VirtualGrid {
    config: VirtualGridConfig,
    current_price: Price,
    lower_price: Price,
    upper_price: Price,
    grid_count: u32,
    grid_lines: Vec<Price>,
    grid_interval_value: Decimal,
    pending_buy_price: Price,
    pending_sell_price: Price,
    buy_crosses: u64,
    sell_crosses: u64,
    complete_cycles: u64,
    cycle_events: VecDeque<DateTime<Utc>>,
    started_at: DateTime<Utc>,
    last_update_at: DateTime<Utc>,
    cycles_per_hour: Decimal,
    estimated_apr: Decimal,
}

struct VirtualGridGeometry {
    lower_price: Price,
    upper_price: Price,
    grid_count: u32,
    grid_lines: Vec<Price>,
    grid_interval_value: Decimal,
    pending_buy_price: Price,
    pending_sell_price: Price,
}

impl VirtualGrid {
    /// Constructs a virtual two-sided grid at an explicit start time.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for non-positive parameters, an empty grid,
    /// or derived prices outside the domain.
    pub fn new(
        config: VirtualGridConfig,
        started_at: DateTime<Utc>,
    ) -> Result<Self, StrategyError> {
        Self::validate_config(&config)?;
        let geometry = Self::derive_geometry(&config)?;

        Ok(Self {
            current_price: config.initial_price,
            config,
            lower_price: geometry.lower_price,
            upper_price: geometry.upper_price,
            grid_count: geometry.grid_count,
            grid_lines: geometry.grid_lines,
            grid_interval_value: geometry.grid_interval_value,
            pending_buy_price: geometry.pending_buy_price,
            pending_sell_price: geometry.pending_sell_price,
            buy_crosses: 0,
            sell_crosses: 0,
            complete_cycles: 0,
            cycle_events: VecDeque::new(),
            started_at,
            last_update_at: started_at,
            cycles_per_hour: Decimal::ZERO,
            estimated_apr: Decimal::ZERO,
        })
    }

    fn validate_config(config: &VirtualGridConfig) -> Result<(), StrategyError> {
        if config.initial_price.as_decimal() <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "virtual grid initial price must be positive",
            ));
        }
        if config.grid_width_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "virtual grid width must be positive",
            ));
        }
        if config.grid_interval_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "virtual grid interval must be positive",
            ));
        }
        Ok(())
    }

    fn derive_geometry(config: &VirtualGridConfig) -> Result<VirtualGridGeometry, StrategyError> {
        let initial = config.initial_price.as_decimal();
        let grid_count = Self::derive_grid_count(config)?;
        let (lower_value, upper_value) = Self::derive_bounds(config)?;
        let grid_lines = Self::build_grid_lines(lower_value, upper_value, grid_count)?;
        let grid_interval_value = initial
            .checked_mul(
                config
                    .grid_interval_percent
                    .checked_div(Decimal::ONE_HUNDRED)
                    .ok_or(StrategyError::InvalidFinancialValue(
                        "virtual grid interval rate",
                    ))?,
            )
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid interval value",
            ))?;
        let pending_buy_price = Self::price(
            initial.checked_sub(grid_interval_value).ok_or(
                StrategyError::InvalidFinancialValue("virtual grid pending buy price"),
            )?,
            "virtual grid pending buy price",
        )?;
        let pending_sell_price = Self::price(
            initial.checked_add(grid_interval_value).ok_or(
                StrategyError::InvalidFinancialValue("virtual grid pending sell price"),
            )?,
            "virtual grid pending sell price",
        )?;

        Ok(VirtualGridGeometry {
            lower_price: Self::price(lower_value, "virtual grid lower price")?,
            upper_price: Self::price(upper_value, "virtual grid upper price")?,
            grid_count,
            grid_lines,
            grid_interval_value,
            pending_buy_price,
            pending_sell_price,
        })
    }

    fn derive_grid_count(config: &VirtualGridConfig) -> Result<u32, StrategyError> {
        let grid_count = config
            .grid_width_percent
            .checked_div(config.grid_interval_percent)
            .ok_or(StrategyError::InvalidFinancialValue("virtual grid count"))?
            .floor()
            .to_u32()
            .ok_or(StrategyError::InvalidConfig(
                "virtual grid count is too large",
            ))?;
        if grid_count == 0 {
            return Err(StrategyError::InvalidConfig(
                "virtual grid must contain at least one interval",
            ));
        }
        if grid_count > MAX_VIRTUAL_GRID_LEVELS {
            return Err(StrategyError::InvalidConfig(
                "virtual grid count exceeds the business limit",
            ));
        }
        Ok(grid_count)
    }

    fn derive_bounds(config: &VirtualGridConfig) -> Result<(Decimal, Decimal), StrategyError> {
        let initial = config.initial_price.as_decimal();
        let half_width = config
            .grid_width_percent
            .checked_div(Decimal::from(200))
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid half width",
            ))?;
        let lower_factor =
            Decimal::ONE
                .checked_sub(half_width)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid lower price factor",
                ))?;
        let upper_factor =
            Decimal::ONE
                .checked_add(half_width)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid upper price factor",
                ))?;
        let lower_value =
            initial
                .checked_mul(lower_factor)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid lower price",
                ))?;
        let upper_value =
            initial
                .checked_mul(upper_factor)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid upper price",
                ))?;
        Ok((lower_value, upper_value))
    }

    fn build_grid_lines(
        lower_value: Decimal,
        upper_value: Decimal,
        grid_count: u32,
    ) -> Result<Vec<Price>, StrategyError> {
        let grid_span =
            upper_value
                .checked_sub(lower_value)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid price span",
                ))?;
        let line_step = grid_span.checked_div(Decimal::from(grid_count)).ok_or(
            StrategyError::InvalidFinancialValue("virtual grid line step"),
        )?;
        let line_count =
            (grid_count as usize)
                .checked_add(1)
                .ok_or(StrategyError::InvalidConfig(
                    "virtual grid line count is too large",
                ))?;
        let mut grid_lines = Vec::new();
        grid_lines
            .try_reserve_exact(line_count)
            .map_err(|_| StrategyError::InvalidConfig("virtual grid line allocation failed"))?;
        for index in 0..=grid_count {
            let offset = line_step.checked_mul(Decimal::from(index)).ok_or(
                StrategyError::InvalidFinancialValue("virtual grid line offset"),
            )?;
            let line = lower_value
                .checked_add(offset)
                .ok_or(StrategyError::InvalidFinancialValue("virtual grid line"))?;
            grid_lines.push(Self::price(line, "virtual grid line")?);
        }
        Ok(grid_lines)
    }

    pub const fn config(&self) -> &VirtualGridConfig {
        &self.config
    }

    pub const fn current_price(&self) -> Price {
        self.current_price
    }

    pub const fn lower_price(&self) -> Price {
        self.lower_price
    }

    pub const fn upper_price(&self) -> Price {
        self.upper_price
    }

    pub const fn grid_count(&self) -> u32 {
        self.grid_count
    }

    pub fn grid_lines(&self) -> &[Price] {
        &self.grid_lines
    }

    pub const fn grid_interval_value(&self) -> Decimal {
        self.grid_interval_value
    }

    pub const fn pending_buy_price(&self) -> Price {
        self.pending_buy_price
    }

    pub const fn pending_sell_price(&self) -> Price {
        self.pending_sell_price
    }

    pub const fn buy_crosses(&self) -> u64 {
        self.buy_crosses
    }

    pub const fn sell_crosses(&self) -> u64 {
        self.sell_crosses
    }

    pub const fn complete_cycles(&self) -> u64 {
        self.complete_cycles
    }

    pub const fn cycles_per_hour(&self) -> Decimal {
        self.cycles_per_hour
    }

    pub const fn estimated_apr(&self) -> Decimal {
        self.estimated_apr
    }

    /// Processes at most one fill per snapshot, matching the legacy ticker seam.
    /// Applies one price observation and processes at most one pending fill.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for non-monotonic timestamps or a derived
    /// pending price outside the domain.
    pub fn update_price_at(
        &mut self,
        new_price: Price,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<GridFill>, StrategyError> {
        if timestamp < self.last_update_at {
            return Err(StrategyError::InvalidConfig(
                "virtual grid timestamps must be monotonic",
            ));
        }
        let mut next_pending_buy = self.pending_buy_price;
        let mut next_pending_sell = self.pending_sell_price;
        let mut next_buy_crosses = self.buy_crosses;
        let mut next_sell_crosses = self.sell_crosses;
        let mut fill = None;

        if new_price <= next_pending_buy {
            let fill_price = self.pending_buy_price.as_decimal();
            next_pending_sell = Self::price(
                fill_price.checked_add(self.grid_interval_value).ok_or(
                    StrategyError::InvalidFinancialValue("virtual grid pending sell price"),
                )?,
                "virtual grid pending sell price",
            )?;
            next_pending_buy = Self::price(
                fill_price.checked_sub(self.grid_interval_value).ok_or(
                    StrategyError::InvalidFinancialValue("virtual grid pending buy price"),
                )?,
                "virtual grid pending buy price",
            )?;
            next_buy_crosses =
                next_buy_crosses
                    .checked_add(1)
                    .ok_or(StrategyError::InvalidFinancialValue(
                        "virtual grid buy cross count",
                    ))?;
            fill = Some(GridFill::Buy);
        } else if new_price >= next_pending_sell {
            let fill_price = self.pending_sell_price.as_decimal();
            next_pending_buy = Self::price(
                fill_price.checked_sub(self.grid_interval_value).ok_or(
                    StrategyError::InvalidFinancialValue("virtual grid pending buy price"),
                )?,
                "virtual grid pending buy price",
            )?;
            next_pending_sell = Self::price(
                fill_price.checked_add(self.grid_interval_value).ok_or(
                    StrategyError::InvalidFinancialValue("virtual grid pending sell price"),
                )?,
                "virtual grid pending sell price",
            )?;
            next_sell_crosses =
                next_sell_crosses
                    .checked_add(1)
                    .ok_or(StrategyError::InvalidFinancialValue(
                        "virtual grid sell cross count",
                    ))?;
            fill = Some(GridFill::Sell);
        }

        let next_complete_cycles = next_buy_crosses.min(next_sell_crosses);
        let completed_cycle = next_complete_cycles > self.complete_cycles;
        if completed_cycle {
            if self.cycle_events.len() >= MAX_VIRTUAL_GRID_CYCLE_EVENTS {
                return Err(StrategyError::InvalidConfig(
                    "virtual grid cycle history exceeds the business limit",
                ));
            }
            self.cycle_events
                .try_reserve(1)
                .map_err(|_| StrategyError::InvalidConfig("virtual grid cycle history is full"))?;
        }

        self.current_price = new_price;
        self.last_update_at = timestamp;
        self.pending_buy_price = next_pending_buy;
        self.pending_sell_price = next_pending_sell;
        self.buy_crosses = next_buy_crosses;
        self.sell_crosses = next_sell_crosses;
        self.complete_cycles = next_complete_cycles;
        if completed_cycle {
            self.cycle_events.push_back(timestamp);
        }
        Ok(fill)
    }

    /// Calculates rolling-window annualized return at `now`.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a non-positive or
    /// unrepresentably small window, or propagates APR validation errors.
    pub fn calculate_apr_at(
        &mut self,
        now: DateTime<Utc>,
        window: Duration,
    ) -> Result<Decimal, StrategyError> {
        if window <= Duration::zero() {
            return Err(StrategyError::InvalidConfig("APR window must be positive"));
        }
        if window > Duration::seconds(MAX_APR_WINDOW_SECONDS) {
            return Err(StrategyError::InvalidConfig(
                "APR window exceeds the business limit",
            ));
        }
        if now < self.started_at || now < self.last_update_at {
            return Err(StrategyError::InvalidConfig(
                "APR query time must not precede virtual grid state",
            ));
        }
        let runtime = now.signed_duration_since(self.started_at);
        if runtime < Duration::minutes(1) {
            self.cycles_per_hour = Decimal::ZERO;
            self.estimated_apr = Decimal::ZERO;
            return Ok(Decimal::ZERO);
        }

        let effective_window = runtime.min(window);
        let window_start =
            now.checked_sub_signed(effective_window)
                .ok_or(StrategyError::InvalidConfig(
                    "APR window precedes representable time",
                ))?;
        let cycle_count = self
            .cycle_events
            .iter()
            .filter(|event| **event >= window_start && **event <= now)
            .count();
        if runtime >= window {
            while self
                .cycle_events
                .front()
                .is_some_and(|event| *event < window_start)
            {
                self.cycle_events.pop_front();
            }
        }
        if cycle_count == 0 {
            self.cycles_per_hour = Decimal::ZERO;
            self.estimated_apr = Decimal::ZERO;
            return Ok(Decimal::ZERO);
        }

        let window_milliseconds = effective_window.num_milliseconds();
        if window_milliseconds <= 0 {
            return Err(StrategyError::InvalidConfig(
                "APR window resolution is too small",
            ));
        }
        let window_hours = Decimal::from(window_milliseconds)
            .checked_div(Decimal::from(3_600_000))
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid APR window",
            ))?;
        let cycles_per_hour = Decimal::from(u64::try_from(cycle_count).unwrap_or(u64::MAX))
            .checked_div(window_hours)
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid cycles per hour",
            ))?;
        let estimated_apr = AprCalculator::annualized(
            self.config.grid_interval_percent,
            self.config.grid_width_percent,
            cycles_per_hour,
        )?;
        self.cycles_per_hour = cycles_per_hour;
        self.estimated_apr = estimated_apr;
        Ok(estimated_apr)
    }

    pub fn recent_cycles_at(&self, now: DateTime<Utc>, window: Duration) -> usize {
        if window <= Duration::zero()
            || window > Duration::seconds(MAX_APR_WINDOW_SECONDS)
            || now < self.started_at
            || now < self.last_update_at
        {
            return 0;
        }
        let Some(window_start) = now.checked_sub_signed(window) else {
            return 0;
        };
        self.cycle_events
            .iter()
            .filter(|event| **event >= window_start && **event <= now)
            .count()
    }

    fn price(value: Decimal, name: &'static str) -> Result<Price, StrategyError> {
        Price::new(value).map_err(|_| StrategyError::InvalidFinancialValue(name))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AprCalculator;

impl AprCalculator {
    pub const ORDER_VALUE_USDC: Decimal = Decimal::TEN;
    pub const FEE_RATE_PERCENT: Decimal = Decimal::from_parts(4, 0, 0, false, 3);
    pub const HOURS_PER_YEAR: Decimal = Decimal::from_parts(8_760, 0, 0, false, 0);

    /// Calculates annualized APR using the legacy scanner formula.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when width/interval are not
    /// positive or the cycle rate is negative.
    pub fn annualized(
        grid_interval_percent: Decimal,
        grid_width_percent: Decimal,
        cycles_per_hour: Decimal,
    ) -> Result<Decimal, StrategyError> {
        if grid_interval_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "APR grid interval must be positive",
            ));
        }
        if grid_width_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "APR grid width must be positive",
            ));
        }
        if cycles_per_hour < Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "APR cycle rate must not be negative",
            ));
        }
        let net_profit_rate = grid_interval_percent
            .checked_sub(Self::FEE_RATE_PERCENT)
            .ok_or(StrategyError::InvalidFinancialValue("APR net profit rate"))?;
        if net_profit_rate <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        net_profit_rate
            .checked_mul(grid_interval_percent)
            .and_then(|value| value.checked_div(grid_width_percent))
            .and_then(|value| value.checked_mul(cycles_per_hour))
            .and_then(|value| value.checked_mul(Self::HOURS_PER_YEAR))
            .ok_or(StrategyError::InvalidFinancialValue("annualized APR"))
    }

    /// Calculates capital allocated across every configured grid interval.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when width or interval is not
    /// positive.
    pub fn total_capital(
        grid_width_percent: Decimal,
        grid_interval_percent: Decimal,
    ) -> Result<Decimal, StrategyError> {
        if grid_width_percent <= Decimal::ZERO || grid_interval_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "capital grid width and interval must be positive",
            ));
        }
        Self::ORDER_VALUE_USDC
            .checked_mul(grid_width_percent)
            .and_then(|value| value.checked_div(grid_interval_percent))
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid total capital",
            ))
    }

    /// Calculates the estimated profit for one completed virtual-grid cycle.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when the result cannot
    /// be represented by [`Decimal`].
    pub fn profit_per_cycle(grid_interval_percent: Decimal) -> Result<Decimal, StrategyError> {
        let net_rate = grid_interval_percent
            .checked_sub(Self::FEE_RATE_PERCENT)
            .ok_or(StrategyError::InvalidFinancialValue(
                "virtual grid net cycle rate",
            ))?;
        if net_rate <= Decimal::ZERO {
            Ok(Decimal::ZERO)
        } else {
            Self::ORDER_VALUE_USDC
                .checked_mul(net_rate)
                .and_then(|value| value.checked_div(Decimal::ONE_HUNDRED))
                .ok_or(StrategyError::InvalidFinancialValue(
                    "virtual grid profit per cycle",
                ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RatingGrade {
    S,
    A,
    B,
    C,
    D,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rating {
    pub grade: RatingGrade,
    pub score: Decimal,
}

impl Rating {
    pub fn calculate(
        estimated_apr: Decimal,
        cycles_per_hour: Decimal,
        volume_24h_usdc: Decimal,
    ) -> Self {
        let (grade, mut score) = if estimated_apr >= Decimal::from(500) {
            (RatingGrade::S, Decimal::from(95))
        } else if estimated_apr >= Decimal::from(300) {
            (RatingGrade::A, Decimal::from(85))
        } else if estimated_apr >= Decimal::from(150) {
            (RatingGrade::B, Decimal::from(75))
        } else if estimated_apr >= Decimal::from(50) {
            (RatingGrade::C, Decimal::from(60))
        } else {
            (RatingGrade::D, Decimal::from(40))
        };

        if cycles_per_hour > Decimal::from(50) {
            score += Decimal::from(5);
        } else if cycles_per_hour < Decimal::from(5) {
            score -= Decimal::from(10);
        }
        if volume_24h_usdc >= Decimal::from(10_000_000) {
            score += Decimal::from(5);
        } else if volume_24h_usdc < Decimal::from(500_000) {
            score -= Decimal::from(10);
        }

        Self {
            grade,
            score: score.clamp(Decimal::ZERO, Decimal::ONE_HUNDRED),
        }
    }
}
