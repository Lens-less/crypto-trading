use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{Price, Symbol};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::StrategyError;

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

        let grid_count = (config.grid_width_percent / config.grid_interval_percent)
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

        let initial = config.initial_price.as_decimal();
        let half_width = config.grid_width_percent / Decimal::from(200);
        let lower_value = initial * (Decimal::ONE - half_width);
        let upper_value = initial * (Decimal::ONE + half_width);
        let lower_price = Self::price(lower_value, "virtual grid lower price")?;
        let upper_price = Self::price(upper_value, "virtual grid upper price")?;
        let line_step = (upper_value - lower_value) / Decimal::from(grid_count);
        let grid_lines = (0..=grid_count)
            .map(|index| {
                Self::price(
                    lower_value + line_step * Decimal::from(index),
                    "virtual grid line",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let grid_interval_value = initial * (config.grid_interval_percent / Decimal::ONE_HUNDRED);
        let pending_buy_price = Self::price(
            initial - grid_interval_value,
            "virtual grid pending buy price",
        )?;
        let pending_sell_price = Self::price(
            initial + grid_interval_value,
            "virtual grid pending sell price",
        )?;

        Ok(Self {
            current_price: config.initial_price,
            config,
            lower_price,
            upper_price,
            grid_count,
            grid_lines,
            grid_interval_value,
            pending_buy_price,
            pending_sell_price,
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
        self.current_price = new_price;
        self.last_update_at = timestamp;

        if new_price <= self.pending_buy_price {
            let fill_price = self.pending_buy_price.as_decimal();
            self.buy_crosses += 1;
            self.pending_sell_price = Self::price(
                fill_price + self.grid_interval_value,
                "virtual grid pending sell price",
            )?;
            self.pending_buy_price = Self::price(
                fill_price - self.grid_interval_value,
                "virtual grid pending buy price",
            )?;
            self.update_cycle_count(timestamp);
            return Ok(Some(GridFill::Buy));
        }

        if new_price >= self.pending_sell_price {
            let fill_price = self.pending_sell_price.as_decimal();
            self.sell_crosses += 1;
            self.pending_buy_price = Self::price(
                fill_price - self.grid_interval_value,
                "virtual grid pending buy price",
            )?;
            self.pending_sell_price = Self::price(
                fill_price + self.grid_interval_value,
                "virtual grid pending sell price",
            )?;
            self.update_cycle_count(timestamp);
            return Ok(Some(GridFill::Sell));
        }

        Ok(None)
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
        let runtime = now - self.started_at;
        if runtime < Duration::minutes(1) {
            self.cycles_per_hour = Decimal::ZERO;
            self.estimated_apr = Decimal::ZERO;
            return Ok(Decimal::ZERO);
        }

        let effective_window = runtime.min(window);
        let window_start = now - effective_window;
        if runtime >= window {
            while self
                .cycle_events
                .front()
                .is_some_and(|event| *event < window_start)
            {
                self.cycle_events.pop_front();
            }
        }
        let cycle_count = self
            .cycle_events
            .iter()
            .filter(|event| **event >= window_start)
            .count();
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
        let window_hours = Decimal::from(window_milliseconds) / Decimal::from(3_600_000);
        self.cycles_per_hour =
            Decimal::from(u64::try_from(cycle_count).unwrap_or(u64::MAX)) / window_hours;
        self.estimated_apr = AprCalculator::annualized(
            self.config.grid_interval_percent,
            self.config.grid_width_percent,
            self.cycles_per_hour,
        )?;
        Ok(self.estimated_apr)
    }

    pub fn recent_cycles_at(&self, now: DateTime<Utc>, window: Duration) -> usize {
        let window_start = now - window;
        self.cycle_events
            .iter()
            .filter(|event| **event >= window_start)
            .count()
    }

    fn update_cycle_count(&mut self, timestamp: DateTime<Utc>) {
        let previous = self.complete_cycles;
        self.complete_cycles = self.buy_crosses.min(self.sell_crosses);
        if self.complete_cycles > previous {
            self.cycle_events.push_back(timestamp);
        }
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
        let net_profit_rate = grid_interval_percent - Self::FEE_RATE_PERCENT;
        if net_profit_rate <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        Ok(net_profit_rate * grid_interval_percent / grid_width_percent
            * cycles_per_hour
            * Self::HOURS_PER_YEAR)
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
        Ok(Self::ORDER_VALUE_USDC * grid_width_percent / grid_interval_percent)
    }

    pub fn profit_per_cycle(grid_interval_percent: Decimal) -> Decimal {
        let net_rate = grid_interval_percent - Self::FEE_RATE_PERCENT;
        if net_rate <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            Self::ORDER_VALUE_USDC * net_rate / Decimal::ONE_HUNDRED
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
