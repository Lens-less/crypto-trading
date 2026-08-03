use crypto_trading_domain::{MarketType, Money, Price, Quantity, Side};
use rust_decimal::Decimal;

use crate::{BacktestError, engine::TradeFill};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerSnapshot {
    pub cash: Money,
    pub position_qty: Decimal,
    pub average_entry_price: Option<Price>,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub equity: Money,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppliedFill {
    pub realized_pnl_delta: Money,
    pub closed_trade_pnl: Option<Money>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ledger {
    cash: Money,
    position_qty: Decimal,
    average_entry_price: Option<Price>,
    open_entry_fees: Money,
    realized_pnl: Money,
}

impl Ledger {
    pub(crate) fn new(initial_cash: Money) -> Result<Self, BacktestError> {
        if initial_cash.as_decimal() < Decimal::ZERO {
            return Err(BacktestError::InvalidInitialCash);
        }

        Ok(Self {
            cash: initial_cash,
            position_qty: Decimal::ZERO,
            average_entry_price: None,
            open_entry_fees: Money::default(),
            realized_pnl: Money::default(),
        })
    }

    pub(crate) fn apply_fill(&mut self, fill: &TradeFill) -> Result<AppliedFill, BacktestError> {
        let mut candidate = self.clone();
        let applied = candidate.apply_fill_to_candidate(fill)?;
        *self = candidate;
        Ok(applied)
    }

    fn apply_fill_to_candidate(&mut self, fill: &TradeFill) -> Result<AppliedFill, BacktestError> {
        let notional = fill_notional(fill)?;
        self.ensure_spot_fill_allowed(fill, notional)?;
        self.apply_cash_movement(fill, notional)?;

        let signed_qty = match fill.side {
            Side::Buy => fill.quantity.as_decimal(),
            Side::Sell => Decimal::ZERO
                .checked_sub(fill.quantity.as_decimal())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        };
        let previous_position = self.position_qty;
        let previous_average = self.average_entry_price;
        let new_position = previous_position
            .checked_add(signed_qty)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let (realized_delta, closed_trade_pnl) =
            if previous_position.is_zero() || same_direction(previous_position, signed_qty) {
                self.apply_opening_fill(fill, previous_position, previous_average)?;
                (negate_money(fill.fee)?, None)
            } else {
                self.apply_closing_fill(fill, previous_position, previous_average, new_position)?
            };

        self.position_qty = new_position;
        if self.position_qty.is_zero() {
            self.average_entry_price = None;
            self.open_entry_fees = Money::default();
        }
        self.realized_pnl = Money::new(
            self.realized_pnl
                .as_decimal()
                .checked_add(realized_delta.as_decimal())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

        Ok(AppliedFill {
            realized_pnl_delta: realized_delta,
            closed_trade_pnl,
        })
    }

    fn ensure_spot_fill_allowed(
        &self,
        fill: &TradeFill,
        notional: Money,
    ) -> Result<(), BacktestError> {
        let is_spot = fill
            .instrument
            .as_ref()
            .is_some_and(|instrument| instrument.market_type == MarketType::Spot);
        if !is_spot {
            return Ok(());
        }

        match fill.side {
            Side::Buy => {
                let required = checked_add_money(notional, fill.fee)?;
                if self.cash < required {
                    return Err(BacktestError::InsufficientBuyingPower {
                        required,
                        available: self.cash,
                    });
                }
            }
            Side::Sell => {
                let available = self.position_qty.max(Decimal::ZERO);
                if fill.quantity.as_decimal() > available {
                    return Err(BacktestError::InsufficientSpotInventory {
                        required: fill.quantity,
                        available: Quantity::new(available)?,
                    });
                }
            }
        }
        Ok(())
    }

    fn apply_cash_movement(
        &mut self,
        fill: &TradeFill,
        notional: Money,
    ) -> Result<(), BacktestError> {
        self.cash = match fill.side {
            Side::Buy => Money::new(
                self.cash
                    .as_decimal()
                    .checked_sub(notional.as_decimal())
                    .and_then(|cash| cash.checked_sub(fill.fee.as_decimal()))
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            ),
            Side::Sell => Money::new(
                self.cash
                    .as_decimal()
                    .checked_add(notional.as_decimal())
                    .and_then(|cash| cash.checked_sub(fill.fee.as_decimal()))
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            ),
        };
        Ok(())
    }

    fn apply_opening_fill(
        &mut self,
        fill: &TradeFill,
        previous_position: Decimal,
        previous_average: Option<Price>,
    ) -> Result<(), BacktestError> {
        self.average_entry_price = Some(match (previous_average, previous_position.is_zero()) {
            (_, true) => fill.fill_price,
            (Some(average), false) => weighted_average(
                average,
                previous_position.abs(),
                fill.fill_price,
                fill.quantity.as_decimal(),
            )?,
            (None, false) => return Err(BacktestError::ArithmeticOverflow),
        });
        self.open_entry_fees = checked_add_money(self.open_entry_fees, fill.fee)?;
        Ok(())
    }

    fn apply_closing_fill(
        &mut self,
        fill: &TradeFill,
        previous_position: Decimal,
        previous_average: Option<Price>,
        new_position: Decimal,
    ) -> Result<(Money, Option<Money>), BacktestError> {
        let close_qty = previous_position.abs().min(fill.quantity.as_decimal());
        let average = previous_average.ok_or(BacktestError::ArithmeticOverflow)?;
        let gross_pnl = closing_gross_pnl(fill, previous_position, average, close_qty)?;
        let realized_delta = checked_add_money(negate_money(fill.fee)?, gross_pnl)?;
        let matched_entry_fee =
            proportional_money(self.open_entry_fees, close_qty, previous_position.abs())?;
        let matched_exit_fee = proportional_money(fill.fee, close_qty, fill.quantity.as_decimal())?;
        let closed_trade_pnl = checked_sub_money(
            checked_sub_money(gross_pnl, matched_entry_fee)?,
            matched_exit_fee,
        )?;

        let remainder = fill
            .quantity
            .as_decimal()
            .checked_sub(close_qty)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        self.average_entry_price = if new_position.is_zero() {
            None
        } else if remainder.is_zero() {
            Some(average)
        } else {
            Some(fill.fill_price)
        };
        self.open_entry_fees = if remainder.is_zero() {
            checked_sub_money(self.open_entry_fees, matched_entry_fee)?
        } else {
            checked_sub_money(fill.fee, matched_exit_fee)?
        };

        Ok((realized_delta, Some(closed_trade_pnl)))
    }

    pub(crate) fn snapshot(&self, mark_price: Price) -> Result<LedgerSnapshot, BacktestError> {
        let unrealized_pnl = Money::new(
            match (self.average_entry_price, self.position_qty.is_zero()) {
                (_, true) => Decimal::ZERO,
                (Some(average), false) if self.position_qty > Decimal::ZERO => mark_price
                    .as_decimal()
                    .checked_sub(average.as_decimal())
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(self.position_qty)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
                (Some(average), false) => average
                    .as_decimal()
                    .checked_sub(mark_price.as_decimal())
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(self.position_qty.abs())
                    .ok_or(BacktestError::ArithmeticOverflow)?,
                (None, false) => return Err(BacktestError::ArithmeticOverflow),
            },
        );
        let equity = Money::new(
            self.cash
                .as_decimal()
                .checked_add(
                    self.position_qty
                        .checked_mul(mark_price.as_decimal())
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

        Ok(LedgerSnapshot {
            cash: self.cash,
            position_qty: self.position_qty,
            average_entry_price: self.average_entry_price,
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            equity,
        })
    }
}

fn fill_notional(fill: &TradeFill) -> Result<Money, BacktestError> {
    fill.fill_price
        .as_decimal()
        .checked_mul(fill.quantity.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn negate_money(value: Money) -> Result<Money, BacktestError> {
    Decimal::ZERO
        .checked_sub(value.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn closing_gross_pnl(
    fill: &TradeFill,
    previous_position: Decimal,
    average: Price,
    close_qty: Decimal,
) -> Result<Money, BacktestError> {
    let price_delta = if previous_position > Decimal::ZERO {
        fill.fill_price
            .as_decimal()
            .checked_sub(average.as_decimal())
    } else {
        average
            .as_decimal()
            .checked_sub(fill.fill_price.as_decimal())
    }
    .ok_or(BacktestError::ArithmeticOverflow)?;
    price_delta
        .checked_mul(close_qty)
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn checked_add_money(left: Money, right: Money) -> Result<Money, BacktestError> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn checked_sub_money(left: Money, right: Money) -> Result<Money, BacktestError> {
    left.as_decimal()
        .checked_sub(right.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn proportional_money(total: Money, part: Decimal, whole: Decimal) -> Result<Money, BacktestError> {
    if whole <= Decimal::ZERO || part < Decimal::ZERO || part > whole {
        return Err(BacktestError::ArithmeticOverflow);
    }
    if part == whole {
        return Ok(total);
    }
    total
        .as_decimal()
        .checked_mul(part)
        .and_then(|value| value.checked_div(whole))
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn same_direction(left: Decimal, right: Decimal) -> bool {
    (left > Decimal::ZERO && right > Decimal::ZERO)
        || (left < Decimal::ZERO && right < Decimal::ZERO)
}

fn weighted_average(
    left_price: Price,
    left_qty: Decimal,
    right_price: Price,
    right_qty: Decimal,
) -> Result<Price, BacktestError> {
    let total_qty = left_qty
        .checked_add(right_qty)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let weighted = left_price
        .as_decimal()
        .checked_mul(left_qty)
        .and_then(|left| {
            right_price
                .as_decimal()
                .checked_mul(right_qty)
                .and_then(|right| left.checked_add(right))
        })
        .ok_or(BacktestError::ArithmeticOverflow)?
        .checked_div(total_qty)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Ok(Price::new(weighted)?)
}
