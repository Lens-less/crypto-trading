use rust_decimal::Decimal;

use crate::{
    BacktestError,
    engine::{Side, TradeFill},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerSnapshot {
    pub cash: Decimal,
    pub position_qty: Decimal,
    pub average_entry_price: Option<Decimal>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub equity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppliedFill {
    pub realized_pnl_delta: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ledger {
    cash: Decimal,
    position_qty: Decimal,
    average_entry_price: Option<Decimal>,
    realized_pnl: Decimal,
}

impl Ledger {
    pub(crate) fn new(initial_cash: Decimal) -> Result<Self, BacktestError> {
        if initial_cash < Decimal::ZERO {
            return Err(BacktestError::InvalidInitialCash);
        }

        Ok(Self {
            cash: initial_cash,
            position_qty: Decimal::ZERO,
            average_entry_price: None,
            realized_pnl: Decimal::ZERO,
        })
    }

    pub(crate) fn apply_fill(&mut self, fill: &TradeFill) -> Result<AppliedFill, BacktestError> {
        let notional = fill
            .fill_price
            .checked_mul(fill.quantity)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        self.cash = match fill.side {
            Side::Buy => self
                .cash
                .checked_sub(notional)
                .and_then(|cash| cash.checked_sub(fill.fee))
                .ok_or(BacktestError::ArithmeticOverflow)?,
            Side::Sell => self
                .cash
                .checked_add(notional)
                .and_then(|cash| cash.checked_sub(fill.fee))
                .ok_or(BacktestError::ArithmeticOverflow)?,
        };

        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => Decimal::ZERO
                .checked_sub(fill.quantity)
                .ok_or(BacktestError::ArithmeticOverflow)?,
        };
        let previous_position = self.position_qty;
        let previous_average = self.average_entry_price;
        let new_position = previous_position
            .checked_add(signed_qty)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let mut realized_delta = Decimal::ZERO
            .checked_sub(fill.fee)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        if previous_position.is_zero() || same_direction(previous_position, signed_qty) {
            self.average_entry_price =
                Some(match (previous_average, previous_position.is_zero()) {
                    (_, true) => fill.fill_price,
                    (Some(average), false) => weighted_average(
                        average,
                        previous_position.abs(),
                        fill.fill_price,
                        fill.quantity,
                    )?,
                    (None, false) => return Err(BacktestError::ArithmeticOverflow),
                });
        } else {
            let close_qty = previous_position.abs().min(fill.quantity);
            let average = previous_average.ok_or(BacktestError::ArithmeticOverflow)?;
            let gross_pnl = if previous_position > Decimal::ZERO {
                fill.fill_price
                    .checked_sub(average)
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(close_qty)
                    .ok_or(BacktestError::ArithmeticOverflow)?
            } else {
                average
                    .checked_sub(fill.fill_price)
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(close_qty)
                    .ok_or(BacktestError::ArithmeticOverflow)?
            };
            realized_delta = realized_delta
                .checked_add(gross_pnl)
                .ok_or(BacktestError::ArithmeticOverflow)?;

            let remainder = fill
                .quantity
                .checked_sub(close_qty)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            self.average_entry_price = if new_position.is_zero() {
                None
            } else if remainder.is_zero() {
                Some(average)
            } else {
                Some(fill.fill_price)
            };
        }

        self.position_qty = new_position;
        if self.position_qty.is_zero() {
            self.average_entry_price = None;
        }
        self.realized_pnl = self
            .realized_pnl
            .checked_add(realized_delta)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        Ok(AppliedFill {
            realized_pnl_delta: realized_delta,
        })
    }

    pub(crate) fn snapshot(&self, mark_price: Decimal) -> Result<LedgerSnapshot, BacktestError> {
        if mark_price <= Decimal::ZERO {
            return Err(BacktestError::NonPositivePrice);
        }

        let unrealized_pnl = match (self.average_entry_price, self.position_qty.is_zero()) {
            (_, true) => Decimal::ZERO,
            (Some(average), false) if self.position_qty > Decimal::ZERO => mark_price
                .checked_sub(average)
                .ok_or(BacktestError::ArithmeticOverflow)?
                .checked_mul(self.position_qty)
                .ok_or(BacktestError::ArithmeticOverflow)?,
            (Some(average), false) => average
                .checked_sub(mark_price)
                .ok_or(BacktestError::ArithmeticOverflow)?
                .checked_mul(self.position_qty.abs())
                .ok_or(BacktestError::ArithmeticOverflow)?,
            (None, false) => return Err(BacktestError::ArithmeticOverflow),
        };
        let equity = self
            .cash
            .checked_add(
                self.position_qty
                    .checked_mul(mark_price)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .ok_or(BacktestError::ArithmeticOverflow)?;

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

fn same_direction(left: Decimal, right: Decimal) -> bool {
    (left > Decimal::ZERO && right > Decimal::ZERO)
        || (left < Decimal::ZERO && right < Decimal::ZERO)
}

fn weighted_average(
    left_price: Decimal,
    left_qty: Decimal,
    right_price: Decimal,
    right_qty: Decimal,
) -> Result<Decimal, BacktestError> {
    let total_qty = left_qty
        .checked_add(right_qty)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    left_price
        .checked_mul(left_qty)
        .and_then(|left| {
            right_price
                .checked_mul(right_qty)
                .and_then(|right| left.checked_add(right))
        })
        .ok_or(BacktestError::ArithmeticOverflow)?
        .checked_div(total_qty)
        .ok_or(BacktestError::ArithmeticOverflow)
}
