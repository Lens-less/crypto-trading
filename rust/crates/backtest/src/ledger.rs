use crypto_trading_domain::{Money, Price, Side};
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ledger {
    cash: Money,
    position_qty: Decimal,
    average_entry_price: Option<Price>,
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
            realized_pnl: Money::default(),
        })
    }

    pub(crate) fn apply_fill(&mut self, fill: &TradeFill) -> Result<AppliedFill, BacktestError> {
        let notional = Money::new(
            fill.fill_price
                .as_decimal()
                .checked_mul(fill.quantity.as_decimal())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

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
        let mut realized_delta = Money::new(
            Decimal::ZERO
                .checked_sub(fill.fee.as_decimal())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

        if previous_position.is_zero() || same_direction(previous_position, signed_qty) {
            self.average_entry_price =
                Some(match (previous_average, previous_position.is_zero()) {
                    (_, true) => fill.fill_price,
                    (Some(average), false) => weighted_average(
                        average,
                        previous_position.abs(),
                        fill.fill_price,
                        fill.quantity.as_decimal(),
                    )?,
                    (None, false) => return Err(BacktestError::ArithmeticOverflow),
                });
        } else {
            let close_qty = previous_position.abs().min(fill.quantity.as_decimal());
            let average = previous_average.ok_or(BacktestError::ArithmeticOverflow)?;
            let gross_pnl = Money::new(if previous_position > Decimal::ZERO {
                fill.fill_price
                    .as_decimal()
                    .checked_sub(average.as_decimal())
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(close_qty)
                    .ok_or(BacktestError::ArithmeticOverflow)?
            } else {
                average
                    .as_decimal()
                    .checked_sub(fill.fill_price.as_decimal())
                    .ok_or(BacktestError::ArithmeticOverflow)?
                    .checked_mul(close_qty)
                    .ok_or(BacktestError::ArithmeticOverflow)?
            });
            realized_delta = Money::new(
                realized_delta
                    .as_decimal()
                    .checked_add(gross_pnl.as_decimal())
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            );

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
        }

        self.position_qty = new_position;
        if self.position_qty.is_zero() {
            self.average_entry_price = None;
        }
        self.realized_pnl = Money::new(
            self.realized_pnl
                .as_decimal()
                .checked_add(realized_delta.as_decimal())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

        Ok(AppliedFill {
            realized_pnl_delta: realized_delta,
        })
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
