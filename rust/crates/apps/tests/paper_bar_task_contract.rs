use chrono::{Duration, TimeZone, Utc};
use crypto_trading_cli::{PaperBarAction, PaperBarTask, PaperBarTaskError, PaperBarTaskState};
use crypto_trading_domain::{Price, Side};
use crypto_trading_strategy::{
    Bar, BarStrategy, BarStrategyContext, SlowTimeSeriesMomentum, TargetExposure,
};
use rust_decimal::Decimal;
use std::cmp::Ordering;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn bar(day: i64, close: &str) -> Bar {
    let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
    Bar::new(
        open_time,
        open_time + Duration::days(1) - Duration::milliseconds(1),
        price(close),
        price(close),
        price(close),
        price(close),
        Decimal::ONE,
        decimal("100"),
        1,
    )
    .unwrap()
}

fn expected_action(previous: Decimal, next: Decimal) -> PaperBarAction {
    match next.cmp(&previous) {
        Ordering::Greater => PaperBarAction::Rebalance {
            side: Side::Buy,
            target: TargetExposure::new(next).unwrap(),
        },
        Ordering::Less => PaperBarAction::Rebalance {
            side: Side::Sell,
            target: TargetExposure::new(next).unwrap(),
        },
        Ordering::Equal => PaperBarAction::Hold,
    }
}

#[derive(Debug)]
struct FixedTargetStrategy(TargetExposure);

impl BarStrategy for FixedTargetStrategy {
    fn target_exposure(
        &mut self,
        _context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, crypto_trading_strategy::StrategyError> {
        Ok(self.0)
    }
}

#[test]
fn paper_bar_task_matches_shared_strategy_targets_and_rebalance_directions() {
    let bars = [bar(0, "100"), bar(1, "90"), bar(2, "110"), bar(3, "80")];
    let mut paper = PaperBarTask::new(SlowTimeSeriesMomentum::new(2, 1).unwrap());
    let mut strategy = SlowTimeSeriesMomentum::new(2, 1).unwrap();
    let mut previous_target = Decimal::ZERO;

    for index in 0..bars.len() {
        let decision = paper.on_bar(bars[index].clone()).unwrap();
        let expected_target = strategy
            .target_exposure(&BarStrategyContext {
                history: &bars[..=index],
                decided_at: bars[index].close_time,
                bar_index: index,
                current_target: previous_target,
            })
            .unwrap()
            .as_decimal();

        assert_eq!(decision.target.as_decimal(), expected_target);
        assert_eq!(
            decision.action,
            expected_action(previous_target, expected_target)
        );
        previous_target = expected_target;
    }
}

#[test]
fn paper_bar_task_rejects_duplicate_out_of_order_and_overlapping_bars() {
    let first = bar(0, "100");
    let duplicate = bar(0, "101");
    let overlapping = Bar::new(
        first.close_time,
        first.close_time + Duration::hours(1) - Duration::milliseconds(1),
        price("101"),
        price("101"),
        price("101"),
        price("101"),
        Decimal::ONE,
        decimal("100"),
        1,
    )
    .unwrap();
    let out_of_order = bar(-1, "99");

    let mut paper = PaperBarTask::new(SlowTimeSeriesMomentum::new(2, 1).unwrap());
    paper.on_bar(first).unwrap();

    assert_eq!(
        paper.on_bar(duplicate),
        Err(PaperBarTaskError::InvalidBarSequence)
    );
    assert_eq!(
        paper.on_bar(overlapping),
        Err(PaperBarTaskError::InvalidBarSequence)
    );
    assert_eq!(
        paper.on_bar(out_of_order),
        Err(PaperBarTaskError::InvalidBarSequence)
    );
}

#[test]
fn paper_bar_task_accepts_absolute_bar_indexes_and_external_actual_targets() {
    let actual_target = TargetExposure::new(decimal("0.6")).unwrap();
    let requested_target = TargetExposure::new(decimal("0.9")).unwrap();
    let mut paper = PaperBarTask::with_state(
        FixedTargetStrategy(requested_target),
        PaperBarTaskState {
            next_bar_index: 5,
            current_target: TargetExposure::new(decimal("0.25")).unwrap(),
        },
    );

    let decision = paper
        .on_bar_with_current_target(bar(0, "100"), actual_target)
        .unwrap();

    assert_eq!(decision.bar_index, 5);
    assert_eq!(decision.target, requested_target);
    assert_eq!(paper.state().next_bar_index, 6);
    assert_eq!(paper.state().current_target, actual_target);
}
