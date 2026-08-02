use thiserror::Error;

use crate::{
    AccountRiskProjectionError, AccountRiskReadModel, ArbitrageMonitorReadModel,
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperatorReadModel,
    PaperAccountProjectionError, PaperAccountReadModel, ReadModelError, ReadOnlyTaskReadModel,
    VirtualGridScannerReadModel, account_risk::ProjectionBuilder as AccountRiskProjectionBuilder,
    alert_read_model::ProjectionBuilder as AlertProjectionBuilder,
    monitor_read_model::MonitorProjectionBuilder, paper_account::ProjectionBuilder as PaperBuilder,
    read_model::ProjectionBuilder as OperatorProjectionBuilder,
    scanner_read_model::ScannerProjectionBuilder, task_read_model::TaskProjectionBuilder,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlPlaneProjectionStats {
    pub state_page_reads: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlPlaneProjection {
    pub operator: OperatorReadModel,
    pub monitor: ArbitrageMonitorReadModel,
    pub alerts: crate::PriceAlertReadModel,
    pub tasks: ReadOnlyTaskReadModel,
    pub scanner: VirtualGridScannerReadModel,
    pub paper_accounts: PaperAccountReadModel,
    pub account_risk: AccountRiskReadModel,
    pub stats: ControlPlaneProjectionStats,
}

#[derive(Debug, Error)]
pub enum ControlPlaneProjectionError {
    #[error(transparent)]
    Projection(#[from] ReadModelError),
    #[error(transparent)]
    PaperAccountProjection(#[from] PaperAccountProjectionError),
    #[error(transparent)]
    AccountRiskProjection(#[from] AccountRiskProjectionError),
}

/// Projects every control-plane state model from one frozen journal pass.
///
/// # Errors
///
/// Returns a projection error when paging does not advance, the journal is
/// malformed, or a money/account-risk interpreter rejects durable state.
pub fn project_control_plane_state(
    snapshot: &JournalSnapshot,
) -> Result<ControlPlaneProjection, ControlPlaneProjectionError> {
    let mut operator = OperatorProjectionBuilder::new(snapshot.journal_id());
    let mut monitor = MonitorProjectionBuilder::new(snapshot.journal_id());
    let mut alerts = AlertProjectionBuilder::new(snapshot.journal_id());
    let mut tasks = TaskProjectionBuilder::new(snapshot.journal_id());
    let mut scanner = ScannerProjectionBuilder::new(snapshot.journal_id());
    let mut paper_accounts = PaperBuilder::new(snapshot.journal_id());
    let mut account_risk = AccountRiskProjectionBuilder::new(snapshot.journal_id());
    let mut stats = ControlPlaneProjectionStats::default();
    let mut cursor = None;

    loop {
        let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())
            .map_err(ReadModelError::Journal)?;
        stats.state_page_reads = stats.state_page_reads.saturating_add(1);

        for event in page.events() {
            operator.observe_event(event)?;
            monitor.observe_event(event);
            alerts.observe_event(event);
            tasks.observe_event(event)?;
            scanner.observe_event(event);
            paper_accounts.observe_event(event);
            account_risk.observe_event(event)?;
        }

        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => {
                alerts.observe_boundary(page.boundary());
                break;
            }
            JournalPageBoundary::PartialTail { offset, bytes } => {
                operator.mark_partial_tail(*offset, *bytes);
                monitor.mark_partial_tail();
                alerts.observe_boundary(page.boundary());
                tasks.mark_partial_tail();
                scanner.mark_partial_tail();
                paper_accounts.mark_partial_tail();
                account_risk.mark_partial_tail();
                break;
            }
            JournalPageBoundary::PageLimit => {
                alerts.observe_boundary(page.boundary());
                let next = page
                    .next_cursor()
                    .cloned()
                    .ok_or_else(non_advancing_error)?;
                if cursor.as_ref().is_some_and(|previous| {
                    previous.next_offset() == next.next_offset()
                        && previous.after_sequence() == next.after_sequence()
                }) {
                    return Err(non_advancing_error());
                }
                cursor = Some(next);
            }
        }
    }

    Ok(ControlPlaneProjection {
        operator: operator.finish(),
        monitor: monitor.finish(),
        alerts: alerts.finish(),
        tasks: tasks.finish(),
        scanner: scanner.finish(),
        paper_accounts: paper_accounts.finish()?,
        account_risk: account_risk.finish(),
        stats,
    })
}

fn non_advancing_error() -> ControlPlaneProjectionError {
    ControlPlaneProjectionError::Projection(ReadModelError::NonAdvancingPage)
}
