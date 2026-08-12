use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Symbol};
use crypto_trading_exchange::{
    BinanceProduct, BinanceTestnetAccountSnapshot, BinanceTestnetBalance,
};
use crypto_trading_runtime::{
    PaperAccountSnapshot, PaperReconciliationEvidence, PaperReconciliationProof,
    PaperReservationPhase, PaperReservationView, ProjectionStatus,
};
use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

pub const TESTNET_RECONCILIATION_SCHEMA_VERSION: u16 = 1;
pub const TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT: &str =
    "I APPLY VERIFIED BINANCE TESTNET RECONCILIATION";

const MAX_SETTLEMENT_ASSET_BYTES: usize = 32;

/// Exact product and instrument boundary for one Testnet/Paper comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestnetReconciliationConfig {
    product: BinanceProduct,
    settlement_asset: String,
    symbol: Symbol,
    reservation_id: Uuid,
}

impl TestnetReconciliationConfig {
    /// Builds a bounded reconciliation scope.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe assets or a nil reservation identity.
    pub fn new(
        product: BinanceProduct,
        settlement_asset: impl Into<String>,
        symbol: Symbol,
        reservation_id: Uuid,
    ) -> Result<Self> {
        let settlement_asset = settlement_asset.into();
        ensure!(
            !settlement_asset.is_empty()
                && settlement_asset.len() <= MAX_SETTLEMENT_ASSET_BYTES
                && settlement_asset
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "settlement asset must contain 1..={MAX_SETTLEMENT_ASSET_BYTES} uppercase ASCII letters or digits"
        );
        ensure!(
            !reservation_id.is_nil(),
            "testnet reconciliation reservation id must not be nil"
        );
        let expected_suffix = match product {
            BinanceProduct::Spot => format!("-{settlement_asset}-SPOT"),
            BinanceProduct::UsdM => format!("-{settlement_asset}-PERP"),
        };
        ensure!(
            symbol.as_str().ends_with(&expected_suffix),
            "selected standard symbol must use settlement asset {settlement_asset} and the selected Binance product"
        );
        Ok(Self {
            product,
            settlement_asset,
            symbol,
            reservation_id,
        })
    }

    #[must_use]
    pub const fn product(&self) -> BinanceProduct {
        self.product
    }

    #[must_use]
    pub fn settlement_asset(&self) -> &str {
        &self.settlement_asset
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }
}

/// Stable fail-closed reasons emitted by the bounded account comparator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestnetReconciliationMismatch {
    MissingSettlementBalance,
    UntrackedAssetBalance,
    AvailableBalanceMismatch,
    LockedBalanceNonZero,
    WalletAvailableDivergence,
    OpenOwnedOrders,
    OpenForeignOrders,
    NonFlatPositions,
}

impl TestnetReconciliationMismatch {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingSettlementBalance => "missing_settlement_balance",
            Self::UntrackedAssetBalance => "untracked_asset_balance",
            Self::AvailableBalanceMismatch => "available_balance_mismatch",
            Self::LockedBalanceNonZero => "locked_balance_non_zero",
            Self::WalletAvailableDivergence => "wallet_available_divergence",
            Self::OpenOwnedOrders => "open_owned_orders",
            Self::OpenForeignOrders => "open_foreign_orders",
            Self::NonFlatPositions => "non_flat_positions",
        }
    }
}

/// Validated local Paper projection frozen before any remote account call.
#[derive(Clone, Debug)]
pub struct TestnetReconciliationPlan {
    config: TestnetReconciliationConfig,
    account: PaperAccountSnapshot,
    reservation: PaperReservationView,
    expected_available: Decimal,
}

impl TestnetReconciliationPlan {
    /// Freezes one exact committed Paper reservation as the local side of the
    /// comparison.
    ///
    /// Only a single Binance product/instrument lane is supported. Mixed
    /// exchange or mixed product reservations fail before credentials are
    /// loaded or remote I/O begins.
    ///
    /// # Errors
    ///
    /// Returns an error for degraded projections, missing/non-committed
    /// reservations, or unsupported reservation legs.
    pub fn new(config: TestnetReconciliationConfig, account: PaperAccountSnapshot) -> Result<Self> {
        ensure!(
            account.projection_status == ProjectionStatus::Complete
                && account.invalid_event_count == 0,
            "paper account projection must be complete before Testnet reconciliation"
        );
        let reservation = account
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == config.reservation_id)
            .cloned()
            .context("paper reservation is absent from the selected account")?;
        ensure!(
            reservation.phase == PaperReservationPhase::Committed,
            "paper reservation must be committed before Testnet reconciliation"
        );
        let expected_market = market_for_product(config.product);
        ensure!(
            reservation.legs.iter().all(|leg| {
                leg.exchange().eq_ignore_ascii_case("binance")
                    && leg.market_type() == expected_market
                    && leg.symbol() == &config.symbol
            }),
            "paper reservation legs must all match the selected Binance Testnet product and symbol"
        );
        let expected_available = account
            .available
            .as_decimal()
            .checked_add(reservation.held_exposure.as_decimal())
            .context("paper post-release availability overflowed")?;
        Ok(Self {
            config,
            account,
            reservation,
            expected_available,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &TestnetReconciliationConfig {
        &self.config
    }

    #[must_use]
    pub const fn expected_available(&self) -> Decimal {
        self.expected_available
    }

    /// Compares the frozen Paper projection to one complete signed Testnet
    /// account snapshot and creates a replayable reconciliation proof.
    ///
    /// The release verdict is deliberately strict: the selected settlement
    /// balance must equal the local post-release availability, wallet and
    /// available balance must converge, and the selected product must have no
    /// open orders or non-flat positions.
    ///
    /// # Errors
    ///
    /// Returns an error for product mismatch, invalid capture time, or proof
    /// construction failure.
    pub fn compare(
        &self,
        remote: &BinanceTestnetAccountSnapshot,
        captured_at: DateTime<Utc>,
    ) -> Result<TestnetReconciliationReport> {
        ensure!(
            remote.product == self.config.product,
            "remote Testnet snapshot product does not match the reconciliation plan"
        );
        let balance = remote
            .balances
            .iter()
            .find(|balance| balance.asset == self.config.settlement_asset);
        let comparison = inspect_remote_account(
            remote,
            balance,
            &self.config.settlement_asset,
            self.expected_available,
        );

        let captured_at = captured_at.max(remote.observed_at);
        let snapshot_sequence = u64::try_from(captured_at.timestamp_micros())
            .context("Testnet reconciliation capture time must be after the Unix epoch")?;
        ensure!(
            snapshot_sequence > 0,
            "Testnet reconciliation snapshot sequence must be positive"
        );
        let product = product_label(self.config.product);
        let snapshot_id = format!("binance-testnet:{product}:{snapshot_sequence}");
        let digest = reconciliation_digest(
            self,
            remote,
            captured_at,
            balance,
            comparison.mismatches.as_slice(),
        )?;
        let mismatch_codes = comparison
            .mismatches
            .iter()
            .map(|mismatch| mismatch.code().to_owned())
            .collect::<Vec<_>>();
        let evidence = PaperReconciliationEvidence::new(
            format!("binance-testnet:{product}"),
            digest,
            self.account.account_id.clone(),
            self.reservation.reservation_id,
            self.reservation.batch_id,
            snapshot_id,
            snapshot_sequence,
            2,
            crypto_trading_domain::Money::new(self.expected_available),
            comparison
                .observed_wallet
                .map(crypto_trading_domain::Money::new),
            comparison
                .observed_available
                .map(crypto_trading_domain::Money::new),
            comparison
                .observed_locked
                .map(crypto_trading_domain::Money::new),
            u32::try_from(remote.orders.len()).context("owned order count exceeds u32")?,
            u32::try_from(remote.foreign_orders.len())
                .context("foreign order count exceeds u32")?,
            u32::try_from(remote.positions.len()).context("position count exceeds u32")?,
            u32::try_from(comparison.untracked_asset_count)
                .context("untracked asset count exceeds u32")?,
            mismatch_codes,
        )
        .context("failed to build canonical Testnet reconciliation evidence")?;
        let proof = PaperReconciliationProof::from_evidence(evidence)
            .context("failed to bind Testnet truth to a Paper reconciliation proof")?;

        Ok(TestnetReconciliationReport {
            schema_version: TESTNET_RECONCILIATION_SCHEMA_VERSION,
            product: self.config.product,
            settlement_asset: self.config.settlement_asset.clone(),
            account_id: self.account.account_id.clone(),
            reservation_id: self.reservation.reservation_id,
            batch_id: self.reservation.batch_id,
            expected_available: self.expected_available,
            observed_wallet: comparison.observed_wallet,
            observed_available: comparison.observed_available,
            observed_locked: comparison.observed_locked,
            owned_order_count: remote.orders.len(),
            foreign_order_count: remote.foreign_orders.len(),
            position_count: remote.positions.len(),
            observed_at: remote.observed_at,
            captured_at,
            mismatches: comparison.mismatches,
            proof,
        })
    }
}

struct RemoteAccountComparison {
    observed_wallet: Option<Decimal>,
    observed_available: Option<Decimal>,
    observed_locked: Option<Decimal>,
    untracked_asset_count: usize,
    mismatches: Vec<TestnetReconciliationMismatch>,
}

fn inspect_remote_account(
    remote: &BinanceTestnetAccountSnapshot,
    settlement_balance: Option<&BinanceTestnetBalance>,
    settlement_asset: &str,
    expected_available: Decimal,
) -> RemoteAccountComparison {
    let mut mismatches = Vec::new();
    let untracked_asset_count = remote
        .balances
        .iter()
        .filter(|balance| {
            balance.asset != settlement_asset
                && (!balance.wallet_balance.is_zero()
                    || !balance.available_balance.is_zero()
                    || balance.locked_balance.is_some_and(|value| !value.is_zero()))
        })
        .count();
    if untracked_asset_count > 0 {
        mismatches.push(TestnetReconciliationMismatch::UntrackedAssetBalance);
    }
    let (observed_wallet, observed_available, observed_locked) =
        if let Some(balance) = settlement_balance {
            if balance.available_balance != expected_available {
                mismatches.push(TestnetReconciliationMismatch::AvailableBalanceMismatch);
            }
            if balance
                .locked_balance
                .is_some_and(|locked| !locked.is_zero())
            {
                mismatches.push(TestnetReconciliationMismatch::LockedBalanceNonZero);
            }
            if balance.wallet_balance != balance.available_balance {
                mismatches.push(TestnetReconciliationMismatch::WalletAvailableDivergence);
            }
            (
                Some(balance.wallet_balance),
                Some(balance.available_balance),
                balance.locked_balance,
            )
        } else {
            mismatches.push(TestnetReconciliationMismatch::MissingSettlementBalance);
            (None, None, None)
        };
    if !remote.orders.is_empty() {
        mismatches.push(TestnetReconciliationMismatch::OpenOwnedOrders);
    }
    if !remote.foreign_orders.is_empty() {
        mismatches.push(TestnetReconciliationMismatch::OpenForeignOrders);
    }
    if !remote.positions.is_empty() {
        mismatches.push(TestnetReconciliationMismatch::NonFlatPositions);
    }
    RemoteAccountComparison {
        observed_wallet,
        observed_available,
        observed_locked,
        untracked_asset_count,
        mismatches,
    }
}

/// Machine-readable result of one complete local/remote comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestnetReconciliationReport {
    pub schema_version: u16,
    pub product: BinanceProduct,
    pub settlement_asset: String,
    pub account_id: String,
    pub reservation_id: Uuid,
    pub batch_id: Uuid,
    pub expected_available: Decimal,
    pub observed_wallet: Option<Decimal>,
    pub observed_available: Option<Decimal>,
    pub observed_locked: Option<Decimal>,
    pub owned_order_count: usize,
    pub foreign_order_count: usize,
    pub position_count: usize,
    pub observed_at: DateTime<Utc>,
    pub captured_at: DateTime<Utc>,
    pub mismatches: Vec<TestnetReconciliationMismatch>,
    pub proof: PaperReconciliationProof,
}

impl TestnetReconciliationReport {
    #[must_use]
    pub fn matches(&self) -> bool {
        self.mismatches.is_empty()
    }
}

fn reconciliation_digest(
    plan: &TestnetReconciliationPlan,
    remote: &BinanceTestnetAccountSnapshot,
    captured_at: DateTime<Utc>,
    selected_balance: Option<&BinanceTestnetBalance>,
    mismatches: &[TestnetReconciliationMismatch],
) -> Result<String> {
    let mut orders = remote.orders.clone();
    orders.sort_by(|left, right| left.id.cmp(&right.id));
    let mut foreign_orders = remote.foreign_orders.clone();
    foreign_orders.sort_by(|left, right| left.id.cmp(&right.id));
    let mut positions = remote.positions.clone();
    positions.sort_by(|left, right| {
        left.symbol
            .cmp(&right.symbol)
            .then(position_side_label(left.side).cmp(position_side_label(right.side)))
    });
    let mut balances = remote.balances.clone();
    balances.sort_by(|left, right| left.asset.cmp(&right.asset));
    let balances = balances.iter().map(balance_material).collect::<Vec<_>>();
    let mismatch_codes = mismatches
        .iter()
        .map(|mismatch| mismatch.code())
        .collect::<Vec<_>>();
    let material = json!({
        "schema_version": TESTNET_RECONCILIATION_SCHEMA_VERSION,
        "authority": "binance_testnet",
        "mainnet_enabled": false,
        "product": product_label(plan.config.product),
        "settlement_asset": &plan.config.settlement_asset,
        "symbol": &plan.config.symbol,
        "captured_at": captured_at,
        "remote_observed_at": remote.observed_at,
        "paper_account": &plan.account,
        "paper_reservation": &plan.reservation,
        "expected_post_release_available": normalized_decimal(plan.expected_available),
        "selected_balance": selected_balance.map(balance_material),
        "balances": balances,
        "orders": orders,
        "foreign_orders": foreign_orders,
        "positions": positions,
        "mismatches": mismatch_codes,
    });
    let bytes = serde_json::to_vec(&material)
        .context("failed to serialize canonical Testnet reconciliation material")?;
    Ok(format!("{:016x}", fnv1a64(&bytes)))
}

fn balance_material(balance: &BinanceTestnetBalance) -> serde_json::Value {
    json!({
        "asset": balance.asset,
        "wallet_balance": normalized_decimal(balance.wallet_balance),
        "available_balance": normalized_decimal(balance.available_balance),
        "locked_balance": balance.locked_balance.map(normalized_decimal),
    })
}

fn normalized_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

const fn market_for_product(product: BinanceProduct) -> MarketType {
    match product {
        BinanceProduct::Spot => MarketType::Spot,
        BinanceProduct::UsdM => MarketType::Perpetual,
    }
}

#[must_use]
pub const fn product_label(product: BinanceProduct) -> &'static str {
    match product {
        BinanceProduct::Spot => "spot",
        BinanceProduct::UsdM => "usdm",
    }
}

const fn position_side_label(side: crypto_trading_domain::PositionSide) -> &'static str {
    match side {
        crypto_trading_domain::PositionSide::Long => "long",
        crypto_trading_domain::PositionSide::Short => "short",
        crypto_trading_domain::PositionSide::Flat => "flat",
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
