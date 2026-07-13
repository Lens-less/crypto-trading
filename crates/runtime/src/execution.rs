use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crypto_trading_domain::OrderIntent;
use crypto_trading_exchange::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, TradingCommand,
    TradingReceipt,
};
use thiserror::Error;

use crate::ExecutionMode;

/// Crosses the exchange seam only after runtime authority and adapter mode agree.
pub struct IntentExecutor<E> {
    exchange: Arc<E>,
    mode: ExecutionMode,
}

impl<E> IntentExecutor<E>
where
    E: ExchangeHandle,
{
    pub const fn new(exchange: Arc<E>, mode: ExecutionMode) -> Self {
        Self { exchange, mode }
    }

    /// Executes each intent in order after validating runtime and adapter mode.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the runtime lacks order authority, the
    /// adapter advertises the wrong safety mode, or an exchange operation fails.
    pub async fn execute_all(
        &self,
        intents: Vec<OrderIntent>,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }
        let expected = execution_mode(self.mode)?;
        require_ready(self.exchange.as_ref(), expected).await?;

        let mut receipts = Vec::with_capacity(intents.len());
        for intent in intents {
            match self
                .exchange
                .execute(TradingCommand::Submit(intent.clone()))
                .await
            {
                Ok(receipt) => receipts.push(receipt),
                Err(source) => {
                    return Err(RuntimeError::PartialExecution {
                        completed: receipts,
                        failed_intent: Box::new(intent),
                        source,
                    });
                }
            }
        }
        Ok(receipts)
    }
}

/// Routes strategy intents to named exchange adapters through one small seam.
pub struct ExchangeRouter {
    exchanges: HashMap<String, Arc<dyn ExchangeHandle>>,
    mode: ExecutionMode,
}

impl ExchangeRouter {
    pub fn new(mode: ExecutionMode) -> Self {
        Self {
            exchanges: HashMap::new(),
            mode,
        }
    }

    pub fn register<E>(&mut self, name: impl Into<String>, exchange: Arc<E>)
    where
        E: ExchangeHandle + 'static,
    {
        self.exchanges.insert(name.into(), exchange);
    }

    /// Routes and executes each intent through its named adapter.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when order authority is absent, an adapter is
    /// missing or has the wrong safety mode, or an exchange operation fails.
    pub async fn execute_all(
        &self,
        intents: Vec<OrderIntent>,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }
        let expected = execution_mode(self.mode)?;
        let mut routed = Vec::with_capacity(intents.len());
        for intent in intents {
            let exchange_name = intent.exchange.clone();
            let exchange = self
                .exchanges
                .get(&exchange_name)
                .ok_or_else(|| RuntimeError::UnknownExchange(exchange_name.clone()))?;
            routed.push((intent, Arc::clone(exchange)));
        }

        let mut validated = HashSet::new();
        for (intent, exchange) in &routed {
            if validated.insert(intent.exchange.clone()) {
                require_ready(exchange.as_ref(), expected).await?;
            }
        }

        let mut receipts = Vec::with_capacity(routed.len());
        for (intent, exchange) in routed {
            match exchange
                .execute(TradingCommand::Submit(intent.clone()))
                .await
            {
                Ok(receipt) => receipts.push(receipt),
                Err(source) => {
                    return Err(RuntimeError::PartialExecution {
                        completed: receipts,
                        failed_intent: Box::new(intent),
                        source,
                    });
                }
            }
        }
        Ok(receipts)
    }
}

fn execution_mode(mode: ExecutionMode) -> Result<ExchangeMode, RuntimeError> {
    match mode {
        ExecutionMode::Paper => Ok(ExchangeMode::Paper),
        ExecutionMode::Monitor => Err(RuntimeError::ModeDisallowsOrders),
        ExecutionMode::Live(_) => Err(RuntimeError::LiveExecutionUnavailable),
    }
}

async fn require_ready(
    exchange: &dyn ExchangeHandle,
    expected: ExchangeMode,
) -> Result<(), RuntimeError> {
    let status = exchange.status().await?;
    if status.mode != expected {
        return Err(RuntimeError::AdapterModeMismatch {
            expected,
            actual: status.mode,
        });
    }
    if status.availability != ExchangeAvailability::Ready {
        return Err(RuntimeError::AdapterUnavailable {
            exchange: status.exchange,
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("monitor mode cannot submit or cancel orders")]
    ModeDisallowsOrders,
    #[error("live execution remains disabled until mandatory risk and reconcile gates are wired")]
    LiveExecutionUnavailable,
    #[error("no exchange adapter is registered for {0}")]
    UnknownExchange(String),
    #[error("runtime expected a {expected:?} adapter but received {actual:?}")]
    AdapterModeMismatch {
        expected: ExchangeMode,
        actual: ExchangeMode,
    },
    #[error("exchange adapter {exchange} is not ready")]
    AdapterUnavailable { exchange: String },
    #[error(
        "execution stopped after a partial outcome; preserve completed receipts and reconcile before retrying: {source}"
    )]
    PartialExecution {
        completed: Vec<TradingReceipt>,
        failed_intent: Box<OrderIntent>,
        #[source]
        source: ExchangeError,
    },
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
}
