use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::timeout,
};

use crate::{
    ExchangeError, ExchangeHandle, ExchangeStatus, MarketSubscription, ReconcileReceipt,
    ReconcileScope, SubscriptionReceipt, TradingCommand, TradingReceipt,
};

/// Cloneable actor handle that caps all queued and in-flight adapter work.
#[derive(Clone)]
pub struct BoundedExchangeHandle {
    sender: mpsc::Sender<Request>,
    in_flight: Arc<Semaphore>,
    capacity: usize,
}

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

enum Request {
    Execute {
        command: TradingCommand,
        response: oneshot::Sender<Result<TradingReceipt, ExchangeError>>,
        _permit: OwnedSemaphorePermit,
    },
    Reconcile {
        scope: ReconcileScope,
        response: oneshot::Sender<Result<ReconcileReceipt, ExchangeError>>,
        _permit: OwnedSemaphorePermit,
    },
    Subscribe {
        subscription: MarketSubscription,
        response: oneshot::Sender<Result<SubscriptionReceipt, ExchangeError>>,
        _permit: OwnedSemaphorePermit,
    },
    Status {
        response: oneshot::Sender<Result<ExchangeStatus, ExchangeError>>,
        _permit: OwnedSemaphorePermit,
    },
}

impl BoundedExchangeHandle {
    /// Starts a single-owner adapter actor on the current Tokio runtime.
    pub fn spawn<H>(inner: Arc<H>, capacity: NonZeroUsize) -> Self
    where
        H: ExchangeHandle + 'static,
    {
        let inner: Arc<dyn ExchangeHandle> = inner;
        Self::spawn_dyn_with_timeout(inner, capacity, DEFAULT_OPERATION_TIMEOUT)
    }

    /// Starts a single-owner adapter actor with an explicit operation timeout.
    pub fn spawn_with_timeout<H>(
        inner: Arc<H>,
        capacity: NonZeroUsize,
        operation_timeout: Duration,
    ) -> Self
    where
        H: ExchangeHandle + 'static,
    {
        let inner: Arc<dyn ExchangeHandle> = inner;
        Self::spawn_dyn_with_timeout(inner, capacity, operation_timeout)
    }

    /// Starts a bounded actor from an object-safe exchange implementation.
    pub fn spawn_dyn(inner: Arc<dyn ExchangeHandle>, capacity: NonZeroUsize) -> Self {
        Self::spawn_dyn_with_timeout(inner, capacity, DEFAULT_OPERATION_TIMEOUT)
    }

    /// Starts a bounded actor from an object-safe adapter with an explicit timeout.
    pub fn spawn_dyn_with_timeout(
        inner: Arc<dyn ExchangeHandle>,
        capacity: NonZeroUsize,
        operation_timeout: Duration,
    ) -> Self {
        let capacity = capacity.get();
        let (sender, mut receiver) = mpsc::channel(capacity);
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                match request {
                    Request::Execute {
                        command,
                        response,
                        _permit,
                    } => {
                        let operation = command.operation();
                        let client_order_id = command.client_order_id();
                        let result = timeout(operation_timeout, inner.execute(command))
                            .await
                            .unwrap_or_else(|_| {
                                Err(ExchangeError::AmbiguousOutcome {
                                    operation,
                                    client_order_id,
                                    reason: "adapter operation timed out after local admission"
                                        .to_owned(),
                                })
                            });
                        let _response_was_dropped = response.send(result);
                    }
                    Request::Reconcile {
                        scope,
                        response,
                        _permit,
                    } => {
                        let result = timeout(operation_timeout, inner.reconcile(scope))
                            .await
                            .unwrap_or_else(|_| {
                                Err(ExchangeError::unavailable(
                                    "exchange reconciliation timed out",
                                ))
                            });
                        let _response_was_dropped = response.send(result);
                    }
                    Request::Subscribe {
                        subscription,
                        response,
                        _permit,
                    } => {
                        let result = timeout(operation_timeout, inner.subscribe(subscription))
                            .await
                            .unwrap_or_else(|_| {
                                Err(ExchangeError::unavailable(
                                    "exchange subscription timed out",
                                ))
                            });
                        let _response_was_dropped = response.send(result);
                    }
                    Request::Status { response, _permit } => {
                        let result = timeout(operation_timeout, inner.status())
                            .await
                            .unwrap_or_else(|_| {
                                Err(ExchangeError::unavailable("exchange status timed out"))
                            });
                        let _response_was_dropped = response.send(result);
                    }
                }
            }
        });
        Self {
            sender,
            in_flight: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    fn try_admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, ExchangeError> {
        Arc::clone(&self.in_flight)
            .try_acquire_owned()
            .map_err(|_| ExchangeError::Backpressure {
                capacity: self.capacity,
            })
    }

    fn send(&self, request: Request) -> Result<(), ExchangeError> {
        self.sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ExchangeError::Backpressure {
                capacity: self.capacity,
            },
            mpsc::error::TrySendError::Closed(_) => {
                ExchangeError::unavailable("exchange command actor is closed")
            }
        })
    }
}

#[async_trait]
impl ExchangeHandle for BoundedExchangeHandle {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        let permit = self.try_admit()?;
        let operation = command.operation();
        let client_order_id = command.client_order_id();
        let (response, receiver) = oneshot::channel();
        self.send(Request::Execute {
            command,
            response,
            _permit: permit,
        })?;
        receiver
            .await
            .map_err(|_| ExchangeError::AmbiguousOutcome {
                operation,
                client_order_id,
                reason: "request was accepted locally but the adapter response was lost".to_owned(),
            })?
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        let permit = self.try_admit()?;
        let (response, receiver) = oneshot::channel();
        self.send(Request::Reconcile {
            scope,
            response,
            _permit: permit,
        })?;
        receiver
            .await
            .map_err(|_| ExchangeError::unavailable("reconciliation response was lost"))?
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        let permit = self.try_admit()?;
        let (response, receiver) = oneshot::channel();
        self.send(Request::Subscribe {
            subscription,
            response,
            _permit: permit,
        })?;
        receiver
            .await
            .map_err(|_| ExchangeError::unavailable("subscription response was lost"))?
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        let permit = self.try_admit()?;
        let (response, receiver) = oneshot::channel();
        self.send(Request::Status {
            response,
            _permit: permit,
        })?;
        receiver
            .await
            .map_err(|_| ExchangeError::unavailable("status response was lost"))?
    }
}
