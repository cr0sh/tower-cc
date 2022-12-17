//! Adjustable rate limits.

use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use tokio::time::{sleep, sleep_until, Instant, Sleep};
use tower_layer::Layer;
use tower_service::Service;
use tracing::trace;

use crate::interval::Interval;

/// [`RateLimitLayer`] with adjustable rates.
///
/// There's a slight difference between this and the original layer; the original [`RateLimitLayer`]
/// resets the timer if [`Service::call`] consumes the last limit budget, but [`VariableRateLimitLayer`]
/// does not. Resets only happen on [`Service::poll_ready`] calls.
///
/// [`RateLimitLayer`]: https://docs.rs/tower/0.4/tower/limit/struct.RateLimitLayer.html
pub struct VariableRateLimitLayer {
    per: Duration,
}

impl VariableRateLimitLayer {
    /// Creates a new varaible rate limit layer.
    pub fn new(per: Duration) -> Self {
        Self { per }
    }
}

impl<S> Layer<S> for VariableRateLimitLayer {
    type Service = VariableRateLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        VariableRateLimit::new(inner, self.per)
    }
}

/// Service created by [`VariableRateLimitLayer`].
pub struct VariableRateLimit<T> {
    inner: T,
    limited: bool,
    interval: Pin<Box<Interval>>,
}

impl<S, Request> Service<Request> for VariableRateLimit<S>
where
    S: Service<Request>,
{
    type Response = <S as Service<Request>>::Response;

    type Error = <S as Service<Request>>::Error;

    type Future = <S as Service<Request>>::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        trace!(self.limited, "poll_ready");
        if !self.limited {
            return self.inner.poll_ready(cx);
        }

        if self.interval.as_mut().poll_tick(cx).is_ready() {
            trace!(self.limited, "reset count");
            self.limited = false;
            ready!(self.inner.poll_ready(cx))?;
            Poll::Ready(Ok(()))
        } else {
            trace!("service is still waiting for the next period");
            Poll::Pending
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        trace!(self.limited, "call (before decrement)");
        if !self.limited {
            self.limited = true;
        } else {
            panic!("service is not ready, which means poll_ready is not resolved yet");
        }
        self.inner.call(req)
    }
}

impl<T> VariableRateLimit<T> {
    pub(crate) fn new(inner: T, per: Duration) -> Self {
        Self {
            inner,
            limited: false,
            interval: Box::pin(Interval::new(per)),
        }
    }

    /// Resets the timer for given duration. This is a new 'zero point' for further ticks.
    pub fn set_per(&mut self, per: Duration) {
        self.interval.as_mut().set_period(per)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::{test, time::sleep};
    use tokio_test::{assert_pending, assert_ready};
    use tower_service::Service;
    use tower_test::{assert_request_eq, mock};

    use crate::{test_util::init_logger_once, variable_rate_limit::VariableRateLimitLayer};

    #[test(start_paused = true)]
    async fn no_rate_change() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(Duration::from_secs(3)));

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(3.5)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());
    }

    #[test(start_paused = true)]
    async fn single_rate_change() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(Duration::from_secs(3)));

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(3.5)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());
        sleep(Duration::from_secs_f64(1.5)).await;
        service.get_mut().set_per(Duration::from_secs(1));
        assert_pending!(service.poll_ready());
        sleep(Duration::from_secs_f64(0.5)).await;
        assert_pending!(service.poll_ready());
        sleep(Duration::from_secs_f64(0.4)).await;
        assert_pending!(service.poll_ready());
        sleep(Duration::from_secs_f64(0.2)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());
    }

    #[test(start_paused = true)]
    async fn long_idle() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(Duration::from_secs(3)));

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        sleep(Duration::from_secs_f64(30.5)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_pending!(service.poll_ready());
    }

    #[test(start_paused = true)]
    async fn long_time() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(Duration::from_millis(300)));

        let sends = Arc::new(AtomicU64::new(0));
        let sends_ = Arc::clone(&sends);
        let recvs = Arc::new(AtomicU64::new(0));
        let recvs_ = Arc::clone(&recvs);
        tokio::spawn(async move {
            loop {
                poll_fn(|cx| service.get_mut().poll_ready(cx))
                    .await
                    .unwrap();

                let response = service.call("hello");
                assert_eq!(response.await.unwrap(), "world");
                sends_.fetch_add(1, Ordering::Relaxed);
            }
        });

        tokio::spawn(async move {
            loop {
                assert_request_eq!(handle, "hello").send_response("world");
                recvs_.fetch_add(1, Ordering::Relaxed);
            }
        });

        sleep(Duration::from_secs(10)).await;

        assert_eq!(sends.load(Ordering::Relaxed), 34);
        assert_eq!(recvs.load(Ordering::Relaxed), 34);
    }
}
