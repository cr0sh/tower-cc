use std::{
    future::Future,
    pin::Pin,
    task::{ready, Poll},
    time::Duration,
};

use tokio::time::{sleep, sleep_until, Sleep};
use tower_layer::Layer;
use tower_service::Service;
use tracing::trace;

use crate::Rate;

pub struct VariableRateLimitLayer {
    rate: Rate,
}

impl VariableRateLimitLayer {
    /// Create a new varaible rate limit layer.
    pub fn new(num: u64, per: Duration) -> Self {
        Self {
            rate: Rate { num, per },
        }
    }
}

impl<S> Layer<S> for VariableRateLimitLayer {
    type Service = VariableRateLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        VariableRateLimit {
            inner,
            rate: self.rate,
            remaining_count: self.rate.num,
            sleep: Box::pin(sleep(self.rate.per)),
        }
    }
}

pub struct VariableRateLimit<T> {
    inner: T,
    rate: Rate,
    remaining_count: u64,
    sleep: Pin<Box<Sleep>>,
}

impl<S, Request> Service<Request> for VariableRateLimit<S>
where
    S: Service<Request>,
{
    type Response = <S as Service<Request>>::Response;

    type Error = <S as Service<Request>>::Error;

    type Future = <S as Service<Request>>::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        trace!(remaining_count = self.remaining_count, "poll_ready");
        if self.remaining_count > 0 {
            return self.inner.poll_ready(cx);
        }

        if self.sleep.as_mut().poll(cx).is_ready() {
            trace!(remaining_count = self.remaining_count, "reset count");
            self.remaining_count = self.rate.num;
            self.sleep
                .set(sleep_until(self.sleep.deadline() + self.rate.per));
            ready!(self.inner.poll_ready(cx))?;
            Poll::Ready(Ok(()))
        } else {
            trace!("service is still waiting for the next period");
            Poll::Pending
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        trace!(
            remaining_count = self.remaining_count,
            "call (before decrement)"
        );
        if self.remaining_count > 0 {
            self.remaining_count -= 1;
        } else {
            panic!("service is not ready, which means poll_ready is not resolved yet");
        }
        self.inner.call(req)
    }
}

impl<T> VariableRateLimit<T> {
    pub fn set_rate(&mut self, num: u64, per: Duration) {
        self.rate = Rate { num, per };
        self.remaining_count = num;
        self.sleep.set(sleep(per));
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Once,
        },
        time::Duration,
    };

    use tokio::{test, time::sleep};
    use tokio_test::{assert_pending, assert_ready};
    use tower_test::{assert_request_eq, mock};

    use crate::varaible_rate_limit::VariableRateLimitLayer;

    fn init_logger_once() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            tracing_subscriber::fmt::init();
        });
    }

    #[test(start_paused = true)]
    async fn no_rate_change() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(2, Duration::from_secs(3)));

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("alpha");
        assert_request_eq!(handle, "alpha").send_response("beta");
        assert_eq!(response.await.unwrap(), "beta");

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(3.5)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("alpha");
        assert_request_eq!(handle, "alpha").send_response("beta");
        assert_eq!(response.await.unwrap(), "beta");

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());
    }

    #[test(start_paused = true)]
    async fn single_rate_change() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(2, Duration::from_secs(3)));

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("alpha");
        assert_request_eq!(handle, "alpha").send_response("beta");
        assert_eq!(response.await.unwrap(), "beta");

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(3.5)).await;

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("hello");
        assert_request_eq!(handle, "hello").send_response("world");
        assert_eq!(response.await.unwrap(), "world");

        assert_ready!(service.poll_ready()).unwrap();
        let response = service.call("alpha");
        assert_request_eq!(handle, "alpha").send_response("beta");
        assert_eq!(response.await.unwrap(), "beta");

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(1.5)).await;

        service.get_mut().set_rate(4, Duration::from_secs(1));

        for _ in 0..4 {
            assert_ready!(service.poll_ready()).unwrap();
            let response = service.call("hello");
            assert_request_eq!(handle, "hello").send_response("world");
            assert_eq!(response.await.unwrap(), "world");
        }

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(0.5)).await;

        assert_pending!(service.poll_ready());
        assert_pending!(service.poll_ready());

        sleep(Duration::from_secs_f64(0.6)).await;

        for _ in 0..4 {
            assert_ready!(service.poll_ready()).unwrap();
            let response = service.call("hello");
            assert_request_eq!(handle, "hello").send_response("world");
            assert_eq!(response.await.unwrap(), "world");
        }

        assert_pending!(service.poll_ready());
    }

    #[test(start_paused = true)]
    async fn long_time() {
        init_logger_once();

        let (mut service, mut handle) =
            mock::spawn_layer(VariableRateLimitLayer::new(2, Duration::from_millis(300)));

        let sends = Arc::new(AtomicU64::new(0));
        let sends_ = Arc::clone(&sends);
        let recvs = Arc::new(AtomicU64::new(0));
        let recvs_ = Arc::clone(&recvs);
        tokio::spawn(async move {
            loop {
                poll_fn(|_| service.poll_ready()).await.unwrap();

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

        assert_eq!(sends.load(Ordering::Relaxed), 2 + 2 * 33);
        assert_eq!(recvs.load(Ordering::Relaxed), 2 + 2 * 33);
    }
}
