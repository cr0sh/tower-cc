//! Additive-Increase, Multiplicative-Decrease congestion control.

//! A layer implementing AIMD congsetion control method.

use std::{pin::Pin, task::Poll, time::Duration};

use tower_layer::Layer;
use tower_service::Service;
use tracing::trace;

use crate::{interval::Interval, variable_rate_limit::VariableRateLimit};
pub struct AimdLayer {
    initial_rps: f64,
}

impl AimdLayer {
    /// Create a new [`AimdLayer`] instance with arbitrarily chosen initial rate, 1 request per 1
    /// second.
    pub fn new() -> Self {
        Self { initial_rps: 1.0 }
    }
}

impl Default for AimdLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for AimdLayer {
    type Service = Aimd<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Aimd {
            inner,
            update_rps: Box::pin(Interval::new(Duration::from_secs(1))),
            limiter: Box::pin(Interval::new(Duration::from_secs_f64(
                1.0 / self.initial_rps,
            ))),
            limited: false,
            slowdown: false,
            rps: self.initial_rps,
        }
    }
}

pub struct Aimd<T> {
    inner: T,
    update_rps: Pin<Box<Interval>>,
    limiter: Pin<Box<Interval>>,
    limited: bool,
    slowdown: bool,
    rps: f64,
}

impl<S, Request> Service<Request> for Aimd<S>
where
    S: Service<Request>,
{
    type Response = <S as Service<Request>>::Response;

    type Error = <S as Service<Request>>::Error;

    type Future = <S as Service<Request>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        trace!(self.slowdown, self.rps, "poll_ready");
        let poll = self.inner.poll_ready(cx);
        let update_tick = self.update_rps.as_mut().poll_tick(cx);
        let limiter_tick = self.limiter.as_mut().poll_tick(cx);

        if limiter_tick.is_ready() {
            self.limited = false;
        }

        if poll.is_pending() {
            self.slowdown = true;
        }

        if update_tick.is_ready() {
            trace!(self.slowdown, self.rps, "inner timer tick");
            if self.slowdown {
                self.rps /= 2.0;
            } else {
                self.rps += 1.0;
            }

            self.limiter
                .as_mut()
                .set_period(Duration::from_secs_f64(1.0 / self.rps));

            self.slowdown = false;

            if self.limited {
                Poll::Pending
            } else {
                poll
            }
        } else {
            Poll::Pending
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if self.limited {
            panic!("attempted to call service without poll_ready");
        }
        self.limited = true;
        self.inner.call(req)
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
    use tower_service::Service;
    use tower_test::{assert_request_eq, mock};

    use crate::{aimd::AimdLayer, test_util::init_logger_once};

    macro_rules! assert_approx_eq {
        ($left:expr, $right:expr) => {
            match ($left, $right) {
                (left, right) => {
                    if (left - right).abs() > 1e-6 && (left - right).abs() / right > 1e-6 {
                        assert_eq!(left, right);
                    }
                }
            }
        };
    }

    #[test(start_paused = true)]
    async fn test_increase() {
        init_logger_once();

        let (mut service, mut handle) = mock::spawn_layer(AimdLayer::new());
        assert_approx_eq!(service.get_mut().rps, 1.0);

        let last_rps = Arc::new(AtomicU64::new(0));
        let last_rps_ = Arc::clone(&last_rps);

        tokio::spawn(async move {
            loop {
                poll_fn(|cx| service.get_mut().poll_ready(cx))
                    .await
                    .unwrap();

                let response = service.call("hello");
                assert_eq!(response.await.unwrap(), "world");
                last_rps_.store(service.get_ref().rps as u64, Ordering::Relaxed);
            }
        });

        tokio::spawn(async move {
            loop {
                assert_request_eq!(handle, "hello").send_response("world");
            }
        });
        sleep(Duration::from_secs_f64(3.1)).await;
        assert_eq!(last_rps.load(Ordering::Relaxed), 4);
    }
}
