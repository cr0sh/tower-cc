use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use pin_project::pin_project;
use tokio::time::{sleep, Instant, Sleep};

/// A periodic ticker like [`Interval`](tokio::time::Interval), but allowing updating the period
/// in-place, and does not fire on creation immediately.
#[pin_project]
pub struct Interval {
    period: Duration,
    #[pin]
    sleep: Sleep,
}

impl Interval {
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            sleep: sleep(period),
        }
    }

    pub fn poll_tick(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Instant> {
        let mut this = self.project();
        if this.sleep.as_mut().poll(cx).is_ready() {
            let next = this.sleep.deadline() + *this.period;
            let now = Instant::now();
            this.sleep
                .as_mut()
                .reset(if now > next { now + *this.period } else { next });

            Poll::Ready(Instant::now())
        } else {
            Poll::Pending
        }
    }

    /// Sets a new period and resets the timer.
    pub fn set_period(self: Pin<&mut Self>, period: Duration) {
        let mut this = self.project();
        *this.period = period;
        this.sleep.as_mut().reset(Instant::now() + period)
    }
}
