//! Congestion control layers for [`tower`](https://docs.rs/tower/latest/tower) services.
//!
//! For rate-limited [`Service`]s(e.g. `RateLimitLayer`), this crate can be useful to
//! reduce latency variance of each requests to the services.
//! Most algorithms/terms are from TCP congestion control, but they may have little subtle
//! differences.
//!
//! [`Service`]: tower_service::Service

use std::time::Duration;

pub mod aimd;
pub mod variable_rate_limit;
pub mod interval;

#[cfg(test)]
mod test_util {
    use std::sync::Once;

    pub(crate) fn init_logger_once() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            tracing_subscriber::fmt::init();
        });
    }
}
