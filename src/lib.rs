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
pub mod varaible_rate_limit;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Rate {
    num: u64,
    per: Duration,
}
