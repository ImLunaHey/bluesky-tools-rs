//! An ergonomic, testable toolkit for building Bluesky bots.

mod bot;
mod cache;
mod facets;
mod models;
mod rate_limit;
mod transport;

pub use bot::{Bot, BotConfig, BotError, Session};
pub use cache::CacheConfig;
pub use models::*;
pub use rate_limit::RateLimitConfig;
pub use transport::{
    HttpTransport, Method, Request, RequestBody, Response, Transport, TransportError,
};

pub use bluesky_jetstream as jetstream;
pub use bluesky_richtext as richtext;

use unicode_segmentation::UnicodeSegmentation;

/// Counts user-perceived characters rather than Unicode scalar values or bytes.
#[must_use]
pub fn grapheme_length(text: &str) -> usize {
    text.graphemes(true).count()
}
