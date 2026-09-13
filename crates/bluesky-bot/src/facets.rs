//! Automatic link, mention, and hashtag detection.

use crate::Bot;
use bluesky_richtext::{ByteSlice, Facet, FacetFeature};
use regex::Regex;
use std::sync::LazyLock;

static LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"https?://[^\s<>\"]+"#).expect("valid link regex"));
static MENTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\.[A-Za-z]{2,}")
        .expect("valid mention regex")
});
static TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"#[\p{L}\p{N}_]+").expect("valid tag regex"));

impl Bot {
    /// Detects links, resolvable mentions, and hashtags in plain text.
    ///
    /// Mention handles are resolved through `app.bsky.actor.getProfile`; invalid
    /// or unavailable handles are left undecorated.
    pub async fn detect_facets(&self, text: &str) -> Vec<Facet> {
        let mut candidates = Vec::new();
        for found in LINK.find_iter(text) {
            let trimmed = found
                .as_str()
                .trim_end_matches(['.', ',', '!', '?', ':', ';', ')', ']']);
            let end = found.start() + trimmed.len();
            candidates.push((
                found.start(),
                end,
                FacetFeature::Link {
                    uri: trimmed.into(),
                },
            ));
        }
        for found in TAG.find_iter(text) {
            if found.start() > 0
                && !text[..found.start()]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_whitespace)
            {
                continue;
            }
            candidates.push((
                found.start(),
                found.end(),
                FacetFeature::Tag {
                    tag: found.as_str()[1..].into(),
                },
            ));
        }
        for found in MENTION.find_iter(text) {
            if found.start() > 0
                && !text[..found.start()]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_whitespace)
            {
                continue;
            }
            if let Ok(profile) = self.get_profile(&found.as_str()[1..]).await {
                candidates.push((
                    found.start(),
                    found.end(),
                    FacetFeature::Mention { did: profile.did },
                ));
            }
        }
        candidates.sort_by_key(|(start, end, _)| (*start, *end));
        let mut facets = Vec::new();
        let mut previous_end = 0;
        for (start, end, feature) in candidates {
            if start < previous_end {
                continue;
            }
            facets.push(Facet {
                index: ByteSlice {
                    byte_start: start,
                    byte_end: end,
                },
                features: vec![feature],
            });
            previous_end = end;
        }
        facets
    }
}
