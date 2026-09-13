//! Byte-accurate construction of Bluesky rich text facets.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A UTF-8 byte range within a post's text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ByteSlice {
    /// Inclusive byte offset.
    pub byte_start: usize,
    /// Exclusive byte offset.
    pub byte_end: usize,
}

/// Decoration attached to a rich text range.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum FacetFeature {
    /// A web link.
    #[serde(rename = "app.bsky.richtext.facet#link")]
    Link {
        /// Link destination.
        uri: String,
    },
    /// A mention of an AT Protocol account.
    #[serde(rename = "app.bsky.richtext.facet#mention")]
    Mention {
        /// Mentioned account DID.
        did: String,
    },
    /// An inline hashtag.
    #[serde(rename = "app.bsky.richtext.facet#tag")]
    Tag {
        /// Tag without the leading `#`.
        tag: String,
    },
}

/// A byte range and the decorations applied to it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Facet {
    /// Location in the UTF-8 encoded text.
    pub index: ByteSlice,
    /// One or more decorations for this range.
    pub features: Vec<FacetFeature>,
}

/// Completed rich text ready for an `app.bsky.feed.post` record.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RichText {
    /// Composed text.
    pub text: String,
    /// Facets referring to byte ranges within `text`.
    pub facets: Vec<Facet>,
}

/// Structural error in rich text received from an external source.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RichTextError {
    /// A facet begins after it ends.
    #[error("facet {facet} starts at byte {start}, after its end at byte {end}")]
    ReversedRange {
        /// Zero-based facet position.
        facet: usize,
        /// Invalid start offset.
        start: usize,
        /// End offset.
        end: usize,
    },
    /// A facet points beyond the text.
    #[error("facet {facet} ends at byte {end}, beyond text length {text_len}")]
    OutOfBounds {
        /// Zero-based facet position.
        facet: usize,
        /// Invalid end offset.
        end: usize,
        /// Encoded text length.
        text_len: usize,
    },
    /// A facet boundary splits a UTF-8 code point.
    #[error("facet {facet} boundary at byte {offset} is not a UTF-8 character boundary")]
    InvalidUtf8Boundary {
        /// Zero-based facet position.
        facet: usize,
        /// Invalid boundary offset.
        offset: usize,
    },
    /// A facet has no feature.
    #[error("facet {facet} has no features")]
    EmptyFeatures {
        /// Zero-based facet position.
        facet: usize,
    },
}

impl RichText {
    /// Validates every facet against this text's UTF-8 representation.
    ///
    /// # Errors
    /// Returns the first invalid facet range or empty feature list.
    pub fn validate(&self) -> Result<(), RichTextError> {
        for (facet_number, facet) in self.facets.iter().enumerate() {
            let ByteSlice {
                byte_start,
                byte_end,
            } = facet.index;
            if byte_start > byte_end {
                return Err(RichTextError::ReversedRange {
                    facet: facet_number,
                    start: byte_start,
                    end: byte_end,
                });
            }
            if byte_end > self.text.len() {
                return Err(RichTextError::OutOfBounds {
                    facet: facet_number,
                    end: byte_end,
                    text_len: self.text.len(),
                });
            }
            for offset in [byte_start, byte_end] {
                if !self.text.is_char_boundary(offset) {
                    return Err(RichTextError::InvalidUtf8Boundary {
                        facet: facet_number,
                        offset,
                    });
                }
            }
            if facet.features.is_empty() {
                return Err(RichTextError::EmptyFeatures {
                    facet: facet_number,
                });
            }
        }
        Ok(())
    }

    /// Returns the text covered by a facet after validating its byte range.
    ///
    /// # Errors
    /// Returns an error if any facet in this value is structurally invalid or
    /// if `facet` is out of bounds.
    pub fn facet_text(&self, facet: usize) -> Result<Option<&str>, RichTextError> {
        self.validate()?;
        Ok(self
            .facets
            .get(facet)
            .map(|facet| &self.text[facet.index.byte_start..facet.index.byte_end]))
    }
}

/// Incrementally constructs rich text while maintaining UTF-8 byte offsets.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RichTextBuilder {
    text: String,
    facets: Vec<Facet>,
}

impl RichTextBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the text composed so far.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the facets composed so far.
    #[must_use]
    pub fn facets(&self) -> &[Facet] {
        &self.facets
    }

    /// Appends undecorated text.
    pub fn add_text(&mut self, text: impl AsRef<str>) -> &mut Self {
        self.text.push_str(text.as_ref());
        self
    }

    /// Appends text decorated with one feature.
    pub fn add_decorated_text(
        &mut self,
        text: impl AsRef<str>,
        feature: FacetFeature,
    ) -> &mut Self {
        let text = text.as_ref();
        let byte_start = self.text.len();
        self.text.push_str(text);
        self.facets.push(Facet {
            index: ByteSlice {
                byte_start,
                byte_end: self.text.len(),
            },
            features: vec![feature],
        });
        self
    }

    /// Appends linked text.
    pub fn add_link(&mut self, text: impl AsRef<str>, uri: impl Into<String>) -> &mut Self {
        self.add_decorated_text(text, FacetFeature::Link { uri: uri.into() })
    }

    /// Appends a mention.
    pub fn add_mention(&mut self, text: impl AsRef<str>, did: impl Into<String>) -> &mut Self {
        self.add_decorated_text(text, FacetFeature::Mention { did: did.into() })
    }

    /// Appends a hashtag, adding the leading `#` to the text.
    pub fn add_tag(&mut self, tag: impl Into<String>) -> &mut Self {
        let tag = tag.into();
        self.add_decorated_text(format!("#{tag}"), FacetFeature::Tag { tag })
    }

    /// Returns an owned snapshot without consuming this builder.
    #[must_use]
    pub fn build(&self) -> RichText {
        RichText {
            text: self.text.clone(),
            facets: self.facets.clone(),
        }
    }

    /// Consumes the builder without cloning its contents.
    #[must_use]
    pub fn finish(self) -> RichText {
        RichText {
            text: self.text,
            facets: self.facets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_all_supported_features() {
        let mut builder = RichTextBuilder::new();
        builder
            .add_text("Hello ")
            .add_mention("@luna", "did:plc:luna")
            .add_text(" — read ")
            .add_link("this", "https://example.com")
            .add_text(" ")
            .add_tag("rust");

        let rich_text = builder.finish();
        assert_eq!(rich_text.text, "Hello @luna — read this #rust");
        assert_eq!(
            rich_text.facets[0].index,
            ByteSlice {
                byte_start: 6,
                byte_end: 11
            }
        );
        assert_eq!(
            rich_text.facets[1].index,
            ByteSlice {
                byte_start: 21,
                byte_end: 25
            }
        );
        assert_eq!(
            rich_text.facets[2].index,
            ByteSlice {
                byte_start: 26,
                byte_end: 31
            }
        );
    }

    #[test]
    fn indexes_utf8_bytes_instead_of_characters() {
        let mut builder = RichTextBuilder::new();
        builder
            .add_text("🦀 café ")
            .add_link("世界", "https://example.com");
        let facet = &builder.facets()[0];
        assert_eq!(
            facet.index,
            ByteSlice {
                byte_start: 11,
                byte_end: 17
            }
        );
        assert_eq!(
            &builder.text().as_bytes()[facet.index.byte_start..facet.index.byte_end],
            "世界".as_bytes()
        );
    }

    #[test]
    fn build_is_a_snapshot_and_clone_is_independent() {
        let mut original = RichTextBuilder::new();
        original.add_text("one");
        let snapshot = original.build();
        let mut cloned = original.clone();
        cloned.add_text(" two");
        assert_eq!(snapshot.text, "one");
        assert_eq!(original.text(), "one");
        assert_eq!(cloned.text(), "one two");
    }

    #[test]
    fn serializes_to_bluesky_lexicon_shape() {
        let mut builder = RichTextBuilder::new();
        builder.add_tag("rust");
        assert_eq!(
            serde_json::to_value(builder.finish()).unwrap(),
            serde_json::json!({
                "text": "#rust",
                "facets": [{
                    "index": { "byteStart": 0, "byteEnd": 5 },
                    "features": [{
                        "$type": "app.bsky.richtext.facet#tag",
                        "tag": "rust"
                    }]
                }]
            })
        );
    }

    #[test]
    fn empty_decorated_text_matches_reference_builder_behavior() {
        let mut builder = RichTextBuilder::new();
        builder.add_link("", "https://example.com");
        assert_eq!(
            builder.facets()[0].index,
            ByteSlice {
                byte_start: 0,
                byte_end: 0
            }
        );
    }

    #[test]
    fn validates_and_slices_unicode_facets() {
        let mut builder = RichTextBuilder::new();
        builder.add_text("🦀 ").add_tag("rust");
        let value = builder.finish();
        assert_eq!(value.validate(), Ok(()));
        assert_eq!(value.facet_text(0), Ok(Some("#rust")));
        assert_eq!(value.facet_text(1), Ok(None));
    }

    #[test]
    fn rejects_each_invalid_external_facet_shape() {
        let feature = FacetFeature::Tag { tag: "x".into() };
        let value = |index, features| RichText {
            text: "🦀x".into(),
            facets: vec![Facet { index, features }],
        };
        assert!(matches!(
            value(
                ByteSlice {
                    byte_start: 5,
                    byte_end: 4
                },
                vec![feature.clone()]
            )
            .validate(),
            Err(RichTextError::ReversedRange { .. })
        ));
        assert!(matches!(
            value(
                ByteSlice {
                    byte_start: 0,
                    byte_end: 6
                },
                vec![feature.clone()]
            )
            .validate(),
            Err(RichTextError::OutOfBounds { .. })
        ));
        assert!(matches!(
            value(
                ByteSlice {
                    byte_start: 1,
                    byte_end: 4
                },
                vec![feature.clone()]
            )
            .validate(),
            Err(RichTextError::InvalidUtf8Boundary { .. })
        ));
        assert!(matches!(
            value(
                ByteSlice {
                    byte_start: 0,
                    byte_end: 4
                },
                vec![]
            )
            .validate(),
            Err(RichTextError::EmptyFeatures { .. })
        ));
    }
}
