//! High-level Bluesky resource and payload models.
//!
//! Fields intentionally retain the names used by the corresponding Bluesky
//! views; the type-level documentation identifies the wire resource represented.
#![allow(missing_docs)]

use bluesky_richtext::{Facet, RichText};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// AT URI and CID pair identifying an immutable record revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StrongRef {
    pub uri: String,
    pub cid: String,
}

/// Cursor-paginated collection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Page<T> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub items: Vec<T>,
}

/// Viewer-specific relationship state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewerState {
    #[serde(default)]
    pub muted: bool,
    #[serde(default)]
    pub blocked_by: bool,
    #[serde(default)]
    pub following: Option<String>,
    #[serde(default)]
    pub followed_by: Option<String>,
    #[serde(default)]
    pub blocking: Option<String>,
    #[serde(default)]
    pub like: Option<String>,
    #[serde(default)]
    pub repost: Option<String>,
}

/// Bluesky profile view. New server fields are retained in `extra`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub did: String,
    pub handle: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub banner: Option<String>,
    #[serde(default)]
    pub followers_count: Option<u64>,
    #[serde(default)]
    pub follows_count: Option<u64>,
    #[serde(default)]
    pub posts_count: Option<u64>,
    #[serde(default)]
    pub viewer: Option<ViewerState>,
    #[serde(default)]
    pub labels: Vec<Value>,
    #[serde(default)]
    pub indexed_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Profile {
    #[must_use]
    pub fn is_following(&self) -> bool {
        self.viewer.as_ref().is_some_and(|v| v.following.is_some())
    }
    #[must_use]
    pub fn followed_by(&self) -> bool {
        self.viewer
            .as_ref()
            .is_some_and(|v| v.followed_by.is_some())
    }
    #[must_use]
    pub fn is_blocking(&self) -> bool {
        self.viewer.as_ref().is_some_and(|v| v.blocking.is_some())
    }
    #[must_use]
    pub fn is_mutual(&self) -> bool {
        self.is_following() && self.followed_by()
    }
}

/// Bluesky post view.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Post {
    pub uri: String,
    pub cid: String,
    pub author: Profile,
    pub record: Value,
    #[serde(default)]
    pub embed: Option<Value>,
    #[serde(default)]
    pub reply_count: Option<u64>,
    #[serde(default)]
    pub repost_count: Option<u64>,
    #[serde(default)]
    pub like_count: Option<u64>,
    #[serde(default)]
    pub quote_count: Option<u64>,
    #[serde(default)]
    pub indexed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub viewer: Option<ViewerState>,
    #[serde(default)]
    pub labels: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// A post reply's parent and root references.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplyRef {
    pub root: StrongRef,
    pub parent: StrongRef,
}

/// Image dimensions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AspectRatio {
    pub width: u32,
    pub height: u32,
}

/// Previously uploaded image blob and its presentation metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Image {
    pub image: Value,
    #[serde(default)]
    pub alt: String,
    #[serde(default)]
    pub aspect_ratio: Option<AspectRatio>,
}

/// External card embed.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExternalEmbed {
    pub uri: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub thumb: Option<Value>,
}

/// Embed accepted by [`PostPayload`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "$type")]
pub enum PostEmbed {
    #[serde(rename = "app.bsky.embed.images")]
    Images { images: Vec<Image> },
    #[serde(rename = "app.bsky.embed.external")]
    External { external: ExternalEmbed },
    #[serde(rename = "app.bsky.embed.record")]
    Record { record: StrongRef },
    #[serde(rename = "app.bsky.embed.recordWithMedia")]
    RecordWithMedia {
        record: Box<PostEmbed>,
        media: Box<PostEmbed>,
    },
    #[serde(rename = "app.bsky.embed.video")]
    Video {
        video: Value,
        #[serde(default)]
        alt: String,
        #[serde(default, rename = "aspectRatio")]
        aspect_ratio: Option<AspectRatio>,
    },
}

/// Self-applied content label.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelfLabel {
    GraphicMedia,
    Nudity,
    Sexual,
    Porn,
}

/// Text accepted when posting or messaging.
#[derive(Clone, Debug, PartialEq)]
pub enum Text {
    Plain(String),
    Rich(RichText),
}
impl From<String> for Text {
    fn from(value: String) -> Self {
        Self::Plain(value)
    }
}
impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self::Plain(value.into())
    }
}
impl From<RichText> for Text {
    fn from(value: RichText) -> Self {
        Self::Rich(value)
    }
}
impl Text {
    pub(crate) fn parts(self) -> (String, Option<Vec<Facet>>) {
        match self {
            Self::Plain(text) => (text, None),
            Self::Rich(value) => (value.text, Some(value.facets)),
        }
    }
}

/// Post creation input.
#[derive(Clone, Debug, PartialEq)]
pub struct PostPayload {
    pub text: Text,
    pub facets: Option<Vec<Facet>>,
    pub reply: Option<ReplyRef>,
    pub embed: Option<PostEmbed>,
    pub langs: Vec<String>,
    pub labels: Vec<SelfLabel>,
    pub tags: Vec<String>,
    pub threadgate: Option<ThreadgateRules>,
    pub created_at: Option<DateTime<Utc>>,
}
impl PostPayload {
    #[must_use]
    pub fn new(text: impl Into<Text>) -> Self {
        Self {
            text: text.into(),
            facets: None,
            reply: None,
            embed: None,
            langs: vec!["en".into()],
            labels: vec![],
            tags: vec![],
            threadgate: None,
            created_at: None,
        }
    }
}

/// Reply permissions written beside a newly created post.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ThreadgateRules {
    pub allow_mentioned: bool,
    pub allow_following: bool,
    pub allow_lists: Vec<String>,
}

/// Bluesky user list.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserList {
    pub uri: String,
    pub cid: String,
    pub name: String,
    pub purpose: String,
    #[serde(default)]
    pub creator: Option<Profile>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub viewer: Option<ViewerState>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Custom feed generator.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedGenerator {
    pub uri: String,
    pub cid: String,
    pub did: String,
    pub display_name: String,
    pub creator: Profile,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub viewer: Option<ViewerState>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Labeler service view.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Labeler {
    pub uri: String,
    pub cid: String,
    pub creator: Profile,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Starter pack view.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StarterPack {
    pub uri: String,
    pub cid: String,
    pub record: Value,
    pub creator: Profile,
    #[serde(default)]
    pub list: Option<UserList>,
    #[serde(default)]
    pub feeds: Vec<FeedGenerator>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Direct-message conversation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Conversation {
    pub id: String,
    pub rev: String,
    pub members: Vec<Profile>,
    #[serde(default)]
    pub last_message: Option<Value>,
    #[serde(default)]
    pub muted: bool,
    #[serde(default)]
    pub unread_count: u64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Direct message or deletion marker.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum MessageView {
    Message(ChatMessage),
    Deleted(DeletedMessage),
}

/// Direct message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub id: String,
    pub rev: String,
    pub text: String,
    pub sender: Value,
    pub sent_at: DateTime<Utc>,
    #[serde(default)]
    pub facets: Vec<Facet>,
    #[serde(default)]
    pub embed: Option<StrongRef>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Deleted direct-message marker.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletedMessage {
    pub id: String,
    pub rev: String,
    pub sender: Value,
    pub sent_at: DateTime<Utc>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Direct-message creation input.
#[derive(Clone, Debug, PartialEq)]
pub struct MessagePayload {
    pub conversation_id: String,
    pub text: Text,
    pub facets: Option<Vec<Facet>>,
    pub embed: Option<StrongRef>,
}
impl MessagePayload {
    #[must_use]
    pub fn new(conversation_id: impl Into<String>, text: impl Into<Text>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            text: text.into(),
            facets: None,
            embed: None,
        }
    }
}
