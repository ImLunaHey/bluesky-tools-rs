//! Bot client and high-level XRPC operations.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use url::Url;

use crate::{
    CacheConfig, RateLimitConfig,
    cache::Cache,
    models::{
        ChatMessage, Conversation, FeedGenerator, Labeler, MessagePayload, MessageView, Page, Post,
        PostPayload, Profile, StarterPack, StrongRef, UserList,
    },
    rate_limit::RateLimiter,
    transport::{HttpTransport, Method, Request, Transport, TransportError},
};

const CHAT_PROXY: &str = "did:web:api.bsky.chat#bsky_chat";

/// Authentication session returned by an AT Protocol PDS.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    /// Account DID.
    pub did: String,
    /// Current account handle.
    pub handle: String,
    /// Short-lived API token.
    pub access_jwt: String,
    /// Refresh token.
    pub refresh_jwt: String,
    /// Additional session fields.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Bot client configuration.
#[derive(Clone, Debug)]
pub struct BotConfig {
    /// PDS base URL.
    pub service: Url,
    /// Default post languages.
    pub languages: Vec<String>,
    /// Query cache behavior.
    pub cache: CacheConfig,
    /// Request pacing behavior.
    pub rate_limit: RateLimitConfig,
}
impl Default for BotConfig {
    fn default() -> Self {
        Self {
            service: Url::parse("https://bsky.social").expect("default service is valid"),
            languages: vec!["en".into()],
            cache: CacheConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// Error returned by the high-level client.
#[derive(Debug, Error)]
pub enum BotError {
    /// The operation requires a logged-in session.
    #[error("this operation requires an authenticated session")]
    AuthenticationRequired,
    /// The server returned an XRPC error.
    #[error("XRPC request failed with status {status}: {error}: {message}")]
    Xrpc {
        /// HTTP status code.
        status: u16,
        /// Machine-readable XRPC error name.
        error: String,
        /// Human-readable server explanation.
        message: String,
    },
    /// Response JSON did not match the expected shape.
    #[error("invalid XRPC response: {0}")]
    Decode(#[from] serde_json::Error),
    /// The AT URI could not be split into repository, collection, and record key.
    #[error("invalid AT URI: {0}")]
    InvalidAtUri(String),
    /// A required response field was absent.
    #[error("XRPC response is missing required field {0}")]
    MissingField(&'static str),
    /// A mutation attempted to delete a record owned by another repository.
    #[error("can only delete records in the authenticated repository")]
    ForeignRecord,
    /// HTTP transport failed before receiving a response.
    #[error(transparent)]
    Transport(#[from] TransportError),
}

struct Inner {
    transport: Arc<dyn Transport>,
    config: BotConfig,
    session: RwLock<Option<Session>>,
    refresh: Mutex<()>,
    cache: Mutex<Cache>,
    rate_limit: RateLimiter,
}

/// Cloneable Bluesky bot client.
#[derive(Clone)]
pub struct Bot {
    inner: Arc<Inner>,
}

impl Bot {
    /// Creates a production client backed by Reqwest and rustls.
    #[must_use]
    pub fn new(config: BotConfig) -> Self {
        Self::with_transport(config, HttpTransport::default())
    }

    /// Creates a client with a custom or mock transport.
    #[must_use]
    pub fn with_transport(config: BotConfig, transport: impl Transport + 'static) -> Self {
        let cache = Cache::new(config.cache);
        let rate_limit = RateLimiter::new(config.rate_limit);
        Self {
            inner: Arc::new(Inner {
                transport: Arc::new(transport),
                config,
                session: RwLock::new(None),
                refresh: Mutex::new(()),
                cache: Mutex::new(cache),
                rate_limit,
            }),
        }
    }

    /// Returns the active session, if logged in.
    pub async fn session(&self) -> Option<Session> {
        self.inner.session.read().await.clone()
    }

    /// Restores previously persisted credentials.
    pub async fn resume_session(&self, session: Session) {
        *self.inner.session.write().await = Some(session);
        self.inner.cache.lock().await.clear();
    }

    /// Logs in using a handle/email and app password.
    ///
    /// # Errors
    /// Returns an XRPC, transport, or decoding error when login fails.
    pub async fn login(&self, identifier: &str, password: &str) -> Result<Session, BotError> {
        let value = self
            .call(
                Method::Post,
                "com.atproto.server.createSession",
                &[],
                Some(json!({ "identifier": identifier, "password": password })),
                false,
                false,
            )
            .await?;
        let session: Session = serde_json::from_value(value)?;
        self.resume_session(session.clone()).await;
        Ok(session)
    }

    /// Clears local credentials without making a network request.
    pub async fn clear_session(&self) {
        *self.inner.session.write().await = None;
        self.inner.cache.lock().await.clear();
    }

    /// Performs any XRPC query and decodes its output.
    ///
    /// # Errors
    /// Returns an authentication, XRPC, transport, or decoding error.
    pub async fn query<T: DeserializeOwned>(
        &self,
        nsid: &str,
        parameters: &[(&str, String)],
    ) -> Result<T, BotError> {
        Ok(serde_json::from_value(
            self.call(Method::Get, nsid, parameters, None, true, false)
                .await?,
        )?)
    }

    /// Performs any XRPC procedure and decodes its output.
    ///
    /// # Errors
    /// Returns an authentication, XRPC, transport, or decoding error.
    pub async fn procedure<I: Serialize, T: DeserializeOwned>(
        &self,
        nsid: &str,
        input: &I,
    ) -> Result<T, BotError> {
        Ok(serde_json::from_value(
            self.call(
                Method::Post,
                nsid,
                &[],
                Some(serde_json::to_value(input)?),
                true,
                false,
            )
            .await?,
        )?)
    }

    async fn call(
        &self,
        method: Method,
        nsid: &str,
        parameters: &[(&str, String)],
        body: Option<Value>,
        authenticated: bool,
        chat: bool,
    ) -> Result<Value, BotError> {
        let mut url = self
            .inner
            .config
            .service
            .join(&format!("/xrpc/{nsid}"))
            .expect("valid NSID URL");
        url.query_pairs_mut()
            .extend_pairs(parameters.iter().map(|(key, value)| (*key, value)));
        let key = url.to_string();
        if method == Method::Get {
            if let Some(value) = self.inner.cache.lock().await.get(&key) {
                return Ok(value);
            }
        }
        let value = self
            .execute(method, url, body.clone(), authenticated, chat)
            .await?;
        if method == Method::Get {
            self.inner.cache.lock().await.insert(key, value.clone());
        }
        Ok(value)
    }

    async fn execute(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        authenticated: bool,
        chat: bool,
    ) -> Result<Value, BotError> {
        let token = if authenticated {
            Some(
                self.session()
                    .await
                    .ok_or(BotError::AuthenticationRequired)?
                    .access_jwt,
            )
        } else {
            None
        };
        let response = self
            .send(method, url.clone(), body.clone(), token.as_deref(), chat)
            .await?;
        if response.status == 401 && authenticated {
            self.refresh_session(token.as_deref().unwrap_or_default())
                .await?;
            let token = self
                .session()
                .await
                .ok_or(BotError::AuthenticationRequired)?
                .access_jwt;
            return Self::response(self.send(method, url, body, Some(&token), chat).await?);
        }
        Self::response(response)
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        token: Option<&str>,
        chat: bool,
    ) -> Result<crate::Response, BotError> {
        let _permit = self.inner.rate_limit.wait().await;
        let mut headers = BTreeMap::new();
        if let Some(token) = token {
            headers.insert("authorization".into(), format!("Bearer {token}"));
        }
        if chat {
            headers.insert("atproto-proxy".into(), CHAT_PROXY.into());
        }
        Ok(self
            .inner
            .transport
            .execute(Request {
                method,
                url,
                headers,
                body: body.map(crate::RequestBody::Json),
            })
            .await?)
    }

    fn response(response: crate::Response) -> Result<Value, BotError> {
        if (200..300).contains(&response.status) {
            return Ok(response.body);
        }
        Err(BotError::Xrpc {
            status: response.status,
            error: response
                .body
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("UnknownError")
                .into(),
            message: response
                .body
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("request failed")
                .into(),
        })
    }

    async fn refresh_session(&self, failed_access_token: &str) -> Result<(), BotError> {
        let _guard = self.inner.refresh.lock().await;
        let current = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?;
        if current.access_jwt != failed_access_token {
            return Ok(());
        }
        let url = self
            .inner
            .config
            .service
            .join("/xrpc/com.atproto.server.refreshSession")
            .expect("valid URL");
        let response = self
            .send(Method::Post, url, None, Some(&current.refresh_jwt), false)
            .await?;
        let session: Session = serde_json::from_value(Self::response(response)?)?;
        self.resume_session(session).await;
        Ok(())
    }

    async fn field<T: DeserializeOwned>(
        &self,
        nsid: &str,
        parameters: &[(&str, String)],
        field: &str,
    ) -> Result<T, BotError> {
        let value = self
            .call(Method::Get, nsid, parameters, None, true, false)
            .await?;
        Ok(serde_json::from_value(
            value.get(field).cloned().unwrap_or(Value::Null),
        )?)
    }

    /// Fetches one post by AT URI.
    pub async fn get_post(&self, uri: &str) -> Result<Post, BotError> {
        self.get_posts(&[uri])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| BotError::Xrpc {
                status: 404,
                error: "NotFound".into(),
                message: "post not found".into(),
            })
    }
    /// Fetches up to 25 posts by AT URI.
    pub async fn get_posts(&self, uris: &[&str]) -> Result<Vec<Post>, BotError> {
        self.field(
            "app.bsky.feed.getPosts",
            &uris
                .iter()
                .map(|uri| ("uris", (*uri).into()))
                .collect::<Vec<_>>(),
            "posts",
        )
        .await
    }
    /// Fetches one profile by DID or handle.
    pub async fn get_profile(&self, actor: &str) -> Result<Profile, BotError> {
        self.query("app.bsky.actor.getProfile", &[("actor", actor.into())])
            .await
    }
    /// Fetches up to 25 profiles.
    pub async fn get_profiles(&self, actors: &[&str]) -> Result<Vec<Profile>, BotError> {
        self.field(
            "app.bsky.actor.getProfiles",
            &actors
                .iter()
                .map(|actor| ("actors", (*actor).into()))
                .collect::<Vec<_>>(),
            "profiles",
        )
        .await
    }
    /// Fetches a user's posts.
    pub async fn get_user_posts(
        &self,
        actor: &str,
        limit: u8,
        cursor: Option<&str>,
        filter: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.feed_page(
            "app.bsky.feed.getAuthorFeed",
            vec![
                ("actor", actor.into()),
                ("limit", limit.min(100).to_string()),
            ],
            cursor,
            filter.map(|v| ("filter", v)),
        )
        .await
    }
    /// Fetches posts liked by a user.
    pub async fn get_user_likes(
        &self,
        actor: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.feed_page(
            "app.bsky.feed.getActorLikes",
            vec![
                ("actor", actor.into()),
                ("limit", limit.min(100).to_string()),
            ],
            cursor,
            None,
        )
        .await
    }
    /// Fetches the authenticated home timeline.
    pub async fn get_timeline(
        &self,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.feed_page(
            "app.bsky.feed.getTimeline",
            vec![("limit", limit.min(100).to_string())],
            cursor,
            None,
        )
        .await
    }

    async fn page<T: DeserializeOwned>(
        &self,
        nsid: &str,
        field: &str,
        mut params: Vec<(&str, String)>,
        cursor: Option<&str>,
        extra: Option<(&str, &str)>,
    ) -> Result<Page<T>, BotError> {
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.into()));
        }
        if let Some((key, value)) = extra {
            params.push((key, value.into()));
        }
        let value = self
            .call(Method::Get, nsid, &params, None, true, false)
            .await?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items: serde_json::from_value(value.get(field).cloned().unwrap_or_else(|| json!([])))?,
        })
    }

    async fn feed_page(
        &self,
        nsid: &str,
        mut params: Vec<(&str, String)>,
        cursor: Option<&str>,
        extra: Option<(&str, &str)>,
    ) -> Result<Page<Post>, BotError> {
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.into()));
        }
        if let Some((key, value)) = extra {
            params.push((key, value.into()));
        }
        let value = self
            .call(Method::Get, nsid, &params, None, true, false)
            .await?;
        let items = value
            .get("feed")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|item| {
                serde_json::from_value(item.get("post").cloned().unwrap_or_else(|| item.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items,
        })
    }

    async fn nested_page<T: DeserializeOwned>(
        &self,
        nsid: &str,
        field: &str,
        nested: &str,
        mut params: Vec<(&str, String)>,
        cursor: Option<&str>,
    ) -> Result<Page<T>, BotError> {
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.into()));
        }
        let value = self
            .call(Method::Get, nsid, &params, None, true, false)
            .await?;
        let items = value
            .get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|item| {
                serde_json::from_value(item.get(nested).cloned().unwrap_or_else(|| item.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items,
        })
    }

    /// Fetches a user list and its members.
    pub async fn get_list(
        &self,
        uri: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<(UserList, Page<Profile>), BotError> {
        let mut params = vec![("list", uri.into()), ("limit", limit.min(100).to_string())];
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.into()));
        }
        let value = self
            .call(
                Method::Get,
                "app.bsky.graph.getList",
                &params,
                None,
                true,
                false,
            )
            .await?;
        Ok((
            serde_json::from_value(value.get("list").cloned().unwrap_or(Value::Null))?,
            Page {
                cursor: value
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                items: value
                    .get("items")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|item| {
                        serde_json::from_value(
                            item.get("subject").cloned().unwrap_or_else(|| item.clone()),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            },
        ))
    }
    /// Fetches lists created by an account.
    pub async fn get_user_lists(
        &self,
        actor: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<UserList>, BotError> {
        self.page(
            "app.bsky.graph.getLists",
            "lists",
            vec![
                ("actor", actor.into()),
                ("limit", limit.min(100).to_string()),
            ],
            cursor,
            None,
        )
        .await
    }
    /// Fetches posts from a list feed.
    pub async fn get_list_feed(
        &self,
        list: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.feed_page(
            "app.bsky.feed.getListFeed",
            vec![("list", list.into()), ("limit", limit.min(100).to_string())],
            cursor,
            None,
        )
        .await
    }
    /// Fetches one custom feed generator.
    pub async fn get_feed_generator(&self, uri: &str) -> Result<FeedGenerator, BotError> {
        self.get_feed_generators(&[uri])
            .await?
            .into_iter()
            .next()
            .ok_or_else(not_found)
    }
    /// Fetches custom feed generators.
    pub async fn get_feed_generators(&self, uris: &[&str]) -> Result<Vec<FeedGenerator>, BotError> {
        self.field(
            "app.bsky.feed.getFeedGenerators",
            &uris
                .iter()
                .map(|uri| ("feeds", (*uri).into()))
                .collect::<Vec<_>>(),
            "feeds",
        )
        .await
    }
    /// Fetches posts from a custom feed.
    pub async fn get_feed(
        &self,
        feed: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.feed_page(
            "app.bsky.feed.getFeed",
            vec![("feed", feed.into()), ("limit", limit.min(100).to_string())],
            cursor,
            None,
        )
        .await
    }
    /// Fetches one labeler service.
    pub async fn get_labeler(&self, did: &str) -> Result<Labeler, BotError> {
        self.get_labelers(&[did])
            .await?
            .into_iter()
            .next()
            .ok_or_else(not_found)
    }
    /// Fetches labeler services.
    pub async fn get_labelers(&self, dids: &[&str]) -> Result<Vec<Labeler>, BotError> {
        self.field(
            "app.bsky.labeler.getServices",
            &dids
                .iter()
                .map(|did| ("dids", (*did).into()))
                .collect::<Vec<_>>(),
            "views",
        )
        .await
    }
    /// Fetches one starter pack.
    pub async fn get_starter_pack(&self, uri: &str) -> Result<StarterPack, BotError> {
        let value: Value = self
            .query(
                "app.bsky.graph.getStarterPack",
                &[("starterPack", uri.into())],
            )
            .await?;
        Ok(serde_json::from_value(
            value.get("starterPack").cloned().unwrap_or(value),
        )?)
    }
    /// Fetches starter packs by URI.
    pub async fn get_starter_packs(&self, uris: &[&str]) -> Result<Vec<StarterPack>, BotError> {
        self.field(
            "app.bsky.graph.getStarterPacks",
            &uris
                .iter()
                .map(|uri| ("uris", (*uri).into()))
                .collect::<Vec<_>>(),
            "starterPacks",
        )
        .await
    }
    /// Fetches starter packs created by an account.
    pub async fn get_user_starter_packs(
        &self,
        actor: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<StarterPack>, BotError> {
        self.page(
            "app.bsky.graph.getActorStarterPacks",
            "starterPacks",
            vec![
                ("actor", actor.into()),
                ("limit", limit.min(100).to_string()),
            ],
            cursor,
            None,
        )
        .await
    }
    /// Fetches the thread surrounding a post.
    pub async fn get_post_thread(
        &self,
        uri: &str,
        depth: u8,
        parent_height: u8,
    ) -> Result<Value, BotError> {
        self.query(
            "app.bsky.feed.getPostThread",
            &[
                ("uri", uri.into()),
                ("depth", depth.min(100).to_string()),
                ("parentHeight", parent_height.min(100).to_string()),
            ],
        )
        .await
    }
    /// Fetches accounts that liked a post.
    pub async fn get_likes(
        &self,
        uri: &str,
        cid: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<Page<Profile>, BotError> {
        let mut params = vec![("uri", uri.into()), ("limit", "100".into())];
        if let Some(cid) = cid {
            params.push(("cid", cid.into()));
        }
        self.nested_page("app.bsky.feed.getLikes", "likes", "actor", params, cursor)
            .await
    }
    /// Fetches accounts that reposted a post.
    pub async fn get_reposts(
        &self,
        uri: &str,
        cursor: Option<&str>,
    ) -> Result<Page<Profile>, BotError> {
        self.page(
            "app.bsky.feed.getRepostedBy",
            "repostedBy",
            vec![("uri", uri.into()), ("limit", "100".into())],
            cursor,
            None,
        )
        .await
    }
    /// Fetches posts quoting a post.
    pub async fn get_quotes(
        &self,
        uri: &str,
        cursor: Option<&str>,
    ) -> Result<Page<Post>, BotError> {
        self.page(
            "app.bsky.feed.getQuotes",
            "posts",
            vec![("uri", uri.into()), ("limit", "100".into())],
            cursor,
            None,
        )
        .await
    }

    /// Creates a post and returns its strong reference.
    pub async fn post(&self, mut payload: PostPayload) -> Result<StrongRef, BotError> {
        let threadgate = payload.threadgate.take();
        if payload.langs.is_empty() {
            payload.langs.clone_from(&self.inner.config.languages);
        }
        let (text, rich_facets) = payload.text.parts();
        let facets = payload.facets.or(rich_facets);
        let mut record = json!({ "$type": "app.bsky.feed.post", "text": text, "createdAt": payload.created_at.unwrap_or_else(Utc::now), "langs": payload.langs });
        if let Some(value) = facets {
            record["facets"] = serde_json::to_value(value)?;
        }
        if let Some(value) = payload.reply {
            record["reply"] = serde_json::to_value(value)?;
        }
        if let Some(value) = payload.embed {
            record["embed"] = serde_json::to_value(value)?;
        }
        if !payload.labels.is_empty() {
            record["labels"] = json!({ "$type": "com.atproto.label.defs#selfLabels", "values": payload.labels.into_iter().map(|val| json!({ "val": val })).collect::<Vec<_>>() });
        }
        if !payload.tags.is_empty() {
            record["tags"] = serde_json::to_value(payload.tags)?;
        }
        let reference = self.create_record("app.bsky.feed.post", record).await?;
        if let Some(rules) = threadgate {
            let (_, _, rkey) = parse_at_uri(&reference.uri)?;
            let mut allow = Vec::new();
            if rules.allow_mentioned {
                allow.push(json!({ "$type": "app.bsky.feed.threadgate#mentionRule" }));
            }
            if rules.allow_following {
                allow.push(json!({ "$type": "app.bsky.feed.threadgate#followingRule" }));
            }
            allow.extend(
                rules.allow_lists.into_iter().map(
                    |list| json!({ "$type": "app.bsky.feed.threadgate#listRule", "list": list }),
                ),
            );
            self.create_record_with_rkey("app.bsky.feed.threadgate", Some(rkey), json!({ "$type": "app.bsky.feed.threadgate", "post": reference.uri, "allow": allow, "createdAt": Utc::now() })).await?;
        }
        Ok(reference)
    }

    /// Creates an arbitrary record in the authenticated repository.
    pub async fn create_record(
        &self,
        collection: &str,
        record: Value,
    ) -> Result<StrongRef, BotError> {
        self.create_record_with_rkey(collection, None, record).await
    }

    async fn create_record_with_rkey(
        &self,
        collection: &str,
        rkey: Option<&str>,
        record: Value,
    ) -> Result<StrongRef, BotError> {
        let repo = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?
            .did;
        let mut body = json!({ "repo": repo, "collection": collection, "record": record });
        if let Some(rkey) = rkey {
            body["rkey"] = json!(rkey);
        }
        let result = self
            .call(
                Method::Post,
                "com.atproto.repo.createRecord",
                &[],
                Some(body),
                true,
                false,
            )
            .await?;
        self.inner.cache.lock().await.clear();
        Ok(serde_json::from_value(result)?)
    }

    /// Uploads a blob for use in an image, video, or external embed.
    pub async fn upload_blob(
        &self,
        data: Vec<u8>,
        content_type: impl Into<String>,
    ) -> Result<Value, BotError> {
        let token = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?
            .access_jwt;
        let url = self
            .inner
            .config
            .service
            .join("/xrpc/com.atproto.repo.uploadBlob")
            .expect("valid URL");
        let _permit = self.inner.rate_limit.wait().await;
        let response = self
            .inner
            .transport
            .execute(Request {
                method: Method::Post,
                url,
                headers: BTreeMap::from([("authorization".into(), format!("Bearer {token}"))]),
                body: Some(crate::RequestBody::Bytes {
                    content_type: content_type.into(),
                    data,
                }),
            })
            .await?;
        Ok(Self::response(response)?
            .get("blob")
            .cloned()
            .unwrap_or(Value::Null))
    }

    /// Deletes a record identified by AT URI.
    pub async fn delete_record(&self, uri: &str) -> Result<(), BotError> {
        let (repo, collection, rkey) = parse_at_uri(uri)?;
        let own_did = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?
            .did;
        if repo != own_did {
            return Err(BotError::ForeignRecord);
        }
        self.call(
            Method::Post,
            "com.atproto.repo.deleteRecord",
            &[],
            Some(json!({ "repo": repo, "collection": collection, "rkey": rkey })),
            true,
            false,
        )
        .await?;
        self.inner.cache.lock().await.clear();
        Ok(())
    }
    /// Deletes a post.
    pub async fn delete_post(&self, uri: &str) -> Result<(), BotError> {
        self.delete_record(uri).await
    }
    /// Likes a post or feed generator.
    pub async fn like(&self, subject: &StrongRef) -> Result<StrongRef, BotError> {
        self.create_record(
            "app.bsky.feed.like",
            json!({ "$type": "app.bsky.feed.like", "subject": subject, "createdAt": Utc::now() }),
        )
        .await
    }
    /// Removes a known like record.
    pub async fn unlike(&self, like_uri: &str) -> Result<(), BotError> {
        let uri = if like_uri.contains("/app.bsky.feed.like/") {
            like_uri.into()
        } else {
            self.get_post(like_uri)
                .await?
                .viewer
                .and_then(|viewer| viewer.like)
                .ok_or_else(not_found)?
        };
        self.delete_record(&uri).await
    }
    /// Reposts a post.
    pub async fn repost(&self, subject: &StrongRef) -> Result<StrongRef, BotError> {
        self.create_record(
            "app.bsky.feed.repost",
            json!({ "$type": "app.bsky.feed.repost", "subject": subject, "createdAt": Utc::now() }),
        )
        .await
    }
    /// Removes a known repost record.
    pub async fn delete_repost(&self, repost_uri: &str) -> Result<(), BotError> {
        let uri = if repost_uri.contains("/app.bsky.feed.repost/") {
            repost_uri.into()
        } else {
            self.get_post(repost_uri)
                .await?
                .viewer
                .and_then(|viewer| viewer.repost)
                .ok_or_else(not_found)?
        };
        self.delete_record(&uri).await
    }
    /// Follows an account.
    pub async fn follow(&self, did: &str) -> Result<StrongRef, BotError> {
        self.create_record(
            "app.bsky.graph.follow",
            json!({ "$type": "app.bsky.graph.follow", "subject": did, "createdAt": Utc::now() }),
        )
        .await
    }
    /// Unfollows using a known follow record URI.
    pub async fn unfollow(&self, follow_uri: &str) -> Result<(), BotError> {
        let uri = if follow_uri.starts_with("at://") {
            follow_uri.into()
        } else {
            self.get_profile(follow_uri)
                .await?
                .viewer
                .and_then(|viewer| viewer.following)
                .ok_or_else(not_found)?
        };
        self.delete_record(&uri).await
    }
    /// Blocks an account.
    pub async fn block(&self, did: &str) -> Result<StrongRef, BotError> {
        self.create_record(
            "app.bsky.graph.block",
            json!({ "$type": "app.bsky.graph.block", "subject": did, "createdAt": Utc::now() }),
        )
        .await
    }
    /// Unblocks using a known block record URI.
    pub async fn unblock(&self, block_uri: &str) -> Result<(), BotError> {
        let uri = if block_uri.starts_with("at://") {
            block_uri.into()
        } else {
            self.get_profile(block_uri)
                .await?
                .viewer
                .and_then(|viewer| viewer.blocking)
                .ok_or_else(not_found)?
        };
        self.delete_record(&uri).await
    }
    /// Blocks every account on a moderation list.
    pub async fn block_list(&self, list: &str) -> Result<StrongRef, BotError> {
        self.create_record("app.bsky.graph.listblock", json!({ "$type": "app.bsky.graph.listblock", "subject": list, "createdAt": Utc::now() })).await
    }
    /// Removes a known moderation-list block record.
    pub async fn unblock_list(&self, block_uri: &str) -> Result<(), BotError> {
        self.delete_record(block_uri).await
    }
    /// Mutes an account.
    pub async fn mute(&self, actor: &str) -> Result<(), BotError> {
        self.unit("app.bsky.graph.muteActor", json!({ "actor": actor }), false)
            .await
    }
    /// Unmutes an account.
    pub async fn unmute(&self, actor: &str) -> Result<(), BotError> {
        self.unit(
            "app.bsky.graph.unmuteActor",
            json!({ "actor": actor }),
            false,
        )
        .await
    }
    /// Mutes every account on a moderation list.
    pub async fn mute_list(&self, list: &str) -> Result<(), BotError> {
        self.unit(
            "app.bsky.graph.muteActorList",
            json!({ "list": list }),
            false,
        )
        .await
    }
    /// Unmutes every account on a moderation list.
    pub async fn unmute_list(&self, list: &str) -> Result<(), BotError> {
        self.unit(
            "app.bsky.graph.unmuteActorList",
            json!({ "list": list }),
            false,
        )
        .await
    }

    /// Applies or negates labels on an account or record through Ozone.
    pub async fn label(
        &self,
        subject: Value,
        create: &[&str],
        negate: &[&str],
        comment: Option<&str>,
    ) -> Result<Value, BotError> {
        let created_by = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?
            .did;
        let mut event = json!({ "$type": "tools.ozone.moderation.defs#modEventLabel", "createLabelVals": create, "negateLabelVals": negate });
        if let Some(comment) = comment {
            event["comment"] = json!(comment);
        }
        self.call(
            Method::Post,
            "tools.ozone.moderation.emitEvent",
            &[],
            Some(json!({ "event": event, "subject": subject, "createdBy": created_by })),
            true,
            false,
        )
        .await
    }

    /// Applies labels to an account DID.
    pub async fn label_account(
        &self,
        did: &str,
        labels: &[&str],
        comment: Option<&str>,
    ) -> Result<Value, BotError> {
        self.label(
            json!({ "$type": "com.atproto.admin.defs#repoRef", "did": did }),
            labels,
            &[],
            comment,
        )
        .await
    }
    /// Negates labels previously applied to an account DID.
    pub async fn negate_account_labels(
        &self,
        did: &str,
        labels: &[&str],
        comment: Option<&str>,
    ) -> Result<Value, BotError> {
        self.label(
            json!({ "$type": "com.atproto.admin.defs#repoRef", "did": did }),
            &[],
            labels,
            comment,
        )
        .await
    }
    /// Applies labels to a strong record reference.
    pub async fn label_record(
        &self,
        record: &StrongRef,
        labels: &[&str],
        comment: Option<&str>,
    ) -> Result<Value, BotError> {
        self.label(
            json!({ "$type": "com.atproto.repo.strongRef", "uri": record.uri, "cid": record.cid }),
            labels,
            &[],
            comment,
        )
        .await
    }
    /// Negates labels previously applied to a record.
    pub async fn negate_record_labels(
        &self,
        record: &StrongRef,
        labels: &[&str],
        comment: Option<&str>,
    ) -> Result<Value, BotError> {
        self.label(
            json!({ "$type": "com.atproto.repo.strongRef", "uri": record.uri, "cid": record.cid }),
            &[],
            labels,
            comment,
        )
        .await
    }

    /// Resolves a handle to a DID.
    pub async fn resolve_handle(&self, handle: &str) -> Result<String, BotError> {
        let value = self
            .call(
                Method::Get,
                "com.atproto.identity.resolveHandle",
                &[("handle", handle.into())],
                None,
                false,
                false,
            )
            .await?;
        value
            .get("did")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(BotError::MissingField("did"))
    }
    /// Updates the authenticated account's handle.
    pub async fn update_handle(&self, handle: &str) -> Result<(), BotError> {
        self.unit(
            "com.atproto.identity.updateHandle",
            json!({ "handle": handle }),
            false,
        )
        .await?;
        if let Some(session) = self.inner.session.write().await.as_mut() {
            session.handle = handle.into();
        }
        Ok(())
    }
    /// Sets who may initiate new conversations with the authenticated account.
    pub async fn set_chat_preference(&self, preference: &str) -> Result<StrongRef, BotError> {
        self.put_record(
            "chat.bsky.actor.declaration",
            "self",
            json!({ "$type": "chat.bsky.actor.declaration", "allowIncoming": preference }),
        )
        .await
    }
    /// Replaces a record at a known record key.
    pub async fn put_record(
        &self,
        collection: &str,
        rkey: &str,
        record: Value,
    ) -> Result<StrongRef, BotError> {
        let repo = self
            .session()
            .await
            .ok_or(BotError::AuthenticationRequired)?
            .did;
        let result = self.call(Method::Post, "com.atproto.repo.putRecord", &[], Some(json!({ "repo": repo, "collection": collection, "rkey": rkey, "record": record })), true, false).await?;
        self.inner.cache.lock().await.clear();
        Ok(serde_json::from_value(result)?)
    }
    /// Replaces the authenticated account's private preference array.
    pub async fn put_preferences(&self, preferences: Vec<Value>) -> Result<Value, BotError> {
        self.call(
            Method::Post,
            "app.bsky.actor.putPreferences",
            &[],
            Some(json!({ "preferences": preferences })),
            true,
            false,
        )
        .await
    }
    /// Fetches the authenticated account's private preference array.
    pub async fn get_preferences(&self) -> Result<Vec<Value>, BotError> {
        self.field("app.bsky.actor.getPreferences", &[], "preferences")
            .await
    }

    /// Polls account notifications, suitable for a low-resource event loop.
    pub async fn list_notifications(
        &self,
        limit: u8,
        cursor: Option<&str>,
        seen_at: Option<&str>,
    ) -> Result<Page<Value>, BotError> {
        let mut params = vec![("limit", limit.min(100).to_string())];
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.into()));
        }
        if let Some(seen_at) = seen_at {
            params.push(("seenAt", seen_at.into()));
        }
        self.page(
            "app.bsky.notification.listNotifications",
            "notifications",
            params,
            None,
            None,
        )
        .await
    }
    /// Marks notifications at or before a timestamp as seen.
    pub async fn update_notifications_seen(&self, seen_at: &str) -> Result<(), BotError> {
        self.unit(
            "app.bsky.notification.updateSeen",
            json!({ "seenAt": seen_at }),
            false,
        )
        .await
    }
    /// Polls the Bluesky chat event log from a cursor.
    pub async fn get_chat_log(&self, cursor: Option<&str>) -> Result<Page<Value>, BotError> {
        let params = cursor.map_or_else(Vec::new, |cursor| vec![("cursor", cursor.into())]);
        let value = self.chat_query("chat.bsky.convo.getLog", &params).await?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items: serde_json::from_value(value.get("logs").cloned().unwrap_or_else(|| json!([])))?,
        })
    }

    async fn unit(&self, nsid: &str, body: Value, chat: bool) -> Result<(), BotError> {
        self.call(Method::Post, nsid, &[], Some(body), true, chat)
            .await?;
        self.inner.cache.lock().await.clear();
        Ok(())
    }

    /// Gets or creates a conversation containing the specified members.
    pub async fn get_conversation_for_members(
        &self,
        members: &[&str],
    ) -> Result<Conversation, BotError> {
        self.chat_query(
            "chat.bsky.convo.getConvoForMembers",
            &members
                .iter()
                .map(|did| ("members", (*did).into()))
                .collect::<Vec<_>>(),
        )
        .await
        .and_then(|v| {
            Ok(serde_json::from_value(
                v.get("convo").cloned().unwrap_or(v),
            )?)
        })
    }
    /// Fetches a conversation by ID.
    pub async fn get_conversation(&self, id: &str) -> Result<Conversation, BotError> {
        self.chat_query("chat.bsky.convo.getConvo", &[("convoId", id.into())])
            .await
            .and_then(|v| {
                Ok(serde_json::from_value(
                    v.get("convo").cloned().unwrap_or(v),
                )?)
            })
    }
    /// Lists conversations.
    pub async fn list_conversations(
        &self,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<Conversation>, BotError> {
        let mut params = vec![("limit", limit.min(100).to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.into()));
        }
        let value = self
            .chat_query("chat.bsky.convo.listConvos", &params)
            .await?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items: serde_json::from_value(
                value.get("convos").cloned().unwrap_or_else(|| json!([])),
            )?,
        })
    }
    /// Fetches messages in a conversation.
    pub async fn get_messages(
        &self,
        id: &str,
        limit: u8,
        cursor: Option<&str>,
    ) -> Result<Page<MessageView>, BotError> {
        let mut params = vec![
            ("convoId", id.into()),
            ("limit", limit.min(100).to_string()),
        ];
        if let Some(c) = cursor {
            params.push(("cursor", c.into()));
        }
        let value = self
            .chat_query("chat.bsky.convo.getMessages", &params)
            .await?;
        Ok(Page {
            cursor: value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            items: serde_json::from_value(
                value.get("messages").cloned().unwrap_or_else(|| json!([])),
            )?,
        })
    }
    /// Sends one direct message.
    pub async fn send_message(&self, payload: MessagePayload) -> Result<ChatMessage, BotError> {
        let (text, rich) = payload.text.parts();
        let mut message = json!({ "text": text });
        if let Some(facets) = payload.facets.or(rich) {
            message["facets"] = serde_json::to_value(facets)?;
        }
        if let Some(embed) = payload.embed {
            message["embed"] = serde_json::to_value(embed)?;
        }
        let body = json!({ "convoId": payload.conversation_id, "message": message });
        let value = self.chat_value("chat.bsky.convo.sendMessage", body).await?;
        Ok(serde_json::from_value(
            value.get("message").cloned().unwrap_or(value),
        )?)
    }
    /// Sends up to 100 direct messages in one request.
    pub async fn send_messages(
        &self,
        payloads: Vec<MessagePayload>,
    ) -> Result<Vec<ChatMessage>, BotError> {
        let mut items = Vec::with_capacity(payloads.len());
        for payload in payloads.into_iter().take(100) {
            let (text, rich) = payload.text.parts();
            let mut message = json!({ "text": text });
            if let Some(facets) = payload.facets.or(rich) {
                message["facets"] = serde_json::to_value(facets)?;
            }
            if let Some(embed) = payload.embed {
                message["embed"] = serde_json::to_value(embed)?;
            }
            items.push(json!({ "convoId": payload.conversation_id, "message": message }));
        }
        let value = self
            .chat_value(
                "chat.bsky.convo.sendMessageBatch",
                json!({ "items": items }),
            )
            .await?;
        Ok(serde_json::from_value(
            value.get("items").cloned().unwrap_or_else(|| json!([])),
        )?)
    }
    /// Deletes a direct message for the authenticated account.
    pub async fn delete_message(
        &self,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<(), BotError> {
        self.unit(
            "chat.bsky.convo.deleteMessageForSelf",
            json!({ "convoId": conversation_id, "messageId": message_id }),
            true,
        )
        .await
    }
    /// Leaves a direct-message conversation.
    pub async fn leave_conversation(&self, id: &str) -> Result<(), BotError> {
        self.unit("chat.bsky.convo.leaveConvo", json!({ "convoId": id }), true)
            .await
    }
    /// Updates read state for a conversation.
    pub async fn mark_conversation_read(
        &self,
        id: &str,
        message_id: Option<&str>,
    ) -> Result<Conversation, BotError> {
        let mut body = json!({ "convoId": id });
        if let Some(m) = message_id {
            body["messageId"] = json!(m);
        }
        let value = self.chat_value("chat.bsky.convo.updateRead", body).await?;
        Ok(serde_json::from_value(
            value.get("convo").cloned().unwrap_or(value),
        )?)
    }
    /// Mutes a conversation.
    pub async fn mute_conversation(&self, id: &str) -> Result<Conversation, BotError> {
        self.chat_convo("chat.bsky.convo.muteConvo", id).await
    }
    /// Unmutes a conversation.
    pub async fn unmute_conversation(&self, id: &str) -> Result<Conversation, BotError> {
        self.chat_convo("chat.bsky.convo.unmuteConvo", id).await
    }
    async fn chat_convo(&self, nsid: &str, id: &str) -> Result<Conversation, BotError> {
        let value = self.chat_value(nsid, json!({ "convoId": id })).await?;
        Ok(serde_json::from_value(
            value.get("convo").cloned().unwrap_or(value),
        )?)
    }
    async fn chat_value(&self, nsid: &str, body: Value) -> Result<Value, BotError> {
        self.call(Method::Post, nsid, &[], Some(body), true, true)
            .await
    }
    async fn chat_query(
        &self,
        nsid: &str,
        parameters: &[(&str, String)],
    ) -> Result<Value, BotError> {
        self.call(Method::Get, nsid, parameters, None, true, true)
            .await
    }
}

fn not_found() -> BotError {
    BotError::Xrpc {
        status: 404,
        error: "NotFound".into(),
        message: "resource not found".into(),
    }
}

fn parse_at_uri(uri: &str) -> Result<(&str, &str, &str), BotError> {
    let path = uri
        .strip_prefix("at://")
        .ok_or_else(|| BotError::InvalidAtUri(uri.into()))?;
    let mut parts = path.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(repo), Some(collection), Some(rkey), None)
            if !repo.is_empty() && !collection.is_empty() && !rkey.is_empty() =>
        {
            Ok((repo, collection, rkey))
        }
        _ => Err(BotError::InvalidAtUri(uri.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RequestBody, Response};
    use async_trait::async_trait;
    use std::{collections::VecDeque, sync::Mutex as StdMutex};

    struct MockTransport {
        responses: StdMutex<VecDeque<Response>>,
        requests: StdMutex<Vec<Request>>,
    }
    impl MockTransport {
        fn new(responses: Vec<Response>) -> Self {
            Self {
                responses: StdMutex::new(responses.into()),
                requests: StdMutex::new(vec![]),
            }
        }
        fn requests(&self) -> Vec<Request> {
            self.requests.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl Transport for Arc<MockTransport> {
        async fn execute(&self, request: Request) -> Result<Response, TransportError> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| TransportError("no mock response".into()))
        }
    }
    fn response(status: u16, body: Value) -> Response {
        Response { status, body }
    }
    fn config() -> BotConfig {
        BotConfig {
            service: Url::parse("https://pds.example").unwrap(),
            cache: CacheConfig {
                capacity: 20,
                ttl: std::time::Duration::from_secs(60),
            },
            rate_limit: RateLimitConfig {
                max_concurrent: 10,
                min_interval: std::time::Duration::ZERO,
            },
            ..BotConfig::default()
        }
    }
    fn session(access: &str, refresh: &str) -> Session {
        Session {
            did: "did:plc:bot".into(),
            handle: "bot.test".into(),
            access_jwt: access.into(),
            refresh_jwt: refresh.into(),
            extra: BTreeMap::new(),
        }
    }
    fn profile() -> Value {
        json!({ "did": "did:plc:alice", "handle": "alice.test", "viewer": { "following": "at://follow" } })
    }

    #[tokio::test]
    async fn logs_in_and_persists_the_session() {
        let transport = Arc::new(MockTransport::new(vec![response(
            200,
            serde_json::to_value(session("access", "refresh")).unwrap(),
        )]));
        let bot = Bot::with_transport(config(), transport.clone());
        let result = bot.login("bot.test", "password").await.unwrap();
        assert_eq!(result.access_jwt, "access");
        let requests = transport.requests();
        assert_eq!(requests[0].method(), Method::Post);
        assert_eq!(
            requests[0].url().path(),
            "/xrpc/com.atproto.server.createSession"
        );
        assert_eq!(
            requests[0].body(),
            Some(&RequestBody::Json(
                json!({ "identifier": "bot.test", "password": "password" })
            ))
        );
    }

    #[tokio::test]
    async fn caches_identical_queries_and_clears_cache_on_session_change() {
        let transport = Arc::new(MockTransport::new(vec![
            response(200, profile()),
            response(200, profile()),
        ]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("one", "refresh")).await;
        assert!(bot.get_profile("alice.test").await.unwrap().is_following());
        bot.get_profile("alice.test").await.unwrap();
        assert_eq!(transport.requests().len(), 1);
        bot.resume_session(session("two", "refresh")).await;
        bot.get_profile("alice.test").await.unwrap();
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn refreshes_once_after_an_expired_access_token() {
        let transport = Arc::new(MockTransport::new(vec![
            response(401, json!({ "error": "ExpiredToken" })),
            response(
                200,
                serde_json::to_value(session("new-access", "new-refresh")).unwrap(),
            ),
            response(200, profile()),
        ]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("old-access", "refresh")).await;
        bot.get_profile("alice.test").await.unwrap();
        let requests = transport.requests();
        assert_eq!(requests[0].headers()["authorization"], "Bearer old-access");
        assert_eq!(
            requests[1].url().path(),
            "/xrpc/com.atproto.server.refreshSession"
        );
        assert_eq!(requests[1].headers()["authorization"], "Bearer refresh");
        assert_eq!(requests[2].headers()["authorization"], "Bearer new-access");
    }

    #[tokio::test]
    async fn creates_and_deletes_records_with_exact_at_uri_parts() {
        let transport = Arc::new(MockTransport::new(vec![
            response(
                200,
                json!({ "uri": "at://did:plc:bot/app.bsky.graph.follow/key", "cid": "bafy" }),
            ),
            response(200, Value::Null),
        ]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        let reference = bot.follow("did:plc:alice").await.unwrap();
        bot.unfollow(&reference.uri).await.unwrap();
        let requests = transport.requests();
        let RequestBody::Json(created) = requests[0].body().unwrap() else {
            panic!()
        };
        assert_eq!(created["collection"], "app.bsky.graph.follow");
        assert_eq!(created["record"]["subject"], "did:plc:alice");
        let RequestBody::Json(deleted) = requests[1].body().unwrap() else {
            panic!()
        };
        assert_eq!(
            deleted,
            &json!({ "repo": "did:plc:bot", "collection": "app.bsky.graph.follow", "rkey": "key" })
        );
        assert!(matches!(
            parse_at_uri("https://bad"),
            Err(BotError::InvalidAtUri(_))
        ));
    }

    #[tokio::test]
    async fn chat_queries_are_gets_routed_through_the_chat_proxy() {
        let transport = Arc::new(MockTransport::new(vec![response(
            200,
            json!({ "convos": [], "cursor": "next" }),
        )]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        assert_eq!(
            bot.list_conversations(25, None)
                .await
                .unwrap()
                .cursor
                .as_deref(),
            Some("next")
        );
        let request = &transport.requests()[0];
        assert_eq!(request.method(), Method::Get);
        assert_eq!(request.headers()["atproto-proxy"], CHAT_PROXY);
        assert!(request.url().query().unwrap().contains("limit=25"));
    }

    #[tokio::test]
    async fn unwraps_feed_view_posts() {
        let transport = Arc::new(MockTransport::new(vec![response(
            200,
            json!({ "feed": [{ "post": { "uri": "at://post", "cid": "bafy", "author": profile(), "record": { "text": "hello" } }, "reason": { "$type": "reason" } }] }),
        )]));
        let bot = Bot::with_transport(config(), transport);
        bot.resume_session(session("access", "refresh")).await;
        let page = bot.get_timeline(100, None).await.unwrap();
        assert_eq!(page.items[0].record["text"], "hello");
    }

    #[tokio::test]
    async fn uploads_binary_blobs_without_json_encoding() {
        let transport = Arc::new(MockTransport::new(vec![response(
            200,
            json!({ "blob": { "ref": "blob" } }),
        )]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        assert_eq!(
            bot.upload_blob(vec![1, 2, 3], "image/png").await.unwrap()["ref"],
            "blob"
        );
        assert_eq!(
            transport.requests()[0].body(),
            Some(&RequestBody::Bytes {
                content_type: "image/png".into(),
                data: vec![1, 2, 3]
            })
        );
    }

    #[tokio::test]
    async fn detects_unicode_safe_links_tags_and_resolved_mentions() {
        let transport = Arc::new(MockTransport::new(vec![response(200, profile())]));
        let bot = Bot::with_transport(config(), transport);
        bot.resume_session(session("access", "refresh")).await;
        let text = "🦀 @alice.test see https://example.com/test. #rust";
        let facets = bot.detect_facets(text).await;
        assert_eq!(
            &text.as_bytes()[facets[0].index.byte_start..facets[0].index.byte_end],
            b"@alice.test"
        );
        assert_eq!(
            &text[facets[1].index.byte_start..facets[1].index.byte_end],
            "https://example.com/test"
        );
        assert_eq!(
            &text[facets[2].index.byte_start..facets[2].index.byte_end],
            "#rust"
        );
    }

    #[tokio::test]
    async fn omits_absent_optional_chat_fields() {
        let message = json!({ "id": "m", "rev": "r", "text": "hello", "sender": { "did": "did:plc:bot" }, "sentAt": "2026-01-01T00:00:00Z" });
        let transport = Arc::new(MockTransport::new(vec![response(200, message)]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        bot.send_message(MessagePayload::new("convo", "hello"))
            .await
            .unwrap();
        let RequestBody::Json(body) = transport.requests()[0].body().unwrap().clone() else {
            panic!()
        };
        assert_eq!(body["message"], json!({ "text": "hello" }));
    }

    #[tokio::test]
    async fn creates_a_threadgate_with_the_posts_record_key() {
        let transport = Arc::new(MockTransport::new(vec![
            response(
                200,
                json!({ "uri": "at://did:plc:bot/app.bsky.feed.post/post-key", "cid": "post-cid" }),
            ),
            response(
                200,
                json!({ "uri": "at://did:plc:bot/app.bsky.feed.threadgate/post-key", "cid": "gate-cid" }),
            ),
        ]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        let mut payload = PostPayload::new("hello");
        payload.threadgate = Some(crate::ThreadgateRules {
            allow_mentioned: true,
            allow_following: false,
            allow_lists: vec!["at://did:plc:bot/app.bsky.graph.list/list".into()],
        });
        bot.post(payload).await.unwrap();
        let requests = transport.requests();
        let RequestBody::Json(gate) = requests[1].body().unwrap() else {
            panic!()
        };
        assert_eq!(gate["rkey"], "post-key");
        assert_eq!(gate["record"]["allow"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn preserves_structured_xrpc_errors() {
        let transport = Arc::new(MockTransport::new(vec![response(
            400,
            json!({ "error": "InvalidRequest", "message": "bad actor" }),
        )]));
        let bot = Bot::with_transport(config(), transport);
        bot.resume_session(session("access", "refresh")).await;
        let error = bot.get_profile("bad").await.unwrap_err();
        assert!(
            matches!(error, BotError::Xrpc { status: 400, ref error, ref message } if error == "InvalidRequest" && message == "bad actor")
        );
    }

    #[tokio::test]
    async fn refuses_to_delete_another_repositorys_record() {
        let transport = Arc::new(MockTransport::new(vec![]));
        let bot = Bot::with_transport(config(), transport.clone());
        bot.resume_session(session("access", "refresh")).await;
        assert!(matches!(
            bot.delete_record("at://did:plc:other/app.bsky.feed.post/key")
                .await,
            Err(BotError::ForeignRecord)
        ));
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn unwraps_list_members_and_like_actors() {
        let transport = Arc::new(MockTransport::new(vec![
            response(
                200,
                json!({ "list": { "uri": "at://list", "cid": "list-cid", "name": "Friends", "purpose": "app.bsky.graph.defs#curatelist" }, "items": [{ "subject": profile() }] }),
            ),
            response(
                200,
                json!({ "likes": [{ "actor": profile(), "createdAt": "2026-01-01T00:00:00Z" }] }),
            ),
        ]));
        let bot = Bot::with_transport(config(), transport);
        bot.resume_session(session("access", "refresh")).await;
        let (_, members) = bot.get_list("at://list", 100, None).await.unwrap();
        assert_eq!(members.items[0].handle, "alice.test");
        let likes = bot.get_likes("at://post", None, None).await.unwrap();
        assert_eq!(likes.items[0].did, "did:plc:alice");
    }
}
