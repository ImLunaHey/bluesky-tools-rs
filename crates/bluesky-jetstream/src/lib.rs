//! Typed, reconnecting client for the AT Protocol Jetstream service.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

/// Default public Jetstream subscription endpoint.
pub const DEFAULT_ENDPOINT: &str = "wss://jetstream1.us-east.bsky.network/subscribe";

/// Configuration used to establish a subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JetstreamConfig {
    /// WebSocket subscription endpoint.
    pub endpoint: Url,
    /// Collections to receive. Empty receives every collection.
    pub wanted_collections: Vec<String>,
    /// Repositories to receive. Empty receives every repository.
    pub wanted_dids: Vec<String>,
    /// Maximum event size requested from the server. Zero means unlimited.
    pub max_message_size_bytes: u64,
    /// Microsecond cursor from which processing should resume.
    pub cursor: Option<u64>,
    /// Delay before reconnecting after an unexpected disconnect.
    pub reconnect_delay: Duration,
}

impl Default for JetstreamConfig {
    fn default() -> Self {
        Self {
            endpoint: Url::parse(DEFAULT_ENDPOINT).expect("the default Jetstream URL is valid"),
            wanted_collections: Vec::new(),
            wanted_dids: Vec::new(),
            max_message_size_bytes: 0,
            cursor: None,
            reconnect_delay: Duration::from_secs(1),
        }
    }
}

impl JetstreamConfig {
    /// Produces the exact subscription URL for this configuration and cursor.
    #[must_use]
    pub fn subscription_url(&self, cursor: Option<u64>) -> Url {
        let mut url = self.endpoint.clone();
        {
            let mut query = url.query_pairs_mut();
            for collection in &self.wanted_collections {
                query.append_pair("wantedCollections", collection);
            }
            for did in &self.wanted_dids {
                query.append_pair("wantedDids", did);
            }
            if self.max_message_size_bytes > 0 {
                query.append_pair(
                    "maxMessageSizeBytes",
                    &self.max_message_size_bytes.to_string(),
                );
            }
            if let Some(cursor) = cursor.or(self.cursor) {
                query.append_pair("cursor", &cursor.to_string());
            }
        }
        url
    }
}

/// Options that can be changed without reconnecting.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionsUpdate {
    /// Replace the collection filter, including with an empty list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wanted_collections: Option<Vec<String>>,
    /// Replace the repository filter, including with an empty list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wanted_dids: Option<Vec<String>>,
    /// Replace the requested maximum message size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct OptionsUpdateMessage<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    payload: &'a OptionsUpdate,
}

/// Operation represented by a commit event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitOperation {
    /// Record creation.
    Create,
    /// Record replacement.
    Update,
    /// Record deletion.
    Delete,
}

/// A repository commit delivered by Jetstream.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Commit {
    /// Operation applied to the record.
    pub operation: CommitOperation,
    /// Repository revision.
    pub rev: String,
    /// Record collection NSID.
    pub collection: String,
    /// Record key.
    pub rkey: String,
    /// Record body, absent for deletions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<Value>,
    /// Record CID, absent for deletions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,
}

impl Commit {
    /// Deserializes the record body into an application-specific lexicon type.
    ///
    /// # Errors
    /// Returns a JSON decoding error when the record does not match `T`.
    pub fn record_as<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.record.clone().map(serde_json::from_value).transpose()
    }
}

/// Account status payload. Unknown fields are preserved for protocol evolution.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Account {
    /// Account DID.
    pub did: String,
    /// Whether the account is active, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// Account status, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Additional protocol fields.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Identity update payload. Unknown fields are preserved for protocol evolution.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Identity {
    /// Account DID.
    pub did: String,
    /// Latest handle, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    /// Latest signing key, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Additional protocol fields.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// A validated event delivered by Jetstream.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Event {
    /// Repository commit.
    #[serde(rename = "commit")]
    Commit {
        /// Repository DID.
        did: String,
        /// Event timestamp in Unix microseconds.
        time_us: u64,
        /// Commit payload.
        commit: Commit,
    },
    /// Account status update.
    #[serde(rename = "account")]
    Account {
        /// Repository DID.
        did: String,
        /// Event timestamp in Unix microseconds.
        time_us: u64,
        /// Account status payload.
        account: Account,
    },
    /// Identity update.
    #[serde(rename = "identity")]
    Identity {
        /// Repository DID.
        did: String,
        /// Event timestamp in Unix microseconds.
        time_us: u64,
        /// Identity payload.
        identity: Identity,
    },
}

impl Event {
    /// Event timestamp in Unix microseconds.
    #[must_use]
    pub const fn time_us(&self) -> u64 {
        match self {
            Self::Commit { time_us, .. }
            | Self::Account { time_us, .. }
            | Self::Identity { time_us, .. } => *time_us,
        }
    }

    /// Returns the commit when this event affects the requested collection.
    #[must_use]
    pub fn commit_for(&self, collection: &str) -> Option<&Commit> {
        match self {
            Self::Commit { commit, .. } if commit.collection == collection => Some(commit),
            _ => None,
        }
    }

    /// Returns a matching record creation.
    #[must_use]
    pub fn creation_for(&self, collection: &str) -> Option<&Commit> {
        self.commit_for(collection)
            .filter(|commit| commit.operation == CommitOperation::Create)
    }

    /// Returns a matching record update.
    #[must_use]
    pub fn update_for(&self, collection: &str) -> Option<&Commit> {
        self.commit_for(collection)
            .filter(|commit| commit.operation == CommitOperation::Update)
    }

    /// Returns a matching record deletion.
    #[must_use]
    pub fn deletion_for(&self, collection: &str) -> Option<&Commit> {
        self.commit_for(collection)
            .filter(|commit| commit.operation == CommitOperation::Delete)
    }

    fn validate(self) -> Result<Self, JetstreamError> {
        match &self {
            Self::Commit { commit, .. } => {
                if commit.collection.is_empty() || commit.rkey.is_empty() || commit.rev.is_empty() {
                    return Err(JetstreamError::MalformedEvent(
                        "commit identifiers must not be empty",
                    ));
                }
                if commit.operation == CommitOperation::Create && commit.record.is_none() {
                    return Err(JetstreamError::MalformedEvent(
                        "create commit is missing its record",
                    ));
                }
            }
            Self::Account { account, .. } if account.did.is_empty() => {
                return Err(JetstreamError::MalformedEvent("account DID is empty"));
            }
            Self::Identity { identity, .. } if identity.did.is_empty() => {
                return Err(JetstreamError::MalformedEvent("identity DID is empty"));
            }
            _ => {}
        }
        Ok(self)
    }
}

/// Non-data notifications produced by the connection.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionEvent {
    /// A connection was established.
    Open,
    /// A connection ended. Unexpected closures are followed by a reconnect.
    Close,
    /// A valid Jetstream event arrived.
    Event(Event),
    /// A recoverable connection or message error occurred.
    Error(String),
}

/// Errors returned directly to a caller.
#[derive(Debug, Error)]
pub enum JetstreamError {
    /// The connection task has stopped.
    #[error("Jetstream connection is closed")]
    Closed,
    /// An event failed structural validation.
    #[error("malformed Jetstream event: {0}")]
    MalformedEvent(&'static str),
    /// An options update could not be encoded.
    #[error("could not encode options update: {0}")]
    Encode(#[from] serde_json::Error),
}

enum Command {
    Update(OptionsUpdate),
    Close,
}

/// Configured Jetstream client.
#[derive(Clone, Debug)]
pub struct Jetstream {
    config: JetstreamConfig,
}

impl Jetstream {
    /// Creates a client using the supplied configuration.
    #[must_use]
    pub const fn new(config: JetstreamConfig) -> Self {
        Self { config }
    }

    /// Opens the subscription in a background Tokio task.
    #[must_use]
    pub fn connect(self) -> JetstreamConnection {
        let (events_tx, events_rx) = mpsc::channel(256);
        let (commands_tx, commands_rx) = mpsc::channel(16);
        let cursor = Arc::new(AtomicU64::new(self.config.cursor.unwrap_or(0)));
        let config = Arc::new(RwLock::new(self.config));
        tokio::spawn(run(config.clone(), cursor.clone(), events_tx, commands_rx));
        JetstreamConnection {
            events: events_rx,
            commands: commands_tx,
            cursor,
            config,
        }
    }
}

impl Default for Jetstream {
    fn default() -> Self {
        Self::new(JetstreamConfig::default())
    }
}

/// Handle to a running Jetstream subscription.
pub struct JetstreamConnection {
    events: mpsc::Receiver<ConnectionEvent>,
    commands: mpsc::Sender<Command>,
    cursor: Arc<AtomicU64>,
    config: Arc<RwLock<JetstreamConfig>>,
}

impl JetstreamConnection {
    /// Waits for the next data or lifecycle event.
    pub async fn next(&mut self) -> Option<ConnectionEvent> {
        self.events.recv().await
    }

    /// Last event timestamp observed, suitable for durable checkpointing.
    #[must_use]
    pub fn cursor(&self) -> Option<u64> {
        match self.cursor.load(Ordering::Acquire) {
            0 => None,
            value => Some(value),
        }
    }

    /// Returns a snapshot of the active configuration.
    pub async fn config(&self) -> JetstreamConfig {
        self.config.read().await.clone()
    }

    /// Changes server-side filters for the current connection.
    ///
    /// # Errors
    /// Returns [`JetstreamError::Closed`] if the connection task has stopped.
    pub async fn update_options(&self, update: OptionsUpdate) -> Result<(), JetstreamError> {
        self.commands
            .send(Command::Update(update))
            .await
            .map_err(|_| JetstreamError::Closed)
    }

    /// Stops reconnecting and closes the current socket.
    ///
    /// # Errors
    /// Returns [`JetstreamError::Closed`] if the connection task has stopped.
    pub async fn close(&self) -> Result<(), JetstreamError> {
        self.commands
            .send(Command::Close)
            .await
            .map_err(|_| JetstreamError::Closed)
    }
}

async fn run(
    config: Arc<RwLock<JetstreamConfig>>,
    cursor: Arc<AtomicU64>,
    events: mpsc::Sender<ConnectionEvent>,
    mut commands: mpsc::Receiver<Command>,
) {
    loop {
        let snapshot = config.read().await.clone();
        let current_cursor = match cursor.load(Ordering::Acquire) {
            0 => None,
            value => Some(value),
        };
        let url = snapshot.subscription_url(current_cursor);
        match connect_async(url.as_str()).await {
            Ok((socket, _)) => {
                let _ = events.send(ConnectionEvent::Open).await;
                let (mut writer, mut reader) = socket.split();
                let mut should_close = false;
                loop {
                    tokio::select! {
                        command = commands.recv() => match command {
                            Some(Command::Update(update)) => {
                                let payload = serde_json::to_string(&OptionsUpdateMessage { kind: "options_update", payload: &update });
                                match payload {
                                    Ok(payload) => {
                                        if writer.send(Message::Text(payload.into())).await.is_err() { break; }
                                        apply_update(&config, update).await;
                                    }
                                    Err(error) => { let _ = events.send(ConnectionEvent::Error(error.to_string())).await; }
                                }
                            }
                            Some(Command::Close) | None => { let _ = writer.close().await; should_close = true; break; }
                        },
                        message = reader.next() => match message {
                            Some(Ok(Message::Text(text))) => process_message(&text, &cursor, &events).await,
                            Some(Ok(Message::Binary(bytes))) => match std::str::from_utf8(&bytes) {
                                Ok(text) => process_message(text, &cursor, &events).await,
                                Err(error) => { let _ = events.send(ConnectionEvent::Error(error.to_string())).await; }
                            },
                            Some(Ok(Message::Close(_))) | None => break,
                            Some(Ok(_)) => {}
                            Some(Err(error)) => { let _ = events.send(ConnectionEvent::Error(error.to_string())).await; break; }
                        }
                    }
                }
                let _ = events.send(ConnectionEvent::Close).await;
                if should_close {
                    return;
                }
            }
            Err(error) => {
                let _ = events.send(ConnectionEvent::Error(error.to_string())).await;
            }
        }
        tokio::select! {
            () = tokio::time::sleep(snapshot.reconnect_delay) => {}
            command = commands.recv() => match command {
                Some(Command::Close) | None => return,
                Some(Command::Update(update)) => apply_update(&config, update).await,
            }
        }
    }
}

async fn process_message(text: &str, cursor: &AtomicU64, events: &mpsc::Sender<ConnectionEvent>) {
    match serde_json::from_str::<Event>(text)
        .map_err(JetstreamError::from)
        .and_then(Event::validate)
    {
        Ok(event) => {
            cursor.fetch_max(event.time_us(), Ordering::AcqRel);
            let _ = events.send(ConnectionEvent::Event(event)).await;
        }
        Err(error) => {
            let _ = events.send(ConnectionEvent::Error(error.to_string())).await;
        }
    }
}

async fn apply_update(config: &RwLock<JetstreamConfig>, update: OptionsUpdate) {
    let mut config = config.write().await;
    if let Some(value) = update.wanted_collections {
        config.wanted_collections = value;
    }
    if let Some(value) = update.wanted_dids {
        config.wanted_dids = value;
    }
    if let Some(value) = update.max_message_size_bytes {
        config.max_message_size_bytes = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::{
        accept_async, accept_hdr_async,
        tungstenite::handshake::server::{Request, Response},
    };

    fn commit_json(time_us: u64) -> String {
        serde_json::json!({
            "did": "did:plc:alice",
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "rev": "3abc",
                "collection": "app.bsky.feed.post",
                "rkey": "one",
                "cid": "bafyrecord",
                "record": { "$type": "app.bsky.feed.post", "text": "hello" }
            }
        })
        .to_string()
    }

    #[test]
    fn builds_compatible_subscription_url() {
        let config = JetstreamConfig {
            endpoint: Url::parse("wss://example.com/subscribe").unwrap(),
            wanted_collections: vec!["app.bsky.feed.post".into(), "app.bsky.feed.like".into()],
            wanted_dids: vec!["did:plc:alice".into()],
            max_message_size_bytes: 1_000_000,
            cursor: Some(42),
            ..JetstreamConfig::default()
        };
        let url = config.subscription_url(None);
        let pairs: Vec<_> = url.query_pairs().collect();
        assert_eq!(
            pairs,
            vec![
                ("wantedCollections".into(), "app.bsky.feed.post".into()),
                ("wantedCollections".into(), "app.bsky.feed.like".into()),
                ("wantedDids".into(), "did:plc:alice".into()),
                ("maxMessageSizeBytes".into(), "1000000".into()),
                ("cursor".into(), "42".into()),
            ]
        );
        assert!(
            config
                .subscription_url(Some(99))
                .as_str()
                .ends_with("cursor=99")
        );
    }

    #[test]
    fn deserializes_and_filters_each_commit_operation() {
        for (operation, expected) in [
            ("create", CommitOperation::Create),
            ("update", CommitOperation::Update),
            ("delete", CommitOperation::Delete),
        ] {
            let mut value: Value = serde_json::from_str(&commit_json(7)).unwrap();
            value["commit"]["operation"] = Value::String(operation.into());
            if operation == "delete" {
                value["commit"].as_object_mut().unwrap().remove("record");
            }
            let event: Event = serde_json::from_value(value).unwrap();
            let commit = event.commit_for("app.bsky.feed.post").unwrap();
            assert_eq!(commit.operation, expected);
            assert_eq!(
                event.creation_for("app.bsky.feed.post").is_some(),
                expected == CommitOperation::Create
            );
            assert_eq!(
                event.update_for("app.bsky.feed.post").is_some(),
                expected == CommitOperation::Update
            );
            assert_eq!(
                event.deletion_for("app.bsky.feed.post").is_some(),
                expected == CommitOperation::Delete
            );
            assert!(event.commit_for("app.bsky.feed.like").is_none());
        }
    }

    #[test]
    fn decodes_records_into_caller_owned_lexicon_types() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Post {
            text: String,
        }
        let event: Event = serde_json::from_str(&commit_json(7)).unwrap();
        let commit = event.commit_for("app.bsky.feed.post").unwrap();
        assert_eq!(
            commit.record_as::<Post>().unwrap(),
            Some(Post {
                text: "hello".into()
            })
        );
    }

    #[test]
    fn preserves_unknown_account_and_identity_fields() {
        let account: Event = serde_json::from_value(serde_json::json!({
            "did": "did:plc:alice", "time_us": 1, "kind": "account",
            "account": { "did": "did:plc:alice", "active": false, "status": "suspended", "future": 1 }
        })).unwrap();
        let identity: Event = serde_json::from_value(serde_json::json!({
            "did": "did:plc:alice", "time_us": 2, "kind": "identity",
            "identity": { "did": "did:plc:alice", "handle": "alice.test", "future": true }
        }))
        .unwrap();
        match account {
            Event::Account { account, .. } => assert_eq!(account.extra["future"], 1),
            _ => panic!(),
        }
        match identity {
            Event::Identity { identity, .. } => assert_eq!(identity.extra["future"], true),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn reports_bad_messages_without_advancing_cursor() {
        let (tx, mut rx) = mpsc::channel(2);
        let cursor = AtomicU64::new(0);
        process_message(r#"{"did":"x","time_us":12,"kind":"commit","commit":{"operation":"create","rev":"","collection":"c","rkey":"r"}}"#, &cursor, &tx).await;
        assert!(matches!(rx.recv().await, Some(ConnectionEvent::Error(_))));
        assert_eq!(cursor.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn connects_receives_updates_options_and_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(commit_json(123).into()))
                .await
                .unwrap();
            let update = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&update).unwrap();
            assert_eq!(value["type"], "options_update");
            assert_eq!(value["payload"]["wantedCollections"], serde_json::json!([]));
            socket.close(None).await.unwrap();
        });

        let config = JetstreamConfig {
            endpoint: Url::parse(&format!("ws://{address}/subscribe")).unwrap(),
            reconnect_delay: Duration::from_secs(60),
            ..JetstreamConfig::default()
        };
        let mut connection = Jetstream::new(config).connect();
        assert_eq!(connection.next().await, Some(ConnectionEvent::Open));
        assert!(matches!(
            connection.next().await,
            Some(ConnectionEvent::Event(Event::Commit { time_us: 123, .. }))
        ));
        assert_eq!(connection.cursor(), Some(123));
        connection
            .update_options(OptionsUpdate {
                wanted_collections: Some(vec![]),
                ..OptionsUpdate::default()
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(connection.next().await, Some(ConnectionEvent::Close));
        connection.close().await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn reconnects_from_the_latest_cursor() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (path_tx, path_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = accept_async(first).await.unwrap();
            first
                .send(Message::Text(commit_json(777).into()))
                .await
                .unwrap();
            first.close(None).await.unwrap();

            let (second, _) = listener.accept().await.unwrap();
            let second = accept_hdr_async(second, move |request: &Request, response: Response| {
                path_tx.send(request.uri().to_string()).unwrap();
                Ok(response)
            })
            .await
            .unwrap();
            drop(second);
        });
        let mut connection = Jetstream::new(JetstreamConfig {
            endpoint: Url::parse(&format!("ws://{address}/subscribe")).unwrap(),
            reconnect_delay: Duration::from_millis(1),
            ..JetstreamConfig::default()
        })
        .connect();

        assert_eq!(connection.next().await, Some(ConnectionEvent::Open));
        assert!(matches!(
            connection.next().await,
            Some(ConnectionEvent::Event(_))
        ));
        assert_eq!(connection.next().await, Some(ConnectionEvent::Close));
        assert_eq!(connection.next().await, Some(ConnectionEvent::Open));
        assert_eq!(path_rx.await.unwrap(), "/subscribe?cursor=777");
        connection.close().await.unwrap();
        server.await.unwrap();
    }
}
