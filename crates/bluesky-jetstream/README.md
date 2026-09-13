# bluesky-jetstream

An asynchronous, typed and reconnecting AT Protocol Jetstream client.

```rust,no_run
use bluesky_jetstream::{ConnectionEvent, Event, Jetstream, JetstreamConfig};

#[tokio::main]
async fn main() {
    let mut connection = Jetstream::new(JetstreamConfig {
        wanted_collections: vec!["app.bsky.feed.post".into()],
        ..JetstreamConfig::default()
    }).connect();

    while let Some(notification) = connection.next().await {
        if let ConnectionEvent::Event(Event::Commit { commit, .. }) = notification {
            println!("{} {}", commit.collection, commit.rkey);
        }
    }
}
```

The client keeps the latest microsecond cursor, resumes from it after reconnecting,
supports live filter updates, and exposes malformed messages as recoverable errors.
