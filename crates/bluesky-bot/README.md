# bluesky-bot

An asynchronous, testable, high-level Bluesky bot client.

```rust,no_run
use bluesky_bot::{Bot, BotConfig, PostPayload};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bot = Bot::new(BotConfig::default());
    bot.login("bot.example.com", "app-password").await?;
    let post = bot.post(PostPayload::new("Hello from Rust!")).await?;
    println!("{}", post.uri);
    Ok(())
}
```

## Capabilities

- login, persisted session restoration, serialized token refresh, and logout;
- bounded query caching and configurable request concurrency/pacing;
- posts, threads, rich text, embeds, uploads, replies, threadgates, and engagement;
- profiles, follows, mutes, blocks, lists, feeds, labelers, and starter packs;
- Ozone account and record labels;
- Bluesky chat conversations, messages, read state, muting, and batch sends;
- typed Jetstream integration through the re-exported `jetstream` module;
- arbitrary XRPC queries and procedures for custom or newly introduced lexicons;
- pluggable transport with no network access required by tests.

Resource payloads preserve unknown response fields, allowing additive protocol changes without data loss.
