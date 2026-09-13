# bluesky-richtext

Build byte-accurate Bluesky rich text facets without manually tracking UTF-8 offsets.

```rust
use bluesky_richtext::RichTextBuilder;

let mut builder = RichTextBuilder::new();
builder
    .add_text("Hello ")
    .add_mention("@luna", "did:plc:example")
    .add_text(" — see ")
    .add_link("the docs", "https://docs.bsky.app")
    .add_text(" ")
    .add_tag("rust");

let rich_text = builder.finish();
rich_text.validate()?;
# Ok::<(), bluesky_richtext::RichTextError>(())
```

`RichText` and its facets serialize directly to the corresponding Bluesky lexicon shape.
