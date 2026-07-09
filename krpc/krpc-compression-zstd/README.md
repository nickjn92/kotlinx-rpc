# Overview

This is the `:krpc:krpc-compression-zstd` module of kotlinx-rpc -- Zstandard compression for serialized kRPC messages.
It is a Kotlin Multiplatform module (JVM and a subset of Native targets; see `gradle.properties` for exclusions,
which mirror the targets published by the underlying `com.squareup.zstd:zstd-kmp` binding).

`KrpcConfigBuilder.Connector.zstd()` -- enables Zstd compression in the `connector { }` DSL

## How it works

- Compression is negotiated during the kRPC handshake via the `MESSAGE_COMPRESSION` plugin:
  it is used only when both endpoints configure a codec with the same name (`"zstd"`).
  Peers without a matching codec (including older library versions) fall back to uncompressed messages.
- The handshake also carries each endpoint's `maxDecompressedMessageSize` and the maximum
  compressed-envelope version it accepts. Senders don't compress messages that would exceed
  the peer's limit and disable compression when they can't produce an acceptable envelope version.
- Compressed messages are wrapped in an envelope (`krpc-cmp` magic, version, message type, codec name, payload).
  String messages use a Base64 string envelope so the transport message kind is preserved --
  a string-only `KrpcTransport` keeps working with compression enabled.
- Messages below `minimumMessageSize`, and messages that compression does not make smaller,
  are sent as-is. Envelope and codec plumbing lives in `krpc-core`
  (`kotlinx.rpc.krpc.internal.KrpcCompressedMessage`); this module only provides the Zstd codec.
- Each envelope contains exactly one Zstd frame. The decoder caps the frame window at 8 MiB and separately
  bounds compressed input and decompressed output before deserialization. Invalid envelopes terminate the
  connection so pending calls fail.
- At most two compressed envelopes are decoded concurrently within a process, bounding aggregate
  decompression allocations without making one slow connection block every other connection.

## Dependency resolution note

`com.squareup.zstd` is not yet mirrored in the JetBrains `build-deps` proxy repository,
so the settings conventions (`conventions-repositories.settings.gradle.kts`) declare a
content-filtered Maven Central fallback for that group only — it applies to every consumer
of this module. Remove it once the group is mirrored (see the KRPC-529 TODO there).

User-facing documentation: [configuration topic](../../docs/pages/kotlinx-rpc/topics/configuration.topic), "Message compression" chapter.
