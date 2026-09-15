# NetworkCompatible

## NetherNet Transport Fork

This fork carries fixes to `netty-transport-nethernet` for the
[Geyser fork's portal bridge](https://github.com/eofihbzefhzb/Geyser), which accepts Bedrock
players' NetherNet connections when they join from the Xbox friends list. Only
`transport-nethernet` is changed; `transport-raknet` is upstream's.

### The three forks

Each README lists what its own fork changes. The setup guide for the whole stack is the
[Broadcaster README](https://github.com/eofihbzefhzb/Broadcaster#setup).

| Fork | Upstream | Role |
|---|---|---|
| [Broadcaster](https://github.com/eofihbzefhzb/Broadcaster) | [MCXboxBroadcast/Broadcaster](https://github.com/MCXboxBroadcast/Broadcaster) | Publishes the Xbox session players see in their friends list |
| [Geyser](https://github.com/eofihbzefhzb/Geyser) | [GeyserMC/Geyser](https://github.com/GeyserMC/Geyser) | Runs the portal bridge, which accepts those players' NetherNet connections. Velocity only |
| **NetworkCompatible** (this repo) | [Kas-tle/NetworkCompatible](https://github.com/Kas-tle/NetworkCompatible) | The NetherNet transport library the portal bridge is built on |

### What this fork changes

- **Peer address:** a connection's remote address comes from the peer's ICE candidates instead of
  staying `0.0.0.0` for everyone. The public `srflx` address wins over `prflx`, then `host`.
  Relay and `.local` candidates are ignored, and a candidate never triggers a DNS lookup.
- **Crashes:** an exception in a native WebRTC callback is caught instead of aborting the JVM. A
  handshake timeout no longer frees the peer connection twice. A connection dropped before Netty
  registered it is closed properly, including when the server channel has already closed.
- **Leaks:** the reassembly buffer is released when a connection closes. A server channel only
  disposes a `PeerConnectionFactory` it created itself. Pending signaling requests fail instead of
  waiting forever when the websocket drops or the request was never sent.
- **Outbound segments** are read from the buffer's reader index.

### Versions and releases

- Versions are the upstream version plus `-ip.<n>`, for example `1.7.4-ip.11`, so they are never
  mistaken for upstream's artifact on Maven Central.
- Bumping `version=` in `gradle.properties` on `master` makes `tag-version.yml` create the tag and
  the GitHub release, then wait until JitPack has built it.
- Geyser uses a new version only once `nethernet =` in its `gradle/libs.versions.toml` is set to
  that tag.
- Upstream's Maven Central `publish.yml` runs only when started by hand; it needs credentials a
  fork does not have.

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.eofihbzefhzb.NetworkCompatible:netty-transport-nethernet:<tag>")
}
```

## Introduction

You can join the [Discord](https://discord.gg/5z4GuSnqmQ) for help with this fork. This raknet portion of this library is a fork of [CloudburstMC/Network](https://github.com/CloudburstMC/Network) with a focus on improving the compatibility of the client side of the library to more closely align with the vanilla Minecraft Bedrock client.

The new package `netty-transport-nethernet` is also included, which provides support for the Nethernet protocol. This is achieved using a JNI wrapper for the native WebRTC library.

## Package Specific Information

See the respective README files for each transport library for more information:

- [netty-transport-raknet](transport-raknet/README.md)
- [netty-transport-nethernet](transport-nethernet/README.md)