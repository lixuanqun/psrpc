# RFC: Brokerless QUIC mesh bus for psrpc

## Motivation

The cross-bus benchmarks in #128 showed a consistent ~6× throughput gap between the
local (in-process) bus and the fastest network bus (NATS):

| bus | w=128 qps | local/n | 
|---|---|---|
| local | 94,741 | 1× |
| NATS | 15,608 | 6.1× slower |
| Redis | 12,495 | 7.6× |
| RabbitMQ | 7,178 | 13.2× |

The gap comes from the broker hop that every RPC pays twice (publish → broker →
subscriber → broker → response), plus broker CPU overhead. In principle psrpc doesn't
need a broker at all — it already tolerates at-most-once delivery (request
timeouts + claims absorb loss) and it already has a local bus that proves in-process
dispatch can hit 94k rps.

This proposes removing the broker entirely: replace it with a direct peer-to-peer
QUIC mesh where every livekit-server node talks to every other node directly, using
gossip for peer discovery and subscription propagation.

## Design

### Architecture

```
┌───────────┐    QUIC stream (per peer, framed)    ┌───────────┐
│  Node A   │ ←─────────────────────────────────→ │  Node B   │
│           │    memberlist gossip (SWIM)          │           │
│  subs:    │ ←─────────────────────────────────→ │  subs:    │
│   room/1  │    NodeMeta + SendReliable           │   room/2  │
│   claim/x │                                      │   claim/y │
└───────────┘                                      └───────────┘
```

- **Peer discovery**: HashiCorp memberlist (SWIM gossip, no central coordinator).
  Each node announces its QUIC address and the set of topics it has local
  subscribers for.
- **Transport**: quic-go, one persistent stream per peer pair. Messages are framed
  with a 4-byte big-endian length prefix. The stream is reused across messages
  (no per-message OpenStream).
- **Subscription propagation**: NodeMeta carries the compact topic set during
  push-pull syncs. On NodeJoin the seed broadcasts its subscriptions via
  SendReliable. On subscribe, the new topic is broadcast to all peers
  immediately.
- **Broadcast topics**: publish sends to every peer whose NodeMeta lists the
  topic, plus local subs. The wire frame carries the topic name so the receiver
  can route.
- **Queue topics**: each queue topic elects a single owner node via
  highest-random-weight (Rendezvous) hashing over the nodes with local queue
  subscribers for that topic. Publishers forward the frame to the owner only;
  the owner round-robins among its local subscribers. Bus-level single
  delivery keeps queue RPCs correct even when the claim is skipped (#120),
  and preserves O(1) frames per publish. Ownership is computed at publish
  time from the live subscription view, so it follows joins and leaves
  without a separate rebalancing protocol; during subscription-propagation
  windows publishers may briefly disagree on the owner (duplicate or drop),
  which psrpc absorbs the same way it absorbs Redis's 5s dedup window
  (request expiry + caller retry).

### Wire frame

```
Outer framing (per message on the persistent stream):
  4 bytes frame length (big-endian uint32)
  frame bytes

Inner frame:
  2 bytes topic length (big-endian uint16)
  topic bytes
  4 bytes payload length (big-endian uint32)
  payload bytes
```

### Why QUIC and not raw UDP / TCP / SCTP

- **No head-of-line blocking**: 128 concurrent RPCs on one UDP socket do not share a
  byte stream. TCP would stall them all behind one lost packet.
- **Connection migration**: QUIC connection IDs survive IP changes — if a node
  changes IP the session continues, no re-handshake.
- **0-RTT**: a reconnecting node can resume with zero additional round trips
  (relevant for livekit-server rolling restarts).
- **Built-in TLS 1.3 + mTLS**: free mutual authentication without a separate
  certificate distribution layer.
- **Stream multiplexing**: one QUIC connection carries many streams — we use one
  stream per peer pair (not per topic), with framed messages multiplexed on it.

### Dependencies added

- `github.com/hashicorp/memberlist` (SWIM gossip, ~3k LOC, stable since 2014)
- `github.com/quic-go/quic-go` (QUIC impl, already used in Go ecosystem, no CGo)

Both are permissively licensed (MPL-2.0 and MIT respectively).

## Prototype results

A proof of concept was built to validate the approach. Full psrpc RPC round-trip
(request → claim → handler → response), same methodology as #128: 500 sequential
samples + sustained throughput at w=1,8,32,128 for 8s each.

**Setup**: two livekit-server processes (seed + peer) on loopback, one RPC server
on the seed, one RPC client on the peer — mirroring separate processes talking
over the mesh.

| bus | seq p50 | seq p99 | w=1 | w=8 | w=32 | w=128 | p99@128 |
|---|---|---|---|---|---|---|---|
| local | ~0s | 1.0ms | 16,104 | 54,480 | 88,027 | **95,326** | 6.2ms |
| **mesh** | **529µs** | 1.6ms | 1,160 | 8,012 | 16,789 | **44,636** | 8.6ms |
| NATS | — | — | 219 | 1,568 | 5,326 | 15,608 | 14.1ms |
| Redis | — | — | 217 | 1,320 | 4,129 | 12,495 | 16.0ms |
| RabbitMQ | — | — | 253 | 1,515 | 4,125 | 7,178 | 54.4ms |
| MQTT | — | — | 209 | 1,504 | 4,737 | 10,184 | 22.0ms |

**Key findings:**

1. **Latency crushes all brokers.** seq p50 = 529µs — a full psrpc RPC takes half
   a millisecond. NATS ~4.5ms. That's 9× faster single-request latency.
2. **Throughput leads by a wide margin.** w=128: 44,636 qps — 2.9× NATS, 3.6× Redis,
   6.2× RabbitMQ, with zero errors.
3. **p99 at 128 workers is 8.6ms** — lower than NATS at *32* workers (no NATS p99
   at 128 was reported, but at 32 it was 14.1ms).
4. **Single-worker throughput is 1,160 qps** — 5.3× NATS. The per-request overhead
   is dominated by QUIC stream write + psrpc claim + proto marshal, not by a broker
   process in the middle.

Unit tests pass clean with `-race` (local round-trip, two-node cross-node,
subscribe-close, queue subscribe, plus the cross-node queue suite:
cluster-wide exactly-once, owner-exclusive delivery, and a node joining an
existing queue topic). The full benchmark triggers a quic-go crash
under `-race` (known upstream issue: quic-go uses unsafe + syscall zero-copy that
is incompatible with the race detector's shadow memory — not our code).

**Cross-node queue throughput** (bus level, three nodes, competing consumers on
all three, 30 topics, `TestBenchMeshQueue`; every width verified sent ==
received over 1M+ messages):

| bus | w=1 | w=8 | w=32 | w=128 |
|---|---|---|---|---|
| mesh-queue | 164,196 | 155,997 | 141,091 | 127,683 |

## Known gaps

This is a design RFC, not a production implementation. The prototype intentionally
leaves these open:

1. **No graceful leave / subscription drain.** A node that shuts down does not
   hand over queue ownership, so in-flight messages addressed to a departed
   owner are dropped (at-most-once; caller retry absorbs it).
2. **NodeMeta is capped at 512 bytes.** Topics that overflow the metadata budget
   are propagated only via the SendReliable path, which has a 1400-byte message
   limit. For deployments with >~150 topics per node, this needs an out-of-band
   subscription sync protocol.
3. **Self-signed TLS with InsecureSkipVerify.** Loopback-safe, but production
   would need mTLS with a pre-shared CA or automated cert provisioning.
4. **One stream per peer pair.** Under extreme concurrency (>1000 workers) the
   single-writer stream becomes a bottleneck. Stream pooling (e.g. 4 streams per
   peer) is a straightforward extension.
5. **No QUIC connection migration or 0-RTT.** quic-go supports both; they are just
   not wired in the prototype yet.

## Relationship to other work

- **#127 (AMQP) and #129 (MQTT)** add two broker-based buses. The mesh bus is a
  third bus implementation that could coexist alongside them, selectable via
  configuration (like NATS vs Redis today). All three share the `MessageBus`
  interface and the conformance suite.
- **The `ClosableMessageBus` interface** introduced in #127/#129 applies equally:
  the mesh bus owns its QUIC connections and memberlist instance, so it must be
  closed.
- **#130 (gzip compression)** is adopted — the mesh bus passes compression opts
  through and uses the same `serialize(msg, "", c)` call as every other bus.

## Questions for maintainers

1. **Does brokerless align with LiveKit's deployment model?** LiveKit already
   supports NATS and Redis — adding a mesh option keeps the choice. The main
   question is whether a gossip-based design works at the cluster sizes LiveKit
   sees in production (typically <50 nodes).
2. **Is the memberlist dependency acceptable?** It's the same library used by
   Consul, Nomad, and Serf. It pulls in a few transitive deps (dns, seed) but adds
   zero CGo or platform-specific code.
3. **Does the psrpc claim mechanism map cleanly enough to queue topics?** The
   claim path already exists in `pkg/client` and `pkg/server`. If the mapping is
   straightforward, the remaining work is ~200 lines of bus logic.
4. **Should this be a separate issue + PR or folded into the existing bus PRs?**
   My preference is to land #127 and #129 first (they're reviewed, CI green, CLA
   signed), then iterate on the mesh bus as a follow-up so each PR stays
   reviewable.

## Appendix: running the prototype

```go
// Seed node
seed, _ := bus.NewMeshMessageBus("0.0.0.0:0", nil)
defer seed.Close()

// Peer node
peer, _ := bus.NewMeshMessageBus("0.0.0.0:0", []string{seed.LocalAddr()})
defer peer.Close()

// Use as a regular psrpc bus
server.NewRPCServer(svcDef, seed)
client.NewRPCClient(svcDef, peer)
```

Dependencies: `go get github.com/hashicorp/memberlist github.com/quic-go/quic-go`