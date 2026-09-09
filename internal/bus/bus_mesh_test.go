package bus

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/psrpc/internal"
	"github.com/livekit/psrpc/pkg/rand"
)

func TestMeshLocalRoundTrip(t *testing.T) {
	// Single-node mesh: publish and subscribe on the same bus.
	b, err := NewMeshMessageBus("127.0.0.1:0", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })

	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|egress|REQ"}
	ctx := context.Background()

	sub, err := Subscribe[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100) // allow gossip to settle

	require.NoError(t, b.Publish(ctx, channel, &internal.Request{RequestId: "local"}))

	select {
	case m := <-sub.Channel():
		require.NotNil(t, m)
		require.Equal(t, "local", m.RequestId)
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery on local mesh bus")
	}

	require.NoError(t, sub.Close())
}

func TestMeshTwoNodeRoundTrip(t *testing.T) {
	// Two-node mesh: node A subscribes, node B publishes.
	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|egress|REQ"}
	ctx := context.Background()

	seed, err := NewMeshMessageBus("127.0.0.1:0", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = seed.Close() })

	// Subscribe BEFORE the peer joins so the topic is in NodeMeta when
	// the peer receives the seed's state during the join handshake.
	sub, err := Subscribe[*internal.Request](ctx, seed, channel, DefaultChannelSize)
	require.NoError(t, err)

	// node B joins node A's cluster.
	seedAddr := seed.list.LocalNode().Address()
	peer, err := NewMeshMessageBus("127.0.0.1:0", []string{seedAddr})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	// Poll until the peer sees the seed's subscription. The seed
	// broadcasts its subscription set via SendReliable on every new
	// subscription, and the peer's NotifyMsg decodes it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		peer.mu.Lock()
		_, ok := peer.remoteSubs[seed.selfAddr][channel.Legacy]
		peer.mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer never received seed's subscription (remoteSubs=%v)", peer.remoteSubs)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// Publish from the peer node.
	require.NoError(t, peer.Publish(ctx, channel, &internal.Request{RequestId: "cross"}))

	select {
	case m := <-sub.Channel():
		require.NotNil(t, m)
		require.Equal(t, "cross", m.RequestId)
	case <-time.After(5 * time.Second):
		t.Fatal("no cross-node delivery on mesh bus")
	}

	require.NoError(t, sub.Close())
}

func TestMeshSubscribeClose(t *testing.T) {
	b, err := NewMeshMessageBus("127.0.0.1:0", nil)
	require.NoError(t, err)
	defer b.Close()

	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|REQ"}
	ctx := context.Background()

	sub, err := Subscribe[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	require.NoError(t, sub.Close())
	// Closing a second time must be safe.
	require.NoError(t, sub.Close())
}

func TestMeshQueueSubscribe(t *testing.T) {
	b, err := NewMeshMessageBus("127.0.0.1:0", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })

	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|REQ"}
	ctx := context.Background()

	sub1, err := SubscribeQueue[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	sub2, err := SubscribeQueue[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100)

	require.NoError(t, b.Publish(ctx, channel, &internal.Request{RequestId: "q1"}))
	require.NoError(t, b.Publish(ctx, channel, &internal.Request{RequestId: "q2"}))

	got := map[string]int{}
	timer := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case m := <-sub1.Channel():
			got[m.RequestId]++
		case m := <-sub2.Channel():
			got[m.RequestId]++
		case <-timer:
			t.Fatalf("only got %d/2 queue deliveries: %v", len(got), got)
		}
	}
	require.Equal(t, map[string]int{"q1": 1, "q2": 1}, got)

	require.NoError(t, sub1.Close())
	require.NoError(t, sub2.Close())
}