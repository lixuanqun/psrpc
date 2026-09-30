package bus

import (
	"context"
	"fmt"
	"sync"
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

// meshCluster is a set of joined mesh buses with helpers to wait for
// subscription propagation.
type meshCluster []*meshMessageBus

func newMeshCluster(t *testing.T, n int) meshCluster {
	t.Helper()
	nodes := make(meshCluster, 0, n)
	for range n {
		var seeds []string
		if len(nodes) > 0 {
			seeds = []string{nodes[0].LocalAddr()}
		}
		b, err := NewMeshMessageBus("127.0.0.1:0", seeds)
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		nodes = append(nodes, b)
	}
	return nodes
}

// waitQueueView blocks until node's remoteQ lists every other node in the
// cluster as a queue subscriber of topic.
func (c meshCluster) waitQueueView(t *testing.T, node *meshMessageBus, topic string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		node.mu.Lock()
		complete := true
		for _, other := range c {
			if other == node {
				continue
			}
			if _, ok := node.remoteQ[other.selfAddr][topic]; !ok {
				complete = false
				break
			}
		}
		node.mu.Unlock()
		if complete {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s never saw peers' queue subscription for %q", node.selfAddr, topic)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// queueCollector tallies deliveries on one subscription until it is closed.
type queueCollector struct {
	mu  sync.Mutex
	got map[string]int
}

func collectQueue(sub Subscription[*internal.Request]) *queueCollector {
	c := &queueCollector{got: map[string]int{}}
	go func() {
		for m := range sub.Channel() {
			c.mu.Lock()
			c.got[m.RequestId]++
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *queueCollector) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.got {
		total += n
	}
	return total
}

func (c *queueCollector) byID() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.got))
	for k, v := range c.got {
		out[k] = v
	}
	return out
}

// TestMeshCrossNodeQueue verifies cluster-wide exactly-once queue delivery:
// every node subscribes to every topic, every node publishes, and each
// message is dispatched exactly once in total.
func TestMeshCrossNodeQueue(t *testing.T) {
	const numTopics = 12 // stay within the NodeMeta/broadcast size caps

	c := newMeshCluster(t, 3)
	ctx := context.Background()

	channels := make([]Channel, numTopics)
	for i := range channels {
		channels[i] = Channel{Legacy: "livekit|" + rand.NewString() + "|REQ"}
	}
	subs := make([][]Subscription[*internal.Request], len(c))
	for i, node := range c {
		for _, ch := range channels {
			sub, err := SubscribeQueue[*internal.Request](ctx, node, ch, DefaultChannelSize)
			require.NoError(t, err)
			subs[i] = append(subs[i], sub)
		}
	}
	for _, node := range c {
		for _, ch := range channels {
			c.waitQueueView(t, node, ch.Legacy)
		}
	}

	collectors := make([][]*queueCollector, len(c))
	for i, nodeSubs := range subs {
		for _, sub := range nodeSubs {
			collectors[i] = append(collectors[i], collectQueue(sub))
		}
	}

	// Publish each topic once, rotating the publishing node.
	for i, ch := range channels {
		require.NoError(t, c[i%len(c)].Publish(ctx, ch, &internal.Request{RequestId: fmt.Sprintf("msg-%d", i)}))
	}

	deadline := time.After(5 * time.Second)
	for {
		total := 0
		for _, nodeCollectors := range collectors {
			for _, col := range nodeCollectors {
				total += col.total()
			}
		}
		if total == numTopics {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("got %d/%d queue deliveries", total, numTopics)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Exactly once per message, cluster-wide.
	all := map[string]int{}
	for _, nodeCollectors := range collectors {
		for _, col := range nodeCollectors {
			for id, n := range col.byID() {
				all[id] += n
			}
		}
	}
	require.Len(t, all, numTopics)
	for id, n := range all {
		require.Equal(t, 1, n, "message %s delivered more than once", id)
	}

	// Distribution: with 12 topics hashed over 3 candidate nodes, every
	// node should own at least one topic.
	for i, nodeCollectors := range collectors {
		nodeTotal := 0
		for _, col := range nodeCollectors {
			nodeTotal += col.total()
		}
		require.Greater(t, nodeTotal, 0, "node %d received no queue messages", i)
	}

	for _, nodeSubs := range subs {
		for _, sub := range nodeSubs {
			require.NoError(t, sub.Close())
		}
	}
}

// TestMeshQueueOwnerExclusive verifies that when the queue owner is a
// remote node, the publishing node does not dispatch locally: delivery
// happens on the owner only.
func TestMeshQueueOwnerExclusive(t *testing.T) {
	c := newMeshCluster(t, 2)
	a, b := c[0], c[1]
	ctx := context.Background()

	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|REQ"}
	subA, err := SubscribeQueue[*internal.Request](ctx, a, channel, DefaultChannelSize)
	require.NoError(t, err)
	subB, err := SubscribeQueue[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	c.waitQueueView(t, a, channel.Legacy)
	c.waitQueueView(t, b, channel.Legacy)

	// Determine the expected owner from A's converged view.
	a.mu.Lock()
	owner := meshQueueOwner(channel.Legacy, a.selfAddr, true, a.remoteQ)
	a.mu.Unlock()
	require.NotEmpty(t, owner)

	const numMsgs = 5
	for i := range numMsgs {
		require.NoError(t, a.Publish(ctx, channel, &internal.Request{RequestId: fmt.Sprintf("ex-%d", i)}))
	}

	colA := collectQueue(subA)
	colB := collectQueue(subB)

	var ownerCol, otherCol *queueCollector
	if owner == a.selfAddr {
		ownerCol, otherCol = colA, colB
	} else {
		ownerCol, otherCol = colB, colA
	}

	deadline := time.Now().Add(5 * time.Second)
	for ownerCol.total() < numMsgs && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, numMsgs, ownerCol.total(), "owner %s should receive all messages", owner)
	require.Equal(t, 0, otherCol.total(), "non-owner must not receive queue deliveries")

	require.NoError(t, subA.Close())
	require.NoError(t, subB.Close())
}

// TestMeshQueueLateJoin verifies queue delivery stays exactly-once across a
// node joining the cluster and subscribing to an existing queue topic.
func TestMeshQueueLateJoin(t *testing.T) {
	c := newMeshCluster(t, 2)
	a, b := c[0], c[1]
	ctx := context.Background()

	channel := Channel{Legacy: "livekit|" + rand.NewString() + "|REQ"}
	subA, err := SubscribeQueue[*internal.Request](ctx, a, channel, DefaultChannelSize)
	require.NoError(t, err)
	subB, err := SubscribeQueue[*internal.Request](ctx, b, channel, DefaultChannelSize)
	require.NoError(t, err)
	c.waitQueueView(t, a, channel.Legacy)
	c.waitQueueView(t, b, channel.Legacy)

	colA := collectQueue(subA)
	colB := collectQueue(subB)

	require.NoError(t, a.Publish(ctx, channel, &internal.Request{RequestId: "before-join"}))
	deadline := time.Now().Add(5 * time.Second)
	for colA.total()+colB.total() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, 1, colA.total()+colB.total())

	// A third node joins and subscribes to the same queue topic.
	nodeC, err := NewMeshMessageBus("127.0.0.1:0", []string{a.LocalAddr()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = nodeC.Close() })
	subC, err := SubscribeQueue[*internal.Request](ctx, nodeC, channel, DefaultChannelSize)
	require.NoError(t, err)
	colC := collectQueue(subC)

	// Wait until A's view includes C as a queue subscriber, so A's
	// owner election accounts for it.
	c = append(c, nodeC)
	c.waitQueueView(t, a, channel.Legacy)

	for i := range 2 {
		require.NoError(t, a.Publish(ctx, channel, &internal.Request{RequestId: fmt.Sprintf("after-join-%d", i)}))
	}
	deadline = time.Now().Add(5 * time.Second)
	for colA.total()+colB.total()+colC.total() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	all := map[string]int{}
	for _, col := range []*queueCollector{colA, colB, colC} {
		for id, n := range col.byID() {
			all[id] += n
		}
	}
	require.Len(t, all, 3)
	for id, n := range all {
		require.Equal(t, 1, n, "message %s delivered more than once across nodes", id)
	}

	require.NoError(t, subA.Close())
	require.NoError(t, subB.Close())
	require.NoError(t, subC.Close())
}