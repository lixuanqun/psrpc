// Package bench measures real psrpc round-trip performance for the mesh
// and local buses. It mirrors the same methodology as the cross-bus
// benchmarks in issue #128: sequential latency (500 samples) and sustained
// throughput at w=1,8,32,128 concurrency (8s each), using real
// psrpc RPC (request → claim → handler → response).
//
// Run: go test ./cmd/meshbench/ -run TestBench -v -count=1
package bench

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/psrpc/internal"
	"github.com/livekit/psrpc/internal/bus"
	"github.com/livekit/psrpc/pkg/client"
	"github.com/livekit/psrpc/pkg/info"
	"github.com/livekit/psrpc/pkg/rand"
	"github.com/livekit/psrpc/pkg/server"
)

const benchDuration = 8 * time.Second

func TestBenchMeshVsLocal(t *testing.T) {
	// Two separate mesh nodes (like two processes over the bus).
	seed, err := bus.NewMeshMessageBus("127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()

	peer, err := bus.NewMeshMessageBus("127.0.0.1:0", []string{seed.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	// Give gossip + subscription broadcast a moment to settle.
	time.Sleep(time.Second)

	runBench(t, "mesh", peer, seed)

	// Local bus baseline.
	local := bus.NewLocalMessageBus()
	runBench(t, "local", local, local)
}

func runBench(t *testing.T, label string, cliBus, srvBus bus.MessageBus) {
	serviceName := "bench_" + rand.NewString()
	rpc := "echo"

	srv := server.NewRPCServer(&info.ServiceDefinition{Name: serviceName, ID: rand.NewString()}, srvBus)
	c, err := client.NewRPCClient(&info.ServiceDefinition{Name: serviceName, ID: rand.NewString()}, cliBus)
	if err != nil {
		t.Fatalf("%s: NewRPCClient: %v", label, err)
	}
	defer srv.Close(true)

	srv.RegisterMethod(rpc, false, false, true, false)
	c.RegisterMethod(rpc, false, false, true, false)
	if err := server.RegisterHandler[*internal.Request, *internal.Response](
		srv, rpc, nil,
		func(ctx context.Context, req *internal.Request) (*internal.Response, error) {
			return &internal.Response{RequestId: req.RequestId}, nil
		}, nil,
	); err != nil {
		t.Fatalf("%s: RegisterHandler: %v", label, err)
	}
	// Allow subscriptions to propagate (mesh needs gossip + reliable broadcast).
	time.Sleep(time.Second)

	ctx := context.Background()

	// Warmup.
	for i := 0; i < 20; i++ {
		if _, err := client.RequestSingle[*internal.Response](
			ctx, c, rpc, nil, &internal.Request{RequestId: rand.NewRequestID()},
		); err != nil {
			t.Fatalf("%s: warmup failed: %v", label, err)
		}
	}

	// Sequential latency. 
	seqN := 500
	seqLats := make([]time.Duration, 0, seqN)
	for i := 0; i < seqN; i++ {
		start := time.Now()
		_, err := client.RequestSingle[*internal.Response](
			ctx, c, rpc, nil, &internal.Request{RequestId: rand.NewRequestID()},
		)
		if err != nil {
			t.Fatalf("%s: seq request %d failed: %v", label, i, err)
		}
		seqLats = append(seqLats, time.Since(start))
	}
	p50, p95, p99, mx := percentiles(seqLats)
	fmt.Fprintf(os.Stderr, "[%s] seq n=%d p50=%v p95=%v p99=%v max=%v\n", label, seqN, p50, p95, p99, mx)

	// Sustained throughput at each concurrency level.
	for _, w := range []int{1, 8, 32, 128} {
		var count, errs atomic.Int64
		var mu sync.Mutex
		var lats []time.Duration
		deadline := time.Now().Add(benchDuration)
		var wg sync.WaitGroup
		for i := 0; i < w; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var local []time.Duration
				for time.Now().Before(deadline) {
					start := time.Now()
					_, err := client.RequestSingle[*internal.Response](
						ctx, c, rpc, nil, &internal.Request{RequestId: rand.NewRequestID()},
					)
					if err != nil {
						errs.Add(1)
						continue
					}
					local = append(local, time.Since(start))
					count.Add(1)
				}
				mu.Lock()
				lats = append(lats, local...)
				mu.Unlock()
			}()
		}
		wg.Wait()
		p50, p95, p99, mx := percentiles(lats)
		qps := float64(count.Load()) / benchDuration.Seconds()
		fmt.Fprintf(os.Stderr, "[%s] w=%-4d n=%d qps=%d err=%d p50=%v p95=%v p99=%v max=%v\n",
			label, w, count.Load(), int(qps), errs.Load(), p50, p95, p99, mx)
	}
}

func percentiles(ds []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(ds) == 0 {
		return
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	p := func(q float64) time.Duration { return s[int(float64(len(s)-1)*q)] }
	return p(0.50), p(0.95), p(0.99), s[len(s)-1]
}