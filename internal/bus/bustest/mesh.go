package bustest

import (
	"testing"

	"github.com/ory/dockertest/v4"

	"github.com/livekit/psrpc/internal/bus"
)

func init() {
	RegisterServer("Mesh", NewMesh)
}

// NewMesh creates a single-node mesh bus for the conformance suite.
// The bus operates without any remote peers; local delivery is tested.
func NewMesh(t testing.TB, _ dockertest.Pool) Server {
	return &meshServer{}
}

type meshServer struct{}

func (s *meshServer) Connect(t testing.TB, opts ...bus.BusOption) bus.MessageBus {
	b, err := bus.NewMeshMessageBus("127.0.0.1:0", nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = b.Close()
	})
	return b
}