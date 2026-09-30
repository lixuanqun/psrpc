// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bus provides the brokerless QUIC mesh message bus.
//
// The mesh bus replaces a central broker with direct peer-to-peer QUIC
// connections, using HashiCorp memberlist (SWIM gossip) for peer discovery
// and subscription propagation. This eliminates the broker hop that accounts
// for ~6x of the throughput gap between the local bus (~94k rps) and the
// fastest network bus (NATS, ~15.6k rps) in cross-bus benchmarks.
//
// Architecture:
//   - Every node joins a gossip cluster and announces its QUIC address and
//     the set of topics it has local subscribers for.
//   - Publish forwards to every peer that has remote subscribers for the
//     topic, plus local delivery. The wire frame carries the topic name so
//     the receiver can route to the correct local subscriber list.
//   - Queue topics use round-robin among local subscribers only; cross-node
//     queue semantics are not yet implemented (claim-based coordination on
//     the roadmap).
//   - QUIC connections are established lazily per peer pair; each pair
//     shares one persistent stream, with length-prefixed frames multiplexed
//     on it.
//
// Known gaps (prototype):
//   - No graceful leave / subscription drain,
//   - Queue topics are local-only (no cross-node competing consumers),
//   - NodeMeta caps subscription sets at 512 bytes,
//   - Self-signed TLS with InsecureSkipVerify (suitable for loopback testing).
package bus

import (
	"context"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

const (
	meshGossipInterval   = 200 * time.Millisecond
	meshPushPullInterval = 30 * time.Second
	meshQUICKeepAlive    = 15 * time.Second
	meshDialTimeout      = 3 * time.Second

	// meshMaxFrameLen caps the length-prefixed frame size on the wire so a
	// corrupted or hostile length cannot trigger an unbounded allocation.
	meshMaxFrameLen = 16 << 20
)

var errMeshClosed = errors.New("psrpc: mesh message bus closed")

// meshMessageBus is a brokerless, peer-to-peer MessageBus over QUIC with
// gossip-based peer discovery.
type meshMessageBus struct {
	selfAddr string // QUIC advertise address

	list      *memberlist.Memberlist
	eventCh   chan memberlist.NodeEvent
	ln        *quic.Listener

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	peers       map[string]*meshPeer          // addr → peer
	subs        map[string]*meshSubList       // local broadcast subscribers
	queues      map[string]*meshSubList       // local queue subscribers
	remoteSubs  map[string]map[string]struct{} // peer addr → {topic: struct{}}
	remoteQ     map[string]map[string]struct{} // peer addr → {topic: struct{}}
	closed      bool

	c       *compressor
	maxSize int
}

// NewMeshMessageBus creates a mesh bus that joins the given gossip cluster.
// bindAddr is the UDP address this node listens on for QUIC (e.g. "0.0.0.0:0").
// seedPeers is an initial list of cluster members to join (at least one
// existing member, or empty if this is the first node).
func NewMeshMessageBus(bindAddr string, seedPeers []string, opts ...BusOption) (*meshMessageBus, error) {
	o := getBusOpts(opts...)
	ctx, cancel := context.WithCancel(context.Background())

	// QUIC listener.
	udpAddr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("psrpc: mesh resolve bind addr: %w", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("psrpc: mesh listen: %w", err)
	}
	tlsConf := meshTLSConfig()
	quicLn, err := quic.Listen(udpConn, tlsConf, &quic.Config{
		MaxIdleTimeout:  meshQUICKeepAlive * 3,
		KeepAlivePeriod: meshQUICKeepAlive,
	})
	if err != nil {
		_ = udpConn.Close()
		cancel()
		return nil, fmt.Errorf("psrpc: mesh quic listen: %w", err)
	}
	selfAddr := quicLn.Addr().String()

	eventCh := make(chan memberlist.NodeEvent, 64)
	b := &meshMessageBus{
		selfAddr:    selfAddr,
		eventCh:     eventCh,
		ln:          quicLn,
		ctx:         ctx,
		cancel:      cancel,
		peers:       map[string]*meshPeer{},
		subs:        map[string]*meshSubList{},
		queues:      map[string]*meshSubList{},
		remoteSubs:  map[string]map[string]struct{}{},
		remoteQ:     map[string]map[string]struct{}{},
		c:           newCompressor(o.Compression),
		maxSize:     o.Compression.MaxDecompressedSize,
	}

	// memberlist on a separate UDP port from QUIC.
	mlConf := memberlist.DefaultLANConfig()
	mlConf.Name = selfAddr
	mlConf.BindAddr = udpAddr.IP.String()
	mlConf.BindPort = 0 // ephemeral
	mlConf.AdvertiseAddr = udpAddr.IP.String()
	mlConf.GossipInterval = meshGossipInterval
	mlConf.PushPullInterval = meshPushPullInterval
	mlConf.Delegate = b
	mlConf.Events = &memberlist.ChannelEventDelegate{Ch: eventCh}

	list, err := memberlist.Create(mlConf)
	if err != nil {
		_ = quicLn.Close()
		_ = udpConn.Close()
		cancel()
		return nil, fmt.Errorf("psrpc: mesh create memberlist: %w", err)
	}
	b.list = list

	go b.acceptLoop()
	go b.gossipEventLoop()

	if len(seedPeers) > 0 {
		if _, err := list.Join(seedPeers); err != nil {
			_ = list.Shutdown()
			_ = quicLn.Close()
			_ = udpConn.Close()
			cancel()
			return nil, fmt.Errorf("psrpc: mesh join cluster: %w", err)
		}
	}

	return b, nil
}

func (b *meshMessageBus) maxDecompressedSize() int {
	return b.maxSize
}

// LocalAddr returns the memberlist address of this node, for use as a
// seed peer by other nodes joining the cluster.
func (b *meshMessageBus) LocalAddr() string {
	return b.list.LocalNode().Address()
}

// ---- memberlist.Delegate ----

func (b *meshMessageBus) NodeMeta(limit int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	var buf []byte
	// Encoding: per topic: 1 byte flags (0=broadcast, 1=queue) + 2 bytes len + topic
	put := func(topic string, flags byte) bool {
		n := 3 + len(topic)
		if len(buf)+n > limit {
			return false
		}
		buf = append(buf, flags)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(topic)))
		buf = append(buf, topic...)
		return true
	}
	for t := range b.subs {
		put(t, 0)
	}
	for t := range b.queues {
		put(t, 1)
	}
	return buf
}

func (b *meshMessageBus) NotifyMsg(buf []byte) {
	// Full subscription sync from a peer. Encoding: 2 bytes peer addr
	// length + peer addr + 1 byte flags (0=broadcast, 1=queue) +
	// 2 bytes topic length + topic (repeated).
	if len(buf) < 2 {
		return
	}
	addrLen := int(binary.BigEndian.Uint16(buf[0:]))
	if len(buf) < 2+addrLen {
		return
	}
	addr := string(buf[2 : 2+addrLen])

	subs := make(map[string]struct{})
	queues := make(map[string]struct{})
	off := 2 + addrLen
	for off+3 <= len(buf) {
		flags := buf[off]
		off++
		tLen := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+tLen > len(buf) {
			break
		}
		topic := string(buf[off : off+tLen])
		off += tLen
		if flags == 0 {
			subs[topic] = struct{}{}
		} else {
			queues[topic] = struct{}{}
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.remoteSubs[addr] = subs
	b.remoteQ[addr] = queues
}

func (b *meshMessageBus) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (b *meshMessageBus) LocalState(join bool) []byte {
	return b.NodeMeta(512)
}

func (b *meshMessageBus) MergeRemoteState([]byte, bool) {}

// ---- gossip events ----

func (b *meshMessageBus) gossipEventLoop() {
	for {
		select {
		case evt := <-b.eventCh:
			switch evt.Event {
			case memberlist.NodeJoin:
				// Push our subscription set to the new node so it
				// can publish to our topics without waiting for the
				// next push-pull cycle. The broadcast was already
				// sent during subscribe, but the peer may not have
				// been in the cluster yet.
				b.broadcastSubs()
				b.updateRemote(evt.Node.Name, evt.Node.Meta)
			case memberlist.NodeLeave:
				b.removePeerLocked(evt.Node.Name)
			}
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *meshMessageBus) updateRemote(addr string, meta []byte) {
	if addr == b.selfAddr {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	subs := make(map[string]struct{})
	queues := make(map[string]struct{})
	off := 0
	for off+3 <= len(meta) {
		flags := meta[off]
		off++
		tLen := int(binary.BigEndian.Uint16(meta[off:]))
		off += 2
		if off+tLen > len(meta) {
			break
		}
		topic := string(meta[off : off+tLen])
		off += tLen
		if flags == 0 {
			subs[topic] = struct{}{}
		} else {
			queues[topic] = struct{}{}
		}
	}
	b.remoteSubs[addr] = subs
	b.remoteQ[addr] = queues
}

func (b *meshMessageBus) removePeerLocked(addr string) {
	if p, ok := b.peers[addr]; ok {
		_ = p.close()
	}
	delete(b.peers, addr)
	delete(b.remoteSubs, addr)
	delete(b.remoteQ, addr)
}

// ---- QUIC accept ----

func (b *meshMessageBus) acceptLoop() {
	for {
		conn, err := b.ln.Accept(b.ctx)
		if err != nil {
			return
		}
		go b.handleIncoming(conn)
	}
}

func (b *meshMessageBus) handleIncoming(conn *quic.Conn) {
	defer conn.CloseWithError(0, "done")
	for {
		stream, err := conn.AcceptStream(b.ctx)
		if err != nil {
			return
		}
		go b.readFrames(stream)
	}
}

// ---- dial ----

func (b *meshMessageBus) getPeer(addr string) (*meshPeer, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errMeshClosed
	}
	p, ok := b.peers[addr]
	if ok && p.isLive() {
		b.mu.Unlock()
		return p, nil
	}
	if !ok {
		p = &meshPeer{addr: addr}
		b.peers[addr] = p
	}
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(b.ctx, meshDialTimeout)
	defer cancel()
	quicConn, err := quic.DialAddr(ctx, addr, meshTLSConfig(), &quic.Config{
		MaxIdleTimeout:  meshQUICKeepAlive * 3,
		KeepAlivePeriod: meshQUICKeepAlive,
	})
	if err != nil {
		return nil, err
	}
	p.setConn(quicConn)
	return p, nil
}

// ---- MessageBus interface ----

func (b *meshMessageBus) Publish(_ context.Context, channel Channel, msg proto.Message) error {
	payload, err := serialize(msg, "", b.c)
	if err != nil {
		return err
	}

	topic := channel.Legacy

	b.mu.Lock()
	closed := b.closed
	var targets []string
	for addr, topics := range b.remoteSubs {
		if _, ok := topics[topic]; ok {
			targets = append(targets, addr)
		}
	}
	subList := b.subs[topic]
	queueList := b.queues[topic]
	b.mu.Unlock()

	if closed {
		return errMeshClosed
	}

	// Local delivery.
	if subList != nil {
		subList.dispatch(payload)
	}
	if queueList != nil {
		queueList.dispatchQueue(payload)
	}

	// Remote delivery.
	if len(targets) == 0 {
		return nil
	}
	// Wire framing: 2 bytes topic length + topic + 4 bytes payload length + payload.
	frame := make([]byte, 2+len(topic)+4+len(payload))
	binary.BigEndian.PutUint16(frame[0:], uint16(len(topic)))
	copy(frame[2:], topic)
	binary.BigEndian.PutUint32(frame[2+len(topic):], uint32(len(payload)))
	copy(frame[2+len(topic)+4:], payload)

	for _, addr := range targets {
		p, err := b.getPeer(addr)
		if err != nil {
			continue
		}
		_ = p.send(frame)
	}
	return nil
}

func (b *meshMessageBus) Subscribe(ctx context.Context, channel Channel, size int) (Reader, error) {
	return b.subscribe(ctx, channel.Legacy, false, size)
}

func (b *meshMessageBus) SubscribeQueue(ctx context.Context, channel Channel, size int) (Reader, error) {
	return b.subscribe(ctx, channel.Legacy, true, size)
}

func (b *meshMessageBus) subscribe(ctx context.Context, topic string, queue bool, size int) (Reader, error) {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &meshSubscription{
		ctx:    subCtx,
		cancel: cancel,
		topic:  topic,
		queue:  queue,
		ch:     make(chan []byte, size),
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		cancel()
		return nil, errMeshClosed
	}
	lists := b.subs
	if queue {
		lists = b.queues
	}
	list := lists[topic]
	if list == nil {
		list = &meshSubList{}
		lists[topic] = list
	}
	list.add(sub)
	b.mu.Unlock()

	// Propagate the subscription set to all peers immediately via
	// memberlist's reliable broadcast, so they can publish to this
	// topic without waiting for the next push-pull cycle.
	b.broadcastSubs()

	return sub, nil
}

// broadcastSubs encodes the local subscription set and sends it to all
// peers via memberlist's reliable broadcast. The encoding carries the
// sender's address so receivers can map the subscription set to the
// correct peer.
func (b *meshMessageBus) broadcastSubs() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.list == nil {
		return
	}

	// Only broadcast if there are other nodes in the cluster.
	members := b.list.Members()
	if len(members) <= 1 {
		return
	}

	// Encode: 2 bytes addr len + addr + [1 byte flags + 2 bytes topic len + topic]*
	addr := b.selfAddr
	n := 2 + len(addr)
	for t := range b.subs {
		n += 3 + len(t)
	}
	for t := range b.queues {
		n += 3 + len(t)
	}
	// Cap at memberlist's message size limit.
	if n > 1400 {
		return
	}
	buf := make([]byte, 0, n)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(addr)))
	buf = append(buf, addr...)
	for t := range b.subs {
		buf = append(buf, 0) // broadcast
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(t)))
		buf = append(buf, t...)
	}
	for t := range b.queues {
		buf = append(buf, 1) // queue
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(t)))
		buf = append(buf, t...)
	}
	for _, m := range members {
		if m.Name != b.selfAddr {
			b.list.SendReliable(m, buf)
		}
	}
}

func (b *meshMessageBus) unsubscribe(topic string, queue bool, sub *meshSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	lists := b.subs
	if queue {
		lists = b.queues
	}
	list, ok := lists[topic]
	if !ok {
		return
	}
	list.remove(sub)
	if list.empty() {
		delete(lists, topic)
	}
}

func (b *meshMessageBus) Close() error {
	b.cancel()
	b.mu.Lock()
	closed := b.closed
	b.closed = true
	var subs []*meshSubscription
	for _, l := range b.subs {
		subs = append(subs, l.all()...)
	}
	for _, l := range b.queues {
		subs = append(subs, l.all()...)
	}
	b.mu.Unlock()

	if closed {
		return nil
	}
	for _, s := range subs {
		s.cancel()
	}
	for _, p := range b.peers {
		_ = p.close()
	}
	_ = b.list.Shutdown()
	_ = b.ln.Close()
	return nil
}

// readFrames reads framed messages from one QUIC stream until closed.
// Each frame: 4 bytes big-endian length + payload. The payload is itself
// a wire frame: 2 bytes topic length + topic + 4 bytes payload len + payload.
func (b *meshMessageBus) readFrames(stream *quic.Stream) {
	defer stream.Close()
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
			return
		}
		frameLen := binary.BigEndian.Uint32(lenBuf[:])
		if frameLen > meshMaxFrameLen {
			return
		}
		frame := make([]byte, frameLen)
		if _, err := io.ReadFull(stream, frame); err != nil {
			return
		}
		if len(frame) < 2 {
			return
		}
		tLen := binary.BigEndian.Uint16(frame[0:])
		if len(frame) < 2+int(tLen)+4 {
			return
		}
		topic := string(frame[2 : 2+tLen])
		off := 2 + int(tLen)
		pLen := binary.BigEndian.Uint32(frame[off:])
		off += 4
		if len(frame) < off+int(pLen) {
			return
		}
		payload := frame[off : off+int(pLen)]
		b.mu.Lock()
		subList := b.subs[topic]
		queueList := b.queues[topic]
		b.mu.Unlock()
		if subList != nil {
			subList.dispatch(payload)
		}
		if queueList != nil {
			queueList.dispatchQueue(payload)
		}
	}
}

// ---- meshPeer ----

type meshPeer struct {
	addr string

	mu    sync.Mutex
	conn  *quic.Conn
	str   *quic.Stream // persistent outgoing stream
	strMu sync.Mutex   // serialises writes to str
	closed bool
}

func (p *meshPeer) setConn(conn *quic.Conn) {
	p.strMu.Lock()
	p.mu.Lock()
	if p.conn != nil {
		_ = p.conn.CloseWithError(0, "replaced")
	}
	p.conn = conn
	p.str = nil
	p.closed = false
	p.mu.Unlock()
	p.strMu.Unlock()
}

func (p *meshPeer) isLive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.closed && p.conn != nil && p.conn.Context().Err() == nil
}

func (p *meshPeer) send(frame []byte) error {
	p.strMu.Lock()
	defer p.strMu.Unlock()

	p.mu.Lock()
	str := p.str
	conn := p.conn
	p.mu.Unlock()

	if conn == nil {
		return errors.New("no connection")
	}

	if str == nil {
		// Open a persistent outgoing stream. Frame delimiter: 4-byte
		// big-endian length prefix. The receiver reads 4 bytes, then
		// length bytes, in a loop.
		s, err := conn.OpenStream()
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.str = s
		p.mu.Unlock()
		str = s
	}

	// Frame prefix: 4 bytes length + payload, assembled and written in a
	// single Write so a partial failure cannot desynchronize the stream
	// framing for subsequent frames.
	buf := make([]byte, 4+len(frame))
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	copy(buf[4:], frame)
	if _, err := (*str).Write(buf); err != nil {
		p.mu.Lock()
		p.str = nil
		p.mu.Unlock()
		return err
	}
	return nil
}

func (p *meshPeer) close() error {
	p.strMu.Lock()
	p.mu.Lock()
	p.closed = true
	conn := p.conn
	p.conn = nil
	p.str = nil
	p.mu.Unlock()
	p.strMu.Unlock()
	if conn != nil {
		return conn.CloseWithError(0, "closing")
	}
	return nil
}

// ---- meshSubList ----

type meshSubList struct {
	mu   sync.Mutex
	subs []*meshSubscription
	next int
}

func (l *meshSubList) add(sub *meshSubscription) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.subs = append(l.subs, sub)
}

func (l *meshSubList) remove(sub *meshSubscription) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, s := range l.subs {
		if s == sub {
			l.subs = append(l.subs[:i], l.subs[i+1:]...)
			return
		}
	}
}

func (l *meshSubList) empty() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.subs) == 0
}

func (l *meshSubList) all() []*meshSubscription {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*meshSubscription, len(l.subs))
	copy(out, l.subs)
	return out
}

func (l *meshSubList) dispatch(payload []byte) {
	l.mu.Lock()
	subs := make([]*meshSubscription, len(l.subs))
	copy(subs, l.subs)
	l.mu.Unlock()
	for _, sub := range subs {
		sub.write(payload)
	}
}

func (l *meshSubList) dispatchQueue(payload []byte) {
	l.mu.Lock()
	if l.next >= len(l.subs) {
		l.next = 0
	}
	if len(l.subs) == 0 {
		l.mu.Unlock()
		return
	}
	sub := l.subs[l.next]
	l.next++
	l.mu.Unlock()
	sub.write(payload)
}

// ---- meshSubscription ----

type meshSubscription struct {
	ctx    context.Context
	cancel context.CancelFunc
	topic  string
	queue  bool
	ch     chan []byte
}

func (s *meshSubscription) write(payload []byte) {
	select {
	case s.ch <- payload:
	case <-s.ctx.Done():
	}
}

func (s *meshSubscription) read() ([]byte, bool) {
	select {
	case payload, ok := <-s.ch:
		return payload, ok
	case <-s.ctx.Done():
		return nil, false
	}
}

func (s *meshSubscription) Close() error {
	s.cancel()
	return nil
}

// ---- TLS helpers ----

var (
	meshTLSConfOnce sync.Once
	meshTLSConf     *tls.Config
)

func meshTLSConfig() *tls.Config {
	meshTLSConfOnce.Do(func() {
		meshTLSConf = &tls.Config{
			Certificates:       []tls.Certificate{meshCert()},
			InsecureSkipVerify: true, // prototype: self-signed, loopback-only
			NextProtos:         []string{"psrpc-mesh"},
		}
	})
	return meshTLSConf
}

var meshCertCache atomic.Value

func meshCert() tls.Certificate {
	if c, ok := meshCertCache.Load().(tls.Certificate); ok {
		return c
	}
	key, err := rsa.GenerateKey(crand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	certDER, err := x509.CreateCertificate(crand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	meshCertCache.Store(c)
	return c
}