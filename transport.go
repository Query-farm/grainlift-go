// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"
)

// StreamOptions selects an authenticated raw VGI transport. No credentials are
// accepted from application request metadata.
type StreamOptions struct {
	// Mode is tcp, mtls, or iroh-bridge. TCP is loopback-only.
	Mode string
	// LocalPrincipal is the explicit trusted-local identity for plain TCP.
	LocalPrincipal string
	// TLSConfig must supply server certificates and a client CA pool.
	TLSConfig *tls.Config
	// AuthenticateTLS receives a verified TLS chain and returns an authorized
	// stable principal. When nil, the verified leaf SHA-256 fingerprint is used.
	AuthenticateTLS func(tls.ConnectionState) (string, error)
	// AuthorizeIroh must allow the authenticated EndpointId asserted by the
	// explicitly trusted bridge over a private Unix socket.
	AuthorizeIroh                 func(string) bool
	HandshakeTimeout, IdleTimeout time.Duration
	MaxConnections                int
}

// StreamServer owns the listener and active sockets. Close interrupts idle
// reads and handshakes; Service.Close separately closes ADBC handles.
type StreamServer struct {
	service  *Service
	listener net.Listener
	options  StreamOptions
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	peers    map[net.Conn]struct{}
	done     chan struct{}
	once     sync.Once
	slots    chan struct{}
}
type rawConnectionKey struct{}
type networkServer interface {
	ServeNetworkWithContext(context.Context, io.Reader, io.Writer)
}
type rawConnection struct {
	net.Conn
	mu                      sync.Mutex
	remaining, max, written int64
	rejection               error
	idle                    time.Duration
	auth                    *vgirpc.AuthContext
	service                 *Service
}

func (c *rawConnection) Read(p []byte) (int, error) {
	c.mu.Lock()
	remaining := c.remaining
	c.mu.Unlock()
	if remaining <= 0 {
		return 0, errors.New("raw request limit")
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	_ = c.Conn.SetReadDeadline(time.Now().Add(c.idle))
	n, e := c.Conn.Read(p)
	c.mu.Lock()
	c.remaining -= int64(n)
	c.mu.Unlock()
	return n, e
}
func (c *rawConnection) Write(p []byte) (int, error) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(c.idle))
	n, e := c.Conn.Write(p)
	c.mu.Lock()
	c.written += int64(n)
	c.mu.Unlock()
	return n, e
}
func (c *rawConnection) begin()         { c.mu.Lock(); c.written = 0; c.rejection = nil; c.mu.Unlock() }
func (c *rawConnection) reject(e error) { c.mu.Lock(); c.rejection = e; c.mu.Unlock() }
func (c *rawConnection) recoverRequest() {
	if recover() == nil {
		return
	}
	c.mu.Lock()
	e, written := c.rejection, c.written
	c.mu.Unlock()
	if e != nil && written == 0 {
		_ = vgirpc.WriteErrorResponse(c, arrow.NewSchema(nil, nil), e, "", "")
	}
}
func (c *rawConnection) reset() { c.mu.Lock(); c.remaining = c.max; c.mu.Unlock() }

// ServeStreams starts bounded raw VGI handling on an already-bound listener.
// Plain TCP must bind a numeric loopback address. Iroh ingress requires a
// Unix socket in a non-symlink directory owned by the service with mode0700;
// that filesystem boundary designates the bridge and same-UID processes as
// trusted. Do not expose this upstream socket through an untrusted proxy.
func (s *Service) ServeStreams(listener net.Listener, options StreamOptions) (*StreamServer, error) {
	if listener == nil {
		return nil, failure("invalid_arguments", "Missing listener")
	}
	if _, ok := any(s.rpc).(networkServer); !ok {
		return nil, failure("not_implemented", "Raw transports require the VGI network safety release")
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = 5 * time.Second
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = 30 * time.Second
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = s.limits.Sessions * 4
	}
	if options.HandshakeTimeout <= 0 || options.IdleTimeout <= 0 || options.MaxConnections <= 0 {
		return nil, failure("invalid_arguments", "Invalid stream limits")
	}
	switch options.Mode {
	case "tcp":
		a, ok := listener.Addr().(*net.TCPAddr)
		if !ok || !a.IP.IsLoopback() || !validKey(options.LocalPrincipal) {
			return nil, failure("invalid_arguments", "Plain TCP requires loopback and an explicit local principal")
		}
	case "mtls":
		if options.TLSConfig == nil || len(options.TLSConfig.Certificates) == 0 || options.TLSConfig.ClientCAs == nil {
			return nil, failure("invalid_arguments", "mTLS requires a certificate and client trust roots")
		}
		config := options.TLSConfig.Clone()
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.InsecureSkipVerify = false
		if config.MinVersion < tls.VersionTLS12 {
			config.MinVersion = tls.VersionTLS12
		}
		options.TLSConfig = config
	case "iroh-bridge":
		a, ok := listener.Addr().(*net.UnixAddr)
		if !ok || options.AuthorizeIroh == nil {
			return nil, failure("invalid_arguments", "Iroh requires a private Unix bridge socket and peer authorization")
		}
		if !ownedPrivateDirectory(filepath.Dir(a.Name)) {
			return nil, failure("invalid_arguments", "Iroh bridge directory must be owned by this user with mode0700")
		}
	default:
		return nil, failure("invalid_arguments", "Unknown stream transport")
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &StreamServer{service: s, listener: listener, options: options, ctx: ctx, cancel: cancel, peers: map[net.Conn]struct{}{}, done: make(chan struct{}), slots: make(chan struct{}, options.MaxConnections)}
	go server.serve()
	return server, nil
}
func (s *StreamServer) Addr() net.Addr { return s.listener.Addr() }
func (s *StreamServer) serve() {
	defer close(s.done)
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		conn, e := s.listener.Accept()
		if e != nil {
			return
		}
		select {
		case s.slots <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			_ = conn.Close()
			<-s.slots
			return
		}
		s.peers[conn] = struct{}{}
		s.mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				_ = conn.Close()
				s.mu.Lock()
				delete(s.peers, conn)
				s.mu.Unlock()
				<-s.slots
				_ = recover()
			}()
			s.handle(conn)
		}()
	}
}
func (s *StreamServer) handle(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	auth := &vgirpc.AuthContext{Authenticated: true}
	switch s.options.Mode {
	case "tcp":
		remote, ok := conn.RemoteAddr().(*net.TCPAddr)
		if !ok || !remote.IP.IsLoopback() {
			return
		}
		auth.Domain = "local"
		auth.Principal = s.options.LocalPrincipal
	case "mtls":
		secured := tls.Server(conn, s.options.TLSConfig)
		ctx, cancel := context.WithTimeout(s.ctx, s.options.HandshakeTimeout)
		e := secured.HandshakeContext(ctx)
		cancel()
		if e != nil {
			return
		}
		state := secured.ConnectionState()
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return
		}
		principal := ""
		if s.options.AuthenticateTLS != nil {
			principal, e = s.options.AuthenticateTLS(state)
			if e != nil {
				return
			}
		} else {
			digest := sha256.Sum256(state.PeerCertificates[0].Raw)
			principal = hex.EncodeToString(digest[:])
		}
		if !validKey(principal) {
			return
		}
		auth.Domain = "mtls"
		auth.Principal = principal
		conn = secured
	case "iroh-bridge":
		_ = conn.SetReadDeadline(time.Now().Add(s.options.HandshakeTimeout))
		forwarded, e := vgirpc.ReadProxyProtocolV2WithOptions(conn, 512, vgirpc.ProxyProtocolV2Options{AllowIrohIdentity: true})
		if e != nil || !forwarded.HasIrohEndpointID {
			return
		}
		principal := hex.EncodeToString(forwarded.IrohEndpointID[:])
		if !s.options.AuthorizeIroh(principal) {
			return
		}
		auth.Domain = "iroh"
		auth.Principal = principal
	}
	ctx, e := vgirpc.WithConnectionIdentity(s.ctx, auth, nil)
	if e != nil {
		return
	}
	raw := &rawConnection{Conn: conn, remaining: int64(s.service.limits.RequestBytes + s.service.limits.BindBytes), max: int64(s.service.limits.RequestBytes + s.service.limits.BindBytes), idle: s.options.IdleTimeout, auth: auth, service: s.service}
	ctx = context.WithValue(ctx, rawConnectionKey{}, raw)
	defer raw.recoverRequest()
	any(s.service.rpc).(networkServer).ServeNetworkWithContext(ctx, raw, raw)
}

// Close is idempotent and bounds its wait by the caller's deadline.
func (s *StreamServer) Close(ctx context.Context) error {
	s.once.Do(func() {
		s.cancel()
		_ = s.listener.Close()
		s.mu.Lock()
		for conn := range s.peers {
			_ = conn.Close()
		}
		s.mu.Unlock()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ io.ReadWriter = (*rawConnection)(nil)

func serviceFor(ctx context.Context, call *vgirpc.CallContext) *Service {
	if s, ok := call.Implementation.(*Service); ok {
		return s
	}
	if raw, ok := ctx.Value(rawConnectionKey{}).(*rawConnection); ok {
		return raw.service
	}
	panic("Missing Grainlift service context")
}
