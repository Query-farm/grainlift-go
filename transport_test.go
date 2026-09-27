// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"errors"
	"fmt"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func requireNetwork(t *testing.T) {
	t.Helper()
	if _, ok := any(&vgirpc.Server{}).(networkServer); !ok {
		if os.Getenv("GRAINLIFT_REQUIRE_NETWORK") == "1" {
			t.Fatal("VGI network safety release required")
		}
		t.Skip("published VGI transport has no safe raw entrypoint")
	}
}
func rawFixture(t *testing.T) (*Service, *StreamServer, *vgirpc.TcpClient) {
	requireNetwork(t)
	t.Helper()
	conn := &featureConnection{st: &featureStatement{options: map[string]OptionValue{}}, options: map[string]OptionValue{}}
	svc, e := NewService(map[string]Target{"default": {Backend: featureBackend{conn}, Authorize: func(p string) bool { return p == "local-test" }}}, DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server, e := svc.ServeStreams(listener, StreamOptions{Mode: "tcp", LocalPrincipal: "local-test", IdleTimeout: time.Second, MaxConnections: 2})
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().(*net.TCPAddr)
	client, e := vgirpc.NewTcpClient(context.Background(), address.IP.String(), address.Port, vgirpc.WithTcpClientProtocol(ProtocolName), vgirpc.WithTcpClientProtocolVersion(ProtocolVersion))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := server.Close(ctx); e != nil {
			t.Error(e)
		}
		if e := svc.Close(); e != nil {
			t.Error(e)
		}
	})
	return svc, server, client
}
func rawCall[R any](t *testing.T, c *vgirpc.TcpClient, name string, p any) R {
	t.Helper()
	b := testInput(p)
	defer b.Release()
	reply, e := c.CallUnary(context.Background(), name, b, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reply.Release()
	value, e := decodeRecord[R](reply.Batch.Column(0).(*array.Binary).Value(0))
	if e != nil {
		t.Fatal(e)
	}
	return value
}
func TestTCPPersistentUnaryPullAndCancel(t *testing.T) {
	svc, _, c := rawFixture(t)
	sid := rawCall[SessionResponse](t, c, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	st := rawCall[StatementResponse](t, c, "new_statement", sessionParams{sid})
	result := rawCall[ExecuteResponse](t, c, "execute", statementParams{sid, st.StatementID})
	params := testInput(readParams{sid, result.ResultID, 0})
	defer params.Release()
	stream, e := c.OpenProducer(context.Background(), "read_result", params, vgirpc.ClientStreamSchema{Output: testSchema()})
	if e != nil {
		t.Fatal(e)
	}
	batch, ok, e := stream.Next(context.Background())
	if e != nil || !ok {
		t.Fatalf("pull: %v", e)
	}
	if batch.Batch.NumRows() != 3 {
		t.Fatal("batch mismatch")
	}
	batch.Release()
	if e := stream.Cancel(context.Background()); e != nil {
		t.Fatal(e)
	}
	rawCall[OkResponse](t, c, "close_connection", sessionParams{sid})
	if svc.ResourceCounts()["sessions"] != 0 {
		t.Fatal("session cleanup")
	}
}
func TestRawDisconnectClosesActiveCursor(t *testing.T) {
	svc, _, c := rawFixture(t)
	sid := rawCall[SessionResponse](t, c, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	st := rawCall[StatementResponse](t, c, "new_statement", sessionParams{sid})
	result := rawCall[ExecuteResponse](t, c, "execute", statementParams{sid, st.StatementID})
	params := testInput(readParams{sid, result.ResultID, 0})
	defer params.Release()
	stream, e := c.OpenProducer(context.Background(), "read_result", params, vgirpc.ClientStreamSchema{Output: testSchema()})
	if e != nil {
		t.Fatal(e)
	}
	batch, ok, e := stream.Next(context.Background())
	if e != nil || !ok {
		t.Fatal(e)
	}
	batch.Release()
	stream.Abort()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ss, u, e := svc.acquire(context.Background(), &vgirpc.CallContext{Auth: &vgirpc.AuthContext{Authenticated: true, Domain: "local", Principal: "local-test"}}, sid)
		if e != nil {
			t.Fatal(e)
		}
		remaining := len(ss.results)
		u()
		if remaining == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("disconnected result retained")
}
func TestStreamTransportAdmissionValidation(t *testing.T) {
	svc, _, _, _ := setupFeature(t)
	listener, e := net.Listen("tcp", "0.0.0.0:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	for _, options := range []StreamOptions{{Mode: "tcp", LocalPrincipal: "test"}, {Mode: "mtls"}, {Mode: "iroh-bridge", AuthorizeIroh: func(string) bool { return true }}, {Mode: "unknown"}} {
		if server, e := svc.ServeStreams(listener, options); e == nil {
			_ = server.Close(context.Background())
			t.Fatal("unsafe listener accepted")
		}
	}
}
func TestIrohPrivateSocketAndMissingIdentity(t *testing.T) {
	requireNetwork(t)
	svc, _, _, _ := setupFeature(t)
	directory := t.TempDir()
	if e := os.Chmod(directory, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(directory, "worker.sock")
	listener, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	server, e := svc.ServeStreams(listener, StreamOptions{Mode: "iroh-bridge", AuthorizeIroh: func(string) bool { return true }, HandshakeTimeout: 20 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close(context.Background())
	conn, e := net.Dial("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, e := conn.Read(b[:]); e == nil {
		t.Fatal("bridge accepted absent identity")
	}
}
func TestRawServerShutdownInterruptsIdlePeer(t *testing.T) {
	_, server, c := rawFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := server.Close(ctx); e != nil {
		t.Fatal(e)
	}
	params := testInput(OpenConnectionRequest{Target: "default"})
	defer params.Release()
	reply, e := c.CallUnary(ctx, "open_connection", params, nil)
	if reply != nil {
		reply.Release()
	}
	if e == nil {
		t.Fatal("closed server accepted request")
	}
}
func TestRawMalformedNamedRecordReturnsStructuredError(t *testing.T) {
	_, _, client := rawFixture(t)
	record := testRecord(OpenConnectionRequest{Target: "default"})
	defer record.Release()
	empty := record.NewSlice(0, 0)
	defer empty.Release()
	params := testRecord(struct {
		Request []byte `vgirpc:"request"`
	}{testIPC(empty)})
	defer params.Release()
	reply, e := client.CallUnary(context.Background(), "open_connection", params, nil)
	if reply != nil {
		reply.Release()
	}
	var remote *vgirpc.RpcError
	if !errors.As(e, &remote) {
		t.Fatalf("expected structured protocol rejection, got %T: %v", e, e)
	}
}
func TestHTTPMalformedNamedRecordReturnsError(t *testing.T) {
	_, client, _, _ := setupFeature(t)
	record := testRecord(OpenConnectionRequest{Target: "default"})
	defer record.Release()
	empty := record.NewSlice(0, 0)
	defer empty.Release()
	params := testRecord(struct {
		Request []byte `vgirpc:"request"`
	}{testIPC(empty)})
	defer params.Release()
	reply, e := client.CallUnary(context.Background(), "open_connection", params, nil)
	if reply != nil {
		reply.Release()
	}
	if e == nil {
		t.Fatal("zero-row named request accepted")
	}
}
func TestPublishedTransportFailsClosed(t *testing.T) {
	if _, ok := any(&vgirpc.Server{}).(networkServer); ok {
		t.Skip("upstream safe entrypoint present")
	}
	svc, _, _, _ := setupFeature(t)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	if _, e := svc.ServeStreams(listener, StreamOptions{Mode: "tcp", LocalPrincipal: "test"}); e == nil {
		t.Fatal("unsafe published raw entrypoint accepted")
	}
}
func TestRawByteBudgetBoundariesAndReset(t *testing.T) {
	for _, n := range []int{3, 4, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			raw := &rawConnection{Conn: left, remaining: 4, max: 4, idle: time.Second}
			go func() { _, _ = right.Write(make([]byte, n)) }()
			got, e := io.ReadFull(raw, make([]byte, n))
			if n <= 4 && (e != nil || got != n) {
				t.Fatal("valid boundary rejected")
			}
			if n > 4 && (e == nil || got != 4) {
				t.Fatal("limit not enforced")
			}
		})
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := &rawConnection{Conn: left, remaining: 4, max: 4, idle: time.Second}
	go func() { _, _ = right.Write(make([]byte, 8)) }()
	if _, e := io.ReadFull(raw, make([]byte, 4)); e != nil {
		t.Fatal(e)
	}
	raw.reset()
	if _, e := io.ReadFull(raw, make([]byte, 4)); e != nil {
		t.Fatal("next RPC budget did not reset", e)
	}
}
func TestRawIdleReadDeadline(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := &rawConnection{Conn: left, remaining: 4, max: 4, idle: 10 * time.Millisecond}
	_, e := raw.Read(make([]byte, 1))
	timeout, ok := e.(net.Error)
	if !ok || !timeout.Timeout() {
		t.Fatal("idle timeout not enforced")
	}
}
func TestRawPanicContainmentDoesNotAppendOrMislabel(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			raw := &rawConnection{Conn: left, idle: 20 * time.Millisecond}
			if partial {
				raw.written = 1
				raw.rejection = failure("invalid_arguments", "Rejected request")
			}
			go func() { defer left.Close(); defer raw.recoverRequest(); panic("unrelated runtime fault") }()
			_ = right.SetReadDeadline(time.Now().Add(time.Second))
			var data [1]byte
			if n, e := right.Read(data[:]); n != 0 || e != io.EOF {
				t.Fatalf("panic appended an error frame: %d %v", n, e)
			}
		})
	}
}
func TestRawSocketAdmissionBoundary(t *testing.T) {
	_, server, _ := rawFixture(t)
	// rawFixture holds one connection; the configured maximum is two.
	second, e := net.Dial("tcp", server.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	deadline := time.Now().Add(time.Second)
	for len(server.slots) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(server.slots) != 2 {
		t.Fatal("at-limit connection not admitted")
	}
	third, e := net.Dial("tcp", server.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer third.Close()
	_ = third.SetReadDeadline(time.Now().Add(time.Second))
	var data [1]byte
	if _, e := third.Read(data[:]); e != io.EOF {
		t.Fatalf("above-limit peer was not closed: %v", e)
	}
}
