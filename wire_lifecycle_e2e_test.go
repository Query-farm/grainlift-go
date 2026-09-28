// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This fixture crosses the HTTP wire with separately authenticated clients.
// Each open receives an independently owned backend connection.
type independentBackend struct{}

func (independentBackend) Open(context.Context, string, OpenConnectionRequest) (Connection, error) {
	return &featureConnection{
		st:      &featureStatement{options: map[string]OptionValue{}},
		options: map[string]OptionValue{},
	}, nil
}

func independentClients(t *testing.T, idle time.Duration) (*Service, *vgirpc.HttpClient, *vgirpc.HttpClient) {
	t.Helper()
	limits := DefaultLimits()
	limits.Sessions = 2
	limits.IdleTimeout = idle
	svc, err := NewService(map[string]Target{
		"default": {Backend: independentBackend{}, Authorize: func(p string) bool { return p == "alice" || p == "bob" }},
	}, limits)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(svc.HTTPHandler(func(r *http.Request) (*vgirpc.AuthContext, error) {
		principal := r.Header.Get("X-Test-Principal")
		if principal != "alice" && principal != "bob" {
			return nil, errors.New("unauthorized test principal")
		}
		return &vgirpc.AuthContext{Authenticated: true, Domain: "test", Principal: principal}, nil
	}))
	newClient := func(principal string) *vgirpc.HttpClient {
		client, e := vgirpc.NewHttpClient(server.URL,
			vgirpc.WithClientProtocol(ProtocolName),
			vgirpc.WithClientProtocolVersion(ProtocolVersion),
			vgirpc.WithClientHeader("X-Test-Principal", principal))
		if e != nil {
			t.Fatal(e)
		}
		return client
	}
	alice, bob := newClient("alice"), newClient("bob")
	t.Cleanup(func() {
		alice.Close()
		bob.Close()
		server.Close()
		if e := svc.Close(); e != nil {
			t.Error(e)
		}
	})
	return svc, alice, bob
}

func assertWireDenied(t *testing.T, client *vgirpc.HttpClient, method string, request any) {
	t.Helper()
	params := testInput(request)
	defer params.Release()
	reply, err := client.CallUnary(context.Background(), method, params, nil)
	if reply != nil {
		reply.Release()
	}
	if err == nil {
		t.Fatalf("%s accepted unauthorized or over-quota request", method)
	}
}

func TestWireIndependentClientOwnershipAndChurn(t *testing.T) {
	svc, alice, bob := independentClients(t, time.Minute)
	aliceID := callTest[SessionResponse](t, alice, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	defer callTest[OkResponse](t, alice, "close_connection", sessionParams{aliceID})
	for index := 0; index < 64; index++ {
		bobID := callTest[SessionResponse](t, bob, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
		svc.mu.Lock()
		owned := svc.sessions[bobID]
		svc.mu.Unlock()
		bobStatement := callTest[StatementResponse](t, bob, "new_statement", sessionParams{bobID}).StatementID
		bobResult := callTest[ExecuteResponse](t, bob, "execute", statementParams{bobID, bobStatement}).ResultID
		assertWireDenied(t, alice, "close_result", resultParams{bobID, bobResult})
		assertWireDenied(t, alice, "close_connection", sessionParams{bobID})
		assertWireDenied(t, bob, "new_statement", sessionParams{aliceID})
		assertWireDenied(t, alice, "open_connection", OpenConnectionRequest{Target: "default"})
		callTest[OkResponse](t, bob, "close_result", resultParams{bobID, bobResult})
		callTest[OkResponse](t, bob, "close_statement", statementParams{bobID, bobStatement})
		callTest[OkResponse](t, bob, "close_connection", sessionParams{bobID})
		if count := svc.ResourceCounts()["sessions"]; count != 1 {
			t.Fatalf("iteration %d: retained %d sessions", index, count)
		}
		owned.guard.RLock()
		clean := owned.closed && len(owned.statements) == 0 && len(owned.results) == 0 && owned.conn.(*featureConnection).closed
		owned.guard.RUnlock()
		if !clean {
			t.Fatalf("iteration %d: retained child handles or backend connection", index)
		}
		if index%8 == 0 {
			statement := callTest[StatementResponse](t, alice, "new_statement", sessionParams{aliceID}).StatementID
			result := callTest[ExecuteResponse](t, alice, "execute", statementParams{aliceID, statement}).ResultID
			callTest[OkResponse](t, alice, "close_result", resultParams{aliceID, result})
			callTest[OkResponse](t, alice, "close_statement", statementParams{aliceID, statement})
		}
	}
}

func TestWireAbandonedHTTPResultReleasesQuota(t *testing.T) {
	svc, alice, bob := independentClients(t, 80*time.Millisecond)
	aliceID := callTest[SessionResponse](t, alice, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	bobID := callTest[SessionResponse](t, bob, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	statement := callTest[StatementResponse](t, bob, "new_statement", sessionParams{bobID}).StatementID
	_ = callTest[ExecuteResponse](t, bob, "execute", statementParams{bobID, statement})
	svc.mu.Lock()
	abandoned := svc.sessions[bobID]
	svc.mu.Unlock()
	assertWireDenied(t, alice, "open_connection", OpenConnectionRequest{Target: "default"})
	// HTTP does not signal a lost ADBC session; idle expiry must reclaim its
	// live statement/result and quota slot while Alice remains active.
	bob.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		active := callTest[StatementResponse](t, alice, "new_statement", sessionParams{aliceID}).StatementID
		callTest[OkResponse](t, alice, "close_statement", statementParams{aliceID, active})
		if svc.ResourceCounts()["sessions"] == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count := svc.ResourceCounts()["sessions"]; count != 1 {
		t.Fatalf("abandoned session still holds quota: %d", count)
	}
	abandoned.guard.RLock()
	clean := abandoned.closed && len(abandoned.statements) == 0 && len(abandoned.results) == 0 && abandoned.conn.(*featureConnection).closed
	abandoned.guard.RUnlock()
	if !clean {
		t.Fatal("abandoned result, statement, or backend connection retained")
	}
	replacement := callTest[SessionResponse](t, alice, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	callTest[OkResponse](t, alice, "close_connection", sessionParams{replacement})
	callTest[OkResponse](t, alice, "close_connection", sessionParams{aliceID})
	if count := svc.ResourceCounts()["sessions"]; count != 0 {
		t.Fatalf("retained sessions after cleanup: %d", count)
	}
}

func TestWirePartitionCannotCrossPrincipal(t *testing.T) {
	_, alice, bob := independentClients(t, time.Minute)
	aliceID := callTest[SessionResponse](t, alice, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	bobID := callTest[SessionResponse](t, bob, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	statement := callTest[StatementResponse](t, alice, "new_statement", sessionParams{aliceID}).StatementID
	partitions := callTest[PartitionsResponse](t, alice, "execute_partitions", statementParams{aliceID, statement}).Partitions
	if len(partitions) != 1 {
		t.Fatalf("expected one sealed partition, got %d", len(partitions))
	}
	assertWireDenied(t, bob, "read_partition", partitionParams{bobID, partitions[0]})
	if result := callTest[ExecuteResponse](t, alice, "read_partition", partitionParams{aliceID, partitions[0]}); result.ResultID == "" {
		t.Fatal("owner could not read own partition")
	} else {
		callTest[OkResponse](t, alice, "close_result", resultParams{aliceID, result.ResultID})
	}
	callTest[OkResponse](t, alice, "close_connection", sessionParams{aliceID})
	callTest[OkResponse](t, bob, "close_connection", sessionParams{bobID})
}
