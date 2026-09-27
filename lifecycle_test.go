// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testCall(s *Service) *vgirpc.CallContext {
	return &vgirpc.CallContext{Auth: &vgirpc.AuthContext{Authenticated: true, Principal: "test", Domain: "test"}, Implementation: s}
}

type blockingStatement struct {
	UnimplementedStatement
	started, stop chan struct{}
	once          sync.Once
}

func (b *blockingStatement) Execute(context.Context) (*QueryResult, error) {
	close(b.started)
	<-b.stop
	return nil, failure("cancelled", "Cancelled")
}
func (b *blockingStatement) Cancel(context.Context) error {
	b.once.Do(func() { close(b.stop) })
	return nil
}
func TestCancelBypassesBusySession(t *testing.T) {
	s, _, _, p := setupFeature(t)
	ss := s.sessions[p.SessionID]
	block := &blockingStatement{started: make(chan struct{}), stop: make(chan struct{})}
	ss.statements[p.StatementID].backend = block
	done := make(chan error, 1)
	go func() {
		_, e := runStatement(s, context.Background(), testCall(s), p.SessionID, p.StatementID, func(_ *session, st *statement) (*QueryResult, error) { return st.backend.Execute(context.Background()) })
		done <- e
	}()
	<-block.started
	other := testCall(s)
	other.Auth.Principal = "other"
	if _, e := s.cancel(context.Background(), other, p.SessionID, p.StatementID); e == nil {
		t.Fatal("cross-owner cancellation")
	}
	if _, e := s.cancel(context.Background(), testCall(s), p.SessionID, p.StatementID); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel waited behind execute")
	}
}

type delayedBackend struct {
	entered, proceed chan struct{}
	conn             *delayedConnection
}
type delayedConnection struct {
	UnimplementedConnection
	closed atomic.Bool
}

func (c *delayedConnection) Close() error { c.closed.Store(true); return nil }
func (b delayedBackend) Open(context.Context, string, OpenConnectionRequest) (Connection, error) {
	close(b.entered)
	<-b.proceed
	return b.conn, nil
}
func TestShutdownReapsLateOpen(t *testing.T) {
	b := delayedBackend{make(chan struct{}), make(chan struct{}), &delayedConnection{}}
	limits := DefaultLimits()
	limits.LockTimeout = 10 * time.Millisecond
	s, e := NewService(map[string]Target{"default": {Backend: b, Authorize: func(string) bool { return true }}}, limits)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() {
		_, _ = s.open(context.Background(), testCall(s), OpenConnectionRequest{Target: "default"})
		close(done)
	}()
	<-b.entered
	if e := s.Close(); e == nil {
		t.Fatal("expected shutdown timeout")
	}
	close(b.proceed)
	<-done
	if !b.conn.closed.Load() || s.ResourceCounts()["sessions"] != 0 {
		t.Fatal("late connection leaked")
	}
	if _, _, e := s.acquire(context.Background(), testCall(s), "missing"); e == nil {
		t.Fatal("closed service accepted access")
	}
}
func TestIdleReaping(t *testing.T) {
	conn := &delayedConnection{}
	b := immediateBackend{conn}
	limits := DefaultLimits()
	limits.IdleTimeout = 5 * time.Millisecond
	s, e := NewService(map[string]Target{"default": {Backend: b, Authorize: func(string) bool { return true }}}, limits)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e := s.open(context.Background(), testCall(s), OpenConnectionRequest{Target: "default"}); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !conn.closed.Load() {
		time.Sleep(time.Millisecond)
	}
	if !conn.closed.Load() {
		t.Fatal("idle resource leaked")
	}
}

type immediateBackend struct{ conn Connection }

func (b immediateBackend) Open(context.Context, string, OpenConnectionRequest) (Connection, error) {
	return b.conn, nil
}
func TestSessionQuotaBoundary(t *testing.T) {
	for _, count := range []int{1, 2, 3} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			limits := DefaultLimits()
			limits.Sessions = 2
			s, e := NewService(map[string]Target{"default": {Backend: immediateBackend{&UnimplementedConnection{}}, Authorize: func(string) bool { return true }}}, limits)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			var err error
			for i := 0; i < count; i++ {
				_, err = s.open(context.Background(), testCall(s), OpenConnectionRequest{Target: "default"})
			}
			if (count > 2) != (err != nil) {
				t.Fatalf("count %d err %v", count, err)
			}
		})
	}
}
func TestResultQuotaAndInvalidRowsReleaseReader(t *testing.T) {
	s, _, _, p := setupFeature(t)
	ss := s.sessions[p.SessionID]
	s.limits.Results = 2
	for i := 0; i < 3; i++ {
		_, e := s.addResult(ss, "", testQuery())
		if (i == 2) != (e != nil) {
			t.Fatalf("result boundary %d", i)
		}
	}
	bad := int64(-2)
	q := testQuery()
	q.RowsAffected = &bad
	if _, e := s.addResult(ss, "", q); e == nil {
		t.Fatal("negative affected rows")
	}
}
func TestRetainedBufferAccounting(t *testing.T) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < 1024; i++ {
		b.Append(int64(i))
	}
	a := b.NewArray()
	b.Release()
	defer a.Release()
	slice := array.NewSlice(a, 0, 1)
	defer slice.Release()
	batch := array.NewRecordBatch(testSchema(), []arrow.Array{slice}, 1)
	defer batch.Release()
	if recordBytes(batch) < 8192 {
		t.Fatal("slice hid retained backing allocation")
	}
}
func TestRequestDefaultsAndMetadataValidation(t *testing.T) {
	for _, r := range []any{GetInfoRequest{Codes: ptr([]int64{-1})}, GetInfoRequest{Codes: ptr([]int64{1 << 32})}, GetObjectsRequest{Depth: 4}, GetStatisticsRequest{Catalog: ptr("bad\x00name")}} {
		if validateRequest(r) == nil {
			t.Fatal("invalid metadata accepted")
		}
	}
	for _, r := range []any{GetInfoRequest{Codes: nil}, GetInfoRequest{Codes: ptr([]int64{})}, GetObjectsRequest{Depth: 0, TableTypes: ptr([]string{})}} {
		if e := validateRequest(r); e != nil {
			t.Fatal(e)
		}
	}
}
func ptr[T any](v T) *T { return &v }
