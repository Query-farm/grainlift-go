// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"reflect"
	"testing"
)

// counter produces Count rows, three per batch, from exported resumable state.
type counter struct {
	Count, Position int64
	Padding         []byte
}

func init() { RegisterResultProducer(&counter{}) }

func (c *counter) Produce(context.Context) (arrow.RecordBatch, error) {
	if c.Position >= c.Count {
		return nil, nil
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for ; c.Position < c.Count && b.Len() < 3; c.Position++ {
		b.Append(c.Position)
	}
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(testSchema(), []arrow.Array{a}, int64(a.Len())), nil
}

// unregistered is a valid producer that was never registered.
type unregistered struct{ counter }

type producerStatement struct {
	UnimplementedStatement
	producer ResultProducer
}

func (s *producerStatement) Execute(context.Context) (*QueryResult, error) {
	return NewProducerResult(testSchema(), s.producer), nil
}

func values(t *testing.T, b arrow.RecordBatch) []int64 {
	t.Helper()
	if b == nil {
		return nil
	}
	defer b.Release()
	return append([]int64{}, b.Column(0).(*array.Int64).Int64Values()...)
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func status(e error) string {
	var adbc *Error
	if errors.As(e, &adbc) {
		return adbc.Status
	}
	return ""
}

func producerResult(t *testing.T, s *Service, ss *session, p ResultProducer) *ResultCursor {
	t.Helper()
	reply, e := s.addResult(ss, "", NewProducerResult(testSchema(), p))
	if e != nil {
		t.Fatal(e)
	}
	r := ss.results[reply.ResultID]
	if r.producer == nil || r.query.Reader != nil || r.last != nil {
		t.Fatal("producer result retained a reader")
	}
	return &ResultCursor{"session", reply.ResultID, 0, r.producer}
}

func TestProducerRoundTripAndInMemoryReader(t *testing.T) {
	p := &counter{Count: 7}
	if b := values(t, must(p.Produce(context.Background()))); !equal(b, []int64{0, 1, 2}) {
		t.Fatal(b)
	}
	data, e := EncodeResultProducer(p)
	if e != nil {
		t.Fatal(e)
	}
	restored, e := DecodeResultProducer(data)
	if e != nil || !reflect.DeepEqual(restored, ResultProducer(p)) {
		t.Fatalf("round trip: %v", e)
	}
	q := NewProducerResult(testSchema(), &counter{Count: 7})
	defer q.Reader.Release()
	var got []int64
	for q.Reader.Next() {
		got = append(got, q.Reader.RecordBatch().Column(0).(*array.Int64).Int64Values()...)
	}
	if q.Reader.Err() != nil || !equal(got, []int64{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatal(got)
	}
	if _, e := EncodeResultProducer(&unregistered{}); status(e) != "invalid_data" {
		t.Fatal("unregistered producer encoded")
	}
	for _, bad := range [][]byte{nil, []byte("garbage"), forgedTypeName(t)} {
		if _, e := DecodeResultProducer(bad); status(e) != "invalid_data" {
			t.Fatalf("decoded unknown producer %q", bad)
		}
	}
}

func must(b arrow.RecordBatch, e error) arrow.RecordBatch {
	if e != nil {
		panic(e)
	}
	return b
}

// forgedTypeName returns encoded state naming a type that was never registered.
func forgedTypeName(t *testing.T) []byte {
	t.Helper()
	p, e := EncodeResultProducer(&counter{})
	if e != nil {
		t.Fatal(e)
	}
	// Corrupt the registered type name while keeping the gob framing intact.
	for i := 0; i+7 <= len(p); i++ {
		if string(p[i:i+7]) == "counter" {
			copy(p[i:], "counteR")
			return p
		}
	}
	t.Fatal("type name not found")
	return nil
}

func TestProducerSequenceReplayAndExhaustion(t *testing.T) {
	s, _, _, p := setupFeature(t)
	ss := s.sessions[p.SessionID]
	c := producerResult(t, s, ss, &counter{Count: 7})
	initial := *c
	ctx := context.Background()
	first := values(t, must(s.nextProduced(ctx, ss, c)))
	afterFirst := *c
	second := values(t, must(s.nextProduced(ctx, ss, c)))
	if !equal(first, []int64{0, 1, 2}) || !equal(second, []int64{3, 4, 5}) || c.Sequence != 2 {
		t.Fatal(first, second)
	}
	// A retried fetch re-produces the previous batch from its token's state.
	replay := afterFirst
	if again := values(t, must(s.nextProduced(ctx, ss, &replay))); !equal(again, second) || replay.Sequence != 2 {
		t.Fatal("replay", again)
	}
	// Older tokens are rejected without closing the result.
	stale := initial
	if _, e := s.nextProduced(ctx, ss, &stale); status(e) != "invalid_arguments" {
		t.Fatalf("stale sequence: %v", e)
	}
	if last := values(t, must(s.nextProduced(ctx, ss, c))); !equal(last, []int64{6}) {
		t.Fatal(last)
	}
	for i := 0; i < 2; i++ {
		if b, e := s.nextProduced(ctx, ss, c); b != nil || e != nil {
			t.Fatal("exhausted result produced data")
		}
	}
	if len(ss.results) != 1 {
		t.Fatal("exhausted result released before close")
	}
}

func TestProducerFailuresCloseResult(t *testing.T) {
	s, _, _, p := setupFeature(t)
	ss := s.sessions[p.SessionID]
	ctx := context.Background()
	c := producerResult(t, s, ss, &counter{Count: 7})
	c.Producer = []byte("forged")
	if _, e := s.nextProduced(ctx, ss, c); status(e) != "invalid_data" || len(ss.results) != 0 {
		t.Fatalf("unknown producer: %v", e)
	}
	s.limits.ProducerStateBytes = 256
	c = producerResult(t, s, ss, &counter{Count: 7, Padding: make([]byte, 64)})
	c.Producer, _ = EncodeResultProducer(&counter{Count: 7, Padding: make([]byte, 512)})
	if _, e := s.nextProduced(ctx, ss, c); status(e) != "invalid_data" || len(ss.results) != 0 {
		t.Fatalf("oversized state: %v", e)
	}
	if _, e := s.addResult(ss, "", NewProducerResult(testSchema(), &counter{Padding: make([]byte, 512)})); status(e) != "invalid_data" {
		t.Fatalf("oversized initial state: %v", e)
	}
	if _, e := s.addResult(ss, "", NewProducerResult(testSchema(), &unregistered{})); status(e) != "invalid_data" {
		t.Fatalf("unregistered initial state: %v", e)
	}
}

func TestProducerOverHTTPContinuations(t *testing.T) {
	svc, client, _, p := setupFeature(t)
	ss := svc.sessions[p.SessionID]
	ss.statements[p.StatementID].backend = &producerStatement{producer: &counter{Count: 2500}}
	reply := callTest[ExecuteResponse](t, client, "execute", statementParams{p.SessionID, p.StatementID})
	for _, sequence := range []int64{1, -1} {
		params := testInput(readParams{p.SessionID, reply.ResultID, sequence})
		_, e := client.OpenProducer(context.Background(), "read_result", params, vgirpc.ClientStreamSchema{Output: testSchema()})
		params.Release()
		if e == nil {
			t.Fatalf("producer result opened at sequence %d", sequence)
		}
	}
	params := testInput(readParams{p.SessionID, reply.ResultID, 0})
	defer params.Release()
	stream, e := client.OpenProducer(context.Background(), "read_result", params, vgirpc.ClientStreamSchema{Output: testSchema()})
	if e != nil {
		t.Fatal(e)
	}
	var next int64
	for {
		batch, ok, e := stream.Next(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for _, v := range batch.Batch.Column(0).(*array.Int64).Int64Values() {
			if v != next {
				t.Fatalf("row %d is %d", next, v)
			}
			next++
		}
		batch.Release()
	}
	if next != 2500 {
		t.Fatalf("read %d rows", next)
	}
	r := ss.results[reply.ResultID]
	if r == nil || r.query.Reader != nil || r.last != nil || !r.done {
		t.Fatal("producer result retained server-side batches")
	}
	callTest[OkResponse](t, client, "close_result", resultParams{p.SessionID, reply.ResultID})
}

func TestProducerOverRawTCPInMemory(t *testing.T) {
	svc, _, c := rawFixture(t)
	sid := rawCall[SessionResponse](t, c, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	st := rawCall[StatementResponse](t, c, "new_statement", sessionParams{sid})
	svc.sessions[sid].statements[st.StatementID].backend = &producerStatement{producer: &counter{Count: 10}}
	result := rawCall[ExecuteResponse](t, c, "execute", statementParams{sid, st.StatementID})
	params := testInput(readParams{sid, result.ResultID, 0})
	defer params.Release()
	stream, e := c.OpenProducer(context.Background(), "read_result", params, vgirpc.ClientStreamSchema{Output: testSchema()})
	if e != nil {
		t.Fatal(e)
	}
	var rows []int64
	for {
		batch, ok, e := stream.Next(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		rows = append(rows, batch.Batch.Column(0).(*array.Int64).Int64Values()...)
		batch.Release()
	}
	if e := stream.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !equal(rows, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatal(rows)
	}
	rawCall[OkResponse](t, c, "close_connection", sessionParams{sid})
}
