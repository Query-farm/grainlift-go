// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"reflect"
	"sync"
	"sync/atomic"
)

// ResultProducer is serializable result state that produces one batch per
// call. It is an alternative to an array.RecordReader: implement it on a
// pointer to a struct whose exported fields hold everything needed to produce
// the rest of the result, register the type with RegisterResultProducer, and
// return it with NewProducerResult.
//
// Over HTTP the service gob-encodes the producer into the sealed continuation
// token after every batch, so no reader, cursor or replay batch is retained in
// server memory between fetches, and a retried fetch re-produces its batch
// from the token's state. Other transports carry the same encoded state in
// memory. Keep sockets, files and database cursors out of the state; results
// that need them should use a reader instead. The encoded state is bounded by
// Limits.ProducerStateBytes.
//
// Produce returns the next batch, which the caller owns and releases, and
// advances the state, or returns a nil batch at the end of the result. Every
// batch must match the result schema.
type ResultProducer interface {
	Produce(ctx context.Context) (arrow.RecordBatch, error)
}

var producerTypes sync.Map

// RegisterResultProducer registers a concrete producer type, usually a
// pointer such as &RunningTotal{}, so its state can be encoded and restored.
// Call it from an init function. Only registered types are ever decoded.
func RegisterResultProducer(example ResultProducer) {
	if example == nil {
		panic("grainlift: RegisterResultProducer requires a concrete producer")
	}
	vgirpc.RegisterStateType(example)
	producerTypes.Store(reflect.TypeOf(example), struct{}{})
}

func registeredProducer(p ResultProducer) bool {
	if p == nil {
		return false
	}
	_, ok := producerTypes.Load(reflect.TypeOf(p))
	return ok
}

// EncodeResultProducer serializes a registered producer, including its type
// name, as Service does between batches.
func EncodeResultProducer(p ResultProducer) ([]byte, error) {
	if !registeredProducer(p) {
		return nil, failure("invalid_data", "Unregistered result producer")
	}
	var b bytes.Buffer
	if gob.NewEncoder(&b).Encode(&p) != nil {
		return nil, failure("invalid_data", "Result producer state is not serializable")
	}
	return b.Bytes(), nil
}

// DecodeResultProducer restores a producer serialized by EncodeResultProducer.
// Unknown or unregistered types fail with ADBC INVALID_DATA.
func DecodeResultProducer(data []byte) (p ResultProducer, err error) {
	unknown := failure("invalid_data", "Unknown result producer")
	defer func() {
		if recover() != nil {
			p, err = nil, unknown
		}
	}()
	if gob.NewDecoder(bytes.NewReader(data)).Decode(&p) != nil || !registeredProducer(p) {
		return nil, unknown
	}
	return p, nil
}

// NewProducerResult builds a result whose state is carried by a serializable
// producer. The result owns producer. Its Reader drives the producer in memory
// until exhausted, which is convenient for tests; Service instead resumes the
// encoded state from continuation tokens.
func NewProducerResult(schema *arrow.Schema, producer ResultProducer) *QueryResult {
	r := &producerReader{schema: schema, producer: producer}
	r.refs.Store(1)
	return &QueryResult{Reader: r, Producer: producer}
}

type producerReader struct {
	refs     atomic.Int64
	schema   *arrow.Schema
	producer ResultProducer
	batch    arrow.RecordBatch
	err      error
	done     bool
}

func (r *producerReader) Retain() { r.refs.Add(1) }
func (r *producerReader) Release() {
	if r.refs.Add(-1) == 0 && r.batch != nil {
		r.batch.Release()
		r.batch = nil
	}
}
func (r *producerReader) Schema() *arrow.Schema          { return r.schema }
func (r *producerReader) Err() error                     { return r.err }
func (r *producerReader) RecordBatch() arrow.RecordBatch { return r.batch }

// Record implements the deprecated array.RecordReader method.
func (r *producerReader) Record() arrow.RecordBatch { return r.batch }
func (r *producerReader) Next() bool {
	if r.batch != nil {
		r.batch.Release()
		r.batch = nil
	}
	if r.done {
		return false
	}
	b, e := r.producer.Produce(context.Background())
	if e != nil || b == nil {
		r.err, r.done = e, true
		return false
	}
	r.batch = b
	return true
}

var _ array.RecordReader = (*producerReader)(nil)

func (s *Service) encodeProducer(p ResultProducer) ([]byte, error) {
	data, e := EncodeResultProducer(p)
	if e != nil {
		return nil, e
	}
	if len(data) > s.limits.ProducerStateBytes {
		return nil, failure("invalid_data", "Result producer state exceeds configured limit")
	}
	return data, nil
}

// nextProduced resumes the cursor's encoded producer, returns one batch or nil
// at the end of the result, and advances the cursor. Fetching the previous
// sequence re-produces that batch from the token's state.
func (s *Service) nextProduced(ctx context.Context, ss *session, c *ResultCursor) (arrow.RecordBatch, error) {
	r := ss.results[c.ResultID]
	if r == nil || r.producer == nil {
		return nil, failure("not_found", "Unknown result")
	}
	if c.Sequence != r.sequence && c.Sequence != r.sequence-1 {
		return nil, failure("invalid_arguments", "Invalid result sequence")
	}
	if r.done && c.Sequence == r.sequence {
		return nil, nil
	}
	fail := func(e error) (arrow.RecordBatch, error) { s.closeResult(ss, c.ResultID); return nil, e }
	p, e := DecodeResultProducer(c.Producer)
	if e != nil {
		return fail(e)
	}
	b, e := p.Produce(ctx)
	if e != nil {
		if b != nil {
			b.Release()
		}
		return fail(backendError(e))
	}
	if b == nil {
		r.done = true
		return nil, nil
	}
	if !b.Schema().Equal(r.schema) || recordBytes(b) > int64(s.limits.BatchBytes) {
		b.Release()
		return fail(failure("invalid_data", "Invalid or oversized result batch"))
	}
	state, e := s.encodeProducer(p)
	if e != nil {
		b.Release()
		return fail(e)
	}
	r.sequence = max(r.sequence, c.Sequence+1)
	c.Producer = state
	c.Sequence++
	return b, nil
}
