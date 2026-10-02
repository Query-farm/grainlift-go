// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var okSchema = arrow.NewSchema([]arrow.Field{{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}}, nil)
var bindSchema = arrow.NewSchema([]arrow.Field{{Name: "batch_ipc", Type: arrow.BinaryTypes.Binary}, {Name: "finish", Type: arrow.FixedWidthTypes.Boolean}}, nil)

// BindCursor carries only opaque handles in signed transport continuation state.
type BindCursor struct {
	SessionID, StatementID, UploadID string
	Sequence                         int64
}

func (s *Service) startBind(ctx context.Context, call *vgirpc.CallContext, p bindParams, stream bool) (*vgirpc.StreamResult, error) {
	return runStatement(s, ctx, call, p.SessionID, p.StatementID, func(ss *session, st *statement) (*vgirpc.StreamResult, error) {
		if len(p.SchemaIPC) > s.limits.BatchBytes {
			return nil, failure("invalid_arguments", "Binding schema limit exceeded")
		}
		schema, e := parseSchema(p.SchemaIPC)
		if e != nil {
			return nil, e
		}
		clearBinding(st)
		id := identifier()
		st.binding = &binding{id: id, schema: schema, stream: stream}
		return &vgirpc.StreamResult{OutputSchema: okSchema, InputSchema: bindSchema, State: &BindCursor{p.SessionID, p.StatementID, id, 0}}, nil
	})
}
func (c *BindCursor) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, call *vgirpc.CallContext) error {
	s := serviceFor(ctx, call)
	_, e := runStatement(s, ctx, call, c.SessionID, c.StatementID, func(ss *session, st *statement) (OkResponse, error) {
		b := st.binding
		if b == nil || b.id != c.UploadID {
			return ok(failure("invalid_state", "Unknown binding upload"))
		}
		fail := func(e error) (OkResponse, error) { clearBinding(st); return ok(e) }
		if !input.Schema().Equal(bindSchema) || input.NumRows() != 1 || input.Column(0).IsNull(0) || input.Column(1).IsNull(0) {
			return fail(failure("invalid_arguments", "Invalid binding frame"))
		}
		raw := input.Column(0).(*array.Binary).Value(0)
		finish := input.Column(1).(*array.Boolean).Value(0)
		digestInput := append([]byte{}, raw...)
		if finish {
			digestInput = append(digestInput, 1)
		} else {
			digestInput = append(digestInput, 0)
		}
		digest := sha256.Sum256(digestInput)
		if c.Sequence == b.sequence-1 && digest == b.lastDigest {
			c.Sequence++
			return ok(nil)
		}
		if c.Sequence != b.sequence || b.finished {
			return ok(failure("invalid_state", "Invalid binding sequence"))
		}
		if len(raw)+b.bytes > s.limits.BindBytes || len(raw) > s.bindBatchBytes() {
			return fail(failure("invalid_arguments", "Binding limit exceeded"))
		}
		if finish {
			if len(raw) != 0 {
				return fail(failure("invalid_arguments", "Finish frame must be empty"))
			}
			var e error
			if b.stream {
				reader, x := array.NewRecordReader(b.schema, b.batches)
				if x != nil {
					return fail(failure("invalid_data", "Invalid binding stream"))
				}
				e = st.backend.BindStream(ctx, reader)
				reader.Release()
			} else {
				if len(b.batches) != 1 {
					return fail(failure("invalid_arguments", "Bind requires exactly one batch"))
				}
				e = st.backend.Bind(ctx, b.batches[0])
			}
			if e != nil {
				return fail(e)
			}
			b.finished = true
		} else {
			if !bytes.HasSuffix(raw, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
				return fail(failure("invalid_data", "Binding requires complete IPC framing"))
			}
			source := bytes.NewReader(raw)
			reader, e := ipc.NewReader(source, ipc.WithAllocator(&boundedAllocator{limit: s.bindBatchBytes() * 2}))
			if e != nil {
				return fail(failure("invalid_data", "Invalid Arrow binding"))
			}
			defer reader.Release()
			if !reader.Schema().Equal(b.schema) || !reader.Next() {
				return fail(failure("invalid_data", "Binding schema or batch missing"))
			}
			batch := reader.RecordBatch()
			batch.Retain()
			if reader.Next() || reader.Err() != nil || source.Len() != 0 || recordBytes(batch) > int64(s.bindBatchBytes()) {
				batch.Release()
				return fail(failure("invalid_data", "Invalid binding batch"))
			}
			if !b.stream && len(b.batches) > 0 {
				batch.Release()
				return fail(failure("invalid_arguments", "Bind accepts one batch"))
			}
			retained := recordBytes(batch)
			if b.retained+retained > int64(s.limits.BindBytes) {
				batch.Release()
				return fail(failure("invalid_arguments", "Binding retained memory limit exceeded"))
			}
			b.bytes += len(raw)
			b.retained += retained
			b.batches = append(b.batches, batch)
		}
		b.lastDigest = digest
		b.sequence++
		c.Sequence++
		return ok(nil)
	})
	if e != nil {
		return e
	}
	return out.EmitMap(map[string][]interface{}{"ok": {true}})
}
func (c *BindCursor) OnCancel(ctx context.Context, call *vgirpc.CallContext) error {
	s := serviceFor(ctx, call)
	_, e := runStatement(s, ctx, call, c.SessionID, c.StatementID, func(ss *session, st *statement) (OkResponse, error) {
		if st.binding != nil && st.binding.id == c.UploadID && !st.binding.finished {
			clearBinding(st)
		}
		return ok(nil)
	})
	return e
}
