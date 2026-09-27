// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"reflect"
	"sync"
)

// Named records use raw, uncompressed Arrow IPC. The transport owns compression.
// This strict decoder compensates for the upstream codec accepting partial
// nested records: exact schemas, one row, required values and full consumption.
func decodeRecord[T any](raw []byte) (value T, err error) {
	defer func() {
		if recover() != nil {
			err = failure("invalid_arguments", "Invalid typed control allocation")
		}
	}()
	bad := failure("invalid_arguments", "Invalid typed control record")
	if len(raw) > 2<<20 || !bytes.HasSuffix(raw, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		return value, bad
	}
	source := bytes.NewReader(raw)
	r, e := ipc.NewReader(source, ipc.WithAllocator(&boundedAllocator{limit: 4 << 20}))
	if e != nil {
		return value, bad
	}
	defer r.Release()
	expected := any(value).(vgirpc.ArrowSerializable).ArrowSchema()
	if !r.Schema().Equal(expected) || !r.Schema().Metadata().Equal(expected.Metadata()) || !r.Next() {
		return value, bad
	}
	b := r.RecordBatch()
	if b.NumRows() != 1 {
		return value, bad
	}
	dst := reflect.ValueOf(&value).Elem()
	for i, f := range expected.Fields() {
		if e := decodeField(dst.Field(i), b.Column(i), 0, f.Nullable); e != nil {
			return value, bad
		}
	}
	if r.Next() || r.Err() != nil || source.Len() != 0 {
		return value, bad
	}
	return value, nil
}
func decodeField(dst reflect.Value, a arrow.Array, i int, nullable bool) error {
	if a.IsNull(i) {
		if !nullable {
			return failure("invalid_arguments", "Null required value")
		}
		return nil
	}
	if dst.Kind() == reflect.Pointer {
		dst.Set(reflect.New(dst.Type().Elem()))
		dst = dst.Elem()
	}
	switch x := a.(type) {
	case *array.String:
		dst.SetString(x.Value(i))
	case *array.Binary:
		dst.SetBytes(append([]byte{}, x.Value(i)...))
	case *array.Int64:
		dst.SetInt(x.Value(i))
	case *array.Float64:
		dst.SetFloat(x.Value(i))
	case *array.Boolean:
		dst.SetBool(x.Value(i))
	case *array.List:
		start, end := x.ValueOffsets(i)
		v := reflect.MakeSlice(dst.Type(), int(end-start), int(end-start))
		for j := start; j < end; j++ {
			if e := decodeField(v.Index(int(j-start)), x.ListValues(), int(j), false); e != nil {
				return e
			}
		}
		dst.Set(v)
	case *array.Struct:
		fields := x.DataType().(*arrow.StructType).Fields()
		for n, f := range fields {
			if e := decodeField(dst.Field(n), x.Field(n), i, f.Nullable); e != nil {
				return e
			}
		}
	default:
		return failure("invalid_arguments", "Invalid control type")
	}
	return nil
}
func namedUnary[P any, R any](server *vgirpc.Server, name string, handler func(context.Context, *vgirpc.CallContext, requestParams[P]) (R, error)) {
	vgirpc.Unary(server, name, func(ctx context.Context, call *vgirpc.CallContext, p P) (R, error) {
		raw, exists := ctx.Value(requestKey{}).([]byte)
		if !exists {
			var z R
			return z, failure("invalid_arguments", "Missing named control record")
		}
		v, e := decodeRecord[P](raw)
		if e != nil {
			var z R
			return z, e
		}
		return handler(ctx, call, requestParams[P]{v})
	})
}

type requestKey struct{}
type validationKey struct{}
type validationHook struct{}

func (validationHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
	if raw, ok := ctx.Value(rawConnectionKey{}).(*rawConnection); ok {
		raw.begin()
	}
	reject := func(e error) (context.Context, vgirpc.HookToken) {
		if raw, ok := ctx.Value(rawConnectionKey{}).(*rawConnection); ok {
			raw.reject(e)
		}
		return context.WithValue(ctx, validationKey{}, e), nil
	}
	if len(info.RequestData) == 0 {
		return ctx, nil
	}
	r, e := ipc.NewReader(bytes.NewReader(info.RequestData))
	if e != nil {
		return reject(failure("invalid_arguments", "Invalid request"))
	}
	defer r.Release()
	if !r.Next() || r.RecordBatch().NumRows() != 1 {
		return reject(failure("invalid_arguments", "Request must have one row"))
	}
	b := r.RecordBatch()
	for i, f := range b.Schema().Fields() {
		if !f.Nullable && b.Column(i).IsNull(0) {
			return reject(failure("invalid_arguments", "Null required request field"))
		}
	}
	if b.NumCols() == 1 && b.ColumnName(0) == "request" {
		if bin, ok := b.Column(0).(*array.Binary); ok && !bin.IsNull(0) {
			raw := append([]byte{}, bin.Value(0)...)
			if e := validateNamed(info.Method, raw); e != nil {
				return reject(e)
			}
			ctx = context.WithValue(ctx, requestKey{}, raw)
		}
	}
	return ctx, nil
}
func (validationHook) OnDispatchEnd(ctx context.Context, _ vgirpc.HookToken, info vgirpc.DispatchInfo, _ *vgirpc.CallStatistics, _ error) {
	raw, ok := ctx.Value(rawConnectionKey{}).(*rawConnection)
	if !ok {
		return
	}
	defer raw.reset()
	if info.Method != "read_result" && info.Method != "bind" && info.Method != "bind_stream" {
		return
	}
	reader, e := ipc.NewReader(bytes.NewReader(info.RequestData))
	if e != nil {
		return
	}
	defer reader.Release()
	if !reader.Next() {
		return
	}
	b := reader.RecordBatch()
	value := func(name string) string {
		for i, f := range b.Schema().Fields() {
			if f.Name == name {
				if a, ok := b.Column(i).(*array.String); ok && a.Len() == 1 && !a.IsNull(0) {
					return a.Value(0)
				}
			}
		}
		return ""
	}
	service, ok := info.Implementation.(*Service)
	if !ok {
		return
	}
	call := &vgirpc.CallContext{Auth: raw.auth, Implementation: service}
	ss, unlock, e := service.acquire(context.Background(), call, value("session_id"))
	if e != nil {
		return
	}
	defer unlock()
	if info.Method == "read_result" {
		service.closeResult(ss, value("result_id"))
	} else {
		if st := ss.statements[value("statement_id")]; st != nil && st.binding != nil && !st.binding.finished {
			clearBinding(st)
		}
	}
}
func validateNamed(method string, raw []byte) error {
	var e error
	switch method {
	case "open_connection":
		_, e = decodeRecord[OpenConnectionRequest](raw)
	case "set_connection_option":
		_, e = decodeRecord[SetConnectionOptionRequest](raw)
	case "set_statement_option":
		_, e = decodeRecord[SetStatementOptionRequest](raw)
	case "get_info":
		_, e = decodeRecord[GetInfoRequest](raw)
	case "get_objects":
		_, e = decodeRecord[GetObjectsRequest](raw)
	case "get_table_schema":
		_, e = decodeRecord[GetTableSchemaRequest](raw)
	case "get_statistics":
		_, e = decodeRecord[GetStatisticsRequest](raw)
	}
	return e
}

// boundedAllocator limits Arrow-owned allocations before allocating. Arrow's
// Go runtime metadata allocations still require process-level memory isolation.
type boundedAllocator struct {
	mu          sync.Mutex
	used, limit int
}

func (a *boundedAllocator) Allocate(n int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n < 0 || n > a.limit-a.used {
		panic("Arrow allocation limit")
	}
	a.used += n
	return memory.DefaultAllocator.Allocate(n)
}
func (a *boundedAllocator) Reallocate(n int, b []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	delta := n - len(b)
	if n < 0 || delta > a.limit-a.used {
		panic("Arrow allocation limit")
	}
	a.used += delta
	return memory.DefaultAllocator.Reallocate(n, b)
}
func (a *boundedAllocator) Free(b []byte) {
	a.mu.Lock()
	a.used -= len(b)
	a.mu.Unlock()
	memory.DefaultAllocator.Free(b)
}
