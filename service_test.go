// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

type featureBackend struct{ connection *featureConnection }

func (b featureBackend) Open(context.Context, string, OpenConnectionRequest) (Connection, error) {
	return b.connection, nil
}

type featureConnection struct {
	UnimplementedConnection
	st                 *featureStatement
	commits, rollbacks int
	options            map[string]OptionValue
	closed             bool
	statistics         *bool
}

func (c *featureConnection) StatisticsSupported() *bool     { return c.statistics }
func (c *featureConnection) StatisticNamesSupported() *bool { return c.statistics }

func (c *featureConnection) NewStatement(context.Context) (Statement, error) { return c.st, nil }
func (c *featureConnection) Commit(context.Context) error                    { c.commits++; return nil }
func (c *featureConnection) Rollback(context.Context) error                  { c.rollbacks++; return nil }
func (c *featureConnection) SetOption(_ context.Context, k string, v OptionValue) error {
	c.options[k] = v
	return nil
}
func (c *featureConnection) GetOption(_ context.Context, k, kind string) (OptionValue, error) {
	return c.options[k], nil
}
func (c *featureConnection) GetInfo(context.Context, GetInfoRequest) (*QueryResult, error) {
	return testQuery(), nil
}
func (c *featureConnection) GetObjects(context.Context, GetObjectsRequest) (*QueryResult, error) {
	return testQuery(), nil
}
func (c *featureConnection) GetTableTypes(context.Context) (*QueryResult, error) {
	return testQuery(), nil
}
func (c *featureConnection) GetStatisticNames(context.Context) (*QueryResult, error) {
	return testQuery(), nil
}
func (c *featureConnection) GetStatistics(context.Context, GetStatisticsRequest) (*QueryResult, error) {
	return testQuery(), nil
}
func (c *featureConnection) GetTableSchema(context.Context, GetTableSchemaRequest) (*arrow.Schema, error) {
	return testSchema(), nil
}
func (c *featureConnection) ReadPartition(_ context.Context, p []byte) (*QueryResult, error) {
	if string(p) != "part" {
		return nil, errors.New("wrong partition")
	}
	return testQuery(), nil
}
func (c *featureConnection) Close() error { c.closed = true; return nil }

type featureStatement struct {
	UnimplementedStatement
	prepared  bool
	plan      []byte
	boundRows int64
	options   map[string]OptionValue
}

func (s *featureStatement) Prepare(context.Context) error { s.prepared = true; return nil }
func (s *featureStatement) SetSubstraitPlan(_ context.Context, p []byte) error {
	s.plan = append([]byte{}, p...)
	return nil
}
func (s *featureStatement) Bind(_ context.Context, b arrow.RecordBatch) error {
	s.boundRows = b.NumRows()
	return nil
}
func (s *featureStatement) BindStream(_ context.Context, r array.RecordReader) error {
	s.boundRows = 0
	for r.Next() {
		s.boundRows += r.RecordBatch().NumRows()
	}
	return r.Err()
}
func (s *featureStatement) Execute(context.Context) (*QueryResult, error) { return testQuery(), nil }
func (s *featureStatement) ExecuteUpdate(context.Context) (*int64, error) {
	n := int64(7)
	return &n, nil
}
func (s *featureStatement) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	return testSchema(), nil
}
func (s *featureStatement) GetParameterSchema(context.Context) (*arrow.Schema, error) {
	return testSchema(), nil
}
func (s *featureStatement) ExecutePartitions(context.Context) (*PartitionedResult, error) {
	return &PartitionedResult{testSchema(), 7, [][]byte{[]byte("part")}}, nil
}
func (s *featureStatement) SetOption(_ context.Context, k string, v OptionValue) error {
	s.options[k] = v
	return nil
}
func (s *featureStatement) GetOption(_ context.Context, k, kind string) (OptionValue, error) {
	return s.options[k], nil
}
func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
}
func testBatch() arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2, 3}, nil)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	return array.NewRecordBatch(testSchema(), []arrow.Array{a}, 3)
}
func testQuery() *QueryResult {
	b := testBatch()
	defer b.Release()
	r, _ := array.NewRecordReader(testSchema(), []arrow.RecordBatch{b})
	return &QueryResult{Reader: r}
}
func appendValue(b array.Builder, v reflect.Value) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			b.AppendNull()
			return
		}
		v = v.Elem()
	}
	switch x := b.(type) {
	case *array.StringBuilder:
		x.Append(v.String())
	case *array.BinaryBuilder:
		x.Append(v.Bytes())
	case *array.Int64Builder:
		x.Append(v.Int())
	case *array.Float64Builder:
		x.Append(v.Float())
	case *array.BooleanBuilder:
		x.Append(v.Bool())
	case *array.StructBuilder:
		x.Append(true)
		for i := 0; i < v.NumField(); i++ {
			appendValue(x.FieldBuilder(i), v.Field(i))
		}
	case *array.ListBuilder:
		x.Append(true)
		for i := 0; i < v.Len(); i++ {
			appendValue(x.ValueBuilder(), v.Index(i))
		}
	default:
		panic("unsupported test type")
	}
}
func testRecord(v any) arrow.RecordBatch {
	schema := recordSchema(v)
	if a, ok := v.(vgirpc.ArrowSerializable); ok {
		schema = a.ArrowSchema()
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	rv := reflect.ValueOf(v)
	for i := 0; i < rv.NumField(); i++ {
		appendValue(b.Field(i), rv.Field(i))
	}
	return b.NewRecordBatch()
}
func testIPC(b arrow.RecordBatch) []byte {
	var raw bytes.Buffer
	w := ipc.NewWriter(&raw, ipc.WithSchema(b.Schema()))
	if e := w.Write(b); e != nil {
		panic(e)
	}
	if e := w.Close(); e != nil {
		panic(e)
	}
	return raw.Bytes()
}
func testInput(v any) arrow.RecordBatch {
	b := testRecord(v)
	if _, ok := v.(vgirpc.ArrowSerializable); !ok {
		return b
	}
	defer b.Release()
	return testRecord(struct {
		Request []byte `vgirpc:"request"`
	}{testIPC(b)})
}
func callTest[R any](t *testing.T, c *vgirpc.HttpClient, name string, p any) R {
	t.Helper()
	b := testInput(p)
	defer b.Release()
	reply, e := c.CallUnary(context.Background(), name, b, nil)
	if e != nil {
		t.Fatalf("%s: %v", name, e)
	}
	defer reply.Release()
	raw := reply.Batch.Column(0).(*array.Binary).Value(0)
	v, e := decodeRecord[R](raw)
	if e != nil {
		t.Fatalf("%s response: %v", name, e)
	}
	return v
}
func setupFeature(t *testing.T) (*Service, *vgirpc.HttpClient, *featureConnection, StatementResponse) {
	t.Helper()
	st := &featureStatement{options: map[string]OptionValue{}}
	conn := &featureConnection{st: st, options: map[string]OptionValue{}}
	svc, e := NewService(map[string]Target{"default": {Backend: featureBackend{conn}, Authorize: func(string) bool { return true }, AllowedConnectionOptions: map[string]bool{"int": true}}}, DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(svc.HTTPHandler(func(*http.Request) (*vgirpc.AuthContext, error) {
		return &vgirpc.AuthContext{Authenticated: true, Principal: "test", Domain: "test"}, nil
	}))
	client, e := vgirpc.NewHttpClient(server.URL, vgirpc.WithClientProtocol(ProtocolName), vgirpc.WithClientProtocolVersion(ProtocolVersion))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
		if e := svc.Close(); e != nil {
			t.Error(e)
		}
	})
	sid := callTest[SessionResponse](t, client, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	stid := callTest[StatementResponse](t, client, "new_statement", sessionParams{sid})
	return svc, client, conn, stid
}
func TestFullOptionalBackendDispatch(t *testing.T) {
	_, c, conn, p := setupFeature(t)
	sp := statementParams{p.SessionID, p.StatementID}
	cp := sessionParams{p.SessionID}
	callTest[OkResponse](t, c, "prepare", sp)
	if !conn.st.prepared {
		t.Fatal("prepare not dispatched")
	}
	callTest[OkResponse](t, c, "commit", cp)
	callTest[OkResponse](t, c, "rollback", cp)
	if conn.commits != 1 || conn.rollbacks != 1 {
		t.Fatal("transactions not dispatched")
	}
	callTest[OkResponse](t, c, "set_substrait_plan", planParams{p.SessionID, p.StatementID, []byte{0, 255, 3}})
	if !bytes.Equal(conn.st.plan, []byte{0, 255, 3}) {
		t.Fatal("Substrait changed")
	}
	n := int64(9007199254740993)
	v := OptionValue{Kind: "int", IntValue: &n}
	callTest[OkResponse](t, c, "set_connection_option", SetConnectionOptionRequest{p.SessionID, "int", v})
	got := callTest[ValueResponse](t, c, "get_connection_option", connectionOptionParams{p.SessionID, "int", "int"})
	if *got.Value.IntValue != n {
		t.Fatal("integer precision")
	}
	callTest[OkResponse](t, c, "set_statement_option", SetStatementOptionRequest{p.SessionID, p.StatementID, "int", v})
	got = callTest[ValueResponse](t, c, "get_statement_option", statementOptionParams{p.SessionID, p.StatementID, "int", "int"})
	if *got.Value.IntValue != n {
		t.Fatal("statement precision")
	}
	if *callTest[UpdateResponse](t, c, "execute_update", sp).RowsAffected != 7 {
		t.Fatal("row count")
	}
	for _, method := range []string{"execute_schema", "get_parameter_schema"} {
		got := callTest[SchemaResponse](t, c, method, sp)
		schema, e := parseSchema(got.SchemaIPC)
		if e != nil || !schema.Equal(testSchema()) {
			t.Fatal("schema mismatch")
		}
	}
	part := callTest[PartitionsResponse](t, c, "execute_partitions", sp)
	if string(part.Partitions[0]) == "part" {
		t.Fatal("partition not sealed")
	}
	reply := callTest[ExecuteResponse](t, c, "read_partition", partitionParams{p.SessionID, part.Partitions[0]})
	if reply.ResultID == "" {
		t.Fatal("no partition cursor")
	}
	for name, request := range map[string]any{
		"get_info":        GetInfoRequest{SessionID: p.SessionID},
		"get_objects":     GetObjectsRequest{SessionID: p.SessionID},
		"get_statistics":  GetStatisticsRequest{SessionID: p.SessionID, Approximate: true},
		"get_table_types": cp, "get_statistic_names": cp,
	} {
		reply := callTest[ExecuteResponse](t, c, name, request)
		if reply.ResultID == "" {
			t.Fatal(name)
		}
		callTest[OkResponse](t, c, "close_result", resultParams{p.SessionID, reply.ResultID})
	}
	callTest[SchemaResponse](t, c, "get_table_schema", GetTableSchemaRequest{SessionID: p.SessionID, TableName: "table"})
}

func TestSessionStatisticsCapabilities(t *testing.T) {
	_, client, connection, _ := setupFeature(t)
	for _, supported := range []*bool{nil, ptr(false), ptr(true)} {
		connection.statistics = supported
		response := callTest[SessionResponse](t, client, "open_connection", OpenConnectionRequest{Target: "default"})
		if !reflect.DeepEqual(response.StatisticsSupported, supported) || !reflect.DeepEqual(response.StatisticNamesSupported, supported) {
			t.Fatalf("capabilities did not round trip: %+v", response)
		}
	}
}
func TestBindingAndIngestionDispatch(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "bind", true: "bind_stream"}[stream], func(t *testing.T) {
			_, c, conn, p := setupFeature(t)
			rawSchema, _ := schemaIPC(testSchema())
			params := testInput(bindParams{p.SessionID, p.StatementID, rawSchema})
			defer params.Release()
			name := "bind"
			if stream {
				name = "bind_stream"
			}
			upload, e := c.OpenExchange(context.Background(), name, params, vgirpc.ClientStreamSchema{Input: bindSchema, Output: okSchema})
			if e != nil {
				t.Fatal(e)
			}
			defer upload.Close()
			batch := testBatch()
			defer batch.Release()
			for i := 0; i < map[bool]int{false: 1, true: 2}[stream]; i++ {
				frame := testRecord(struct {
					BatchIPC []byte `vgirpc:"batch_ipc"`
					Finish   bool   `vgirpc:"finish"`
				}{testIPC(batch), false})
				reply, e := upload.Exchange(context.Background(), frame)
				frame.Release()
				if e != nil {
					t.Fatal(e)
				}
				reply.Release()
			}
			frame := testRecord(struct {
				BatchIPC []byte `vgirpc:"batch_ipc"`
				Finish   bool   `vgirpc:"finish"`
			}{[]byte{}, true})
			reply, e := upload.Exchange(context.Background(), frame)
			frame.Release()
			if e != nil {
				t.Fatal(e)
			}
			reply.Release()
			if conn.st.boundRows != int64(map[bool]int{false: 3, true: 6}[stream]) {
				t.Fatal("bound row count")
			}
			if *callTest[UpdateResponse](t, c, "execute_update", statementParams{p.SessionID, p.StatementID}).RowsAffected != 7 {
				t.Fatal("ingestion update")
			}
		})
	}
}
func TestPartitionOwnershipTamperingAndExpiry(t *testing.T) {
	svc, _, _, _ := setupFeature(t)
	raw, e := svc.sealPartition("alice", "one", []byte("secret"))
	if e != nil {
		t.Fatal(e)
	}
	for _, args := range [][2]string{{"bob", "one"}, {"alice", "two"}} {
		if _, e := svc.openPartition(args[0], args[1], raw); e == nil {
			t.Fatal("accepted wrong principal/target")
		}
	}
	raw[len(raw)-1] ^= 1
	if _, e := svc.openPartition("alice", "one", raw); e == nil {
		t.Fatal("accepted corruption")
	}
	limits := DefaultLimits()
	limits.IdleTimeout = time.Nanosecond
	expiring, e := NewService(nil, limits)
	if e != nil {
		t.Fatal(e)
	}
	defer expiring.Close()
	raw, _ = expiring.sealPartition("alice", "one", []byte("secret"))
	if _, e := expiring.openPartition("alice", "one", raw); e == nil {
		t.Fatal("accepted expired token")
	}
}
func TestOptionValidationAndServerAuthority(t *testing.T) {
	n := int64(2)
	text := "value"
	v := OptionValue{Kind: "int", IntValue: &n}
	for _, invalid := range []OptionValue{{}, {Kind: "string", IntValue: &n}, {Kind: "int", IntValue: &n, StringValue: &text}} {
		if invalid.validate() == nil {
			t.Fatal("invalid option")
		}
	}
	if _, e := options([]NamedOption{{"key", v}, {"key", v}}, nil, map[string]bool{"key": true}); e == nil {
		t.Fatal("duplicate")
	}
	if _, e := options([]NamedOption{{"key", v}}, map[string]OptionValue{"key": v}, map[string]bool{"key": true}); e == nil {
		t.Fatal("authoritative")
	}
	if _, e := options([]NamedOption{{"key", v}}, nil, nil); e == nil {
		t.Fatal("allowlist")
	}
}
func TestStrictNamedCodec(t *testing.T) {
	b := testRecord(OpenConnectionRequest{Target: "default"})
	defer b.Release()
	raw := testIPC(b)
	if _, e := decodeRecord[OpenConnectionRequest](raw); e != nil {
		t.Fatal(e)
	}
	for _, bad := range [][]byte{append(append([]byte{}, raw...), 0), raw[:len(raw)-9], {0, 1, 2}} {
		if _, e := decodeRecord[OpenConnectionRequest](bad); e == nil {
			t.Fatal("accepted malformed IPC")
		}
	}
}
func TestBackendErrorsAreRedacted(t *testing.T) {
	secret := "do not reveal"
	if bytes.Contains([]byte(backendError(errors.New(secret)).Error()), []byte(secret)) {
		t.Fatal("raw backend error leaked")
	}
	if (&Error{}).ErrorType() != "AdbcError" {
		t.Fatal("native driver error discriminator")
	}
}
func TestAuthoritativeOptionsRejectAcrossScopes(t *testing.T) {
	s, c, _, p := setupFeature(t)
	value := OptionValue{Kind: "string", StringValue: ptr("configured")}
	target := s.targets["default"]
	target.DatabaseOptions = map[string]OptionValue{"database-secret": value}
	target.ConnectionOptions = map[string]OptionValue{"connection-secret": value}
	target.AllowedDatabaseOptions = map[string]bool{"database-secret": true, "connection-secret": true}
	target.AllowedConnectionOptions = target.AllowedDatabaseOptions
	s.targets["default"] = target
	s.sessions[p.SessionID].target = target
	for _, key := range []string{"database-secret", "connection-secret"} {
		for _, method := range []string{"set_connection_option", "set_statement_option"} {
			var request any = SetConnectionOptionRequest{p.SessionID, key, value}
			if method == "set_statement_option" {
				request = SetStatementOptionRequest{p.SessionID, p.StatementID, key, value}
			}
			batch := testInput(request)
			reply, e := c.CallUnary(context.Background(), method, batch, nil)
			batch.Release()
			if reply != nil {
				reply.Release()
			}
			if e == nil {
				t.Fatalf("%s accepted configured %s", method, key)
			}
		}
		for _, database := range []bool{true, false} {
			request := OpenConnectionRequest{Target: "default"}
			if database {
				request.DatabaseOptions = []NamedOption{{key, value}}
			} else {
				request.ConnectionOptions = []NamedOption{{key, value}}
			}
			if _, e := s.open(context.Background(), testCall(s), request); e == nil {
				t.Fatal("cross-scope configured option accepted")
			}
		}
	}
}
func TestErrorDetailsPreserveDuplicates(t *testing.T) {
	e := &Error{Status: "invalid_data", SQLState: "22000", Details: []ErrorDetail{{"same", []byte("one")}, {"same", []byte("two")}}}
	if !e.valid() || bytes.Count([]byte(e.Error()), []byte("same")) != 2 {
		t.Fatal("duplicate details lost")
	}
	for _, bad := range []*Error{{Status: "bad"}, {Status: "io", SQLState: "123456"}, {Status: "io", SQLState: "é000"}} {
		if backendError(bad) == bad {
			t.Fatal("invalid diagnostics accepted")
		}
	}
}
func TestArrowAllocatorBoundary(t *testing.T) {
	a := &boundedAllocator{limit: 8}
	b := a.Allocate(7)
	b = a.Reallocate(8, b)
	a.Free(b)
	defer func() {
		if recover() == nil {
			t.Fatal("allocation beyond boundary accepted")
		}
	}()
	_ = a.Allocate(9)
}
