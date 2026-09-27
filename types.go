// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"encoding/base64"
	"encoding/json"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"reflect"
)

const ProtocolName = "org.queryfarm.Grainlift.v1"
const ProtocolVersion = "0.4.0"

func (OpenConnectionRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (SetConnectionOptionRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (SetStatementOptionRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (GetInfoRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (GetObjectsRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (GetTableSchemaRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (GetStatisticsRequest) VgiRpcParamsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil)
}

// Error preserves downstream ADBC diagnostics. Messages are client-visible;
// never include secrets. Ordinary backend errors are redacted by Service.
type ErrorDetail struct {
	Key   string
	Value []byte
}
type Error struct {
	Status     string
	Message    string
	SQLState   string
	VendorCode int32
	Details    []ErrorDetail
}

func (e *Error) valid() bool {
	statuses := map[string]bool{"unknown": true, "not_implemented": true, "not_found": true, "already_exists": true, "invalid_arguments": true, "invalid_state": true, "invalid_data": true, "integrity": true, "internal": true, "io": true, "cancelled": true, "timeout": true, "unauthenticated": true, "unauthorized": true}
	if !statuses[e.Status] || len(e.Message) > 65536 {
		return false
	}
	if e.SQLState != "" {
		if len(e.SQLState) != 5 {
			return false
		}
		for _, b := range []byte(e.SQLState) {
			if b > 127 {
				return false
			}
		}
	}
	n := len(e.Message)
	for _, v := range e.Details {
		n += len(v.Key) + len(v.Value)
	}
	return n <= 65536
}
func (e *Error) ErrorKind() string { return "adbc." + e.Status }
func (e *Error) ErrorType() string { return "AdbcError" }
func (e *Error) Error() string {
	state := e.SQLState
	if state == "" {
		state = "00000"
	}
	codes := make([]int, len(state))
	for i, b := range []byte(state) {
		codes[i] = int(b)
	}
	details := make([][2]string, 0, len(e.Details))
	for _, v := range e.Details {
		details = append(details, [2]string{v.Key, base64.StdEncoding.EncodeToString(v.Value)})
	}
	b, _ := json.Marshal(map[string]any{"status": e.Status, "message": e.Message, "sqlstate": codes, "vendor_code": e.VendorCode, "details": details})
	return string(b)
}
func failure(status, message string) error { return &Error{Status: status, Message: message} }
func unsupported() error                   { return failure("not_implemented", "Backend capability is not implemented") }
func recordSchema(v any) *arrow.Schema {
	s, e := vgirpc.SchemaForStruct(reflect.TypeOf(v))
	if e != nil {
		panic(e)
	}
	return s
}

// OptionValue is an exactly-one discriminated ADBC option value.
type OptionValue struct {
	Kind        string   `vgirpc:"kind" arrow:"kind"`
	StringValue *string  `vgirpc:"string_value" arrow:"string_value"`
	BytesValue  *[]byte  `vgirpc:"bytes_value" arrow:"bytes_value"`
	IntValue    *int64   `vgirpc:"int_value" arrow:"int_value"`
	DoubleValue *float64 `vgirpc:"double_value" arrow:"double_value"`
}
type NamedOption struct {
	Key   string      `vgirpc:"key" arrow:"key"`
	Value OptionValue `vgirpc:"value" arrow:"value"`
}
type OpenConnectionRequest struct {
	Target            string        `vgirpc:"target" arrow:"target"`
	DatabaseOptions   []NamedOption `vgirpc:"database_options" arrow:"database_options"`
	ConnectionOptions []NamedOption `vgirpc:"connection_options" arrow:"connection_options"`
}
type SetConnectionOptionRequest struct {
	SessionID string      `vgirpc:"session_id" arrow:"session_id"`
	Key       string      `vgirpc:"key" arrow:"key"`
	Value     OptionValue `vgirpc:"value" arrow:"value"`
}
type SetStatementOptionRequest struct {
	SessionID   string      `vgirpc:"session_id" arrow:"session_id"`
	StatementID string      `vgirpc:"statement_id" arrow:"statement_id"`
	Key         string      `vgirpc:"key" arrow:"key"`
	Value       OptionValue `vgirpc:"value" arrow:"value"`
}
type GetInfoRequest struct {
	SessionID string   `vgirpc:"session_id" arrow:"session_id"`
	Codes     *[]int64 `vgirpc:"codes" arrow:"codes"`
}
type GetObjectsRequest struct {
	SessionID  string    `vgirpc:"session_id" arrow:"session_id"`
	Depth      int64     `vgirpc:"depth" arrow:"depth"`
	Catalog    *string   `vgirpc:"catalog" arrow:"catalog"`
	DBSchema   *string   `vgirpc:"db_schema" arrow:"db_schema"`
	TableName  *string   `vgirpc:"table_name" arrow:"table_name"`
	TableTypes *[]string `vgirpc:"table_types" arrow:"table_types"`
	ColumnName *string   `vgirpc:"column_name" arrow:"column_name"`
}
type GetTableSchemaRequest struct {
	SessionID string  `vgirpc:"session_id" arrow:"session_id"`
	Catalog   *string `vgirpc:"catalog" arrow:"catalog"`
	DBSchema  *string `vgirpc:"db_schema" arrow:"db_schema"`
	TableName string  `vgirpc:"table_name" arrow:"table_name"`
}
type GetStatisticsRequest struct {
	SessionID   string  `vgirpc:"session_id" arrow:"session_id"`
	Catalog     *string `vgirpc:"catalog" arrow:"catalog"`
	DBSchema    *string `vgirpc:"db_schema" arrow:"db_schema"`
	TableName   *string `vgirpc:"table_name" arrow:"table_name"`
	Approximate bool    `vgirpc:"approximate" arrow:"approximate"`
}
type OkResponse struct {
	Ok bool `vgirpc:"ok" arrow:"ok"`
}
type SessionResponse struct {
	SessionID string `vgirpc:"session_id" arrow:"session_id"`
}
type StatementResponse struct {
	SessionID   string `vgirpc:"session_id" arrow:"session_id"`
	StatementID string `vgirpc:"statement_id" arrow:"statement_id"`
}
type ExecuteResponse struct {
	ResultID     string `vgirpc:"result_id" arrow:"result_id"`
	RowsAffected *int64 `vgirpc:"rows_affected" arrow:"rows_affected"`
	SchemaIPC    []byte `vgirpc:"schema_ipc" arrow:"schema_ipc"`
}
type SchemaResponse struct {
	SchemaIPC []byte `vgirpc:"schema_ipc" arrow:"schema_ipc"`
}
type ValueResponse struct {
	Value OptionValue `vgirpc:"value" arrow:"value"`
}
type UpdateResponse struct {
	RowsAffected *int64 `vgirpc:"rows_affected" arrow:"rows_affected"`
}
type PartitionsResponse struct {
	RowsAffected int64    `vgirpc:"rows_affected" arrow:"rows_affected"`
	SchemaIPC    []byte   `vgirpc:"schema_ipc" arrow:"schema_ipc"`
	Partitions   [][]byte `vgirpc:"partitions" arrow:"partitions"`
}

func (OpenConnectionRequest) ArrowSchema() *arrow.Schema {
	type plain OpenConnectionRequest
	return recordSchema(plain{})
}

func (SetConnectionOptionRequest) ArrowSchema() *arrow.Schema {
	type plain SetConnectionOptionRequest
	return recordSchema(plain{})
}

func (SetStatementOptionRequest) ArrowSchema() *arrow.Schema {
	type plain SetStatementOptionRequest
	return recordSchema(plain{})
}

func (GetInfoRequest) ArrowSchema() *arrow.Schema {
	type plain GetInfoRequest
	return recordSchema(plain{})
}

func (GetObjectsRequest) ArrowSchema() *arrow.Schema {
	type plain GetObjectsRequest
	return recordSchema(plain{})
}

func (GetTableSchemaRequest) ArrowSchema() *arrow.Schema {
	type plain GetTableSchemaRequest
	return recordSchema(plain{})
}

func (GetStatisticsRequest) ArrowSchema() *arrow.Schema {
	type plain GetStatisticsRequest
	return recordSchema(plain{})
}

func (OkResponse) ArrowSchema() *arrow.Schema { type plain OkResponse; return recordSchema(plain{}) }

func (SessionResponse) ArrowSchema() *arrow.Schema {
	type plain SessionResponse
	return recordSchema(plain{})
}

func (StatementResponse) ArrowSchema() *arrow.Schema {
	type plain StatementResponse
	return recordSchema(plain{})
}

func (ExecuteResponse) ArrowSchema() *arrow.Schema {
	type plain ExecuteResponse
	return recordSchema(plain{})
}

func (SchemaResponse) ArrowSchema() *arrow.Schema {
	type plain SchemaResponse
	return recordSchema(plain{})
}

func (ValueResponse) ArrowSchema() *arrow.Schema {
	type plain ValueResponse
	return recordSchema(plain{})
}

func (UpdateResponse) ArrowSchema() *arrow.Schema {
	type plain UpdateResponse
	return recordSchema(plain{})
}

func (PartitionsResponse) ArrowSchema() *arrow.Schema {
	type plain PartitionsResponse
	return recordSchema(plain{})
}
