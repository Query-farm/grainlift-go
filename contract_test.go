// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"os"
	"testing"
)

type contractField struct {
	Name     string          `json:"name"`
	Nullable bool            `json:"nullable"`
	Type     json.RawMessage `json:"type"`
}
type contractSchema struct {
	Fields []contractField `json:"fields"`
}
type contractMethod struct {
	Name, Kind string
	Request    contractSchema
	Response   *contractSchema
	Input      *contractSchema
}
type contract struct {
	ProtocolName    string `json:"protocol_name"`
	ProtocolVersion string `json:"protocol_version"`
	Records         map[string]contractSchema
	Methods         []contractMethod
}

func loadContract(t *testing.T) contract {
	t.Helper()
	raw, e := os.ReadFile("testdata/contract.json")
	if e != nil {
		t.Fatal(e)
	}
	var c contract
	if e = json.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	return c
}
func contractType(t *testing.T, raw json.RawMessage) arrow.DataType {
	t.Helper()
	var primitive string
	if json.Unmarshal(raw, &primitive) == nil {
		switch primitive {
		case "string":
			return arrow.BinaryTypes.String
		case "binary":
			return arrow.BinaryTypes.Binary
		case "int64":
			return arrow.PrimitiveTypes.Int64
		case "float64":
			return arrow.PrimitiveTypes.Float64
		case "bool":
			return arrow.FixedWidthTypes.Boolean
		}
	}
	var nested struct {
		List   *contractField
		Struct []contractField
	}
	if e := json.Unmarshal(raw, &nested); e != nil {
		t.Fatal(e)
	}
	if nested.List != nil {
		return arrow.ListOfField(contractArrowField(t, *nested.List))
	}
	fields := []arrow.Field{}
	for _, f := range nested.Struct {
		fields = append(fields, contractArrowField(t, f))
	}
	return arrow.StructOf(fields...)
}
func contractArrowField(t *testing.T, f contractField) arrow.Field {
	return arrow.Field{Name: f.Name, Type: contractType(t, f.Type), Nullable: f.Nullable}
}
func contractArrowSchema(t *testing.T, s contractSchema) *arrow.Schema {
	fields := []arrow.Field{}
	for _, f := range s.Fields {
		fields = append(fields, contractArrowField(t, f))
	}
	return arrow.NewSchema(fields, nil)
}
func TestAuthoritativeRecordSchemas(t *testing.T) {
	if path := os.Getenv("GRAINLIFT_CONTRACT"); path != "" {
		source, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		fixture, e := os.ReadFile("testdata/contract.json")
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(source, fixture) {
			t.Fatal("authoritative Grainlift contract artifact changed")
		}
	}
	c := loadContract(t)
	if c.ProtocolName != ProtocolName || c.ProtocolVersion != ProtocolVersion {
		t.Fatal("protocol identity differs")
	}
	records := map[string]*arrow.Schema{
		"OpenConnectionRequest": OpenConnectionRequest{}.ArrowSchema(), "SetConnectionOptionRequest": SetConnectionOptionRequest{}.ArrowSchema(), "SetStatementOptionRequest": SetStatementOptionRequest{}.ArrowSchema(),
		"GetInfoRequest": GetInfoRequest{}.ArrowSchema(), "GetObjectsRequest": GetObjectsRequest{}.ArrowSchema(), "GetTableSchemaRequest": GetTableSchemaRequest{}.ArrowSchema(), "GetStatisticsRequest": GetStatisticsRequest{}.ArrowSchema(),
		"OkResponse": OkResponse{}.ArrowSchema(), "SessionResponse": SessionResponse{}.ArrowSchema(), "StatementResponse": StatementResponse{}.ArrowSchema(),
		"ExecuteResponse": ExecuteResponse{}.ArrowSchema(), "SchemaResponse": SchemaResponse{}.ArrowSchema(), "ValueResponse": ValueResponse{}.ArrowSchema(), "UpdateResponse": UpdateResponse{}.ArrowSchema(), "PartitionsResponse": PartitionsResponse{}.ArrowSchema(),
	}
	for name, got := range records {
		expected, exists := c.Records[name]
		if !exists {
			t.Fatalf("missing authoritative record %s", name)
		}
		if !got.Equal(contractArrowSchema(t, expected)) {
			t.Fatalf("%s schema differs: %s", name, got)
		}
	}
}
func TestRegisteredMethodSchemas(t *testing.T) {
	_, client, _, _ := setupFeature(t)
	description, e := client.DescribeProtocol(context.Background(), ProtocolName)
	if e != nil {
		t.Fatal(e)
	}
	c := loadContract(t)
	expected := map[string]contractMethod{}
	for _, m := range c.Methods {
		expected[m.Name] = m
	}
	if len(description.Methods) != len(expected) {
		t.Fatalf("method count %d != %d", len(description.Methods), len(expected))
	}
	for _, m := range description.Methods {
		want, exists := expected[m.Name]
		if !exists {
			t.Fatalf("unexpected method %s", m.Name)
		}
		reader, e := ipc.NewReader(bytes.NewReader(m.ParamsSchemaIPC))
		if e != nil {
			t.Fatal(e)
		}
		if !reader.Schema().Equal(contractArrowSchema(t, want.Request)) {
			t.Fatalf("%s actual registered input differs: %s", m.Name, reader.Schema())
		}
		reader.Release()
		if want.Kind == "unary" && want.Response != nil {
			reader, e = ipc.NewReader(bytes.NewReader(m.ResultSchemaIPC))
			if e != nil {
				t.Fatal(e)
			}
			if !reader.Schema().Equal(contractArrowSchema(t, *want.Response)) {
				t.Fatalf("%s actual response differs", m.Name)
			}
			reader.Release()
		}
		if m.Name == "read_result" && m.HasHeader {
			if os.Getenv("GRAINLIFT_REQUIRE_CANONICAL_HEADER") == "1" {
				t.Fatal("upstream dynamic no-header reflection fix has not been integrated")
			}
			t.Log("Known vgi-rpc-go v0.28.0 release gate: dynamic no-header producer advertises has_header=true")
		} else if m.HasHeader {
			t.Fatalf("%s unexpectedly advertises a header", m.Name)
		}
	}
}
