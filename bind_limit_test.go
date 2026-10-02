// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// A batch decoded from IPC shares one message body among its buffers; it is
// measured once, not once per buffer.
func TestRecordBytesCountsSharedBuffersOnce(t *testing.T) {
	batch := binaryBatch([][]byte{filled(900<<10, 'a')})
	defer batch.Release()
	raw := testIPC(batch)
	reader, e := ipc.NewReader(bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	if got := recordBytes(reader.RecordBatch()); got > int64(len(raw))*11/10 {
		t.Fatalf("a %d-byte batch measured %d bytes", len(raw), got)
	}
}

// Clients split parameters to fit the advertised request limit, which is all
// they know: a bound batch that fits a request is accepted even when it is
// larger than BatchBytes.
func TestHTTPBindAcceptsBatchesThatFitARequest(t *testing.T) {
	statement := &storageStatement{}
	limits := DefaultLimits() // 2 MiB requests, 1 MiB result batches
	svc, e := NewService(map[string]Target{"default": {Backend: storageBackend{statement}, Authorize: func(string) bool { return true }}}, limits)
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
	stid := callTest[StatementResponse](t, client, "new_statement", sessionParams{sid}).StatementID
	rawSchema, _ := schemaIPC(binarySchema())
	params := testInput(bindParams{sid, stid, rawSchema})
	defer params.Release()
	upload, e := client.OpenExchange(context.Background(), "bind_stream", params, vgirpc.ClientStreamSchema{Input: bindSchema, Output: okSchema})
	if e != nil {
		t.Fatal(e)
	}
	defer upload.Close()
	sent := [][]byte{filled(1500<<10, 'x')} // over BatchBytes, within the request
	batch := binaryBatch(sent)
	defer batch.Release()
	for _, frame := range []struct {
		BatchIPC []byte `vgirpc:"batch_ipc"`
		Finish   bool   `vgirpc:"finish"`
	}{{testIPC(batch), false}, {[]byte{}, true}} {
		record := testRecord(frame)
		reply, e := upload.Exchange(context.Background(), record)
		record.Release()
		if e != nil {
			t.Fatal(e)
		}
		reply.Release()
	}
	if len(statement.bound) != 1 || !bytes.Equal(statement.bound[0], sent[0]) {
		t.Fatal("bound data changed")
	}
}
