// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The presigned GET from AWS's Signature Version 4 documentation
// ("Authenticating Requests: Using Query Parameters").
func TestPresignerMatchesTheAWSDocumentationExample(t *testing.T) {
	p, e := newPresigner("https://s3.amazonaws.com", "examplebucket", "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", true)
	if e != nil {
		t.Fatal(e)
	}
	got := p.presign(http.MethodGet, "test.txt", time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC), 86400*time.Second)
	want := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if got != want {
		t.Fatalf("presigned URL\n got %s\nwant %s", got, want)
	}
}

func TestPresignerPathStyleAndBucketOnlyValidator(t *testing.T) {
	p, _ := newPresigner("https://s3.amazonaws.com", "examplebucket", "us-east-1", "AK", "SK", false)
	got := p.presign(http.MethodPut, "grainlift/a b.arrow", time.Now(), time.Minute)
	if !strings.HasPrefix(got, "https://s3.amazonaws.com/examplebucket/grainlift/a%20b.arrow?") {
		t.Fatalf("path-style URL %s", got)
	}
	for raw, ok := range map[string]bool{
		"https://s3.amazonaws.com/examplebucket/grainlift/x.arrow?sig=1": true,
		"https://s3.amazonaws.com/otherbucket/x.arrow":                   false,
		"https://s3.amazonaws.com/examplebucket-other/x.arrow":           false,
		"http://s3.amazonaws.com/examplebucket/x.arrow":                  false,
		"https://169.254.169.254/examplebucket/x":                        false,
		"::not a url": false,
	} {
		if e := p.validate(raw); (e == nil) != ok {
			t.Errorf("validate(%q) = %v, want ok=%t", raw, e, ok)
		}
	}
}

func TestExternalStorageConfigDefaultsValidationAndRedaction(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret-canary")
	c, e := ExternalStorageConfig{Endpoint: "https://acct.r2.cloudflarestorage.com", Bucket: "b"}.resolved()
	if e != nil {
		t.Fatal(e)
	}
	if c.Region != "auto" || c.URLTTL != 15*time.Minute || c.ThresholdBytes != 1<<20 || c.MaxUploadBytes != 256<<20 {
		t.Fatalf("defaults %v", c)
	}
	if c.AccessKeyID != "env-key" || c.SecretAccessKey != "env-secret-canary" {
		t.Fatal("environment credentials not applied")
	}
	for _, f := range []string{"%v", "%+v", "%#v", "%s"} {
		if out := fmt.Sprintf(f, c); strings.Contains(out, "env-secret-canary") {
			t.Fatalf("%s leaked the secret: %s", f, out)
		}
	}
	if out := fmt.Sprintf("%+v", &c); strings.Contains(out, "env-secret-canary") {
		t.Fatalf("pointer leaked the secret: %s", out)
	}
	valid := ExternalStorageConfig{Endpoint: "https://e.example", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"}
	for name, mutate := range map[string]func(*ExternalStorageConfig){
		"scheme":       func(c *ExternalStorageConfig) { c.Endpoint = "ftp://e.example" },
		"relative":     func(c *ExternalStorageConfig) { c.Endpoint = "/path" },
		"query":        func(c *ExternalStorageConfig) { c.Endpoint = "https://e.example?x=1" },
		"bucket":       func(c *ExternalStorageConfig) { c.Bucket = " " },
		"short TTL":    func(c *ExternalStorageConfig) { c.URLTTL = time.Millisecond },
		"long TTL":     func(c *ExternalStorageConfig) { c.URLTTL = 8 * 24 * time.Hour },
		"threshold":    func(c *ExternalStorageConfig) { c.ThresholdBytes = -1 },
		"upload limit": func(c *ExternalStorageConfig) { c.MaxUploadBytes = -1 },
	} {
		c := valid
		mutate(&c)
		if _, e := c.resolved(); e == nil {
			t.Errorf("%s: accepted invalid config", name)
		}
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, e := (ExternalStorageConfig{Endpoint: "https://e.example", Bucket: "b"}).resolved(); e == nil {
		t.Fatal("accepted missing credentials")
	}
}

func TestBatchLimitBeyondRequestLimitNeedsStorage(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchBytes = limits.RequestBytes * 4
	if _, e := NewService(nil, limits); e == nil {
		t.Fatal("accepted batches larger than an HTTP request without storage")
	}
	storage := &ExternalStorageConfig{Endpoint: "https://e.example", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"}
	svc, e := NewServiceWithOptions(nil, limits, ServiceOptions{ExternalStorage: storage})
	if e != nil {
		t.Fatal(e)
	}
	defer svc.Close()
	if svc.rpc == svc.httpRPC {
		t.Fatal("raw streams share the storage-enabled server")
	}
	limits.BatchBytes = 512 << 20
	if _, e := NewServiceWithOptions(nil, limits, ServiceOptions{ExternalStorage: storage}); e == nil {
		t.Fatal("accepted batches larger than an upload")
	}
}

// fakeS3 is an S3-compatible bucket that accepts only correctly presigned,
// unexpired requests from signer.
type fakeS3 struct {
	t       *testing.T
	signer  *presigner
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	gets    int
}

func (s *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	signed, e := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	seconds, e2 := strconv.Atoi(q.Get("X-Amz-Expires"))
	if e != nil || e2 != nil || time.Now().After(signed.Add(time.Duration(seconds)*time.Second)) {
		http.Error(w, "expired or unsigned", http.StatusForbidden)
		return
	}
	key, ok := strings.CutPrefix(r.URL.EscapedPath(), s.signer.prefix())
	unescaped, e3 := url.PathUnescape(key)
	if !ok || e3 != nil {
		http.Error(w, "not in bucket", http.StatusForbidden)
		return
	}
	want := s.signer.presign(r.Method, unescaped, signed, time.Duration(seconds)*time.Second)
	if want != "http://"+r.Host+r.URL.RequestURI() {
		http.Error(w, "signature mismatch", http.StatusForbidden)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.objects[unescaped] = body
		s.puts++
	case http.MethodGet:
		body, found := s.objects[unescaped]
		if !found {
			http.NotFound(w, r)
			return
		}
		s.gets++
		_, _ = w.Write(body)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

type storageBackend struct{ statement *storageStatement }

func (b storageBackend) Open(context.Context, string, OpenConnectionRequest) (Connection, error) {
	return &storageConnection{statement: b.statement}, nil
}

type storageConnection struct {
	UnimplementedConnection
	statement *storageStatement
}

func (c *storageConnection) NewStatement(context.Context) (Statement, error) { return c.statement, nil }
func (c *storageConnection) Close() error                                    { return nil }

// storageStatement records bound values and returns rows of the given sizes.
type storageStatement struct {
	UnimplementedStatement
	bound [][]byte
	rows  [][]byte
}

func (s *storageStatement) BindStream(_ context.Context, r array.RecordReader) error {
	for r.Next() {
		column := r.RecordBatch().Column(0).(*array.Binary)
		for i := 0; i < column.Len(); i++ {
			s.bound = append(s.bound, bytes.Clone(column.Value(i)))
		}
	}
	return r.Err()
}
func (s *storageStatement) Execute(context.Context) (*QueryResult, error) {
	b := binaryBatch(s.rows)
	defer b.Release()
	r, e := array.NewRecordReader(binarySchema(), []arrow.RecordBatch{b})
	return &QueryResult{Reader: r}, e
}
func (s *storageStatement) Close() error { return nil }

func binarySchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.Binary}}, nil)
}
func binaryBatch(rows [][]byte) arrow.RecordBatch {
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	for _, row := range rows {
		b.Append(row)
	}
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(binarySchema(), []arrow.Array{a}, int64(len(rows)))
}
func filled(n int, c byte) []byte { return bytes.Repeat([]byte{c}, n) }

// A bound batch larger than the HTTP request limit goes to the bucket through
// a presigned upload URL, and a result batch over the threshold comes back
// through a presigned download URL; the data arrives exact both ways.
func TestHTTPExternalStorageRoundTrip(t *testing.T) {
	const keyID, secret = "test-key", "test-secret"
	s3 := &fakeS3{t: t, objects: map[string][]byte{}}
	bucket := httptest.NewServer(s3)
	defer bucket.Close()
	signer, e := newPresigner(bucket.URL, "grainlift-test", "auto", keyID, secret, false)
	if e != nil {
		t.Fatal(e)
	}
	s3.signer = signer

	statement := &storageStatement{rows: [][]byte{filled(300<<10, 'a'), filled(300<<10, 'b'), filled(300<<10, 'c')}}
	limits := DefaultLimits()
	limits.RequestBytes = 256 << 10
	limits.BatchBytes = 2 << 20 // larger than a request: only storage can carry it
	svc, e := NewServiceWithOptions(
		map[string]Target{"default": {Backend: storageBackend{statement}, Authorize: func(string) bool { return true }}},
		limits,
		ServiceOptions{ExternalStorage: &ExternalStorageConfig{
			Endpoint: bucket.URL, Bucket: "grainlift-test", Prefix: "grainlift/",
			AccessKeyID: keyID, SecretAccessKey: secret, ThresholdBytes: 64 << 10,
		}},
	)
	if e != nil {
		t.Fatal(e)
	}
	handler := svc.HTTPHandler(func(*http.Request) (*vgirpc.AuthContext, error) {
		return &vgirpc.AuthContext{Authenticated: true, Principal: "test", Domain: "test"}, nil
	})
	var uploadURLs atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, vgirpc.UploadURLMethod) {
			uploadURLs.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	inBucket := func(raw string) error {
		if strings.HasPrefix(raw, bucket.URL+"/grainlift-test/") {
			return nil
		}
		return errors.New("not the test bucket")
	}
	client, e := vgirpc.NewHttpClient(server.URL,
		vgirpc.WithClientProtocol(ProtocolName), vgirpc.WithClientProtocolVersion(ProtocolVersion),
		vgirpc.WithClientExternalResolution(&vgirpc.ExternalLocationConfig{URLValidator: inBucket}))
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
	caps, e := client.DiscoverCapabilities(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !caps.UploadURLSupport || caps.MaxUploadBytes != 256<<20 || caps.MaxRequestBytes != 256<<10 {
		t.Fatalf("capabilities %+v", caps)
	}

	sid := callTest[SessionResponse](t, client, "open_connection", OpenConnectionRequest{Target: "default"}).SessionID
	stid := callTest[StatementResponse](t, client, "new_statement", sessionParams{sid}).StatementID

	// Bind one 600 KiB batch: over the 256 KiB request limit.
	rawSchema, _ := schemaIPC(binarySchema())
	params := testInput(bindParams{sid, stid, rawSchema})
	defer params.Release()
	upload, e := client.OpenExchange(context.Background(), "bind_stream", params, vgirpc.ClientStreamSchema{Input: bindSchema, Output: okSchema})
	if e != nil {
		t.Fatal(e)
	}
	defer upload.Close()
	sent := [][]byte{filled(300<<10, 'x'), filled(300<<10, 'y')}
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
	if len(statement.bound) != 2 || !bytes.Equal(statement.bound[0], sent[0]) || !bytes.Equal(statement.bound[1], sent[1]) {
		t.Fatal("bound data changed")
	}

	// Read a 900 KiB result batch: over the 64 KiB threshold.
	result := callTest[ExecuteResponse](t, client, "execute", statementParams{sid, stid})
	read := testInput(readParams{sid, result.ResultID, 0})
	defer read.Release()
	stream, e := client.OpenProducer(context.Background(), "read_result", read, vgirpc.ClientStreamSchema{Output: binarySchema()})
	if e != nil {
		t.Fatal(e)
	}
	var got [][]byte
	for {
		b, ok, e := stream.Next(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		column := b.Batch.Column(0).(*array.Binary)
		for i := 0; i < column.Len(); i++ {
			got = append(got, bytes.Clone(column.Value(i)))
		}
		b.Release()
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows", len(got))
	}
	for i := range got {
		if !bytes.Equal(got[i], statement.rows[i]) {
			t.Fatalf("row %d changed", i)
		}
	}

	if n := uploadURLs.Load(); n != 1 {
		t.Fatalf("the client asked for %d upload URLs, want 1", n)
	}
	s3.mu.Lock()
	defer s3.mu.Unlock()
	// One PUT and GET each way: the client's upload, the service's result.
	if s3.puts != 2 || s3.gets != 2 || len(s3.objects) != 2 {
		t.Fatalf("bucket saw %d PUTs, %d GETs, %d objects; want 2 each", s3.puts, s3.gets, len(s3.objects))
	}
	for key := range s3.objects {
		if !strings.HasPrefix(key, "grainlift/") || !strings.HasSuffix(key, ".arrow") {
			t.Fatalf("object key %q", key)
		}
	}
}
