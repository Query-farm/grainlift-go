// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Limits bound handles, incoming data, retained batches and idle resources.
type Limits struct {
	Sessions, Statements, Results, Partitions     int
	RequestBytes, BatchBytes, BindBytes, SQLBytes int
	IdleTimeout, LockTimeout                      time.Duration
	// ProducerStateBytes bounds encoded ResultProducer state carried in a
	// continuation token. It must not exceed RequestBytes/4.
	ProducerStateBytes int
}

func DefaultLimits() Limits {
	return Limits{
		Sessions: 64, Statements: 32, Results: 32, Partitions: 1024,
		RequestBytes: 2 << 20, BatchBytes: 1 << 20, BindBytes: 64 << 20, SQLBytes: 64 << 10,
		IdleTimeout: 5 * time.Minute, LockTimeout: 5 * time.Second,
		ProducerStateBytes: 64 << 10,
	}
}

// Target defines immutable server options and explicitly allowed caller keys.
// Authorize must explicitly permit the authenticated principal.
type Target struct {
	Backend                                          Backend
	Authorize                                        func(principal string) bool
	DatabaseOptions, ConnectionOptions               map[string]OptionValue
	AllowedDatabaseOptions, AllowedConnectionOptions map[string]bool
}
type session struct {
	guard      sync.RWMutex
	gate       chan struct{}
	owner      string
	targetName string
	target     Target
	conn       Connection
	statements map[string]*statement
	results    map[string]*result
	touched    time.Time
	closed     bool
}
type statement struct {
	backend  Statement
	resultID string
	binding  *binding
}
type result struct {
	query       *QueryResult
	schema      *arrow.Schema
	sequence    int64
	last        arrow.RecordBatch
	done        bool
	statementID string
	// producer is the encoded initial ResultProducer state; later states
	// travel only in ResultCursor.
	producer []byte
}
type binding struct {
	id         string
	schema     *arrow.Schema
	batches    []arrow.RecordBatch
	bytes      int
	retained   int64
	sequence   int64
	lastDigest [32]byte
	finished   bool
	stream     bool
}

// Service owns authenticated, process-local ADBC handles. It must be closed
// after its HTTP listener stops accepting requests.
type Service struct {
	mu       sync.Mutex
	sessions map[string]*session
	targets  map[string]Target
	limits   Limits
	secret   [32]byte
	stop     chan struct{}
	stopped  chan struct{}
	closed   bool
	rpc      *vgirpc.Server
	// httpRPC serves HTTP; it is rpc unless external storage is configured,
	// which only HTTP uses.
	httpRPC *vgirpc.Server
	storage *externalStorage
}

// ServiceOptions configures optional Service features.
type ServiceOptions struct {
	// ExternalStorage, when set, sends HTTP requests over Limits.RequestBytes
	// through presigned upload URLs and result batches over its threshold
	// through presigned download URLs in an S3-compatible bucket. It applies
	// to HTTPHandler only; raw streams have no request limit to get around.
	// With it, bound batches may be as large as its MaxUploadBytes, and
	// Limits.BatchBytes may exceed what fits a response (result rows larger
	// than an HTTP request), up to half of its MaxUploadBytes.
	ExternalStorage *ExternalStorageConfig
}

// NewService creates a Service without optional features.
func NewService(targets map[string]Target, limits Limits) (*Service, error) {
	return NewServiceWithOptions(targets, limits, ServiceOptions{})
}

// NewServiceWithOptions creates a Service with optional features.
func NewServiceWithOptions(targets map[string]Target, limits Limits, options ServiceOptions) (*Service, error) {
	if limits.Sessions <= 0 || limits.Statements <= 0 || limits.Results <= 0 || limits.Partitions <= 0 || limits.RequestBytes <= 0 || limits.BatchBytes <= 0 || limits.BindBytes <= 0 || limits.SQLBytes <= 0 || limits.IdleTimeout <= 0 || limits.LockTimeout <= 0 || limits.ProducerStateBytes <= 0 {
		return nil, failure("invalid_arguments", "Invalid service limits")
	}
	var storage *externalStorage
	if options.ExternalStorage != nil {
		var e error
		if storage, e = newExternalStorage(*options.ExternalStorage); e != nil {
			return nil, e
		}
	}
	// A result batch travels inline, in a response no larger than a request,
	// next to its continuation state (sealed, so allow twice its bound) and
	// IPC framing, unless storage can carry what does not fit.
	maxBatch := limits.RequestBytes - 2*limits.ProducerStateBytes - min(responseFramingBytes, limits.RequestBytes/16)
	if storage != nil {
		maxBatch = int(min(storage.maxUploadBytes/2, int64(^uint(0)>>1)))
	}
	if limits.RequestBytes < 4096 || limits.BatchBytes > maxBatch || limits.ProducerStateBytes > limits.RequestBytes/4 {
		return nil, failure("invalid_arguments", "Response limit must allow batch framing")
	}
	s := &Service{sessions: map[string]*session{}, targets: map[string]Target{}, limits: limits, stop: make(chan struct{}), stopped: make(chan struct{}), storage: storage}
	if _, err := rand.Read(s.secret[:]); err != nil {
		return nil, err
	}
	for name, t := range targets {
		if !validText(name) || name == "" || t.Backend == nil || t.Authorize == nil {
			return nil, failure("invalid_arguments", "Invalid target")
		}
		t.DatabaseOptions = cloneOptions(t.DatabaseOptions)
		t.ConnectionOptions = cloneOptions(t.ConnectionOptions)
		t.AllowedDatabaseOptions = cloneAllowed(t.AllowedDatabaseOptions)
		t.AllowedConnectionOptions = cloneAllowed(t.AllowedConnectionOptions)
		for _, opts := range []map[string]OptionValue{t.DatabaseOptions, t.ConnectionOptions} {
			for k, v := range opts {
				if !validKey(k) || v.validate() != nil {
					return nil, failure("invalid_arguments", "Invalid configured option")
				}
			}
		}
		s.targets[name] = t
	}
	var e error
	if s.rpc, e = s.newRPC(); e != nil {
		return nil, e
	}
	s.httpRPC = s.rpc
	if storage != nil {
		if s.httpRPC, e = s.newRPC(); e != nil {
			return nil, e
		}
		s.httpRPC.SetExternalLocation(storage.location)
	}
	go s.reap()
	return s, nil
}

// newRPC builds a VGI server dispatching to s.
func (s *Service) newRPC() (*vgirpc.Server, error) {
	rpc := vgirpc.NewServer()
	rpc.SetServiceName(ProtocolName)
	rpc.SetProtocolVersion(ProtocolVersion)
	rpc.SetImplementation(s)
	rpc.SetDispatchHook(validationHook{})
	s.register(rpc)
	if e := vgirpc.RegisterReflection(rpc); e != nil {
		return nil, e
	}
	return rpc, nil
}
func cloneAllowed(m map[string]bool) map[string]bool {
	n := map[string]bool{}
	for k, v := range m {
		n[k] = v
	}
	return n
}
func cloneOptions(m map[string]OptionValue) map[string]OptionValue {
	n := map[string]OptionValue{}
	for k, v := range m {
		n[k] = v.clone()
	}
	return n
}
func (v OptionValue) clone() OptionValue {
	if v.StringValue != nil {
		x := *v.StringValue
		v.StringValue = &x
	}
	if v.IntValue != nil {
		x := *v.IntValue
		v.IntValue = &x
	}
	if v.DoubleValue != nil {
		x := *v.DoubleValue
		v.DoubleValue = &x
	}
	if v.BytesValue != nil {
		x := append([]byte{}, (*v.BytesValue)...)
		v.BytesValue = &x
	}
	return v
}
func validText(s string) bool { return !strings.ContainsRune(s, 0) }
func validKey(s string) bool  { return s != "" && validText(s) }
func (v OptionValue) validate() error {
	n := 0
	selected := false
	if v.StringValue != nil {
		n++
		selected = v.Kind == "string"
	}
	if v.BytesValue != nil {
		n++
		selected = v.Kind == "bytes"
	}
	if v.IntValue != nil {
		n++
		selected = v.Kind == "int"
	}
	if v.DoubleValue != nil {
		n++
		selected = v.Kind == "double"
	}
	if n != 1 || !selected {
		return failure("invalid_arguments", "Invalid option discriminator")
	}
	return nil
}
func options(client []NamedOption, configured map[string]OptionValue, allowed map[string]bool) ([]NamedOption, error) {
	seen := map[string]bool{}
	out := make([]NamedOption, 0, len(client)+len(configured))
	for _, o := range client {
		if !validKey(o.Key) || seen[o.Key] || o.Value.validate() != nil {
			return nil, failure("invalid_arguments", "Invalid or duplicate option")
		}
		if _, ok := configured[o.Key]; ok {
			return nil, failure("unauthorized", "Server option is authoritative")
		}
		if !allowed[o.Key] {
			return nil, failure("unauthorized", "Caller option is not allowed")
		}
		seen[o.Key] = true
		out = append(out, o)
	}
	for k, v := range configured {
		out = append(out, NamedOption{k, v.clone()})
	}
	return out, nil
}
func principal(call *vgirpc.CallContext) (string, error) {
	if call == nil || call.Auth == nil || !call.Auth.Authenticated || call.Auth.Principal == "" {
		return "", failure("unauthenticated", "Authentication required")
	}
	return call.Auth.Domain + "\x00" + call.Auth.Principal, nil
}
func backendError(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		if e.valid() {
			return e
		}
		return failure("internal", "Backend returned invalid diagnostics")
	}
	if errors.Is(err, context.Canceled) {
		return failure("cancelled", "Request cancelled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return failure("timeout", "Request deadline exceeded")
	}
	return failure("internal", "Backend operation failed")
}
func identifier() string {
	var b [24]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("secure random unavailable")
	}
	return hex.EncodeToString(b[:])
}
func (s *Service) acquire(ctx context.Context, call *vgirpc.CallContext, id string) (*session, func(), error) {
	if e, ok := ctx.Value(validationKey{}).(error); ok {
		return nil, nil, e
	}
	owner, e := principal(call)
	if e != nil {
		return nil, nil, e
	}
	s.mu.Lock()
	ss := s.sessions[id]
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, nil, failure("invalid_state", "Service is closed")
	}
	if ss == nil || ss.owner != owner {
		return nil, nil, failure("not_found", "Unknown connection")
	}
	timer := time.NewTimer(s.limits.LockTimeout)
	defer timer.Stop()
	select {
	case ss.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, backendError(ctx.Err())
	case <-timer.C:
		return nil, nil, failure("timeout", "Connection is busy")
	}
	unlock := func() { s.release(id, ss) }
	if ss.closed {
		unlock()
		return nil, nil, failure("not_found", "Closed connection")
	}
	ss.touched = time.Now()
	return ss, unlock, nil
}
func (s *Service) open(ctx context.Context, call *vgirpc.CallContext, p OpenConnectionRequest) (SessionResponse, error) {
	if e, ok := ctx.Value(validationKey{}).(error); ok {
		return SessionResponse{}, e
	}
	owner, e := principal(call)
	if e != nil {
		return SessionResponse{}, e
	}
	target, ok := s.targets[p.Target]
	if !ok || !target.Authorize(call.Auth.Principal) {
		return SessionResponse{}, failure("unauthorized", "Target is not authorized")
	}
	for _, list := range [][]NamedOption{p.DatabaseOptions, p.ConnectionOptions} {
		for _, option := range list {
			if authoritative(target, option.Key) {
				return SessionResponse{}, failure("unauthorized", "Server option is authoritative")
			}
		}
	}
	p.DatabaseOptions, e = options(p.DatabaseOptions, target.DatabaseOptions, target.AllowedDatabaseOptions)
	if e != nil {
		return SessionResponse{}, e
	}
	p.ConnectionOptions, e = options(p.ConnectionOptions, target.ConnectionOptions, target.AllowedConnectionOptions)
	if e != nil {
		return SessionResponse{}, e
	}
	ss := &session{gate: make(chan struct{}, 1), owner: owner, targetName: p.Target, target: target, statements: map[string]*statement{}, results: map[string]*result{}, touched: time.Now()}
	id := identifier()
	ss.gate <- struct{}{}
	defer func() { s.release(id, ss) }()
	s.mu.Lock()
	if s.closed || len(s.sessions) >= s.limits.Sessions {
		s.mu.Unlock()
		return SessionResponse{}, failure("invalid_state", "Connection limit reached")
	}
	s.sessions[id] = ss
	s.mu.Unlock()
	conn, e := target.Backend.Open(ctx, call.Auth.Principal, p)
	if e != nil || conn == nil {
		ss.closed = true
		s.mu.Lock()
		delete(s.sessions, id)
		s.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		if e == nil {
			e = failure("internal", "Backend returned no connection")
		}
		return SessionResponse{}, backendError(e)
	}
	ss.guard.Lock()
	ss.conn = conn
	ss.guard.Unlock()
	response := SessionResponse{SessionID: id}
	if capabilities, ok := conn.(StatisticsCapabilities); ok {
		response.StatisticsSupported = capabilities.StatisticsSupported()
		response.StatisticNamesSupported = capabilities.StatisticNamesSupported()
	}
	return response, nil
}
func (s *Service) release(id string, ss *session) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed && !ss.closed {
		_ = s.closeSession(id, ss)
	}
	<-ss.gate
}
func (s *Service) closeSession(id string, ss *session) error {
	ss.guard.Lock()
	defer ss.guard.Unlock()
	ss.closed = true
	for rid := range ss.results {
		s.closeResult(ss, rid)
	}
	for _, st := range ss.statements {
		_ = st.backend.Close()
		clearBinding(st)
	}
	ss.statements = map[string]*statement{}
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	if ss.conn != nil {
		return backendError(ss.conn.Close())
	}
	return nil
}
func clearBinding(st *statement) {
	if st.binding != nil {
		for _, b := range st.binding.batches {
			b.Release()
		}
		st.binding = nil
	}
}
func (s *Service) closeResult(ss *session, id string) {
	r := ss.results[id]
	if r == nil {
		return
	}
	if r.query.Reader != nil {
		r.query.Reader.Release()
		r.query.Reader = nil
	}
	if r.last != nil {
		r.last.Release()
		r.last = nil
	}
	delete(ss.results, id)
	if st := ss.statements[r.statementID]; st != nil && st.resultID == id {
		st.resultID = ""
	}
}
func (s *Service) reap() {
	defer close(s.stopped)
	period := s.limits.IdleTimeout / 2
	if period < time.Nanosecond {
		period = time.Nanosecond
	}
	if period > time.Second {
		period = time.Second
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			snapshot := map[string]*session{}
			for id, ss := range s.sessions {
				snapshot[id] = ss
			}
			s.mu.Unlock()
			for id, ss := range snapshot {
				select {
				case ss.gate <- struct{}{}:
					if !ss.closed && time.Since(ss.touched) >= s.limits.IdleTimeout {
						_ = s.closeSession(id, ss)
					}
					<-ss.gate
				default:
				}
			}
		}
	}
}

// Close waits at most LockTimeout for each busy session. Backends must honor
// cancellation; a noncooperative native backend requires process isolation.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stop)
	snapshot := map[string]*session{}
	for id, ss := range s.sessions {
		snapshot[id] = ss
	}
	s.mu.Unlock()
	<-s.stopped
	ctx, cancel := context.WithTimeout(context.Background(), s.limits.LockTimeout)
	defer cancel()
	var result error
	for id, ss := range snapshot {
		ss.guard.RLock()
		if ss.conn != nil {
			_ = ss.conn.Cancel(ctx)
		}
		ss.guard.RUnlock()
		select {
		case ss.gate <- struct{}{}:
			if !ss.closed {
				_ = s.closeSession(id, ss)
			}
			<-ss.gate
		case <-ctx.Done():
			result = failure("timeout", "Backend did not stop before shutdown deadline")
		}
	}
	return result
}

// ResourceCounts returns aggregate ownership counts without exposing handles.
// Call after Close for a complete shutdown report.
func (s *Service) ResourceCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]int{"sessions": len(s.sessions)}
}
func (s *Service) cancel(ctx context.Context, c *vgirpc.CallContext, sid, stid string) (OkResponse, error) {
	if e, ok := ctx.Value(validationKey{}).(error); ok {
		return okResponseError(e)
	}
	owner, e := principal(c)
	if e != nil {
		return ok(e)
	}
	s.mu.Lock()
	ss := s.sessions[sid]
	s.mu.Unlock()
	if ss == nil || ss.owner != owner {
		return ok(failure("not_found", "Unknown connection"))
	}
	ss.guard.RLock()
	defer ss.guard.RUnlock()
	if ss.closed || ss.conn == nil {
		return ok(failure("not_found", "Unknown connection"))
	}
	if stid == "" {
		return ok(backendError(ss.conn.Cancel(ctx)))
	}
	st := ss.statements[stid]
	if st == nil {
		return ok(failure("not_found", "Unknown statement"))
	}
	return ok(backendError(st.backend.Cancel(ctx)))
}
func okResponseError(e error) (OkResponse, error) { return OkResponse{}, e }

// HTTPHandler creates the authenticated, bounded VGI HTTP transport, using
// ServiceOptions.ExternalStorage when configured.
func (s *Service) HTTPHandler(auth vgirpc.AuthenticateFunc) http.Handler {
	h := vgirpc.NewHttpServer(s.httpRPC)
	h.SetProtocolName(ProtocolName)
	h.SetAuthenticate(auth)
	h.SetMaxRequestBytes(int64(s.limits.RequestBytes))
	h.SetMaxResponseBytes(int64(s.limits.RequestBytes))
	if s.storage != nil {
		h.SetUploadURLProvider(s.storage.uploads)
		h.SetMaxUploadBytes(s.storage.maxUploadBytes)
	}
	slots := make(chan struct{}, s.limits.Sessions*2)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "Service busy", http.StatusServiceUnavailable)
			return
		}
		defer func() {
			if recover() != nil {
				http.Error(w, "Invalid protocol request", http.StatusBadRequest)
			}
		}()
		h.ServeHTTP(w, r)
	})
}
func schemaIPC(schema *arrow.Schema) ([]byte, error) {
	if schema == nil {
		return nil, failure("invalid_data", "Missing result schema")
	}
	var b bytes.Buffer
	w := ipc.NewWriter(&b, ipc.WithSchema(schema))
	if e := w.Close(); e != nil {
		return nil, e
	}
	data := b.Bytes()
	if len(data) < 8 {
		return nil, failure("invalid_data", "Invalid schema")
	}
	n := int(binary.LittleEndian.Uint32(data[4:8]))
	if n > len(data)-8 {
		return nil, failure("invalid_data", "Invalid schema framing")
	}
	return append([]byte{}, data[8:8+n]...), nil
}
func parseSchema(data []byte) (*arrow.Schema, error) {
	framed := make([]byte, 8+len(data))
	binary.LittleEndian.PutUint32(framed, ^uint32(0))
	binary.LittleEndian.PutUint32(framed[4:], uint32(len(data)))
	copy(framed[8:], data)
	r, e := ipc.NewReader(bytes.NewReader(framed))
	if e != nil {
		return nil, failure("invalid_data", "Invalid Arrow schema")
	}
	defer r.Release()
	return r.Schema(), nil
}
func (s *Service) addResult(ss *session, statementID string, q *QueryResult) (ExecuteResponse, error) {
	if q == nil || q.Reader == nil {
		return ExecuteResponse{}, failure("invalid_data", "Missing result reader")
	}
	fail := func(e error) (ExecuteResponse, error) { q.Reader.Release(); return ExecuteResponse{}, e }
	if q.RowsAffected != nil && *q.RowsAffected < -1 {
		return fail(failure("invalid_data", "Invalid affected row count"))
	}
	if len(ss.results) >= s.limits.Results {
		return fail(failure("invalid_state", "Result limit reached"))
	}
	data, e := schemaIPC(q.Reader.Schema())
	if e != nil {
		return fail(e)
	}
	if len(data) > s.limits.BatchBytes {
		return fail(failure("invalid_data", "Schema limit exceeded"))
	}
	var producer []byte
	if q.Producer != nil {
		if producer, e = s.encodeProducer(q.Producer); e != nil {
			return fail(e)
		}
	}
	schema := q.Reader.Schema()
	if q.Producer != nil {
		// Producer results resume from encoded state; the in-memory reader is unused.
		q.Reader.Release()
		q.Reader = nil
	}
	id := identifier()
	ss.results[id] = &result{query: q, schema: schema, statementID: statementID, producer: producer}
	if st := ss.statements[statementID]; st != nil {
		st.resultID = id
	}
	return ExecuteResponse{id, q.RowsAffected, data}, nil
}
func recordBytes(b arrow.RecordBatch) int64 {
	var size int64
	// Buffers decoded from IPC are slices of one message body: count each
	// underlying allocation once, not once per buffer that shares it.
	seen := map[*memory.Buffer]bool{}
	var add func(arrow.ArrayData)
	add = func(d arrow.ArrayData) {
		for _, b := range d.Buffers() {
			if b != nil {
				for b.Parent() != nil {
					b = b.Parent()
				}
				if !seen[b] {
					seen[b] = true
					size += int64(b.Cap())
				}
			}
		}
		for _, c := range d.Children() {
			add(c)
		}
		if d.DataType().ID() == arrow.DICTIONARY {
			add(d.Dictionary())
		}
	}
	for _, c := range b.Columns() {
		add(c.Data())
	}
	return size
}

// responseFramingBytes reserves room in a response for IPC framing, the
// schema message and result metadata around one batch (a sixteenth of
// smaller requests).
const responseFramingBytes = 64 << 10

// bindBatchBytes bounds one bound batch. A client splits parameters to fit
// a request (it knows only the advertised request limit), so any batch that
// fits a request is accepted even when BatchBytes, which bounds results, is
// smaller; with object storage, so is any batch up to the upload limit.
func (s *Service) bindBatchBytes() int {
	n := max(s.limits.BatchBytes, s.limits.RequestBytes)
	if s.storage != nil {
		n = max(n, int(min(s.storage.maxUploadBytes, int64(^uint(0)>>1))))
	}
	return n
}

// ResultCursor is the read_result stream state. For ResultProducer results,
// Producer holds the encoded producer, sealed into each HTTP continuation token.
type ResultCursor struct {
	SessionID, ResultID string
	Sequence            int64
	Producer            []byte
}

func init() { vgirpc.RegisterStateType(&ResultCursor{}); vgirpc.RegisterStateType(&BindCursor{}) }
func (c *ResultCursor) Produce(ctx context.Context, out *vgirpc.OutputCollector, call *vgirpc.CallContext) error {
	s := serviceFor(ctx, call)
	ss, unlock, e := s.acquire(ctx, call, c.SessionID)
	if e != nil {
		return e
	}
	defer unlock()
	if c.Producer != nil {
		b, e := s.nextProduced(ctx, ss, c)
		if e != nil {
			return e
		}
		if b == nil {
			return out.Finish()
		}
		return out.Emit(b)
	}
	r := ss.results[c.ResultID]
	if r == nil || r.producer != nil {
		return failure("not_found", "Unknown result")
	}
	if c.Sequence == r.sequence-1 && r.last != nil {
		r.last.Retain()
		c.Sequence++
		return out.Emit(r.last)
	}
	if c.Sequence != r.sequence {
		return failure("invalid_state", "Invalid result sequence")
	}
	if r.done {
		return out.Finish()
	}
	if !r.query.Reader.Next() {
		e := backendError(r.query.Reader.Err())
		r.query.Reader.Release()
		r.query.Reader = nil
		if r.last != nil {
			r.last.Release()
			r.last = nil
		}
		r.done = true
		if e != nil {
			s.closeResult(ss, c.ResultID)
			return e
		}
		return out.Finish()
	}
	b := r.query.Reader.RecordBatch()
	if !b.Schema().Equal(r.schema) || recordBytes(b) > int64(s.limits.BatchBytes) {
		s.closeResult(ss, c.ResultID)
		return failure("invalid_data", "Invalid or oversized result batch")
	}
	if r.last != nil {
		r.last.Release()
	}
	b.Retain()
	r.last = b
	b.Retain()
	r.sequence++
	c.Sequence++
	return out.Emit(b)
}
func (c *ResultCursor) OnCancel(ctx context.Context, call *vgirpc.CallContext) error {
	s := serviceFor(ctx, call)
	ss, u, e := s.acquire(ctx, call, c.SessionID)
	if e != nil {
		return e
	}
	defer u()
	s.closeResult(ss, c.ResultID)
	return nil
}

type partitionToken struct {
	Owner   string
	Target  string
	Expires int64
	Payload []byte
}

func (s *Service) sealPartition(owner, target string, payload []byte) ([]byte, error) {
	data, e := json.Marshal(partitionToken{owner, target, time.Now().Add(s.limits.IdleTimeout).Unix(), payload})
	if e != nil {
		return nil, e
	}
	mac := hmac.New(sha256.New, s.secret[:])
	mac.Write(data)
	out := []byte(base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
	if len(out) > s.limits.BatchBytes {
		return nil, failure("invalid_data", "Partition descriptor limit exceeded")
	}
	return out, nil
}
func (s *Service) openPartition(owner, target string, raw []byte) ([]byte, error) {
	deny := failure("unauthorized", "Invalid partition descriptor")
	if len(raw) > s.limits.BatchBytes {
		return nil, deny
	}
	parts := strings.Split(string(raw), ".")
	if len(parts) != 2 {
		return nil, deny
	}
	data, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil || base64.RawURLEncoding.EncodeToString(data) != parts[0] {
		return nil, deny
	}
	tag, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil || base64.RawURLEncoding.EncodeToString(tag) != parts[1] {
		return nil, deny
	}
	mac := hmac.New(sha256.New, s.secret[:])
	mac.Write(data)
	if !hmac.Equal(mac.Sum(nil), tag) {
		return nil, deny
	}
	var p partitionToken
	if json.Unmarshal(data, &p) != nil || p.Owner != owner || p.Target != target || p.Expires <= time.Now().Unix() {
		return nil, deny
	}
	return p.Payload, nil
}
func authoritative(t Target, key string) bool {
	_, db := t.DatabaseOptions[key]
	_, conn := t.ConnectionOptions[key]
	return db || conn
}
