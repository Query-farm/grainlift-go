# Grainlift Go

Build Go workers that applications access through the ordinary [Grainlift ADBC driver](https://github.com/Query-farm/grainlift). This toolkit implements the Grainlift 0.5.0 server contract: 31 typed methods, authenticated handles, pull-based Arrow results, and backend interfaces for the full ADBC operation surface.

## Status

HTTP, HTTPS, loopback TCP, mTLS TCP, and Iroh use the declared, published
[vgi-rpc-go v0.30.0](https://github.com/Query-farm/vgi-rpc-go/releases/tag/v0.30.0).
The raw adapters require its network-safe serving entrypoint and fail closed
with older VGI versions.

This is an initial SDK. Backend capabilities depend on the implementation you supply; unsupported operations return ADBC `NOT_IMPLEMENTED`. Correctness and interoperability testing have passed, but sustained production workloads and real database backends still need deployment-specific validation. See [validation evidence](VALIDATION.md).

## Quickstart

Requires Go 1.26 or newer. Start with the [hello-world worker](https://github.com/Query-farm/grainlift-hello-world-go), which serves three queries without a database:

```sh
git clone https://github.com/Query-farm/grainlift-hello-world-go.git
cd grainlift-hello-world-go
go run .
```

It accepts anonymous clients because it serves only public, read-only data. Its README shows how to query it from Haybarn/DuckDB SQL and from a Go ADBC client, and the other hosting options.

To work on the SDK alone:

```sh
git clone https://github.com/Query-farm/grainlift-go.git
cd grainlift-go
go test -race -count=1 ./...
```

## Implementing a backend

Implement `Backend.Open` to create a connection and `Connection.NewStatement` to create independent statements. Embed `UnimplementedConnection` and `UnimplementedStatement` to inherit explicit `NOT_IMPLEMENTED` responses, then override the operations your backend supports.

Implement `StatisticsCapabilities` to declare statistics support per connection.
Its `StatisticsSupported()` and `StatisticNamesSupported()` methods return
`*bool`: nil means unknown, false lets clients return `NOT_IMPLEMENTED` locally,
and true keeps remote dispatch. The flags are returned by `open_connection`;
table schemas remain fresh. Upgrade clients and servers together for protocol 0.5.0.

| Backend surface | SDK behavior |
| --- | --- |
| SQL and schema execution | Typed responses and server-owned result cursors |
| Prepare, bind, and bind-stream | Typed requests, bounded uploads, and retained parameter lifetimes |
| Transactions and typed options | Backend dispatch; no transaction emulation |
| Metadata and statistics | Typed control records and pull-based Arrow results |
| Partitioned execution | Principal- and target-bound signed partition tokens |
| Substrait and ingestion | Backend hooks; ingestion uses standard statement options and execute-update |
| Errors | ADBC status, SQLSTATE, vendor code, and ordered binary details |

The interfaces are in [backend.go](backend.go); service configuration is in [service.go](service.go). A complete small backend is in the [example repository](https://github.com/Query-farm/grainlift-hello-world-go/blob/main/hello/hello.go).

`Statement.Execute` transfers ownership of an Arrow `array.RecordReader` to the SDK. Results stay server-side and are pulled one batch at a time, with at most one replay batch retained. Readers are released on exhaustion, close, error, cancellation, expiry, and shutdown.

### Result producers

A result can instead carry its state in the continuation token. Implement `ResultProducer` on a pointer to a struct whose exported fields are the complete resumable state; `Produce` returns the next batch and advances the state, or a nil batch at the end. Register the type with `RegisterResultProducer` in an `init` function and return `NewProducerResult(schema, producer)`.

The service gob-encodes the initial state at execution. Over HTTP each fetch decodes the state from the sealed VGI continuation token, produces one batch, validates its schema and size, and seals the advanced state into the next token, so no reader or replay batch stays in server memory. A retried fetch re-produces the previous batch from its token; older tokens fail with `INVALID_ARGUMENT`. Only registered types are decoded, and encoded state is bounded by `Limits.ProducerStateBytes` (64 KiB by default). Raw transports carry the same encoded state in memory. `EncodeResultProducer` and `DecodeResultProducer` let tests check that a producer resumes identically. Keep sockets, files and database cursors in a reader instead.

`Bind` and `BindStream` lend their batch or reader to the callback; retain it if the backend needs it afterward. Parameter batches remain until query replacement or statement close. Ordinary callbacks serialize per connection. `Cancel` callbacks may run concurrently and must be thread-safe and nonblocking; context cancellation is cooperative.

Construct `NewService(targets, DefaultLimits())`, supply an explicit authorization callback for every target, and host `service.HTTPHandler(authenticate)` with a configured `http.Server`. Stop the listener before calling `Service.Close`.

For development, [`cli.Run`](cli/cli.go) hosts one target on loopback from a worker's own `main`, with `--host http|mtls`, `--port`, `--auth token|anonymous` and the mTLS certificate flags. HTTP authenticates the bearer token in `GRAINLIFT_TOKEN`, generating and printing one when it is unset in token mode.

## Transports and authentication

| Transport | Hosting API | Identity | Current dependency status |
| --- | --- | --- | --- |
| HTTP | `HTTPHandler` | Your authenticated HTTP identity | Published dependency |
| HTTPS | `HTTPHandler` with standard Go TLS | Verified server TLS plus HTTP authentication | Published dependency |
| TCP | `ServeStreams`, mode `tcp` | Explicit shared local principal; numeric loopback only | Published dependency |
| mTLS TCP | `ServeStreams`, mode `mtls` | Verified client chain plus principal callback, or leaf certificate SHA-256 fingerprint | Published dependency |
| Iroh | `ServeStreams`, mode `iroh-bridge` | Allowlisted authenticated endpoint ID | Published dependency |

`HTTPAuthenticator(tokens, anonymousPrincipal)` builds an HTTP authenticator from bearer tokens mapped to principals. Anonymous access is opt-in: with a non-empty `anonymousPrincipal`, requests without an `Authorization` header act as that shared principal in a separate authentication domain, so its handles and continuation tokens cannot be used by a token principal of the same name. A presented token that does not match is rejected, never downgraded to anonymous, and the anonymous principal must differ from every token principal. Enable it only for public, read-only targets.

Sessions and child handles belong to an authentication domain and principal. Target authorization is separate from authentication. Server-configured database and connection options are authoritative; caller-supplied keys require explicit allowlists. Partition tokens additionally bind the target, expiry, and server secret.

Iroh uses raw QUIC through a separately installed VGI Iroh bridge. Its upstream must be a private Unix socket in a non-symlink directory owned by the service UID with mode 0700; restrict the socket to mode 0600. The bridge and same-UID processes are trusted. The adapter is Unix-only.

### Upstream transport contract

VGI v0.30.0 provides `ServeNetworkWithContext`, which disables local
shared-memory negotiation before any network attachment or dispatch. It also
validates exactly one row in nonempty parameter records, retains the canonical
empty representation for no-parameter methods, and advertises dynamic header
schemas accurately. CI requires these capabilities; raw integration cases are
no longer skipped with the declared dependency. The generic pipe-serving API
must not be used for TCP, mTLS, or Iroh adapters.

## Limits and deployment

| Default | Limit |
| --- | --- |
| Sessions | 64 |
| Statements / results | 32 each per session |
| Partition descriptors | 1,024 |
| HTTP request / response body | 2 MiB |
| Arrow batch / schema | 1 MiB |
| Encoded result producer state | 64 KiB |
| Binding input and retained buffers | 64 MiB cumulative |
| SQL | 64 KiB |
| Idle handle expiry | 5 minutes |
| Lock / shutdown wait | 5 seconds |

HTTP admission is bounded to twice the session limit. Raw connections default to four times the session limit, a five-second handshake timeout, and 30-second no-progress read/write deadlines. A raw application RPC has a `RequestBytes + BindBytes` wire-byte budget, including binding frames and stream ticks; completion resets the budget for socket reuse.

Batch accounting includes retained backing buffers. These bounds do not sandbox all upstream Arrow metadata allocations before reads. Use process memory/CPU limits, listener timeouts, and verified TLS outside the trusted local boundary. Nested values use typed Arrow records; Arrow schema and batch fields retain their standard representations. VGI owns transport compression. Separate pre-allocation rejection of nested compressed IPC is not certified.

Sessions, transactions, and live cursors are process-local and require affinity. Shutdown requests cancellation and releases late-finishing callbacks' handles, but a noncooperative backend may require process termination.

Only structured `*Error` diagnostics are returned verbatim; other backend errors are redacted. Never put credentials, SQL values, or other secrets in client-visible diagnostics or logs.

### Large requests and results: object storage

Over HTTP a request is limited to `Limits.RequestBytes`, and every result
batch to `Limits.BatchBytes`, which must fit a response (a request, less twice
`ProducerStateBytes` and up to 64 KiB of framing). A bound batch may be as
large as a request: clients split parameters to fit the advertised request
limit, which is all they know. With
`ServiceOptions.ExternalStorage` the HTTP handler uses an S3-compatible bucket
(AWS S3, Cloudflare R2, MinIO) for
[VGI-RPC external locations](https://vgi-rpc.query.farm/):

- A client whose request is over the limit asks for an upload URL
  (`POST /__upload_url__/init`), PUTs the request to the bucket, and sends only
  a pointer, up to `MaxUploadBytes`.
- A result batch of at least `ThresholdBytes` is stored in the bucket and the
  client is sent a URL to fetch it.
- A bound batch may then be as large as `MaxUploadBytes`, and `BatchBytes`
  may exceed a response (up to half of `MaxUploadBytes`), so rows larger than
  an HTTP request work in both directions.

```go
limits := grainlift.DefaultLimits()
limits.BatchBytes = 64 << 20
svc, err := grainlift.NewServiceWithOptions(targets, limits, grainlift.ServiceOptions{
	ExternalStorage: &grainlift.ExternalStorageConfig{
		Endpoint: "https://<account-id>.r2.cloudflarestorage.com", // or https://s3.<region>.amazonaws.com
		Bucket:   "grainlift-exchange",
		Prefix:   "grainlift/",
		// Region "auto"; credentials from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY;
		// URLTTL 15m; ThresholdBytes 1 MiB; MaxUploadBytes 256 MiB.
	},
})
```

The service presigns the URLs itself (AWS Signature Version 4, no AWS SDK), so
clients need no storage credentials, and it fetches only objects in its own
bucket. Raw streams (`ServeStreams`) are unaffected: they have no request limit.
The service never deletes objects; give the bucket a lifecycle rule that
expires them. Browser clients PUT and GET the bucket directly, so it also needs
a CORS rule allowing `PUT` and `GET` (with `Content-Type` and
`Content-Encoding`) from the page's origin. The `cli` host takes the same
settings as `--storage-endpoint`, `--storage-bucket` and related flags, with
`--max-request-bytes` and `--max-batch-bytes`.

## Testing

```sh
go test -race -count=1 ./...
go vet ./...
test -z "$(gofmt -l .)"
```

Set `GRAINLIFT_CONTRACT=/path/to/grainlift/validation/conformance/contract.json` to require exact parity with the authoritative Rust-exported protocol fixture. Tests also inspect live VGI method schemas.

The shared [native ADBC conformance suite](https://github.com/Query-farm/grainlift/tree/main/validation/conformance) exercises real driver-manager connections, lifecycle, ownership, replay, protocol rejection, and shutdown. [VALIDATION.md](VALIDATION.md) separates published-dependency results from patched-upstream results and explains the remaining release gates.

The HTTP wire lifecycle tests use two independently authenticated clients. They exercise cross-principal session and partition ownership, 64 rounds of session/result/statement cleanup at the two-session limit, and idle expiry of an abandoned session with a live result while another client remains active. They use a synthetic backend; shared transaction visibility and writer contention require a stateful backend.

## Documentation and license

- [Runnable Go worker and client example](https://github.com/Query-farm/grainlift-hello-world-go)
- [Grainlift driver, protocol, and operator documentation](https://github.com/Query-farm/grainlift)
- [VGI Go transport](https://github.com/Query-farm/vgi-rpc-go)
- [Recorded validation](VALIDATION.md)

Licensed under [Apache-2.0](LICENSE).
