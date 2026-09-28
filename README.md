# Grainlift Go

Build Go workers that applications access through the ordinary [Grainlift ADBC driver](https://github.com/Query-farm/grainlift). This toolkit implements the Grainlift 0.4.0 server contract: 31 typed methods, authenticated handles, pull-based Arrow results, and backend interfaces for the full ADBC operation surface.

## Status

HTTP, HTTPS, loopback TCP, mTLS TCP, and Iroh use the declared, published
[vgi-rpc-go v0.30.0](https://github.com/Query-farm/vgi-rpc-go/releases/tag/v0.30.0).
The raw adapters require its network-safe serving entrypoint and fail closed
with older VGI versions.

This is an initial SDK. Backend capabilities depend on the implementation you supply; unsupported operations return ADBC `NOT_IMPLEMENTED`. Correctness and interoperability testing have passed, but sustained production workloads and real database backends still need deployment-specific validation. See [validation evidence](VALIDATION.md).

## Quickstart

Requires Go 1.26 or newer; the example below uses OpenSSL to generate a token. Start with the [companion synthetic worker](https://github.com/Query-farm/grainlift-hello-world-go), using an explicit workspace because the example does not yet pin a released SDK version:

```sh
mkdir grainlift-go-workspace
cd grainlift-go-workspace
git clone https://github.com/Query-farm/grainlift-go.git
git clone https://github.com/Query-farm/grainlift-hello-world-go.git
go work init ./grainlift-go ./grainlift-hello-world-go

cd grainlift-hello-world-go
go build -o grainlift-hello-world-go .
export GRAINLIFT_HELLO_TOKEN="$(openssl rand -hex 32)"
./grainlift-hello-world-go --transport http --port 8080
```

Keep that terminal open. The worker prints its endpoint as one JSON line and stops on Enter, stdin EOF, or SIGINT/SIGTERM. Its README includes an ordinary ADBC client example and the other transport options.

To work on the SDK alone:

```sh
git clone https://github.com/Query-farm/grainlift-go.git
cd grainlift-go
go test -race -count=1 ./...
```

## Implementing a backend

Implement `Backend.Open` to create a connection and `Connection.NewStatement` to create independent statements. Embed `UnimplementedConnection` and `UnimplementedStatement` to inherit explicit `NOT_IMPLEMENTED` responses, then override the operations your backend supports.

| Backend surface | SDK behavior |
| --- | --- |
| SQL and schema execution | Typed responses and server-owned result cursors |
| Prepare, bind, and bind-stream | Typed requests, bounded uploads, and retained parameter lifetimes |
| Transactions and typed options | Backend dispatch; no transaction emulation |
| Metadata and statistics | Typed control records and pull-based Arrow results |
| Partitioned execution | Principal- and target-bound signed partition tokens |
| Substrait and ingestion | Backend hooks; ingestion uses standard statement options and execute-update |
| Errors | ADBC status, SQLSTATE, vendor code, and ordered binary details |

The interfaces are in [backend.go](backend.go); service configuration is in [service.go](service.go). A complete small backend is in the [example repository](https://github.com/Query-farm/grainlift-hello-world-go/blob/main/main.go).

`Statement.Execute` transfers ownership of an Arrow `array.RecordReader` to the SDK. Results stay server-side and are pulled one batch at a time, with at most one replay batch retained. Readers are released on exhaustion, close, error, cancellation, expiry, and shutdown.

`Bind` and `BindStream` lend their batch or reader to the callback; retain it if the backend needs it afterward. Parameter batches remain until query replacement or statement close. Ordinary callbacks serialize per connection. `Cancel` callbacks may run concurrently and must be thread-safe and nonblocking; context cancellation is cooperative.

Construct `NewService(targets, DefaultLimits())`, supply an explicit authorization callback for every target, and host `service.HTTPHandler(authenticate)` with a configured `http.Server`. Stop the listener before calling `Service.Close`.

## Transports and authentication

| Transport | Hosting API | Identity | Current dependency status |
| --- | --- | --- | --- |
| HTTP | `HTTPHandler` | Your authenticated HTTP identity | Published dependency |
| HTTPS | `HTTPHandler` with standard Go TLS | Verified server TLS plus HTTP authentication | Published dependency |
| TCP | `ServeStreams`, mode `tcp` | Explicit shared local principal; numeric loopback only | Published dependency |
| mTLS TCP | `ServeStreams`, mode `mtls` | Verified client chain plus principal callback, or leaf certificate SHA-256 fingerprint | Published dependency |
| Iroh | `ServeStreams`, mode `iroh-bridge` | Allowlisted authenticated endpoint ID | Published dependency |

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
| Binding input and retained buffers | 64 MiB cumulative |
| SQL | 64 KiB |
| Idle handle expiry | 5 minutes |
| Lock / shutdown wait | 5 seconds |

HTTP admission is bounded to twice the session limit. Raw connections default to four times the session limit, a five-second handshake timeout, and 30-second no-progress read/write deadlines. A raw application RPC has a `RequestBytes + BindBytes` wire-byte budget, including binding frames and stream ticks; completion resets the budget for socket reuse.

Batch accounting includes retained backing buffers. These bounds do not sandbox all upstream Arrow metadata allocations before reads. Use process memory/CPU limits, listener timeouts, and verified TLS outside the trusted local boundary. Nested values use typed Arrow records; Arrow schema and batch fields retain their standard representations. VGI owns transport compression. Separate pre-allocation rejection of nested compressed IPC is not certified.

Sessions, transactions, and live cursors are process-local and require affinity. Shutdown requests cancellation and releases late-finishing callbacks' handles, but a noncooperative backend may require process termination.

Only structured `*Error` diagnostics are returned verbatim; other backend errors are redacted. Never put credentials, SQL values, or other secrets in client-visible diagnostics or logs.

## Testing

```sh
go test -race -count=1 ./...
go vet ./...
test -z "$(gofmt -l *.go)"
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
