# Validation

The Go SDK and example were tested on EC2 Linux ARM64 on 2026-09-26/27. The standalone SDK used Go 1.26.0; development workspaces used Go 1.26.8. All compilation, race tests, and native-driver tests ran remotely. These are correctness checks, not throughput measurements or sustained production soak results.

## Dependency configurations

The module declares published `vgi-rpc-go v0.28.0`. HTTP/HTTPS work with that dependency. TCP, mTLS, and Iroh intentionally fail closed because the required network-safe serving API has not been released.

The raw transport matrix used an explicit Go workspace with the [prepared upstream network-safety branch](https://github.com/Query-farm/vgi-rpc-go/tree/grainlift-network-safety), including parameter row-count and dynamic-header reflection fixes. No local module replacement is committed. SDK tests in that workspace required both `GRAINLIFT_REQUIRE_NETWORK=1` and `GRAINLIFT_REQUIRE_CANONICAL_HEADER=1`.

## SDK and example checks

| Check | Published transport | Explicit patched workspace |
| --- | --- | --- |
| SDK race tests, top-level | 24 passed; 6 raw integration cases skipped | 29 passed; 1 published-only fail-closed case skipped |
| SDK race tests, including subtests | 34 passed; 6 skipped | 39 passed; 1 skipped |
| Example race tests | 4 passed; 2 bridge integration cases skipped | 6 passed |
| SDK and example `go vet` | Passed | Passed |
| SDK and example formatting | Passed | Passed |

Additional checks passed:

- Full upstream Go race suite and `go vet ./...`.
- Focused network and parameter-decoder tests with the upstream leak-check build.
- Workflow validation with actionlint.
- Exact protocol fixture parity and live registered method schema checks; canonical header parity requires the patched dependency.

Coverage includes positive optional backend hooks, typed int64 options, cancellation, shutdown, handle ownership, persistent raw pulls, disconnect cleanup, malformed named records over HTTP and TCP, socket admission and byte-budget boundaries, idle/handshake timeouts, panic containment, exact mTLS identities, and bridge startup failure/nonzero exit/idempotent cleanup.

Counts differ from other SDKs because table-driven subtests and fixtures are grouped differently. Shared native ADBC behaviors are the comparison to use.

## Native ADBC interoperability

The language-neutral suite loaded the compiled Grainlift driver through a real ADBC driver manager.

| Transport | Published VGI Go | Explicit patched workspace |
| --- | --- | --- |
| HTTP | 60 passed | 60 passed |
| HTTPS | 61 passed | 61 passed |
| TCP | Intentionally unavailable | 52 passed |
| mTLS TCP | Intentionally unavailable | 61 passed |
| Iroh raw QUIC | Intentionally unavailable | 12 passed |
| Total | 121 passed | 246 passed |

After the upstream parameter decoder was adjusted to retain canonical empty-method requests, the SDK/example race suites were repeated and eight focused native TCP query, lifecycle, and malformed-record cases passed. The final full upstream suite also passed.

The checked Iroh bridge was VGI Rust v0.27.3. TLS tests used verified server/client certificates, separate permitted identities, and an unauthorized certificate. These development fixtures do not substitute for validating a deployment's own trust roots and identity policy.

## Evidence

Machine-readable artifacts, source hashes, and native test logs are checked into the [Grainlift transport results directory](https://github.com/Query-farm/grainlift/tree/main/validation/conformance/results/ec2-20260927-transports).

Key files:

- `sdk-transports-published.json`, `sdk-transports-patched.json`
- `example-transports-published.json`, `example-transports-patched.json`
- `go-final-{http,https,tcp,mtls,iroh}.xml`
- `go-published-final-{http,https}.xml`
- `upstream-network-{race,vet,leakcheck}-final.log`
- `source-sha256.json`

The remote working directory was `/home/ec2-user/Development/grainlift-go-port-20260926`. Its separately named `hello-go-published` and `hello-go-patched` binaries prevent confusion between dependency configurations. The focused follow-up run is recorded there as `tcp-empty-params-regression.xml`.

Earlier HTTP-only development evidence remains in [the initial results directory](https://github.com/Query-farm/grainlift/tree/main/validation/conformance/results/ec2-20260926). The tables above describe the subsequent transport implementation.

## Remaining release gates

Publish and integrate the reviewed upstream transport safety, decoder, and reflection fixes before distributing a raw-capable SDK. Tagged SDK/module releases, sustained real-backend workloads, deployment-specific certificate policies, and process-level resource isolation remain separate work. Repository publication and passing CI do not establish production readiness.
