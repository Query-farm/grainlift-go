// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Package cli hosts one Grainlift target on loopback for development, choosing
// the transport and authentication from command-line flags. Give each worker
// its own command by calling Run from its main function.
//
// Production deployments should configure grainlift.Service, its HTTPHandler
// or ServeStreams directly with their own credentials, limits and listeners.
package cli

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

const (
	tokenVariable      = "GRAINLIFT_TOKEN"
	anonymousPrincipal = "anonymous"
	developer          = "developer"
)

// Options configures Run. The zero value parses os.Args and requires a token.
type Options struct {
	// Args are the command-line arguments; nil means os.Args[1:].
	Args []string
	// Description is shown by --help.
	Description string
	// Auth is the default for --auth: "token" (the default) or "anonymous".
	Auth string
	// Stdout and Stderr receive status lines; nil means os.Stdout and os.Stderr.
	Stdout, Stderr io.Writer
}

type config struct {
	host, auth                           string
	port                                 int
	tlsCert, tlsKey, clientCA, clientURI string
	requestBytes, batchBytes             int
	storage                              grainlift.ExternalStorageConfig
}

// Run serves one target on loopback until ctx is done or the process receives
// SIGINT or SIGTERM. The target's Authorize callback is required as usual.
//
// Flags: --host http (default) or mtls; --port (default 8080; 0 picks a free
// port); --auth token or anonymous (default Options.Auth); and, for mTLS,
// --tls-cert, --tls-key, --client-ca and --client-uri.
//
// HTTP accepts requests up to --max-request-bytes (default 2 MiB), bound
// batches as large as a request, and result batches up to --max-batch-bytes
// (default 1 MiB). With --storage-endpoint and
// --storage-bucket it sends larger requests and results through an
// S3-compatible bucket (HTTP only; see grainlift.ExternalStorageConfig):
// --storage-region (default auto), --storage-prefix, --storage-virtual-hosted,
// --storage-url-ttl, --storage-threshold-bytes and
// --storage-max-upload-bytes; credentials come from AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY. Raise --max-batch-bytes to return result rows larger
// than a request.
//
// HTTP authenticates the bearer token in GRAINLIFT_TOKEN as the "developer"
// principal; when it is unset in token mode, a random token is generated and
// printed to stderr for the client to export. With --auth anonymous, clients
// may also connect without a token as the "anonymous" principal; use it only
// for targets that are safe to expose publicly, such as read-only data. The
// mTLS host instead authorizes the client certificate whose URI SAN equals
// --client-uri, as "developer".
//
// Run returns nil after --help and after a clean shutdown.
func Run(ctx context.Context, name string, target grainlift.Target, options Options) error {
	stdout, stderr := options.Stdout, options.Stderr
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	args := options.Args
	if args == nil {
		args = os.Args[1:]
	}
	c, e := parse(args, options, stderr)
	if errors.Is(e, flag.ErrHelp) {
		return nil
	}
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	limits := grainlift.DefaultLimits()
	limits.RequestBytes, limits.BatchBytes = c.requestBytes, c.batchBytes
	var serviceOptions grainlift.ServiceOptions
	if c.storage.Endpoint != "" {
		serviceOptions.ExternalStorage = &c.storage
	}
	svc, e := grainlift.NewServiceWithOptions(map[string]grainlift.Target{name: target}, limits, serviceOptions)
	if e != nil {
		return e
	}
	defer svc.Close()
	if c.host == "mtls" {
		return serveMTLS(ctx, svc, name, c, stdout)
	}
	return serveHTTP(ctx, svc, name, c, stdout, stderr)
}

func parse(args []string, options Options, stderr io.Writer) (config, error) {
	var c config
	auth := options.Auth
	if auth == "" {
		auth = "token"
	}
	if auth != "token" && auth != "anonymous" {
		return c, errors.New("cli: Options.Auth must be token or anonymous")
	}
	program := filepath.Base(os.Args[0])
	flags := flag.NewFlagSet(program, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&c.host, "host", "http", "`transport`: http (loopback HTTP) or mtls (verified TCP/mTLS)")
	flags.IntVar(&c.port, "port", 8080, "loopback `port`")
	flags.StringVar(&c.auth, "auth", auth, "HTTP access: token requires a bearer token; anonymous also allows clients without one")
	flags.StringVar(&c.tlsCert, "tls-cert", "", "mTLS: server certificate chain (PEM `file`)")
	flags.StringVar(&c.tlsKey, "tls-key", "", "mTLS: server private key (PEM `file`)")
	flags.StringVar(&c.clientCA, "client-ca", "", "mTLS: CA that issues client certificates (PEM `file`)")
	flags.StringVar(&c.clientURI, "client-uri", "", "mTLS: authorized client certificate URI SAN")
	defaults := grainlift.DefaultLimits()
	flags.IntVar(&c.requestBytes, "max-request-bytes", defaults.RequestBytes, "HTTP: largest request and inline response in `bytes`")
	flags.IntVar(&c.batchBytes, "max-batch-bytes", defaults.BatchBytes, "largest bound or result Arrow batch in `bytes` (above half of --max-request-bytes needs --storage-endpoint)")
	flags.StringVar(&c.storage.Endpoint, "storage-endpoint", "", "HTTP: S3-compatible `URL` for large requests and results, e.g. https://<account>.r2.cloudflarestorage.com (credentials from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)")
	flags.StringVar(&c.storage.Bucket, "storage-bucket", "", "storage: `bucket` name")
	flags.StringVar(&c.storage.Region, "storage-region", "auto", "storage: signing `region`")
	flags.StringVar(&c.storage.Prefix, "storage-prefix", "", "storage: object key `prefix`")
	flags.BoolVar(&c.storage.VirtualHostedStyle, "storage-virtual-hosted", false, "storage: address objects as https://<bucket>.<endpoint host>/")
	flags.DurationVar(&c.storage.URLTTL, "storage-url-ttl", 15*time.Minute, "storage: how long presigned URLs stay valid")
	flags.Int64Var(&c.storage.ThresholdBytes, "storage-threshold-bytes", 1<<20, "storage: result batches of at least this many `bytes` go to the bucket")
	flags.Int64Var(&c.storage.MaxUploadBytes, "storage-max-upload-bytes", 256<<20, "storage: largest request a client may upload, in `bytes`")
	flags.Usage = func() {
		fmt.Fprintf(stderr, "Usage: %s [flags]\n", program)
		if options.Description != "" {
			fmt.Fprintf(stderr, "\n%s\n", options.Description)
		}
		fmt.Fprintln(stderr, "\nFlags:")
		flags.PrintDefaults()
	}
	if e := flags.Parse(args); e != nil {
		return c, e
	}
	set := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { set[f.Name] = true })
	invalid := func(message string) (config, error) {
		fmt.Fprintf(stderr, "%s: %s\n", program, message)
		flags.Usage()
		return c, errors.New(message)
	}
	switch {
	case flags.NArg() != 0:
		return invalid("unexpected arguments")
	case c.host != "http" && c.host != "mtls":
		return invalid("--host must be http or mtls")
	case c.auth != "token" && c.auth != "anonymous":
		return invalid("--auth must be token or anonymous")
	case c.port < 0 || c.port > 65535:
		return invalid("--port must be between 0 and 65535")
	case c.host == "mtls" && (c.tlsCert == "" || c.tlsKey == "" || c.clientCA == "" || c.clientURI == ""):
		return invalid("--host mtls requires --tls-cert, --tls-key, --client-ca and --client-uri")
	case c.requestBytes <= 0 || c.batchBytes <= 0:
		return invalid("--max-request-bytes and --max-batch-bytes must be positive")
	case (c.storage.Endpoint == "") != (c.storage.Bucket == ""):
		return invalid("--storage-endpoint and --storage-bucket go together")
	case c.storage.Endpoint != "" && c.host != "http":
		return invalid("--storage-endpoint applies to --host http only")
	}
	if c.storage.Endpoint == "" {
		for _, name := range []string{"storage-region", "storage-prefix", "storage-virtual-hosted", "storage-url-ttl", "storage-threshold-bytes", "storage-max-upload-bytes"} {
			if set[name] {
				return invalid("--" + name + " requires --storage-endpoint")
			}
		}
	}
	return c, nil
}

func listen(port int) (net.Listener, error) {
	return net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
}

func httpAccess(auth string, stdout, stderr io.Writer) (vgirpc.AuthenticateFunc, error) {
	token := os.Getenv(tokenVariable)
	if auth == "anonymous" {
		fmt.Fprintf(stdout, "Anonymous access enabled: clients connect without a token as %q\n", anonymousPrincipal)
		tokens := map[string]string{}
		if token != "" {
			tokens[token] = developer
		}
		return grainlift.HTTPAuthenticator(tokens, anonymousPrincipal)
	}
	if token == "" {
		var raw [24]byte
		if _, e := rand.Read(raw[:]); e != nil {
			return nil, e
		}
		token = base64.RawURLEncoding.EncodeToString(raw[:])
		fmt.Fprintf(stderr, "%s is not set; generated a token for this run:\n", tokenVariable)
		fmt.Fprintf(stderr, "    export %s=%s\n", tokenVariable, token)
	}
	return grainlift.HTTPAuthenticator(map[string]string{token: developer}, "")
}

func serveHTTP(ctx context.Context, svc *grainlift.Service, name string, c config, stdout, stderr io.Writer) error {
	authenticate, e := httpAccess(c.auth, stdout, stderr)
	if e != nil {
		return e
	}
	listener, e := listen(c.port)
	if e != nil {
		return e
	}
	server := &http.Server{
		Handler:           svc.HTTPHandler(authenticate),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	fmt.Fprintf(stdout, "Grainlift target %q listening on http://%s\n", name, listener.Addr())
	if c.storage.Endpoint != "" {
		fmt.Fprintf(stdout, "Large requests and results go through bucket %q at %s\n", c.storage.Bucket, c.storage.Endpoint)
	}
	select {
	case e := <-served:
		return e
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

func serveMTLS(ctx context.Context, svc *grainlift.Service, name string, c config, stdout io.Writer) error {
	certificate, e := tls.LoadX509KeyPair(c.tlsCert, c.tlsKey)
	if e != nil {
		return fmt.Errorf("cli: load server certificate: %w", e)
	}
	ca, e := os.ReadFile(c.clientCA)
	if e != nil {
		return fmt.Errorf("cli: read client CA: %w", e)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("cli: no certificates in client CA")
	}
	listener, e := listen(c.port)
	if e != nil {
		return e
	}
	server, e := svc.ServeStreams(listener, grainlift.StreamOptions{
		Mode:      "mtls",
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: roots, MinVersion: tls.VersionTLS12},
		AuthenticateTLS: func(state tls.ConnectionState) (string, error) {
			if len(state.PeerCertificates) == 0 {
				return "", errors.New("missing client certificate")
			}
			for _, uri := range state.PeerCertificates[0].URIs {
				if uri.String() == c.clientURI {
					return developer, nil
				}
			}
			return "", errors.New("unauthorized client certificate")
		},
	})
	if e != nil {
		_ = listener.Close()
		return e
	}
	fmt.Fprintf(stdout, "Grainlift target %q listening on tls+tcp://%s\n", name, listener.Addr())
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Close(shutdown)
}
