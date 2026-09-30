// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package cli

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	grainlift "github.com/Query-farm/grainlift-go"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) { b.mu.Lock(); defer b.mu.Unlock(); return b.b.Write(p) }
func (b *buffer) String() string              { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

type noBackend struct{}

func (noBackend) Open(context.Context, string, grainlift.OpenConnectionRequest) (grainlift.Connection, error) {
	return nil, errors.New("unused")
}

var target = grainlift.Target{Backend: noBackend{}, Authorize: func(string) bool { return true }}
var listening = regexp.MustCompile(`Grainlift target "hello" listening on (\S+)://(\S+)`)

// start runs the host on an ephemeral port and returns its address and output.
func start(t *testing.T, auth string, args ...string) (string, *buffer, *buffer) {
	t.Helper()
	stdout, stderr := &buffer{}, &buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, "hello", target, Options{Args: append([]string{"--port", "0"}, args...), Auth: auth, Stdout: stdout, Stderr: stderr})
	}()
	t.Cleanup(func() {
		cancel()
		if e := <-done; e != nil {
			t.Error(e)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := listening.FindStringSubmatch(stdout.String()); m != nil {
			return m[2], stdout, stderr
		}
		select {
		case e := <-done:
			t.Fatalf("host exited: %v\n%s", e, stderr.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("host did not start: %s", stderr.String())
	return "", nil, nil
}

// authorized reports whether the HTTP host accepted the request's credentials.
func authorized(t *testing.T, address, authorization string) bool {
	t.Helper()
	request, e := http.NewRequest(http.MethodPost, "http://"+address+"/open_connection", strings.NewReader(""))
	if e != nil {
		t.Fatal(e)
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, e := http.DefaultClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	_ = response.Body.Close()
	return response.StatusCode != http.StatusUnauthorized
}

func TestFlagValidation(t *testing.T) {
	for _, args := range [][]string{
		{"--host", "granian"},
		{"--auth", "none"},
		{"--port", "70000"},
		{"--host", "mtls", "--tls-cert", "cert.pem"},
		{"extra"},
		{"--unknown"},
	} {
		stderr := &buffer{}
		if e := Run(context.Background(), "hello", target, Options{Args: args, Stderr: stderr}); e == nil || !strings.Contains(stderr.String(), "Usage:") {
			t.Errorf("%v accepted: %v", args, e)
		}
	}
	if e := Run(context.Background(), "hello", target, Options{Args: []string{}, Auth: "none"}); e == nil {
		t.Error("invalid default auth accepted")
	}
	stderr := &buffer{}
	if e := Run(context.Background(), "hello", target, Options{Args: []string{"--help"}, Description: "Hello service", Stderr: stderr}); e != nil {
		t.Fatal(e)
	}
	for _, text := range []string{"Hello service", "-host", "-port", "-auth", "-client-uri", "(default 8080)"} {
		if !strings.Contains(stderr.String(), text) {
			t.Errorf("help lacks %q", text)
		}
	}
}

func TestTokenModeGeneratesToken(t *testing.T) {
	t.Setenv(tokenVariable, "")
	address, stdout, stderr := start(t, "")
	match := regexp.MustCompile(`export GRAINLIFT_TOKEN=(\S+)`).FindStringSubmatch(stderr.String())
	if match == nil || strings.Contains(stdout.String(), "Anonymous") {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
	if authorized(t, address, "") || authorized(t, address, "Bearer wrong") || !authorized(t, address, "Bearer "+match[1]) {
		t.Fatal("token mode accepted the wrong credentials")
	}
}

func TestTokenModeUsesEnvironment(t *testing.T) {
	t.Setenv(tokenVariable, "exported-token")
	address, _, stderr := start(t, "anonymous", "--auth", "token")
	if strings.Contains(stderr.String(), "export") {
		t.Fatal("generated a token although one was exported")
	}
	if authorized(t, address, "") || !authorized(t, address, "Bearer exported-token") {
		t.Fatal("token mode accepted the wrong credentials")
	}
}

func TestAnonymousMode(t *testing.T) {
	t.Setenv(tokenVariable, "exported-token")
	address, stdout, stderr := start(t, "anonymous")
	if !strings.Contains(stdout.String(), `Anonymous access enabled: clients connect without a token as "anonymous"`) || stderr.String() != "" {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
	if !authorized(t, address, "") || !authorized(t, address, "Bearer exported-token") || authorized(t, address, "Bearer wrong") {
		t.Fatal("anonymous mode accepted the wrong credentials")
	}
	t.Setenv(tokenVariable, "")
	address, _, _ = start(t, "token", "--auth", "anonymous")
	if !authorized(t, address, "") || authorized(t, address, "Bearer exported-token") {
		t.Fatal("anonymous-only mode accepted the wrong credentials")
	}
}

type issuer struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
}

var serial int64

func issue(t *testing.T, parent *issuer, template *x509.Certificate) (*issuer, []byte, []byte) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	serial++
	template.SerialNumber = big.NewInt(serial)
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	signer, signerKey := template, key
	if parent != nil {
		signer, signerKey = parent.certificate, parent.key
	}
	der, e := x509.CreateCertificate(rand.Reader, template, signer, &key.PublicKey, signerKey)
	if e != nil {
		t.Fatal(e)
	}
	certificate, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	encodedKey, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	return &issuer{certificate, key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encodedKey})
}

func authority(t *testing.T, name string) (*issuer, []byte) {
	t.Helper()
	ca, certificate, _ := issue(t, nil, &x509.Certificate{Subject: pkix.Name{CommonName: name}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign})
	return ca, certificate
}

func client(t *testing.T, ca *issuer, uri string) tls.Certificate {
	t.Helper()
	parsed, e := url.Parse(uri)
	if e != nil {
		t.Fatal(e)
	}
	_, certificate, key := issue(t, ca, &x509.Certificate{Subject: pkix.Name{CommonName: "client"}, URIs: []*url.URL{parsed}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature})
	pair, e := tls.X509KeyPair(certificate, key)
	if e != nil {
		t.Fatal(e)
	}
	return pair
}

func TestMTLSAuthorizesClientURI(t *testing.T) {
	directory := t.TempDir()
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if e := os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
		return path
	}
	serverCA, serverCAPEM := authority(t, "server CA")
	clientCA, clientCAPEM := authority(t, "client CA")
	otherCA, _ := authority(t, "other CA")
	_, serverCert, serverKey := issue(t, serverCA, &x509.Certificate{Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature})
	address, _, _ := start(t, "token", "--host", "mtls",
		"--tls-cert", write("server.pem", serverCert), "--tls-key", write("server-key.pem", serverKey),
		"--client-ca", write("clients-ca.pem", clientCAPEM), "--client-uri", "spiffe://example.org/client")
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(serverCAPEM)
	// accepted reports whether the server keeps an established connection open.
	accepted := func(certificate tls.Certificate) bool {
		conn, e := tls.Dial("tcp", address, &tls.Config{RootCAs: roots, ServerName: "localhost", Certificates: []tls.Certificate{certificate}})
		if e != nil {
			return false
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, e = conn.Read(make([]byte, 1))
		var timeout net.Error
		return errors.As(e, &timeout) && timeout.Timeout()
	}
	if !accepted(client(t, clientCA, "spiffe://example.org/client")) {
		t.Fatal("authorized client rejected")
	}
	if accepted(client(t, clientCA, "spiffe://example.org/other")) || accepted(client(t, otherCA, "spiffe://example.org/client")) {
		t.Fatal("unauthorized client accepted")
	}
}
