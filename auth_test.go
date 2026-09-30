// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPAuthenticatorValidation(t *testing.T) {
	for name, c := range map[string]struct {
		tokens    map[string]string
		anonymous string
	}{
		"nothing":            {nil, ""},
		"empty token":        {map[string]string{"": "alice"}, ""},
		"empty principal":    {map[string]string{"secret": ""}, ""},
		"shared principal":   {map[string]string{"secret": "public"}, "public"},
		"invalid anonymous":  {nil, "a\x00b"},
		"oversize anonymous": {nil, string(make([]byte, 1025))},
	} {
		if _, e := HTTPAuthenticator(c.tokens, c.anonymous); e == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestHTTPAuthenticatorRequests(t *testing.T) {
	anonymousOnly, e := HTTPAuthenticator(nil, "public")
	if e != nil {
		t.Fatal(e)
	}
	both, e := HTTPAuthenticator(map[string]string{"secret": "alice"}, "public")
	if e != nil {
		t.Fatal(e)
	}
	tokenOnly, e := HTTPAuthenticator(map[string]string{"secret": "alice"}, "")
	if e != nil {
		t.Fatal(e)
	}
	request := func(header ...string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/open_connection", nil)
		for _, h := range header {
			r.Header.Add("Authorization", h)
		}
		return r
	}
	for _, c := range []struct {
		name         string
		auth         vgirpc.AuthenticateFunc
		request      *http.Request
		domain, user string
	}{
		{"anonymous", anonymousOnly, request(), anonymousDomain, "public"},
		{"anonymous beside tokens", both, request(), anonymousDomain, "public"},
		{"token beside anonymous", both, request("Bearer secret"), tokenDomain, "alice"},
		{"token", tokenOnly, request("Bearer secret"), tokenDomain, "alice"},
		{"wrong token is not downgraded", both, request("Bearer wrong"), "", ""},
		{"empty header is not downgraded", both, request(""), "", ""},
		{"other scheme is not downgraded", both, request("Basic c2VjcmV0"), "", ""},
		{"token presented to anonymous-only", anonymousOnly, request("Bearer secret"), "", ""},
		{"missing token", tokenOnly, request(), "", ""},
	} {
		got, e := c.auth(c.request)
		if c.user == "" {
			if e == nil || got != nil {
				t.Errorf("%s: accepted", c.name)
			}
			continue
		}
		if e != nil || !got.Authenticated || got.Domain != c.domain || got.Principal != c.user {
			t.Errorf("%s: %+v %v", c.name, got, e)
		}
	}
}

func TestAnonymousAndTokenPrincipalsAreSeparateOverHTTP(t *testing.T) {
	authenticate, e := HTTPAuthenticator(map[string]string{"secret": "alice"}, "public")
	if e != nil {
		t.Fatal(e)
	}
	svc, e := NewService(map[string]Target{"default": {Backend: independentBackend{}, Authorize: func(string) bool { return true }}}, DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(svc.HTTPHandler(authenticate))
	client := func(options ...vgirpc.HttpClientOption) *vgirpc.HttpClient {
		options = append(options, vgirpc.WithClientProtocol(ProtocolName), vgirpc.WithClientProtocolVersion(ProtocolVersion))
		c, e := vgirpc.NewHttpClient(server.URL, options...)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	anonymous := client()
	alice := client(vgirpc.WithClientHeader("Authorization", "Bearer secret"))
	wrong := client(vgirpc.WithClientHeader("Authorization", "Bearer wrong"))
	t.Cleanup(func() {
		anonymous.Close()
		alice.Close()
		wrong.Close()
		server.Close()
		if e := svc.Close(); e != nil {
			t.Error(e)
		}
	})
	open := OpenConnectionRequest{Target: "default"}
	public := callTest[SessionResponse](t, anonymous, "open_connection", open).SessionID
	private := callTest[SessionResponse](t, alice, "open_connection", open).SessionID
	owners := map[string]bool{}
	for _, ss := range svc.sessions {
		owners[ss.owner] = true
	}
	if !owners[anonymousDomain+"\x00public"] || !owners[tokenDomain+"\x00alice"] {
		t.Fatalf("owners %v", owners)
	}
	assertWireDenied(t, alice, "new_statement", sessionParams{public})
	assertWireDenied(t, anonymous, "new_statement", sessionParams{private})
	assertWireDenied(t, wrong, "open_connection", open)
	for _, c := range []struct {
		client *vgirpc.HttpClient
		id     string
	}{{anonymous, public}, {alice, private}} {
		callTest[OkResponse](t, c.client, "close_connection", sessionParams{c.id})
	}
	if svc.ResourceCounts()["sessions"] != 0 {
		t.Fatal("sessions leaked")
	}
}
