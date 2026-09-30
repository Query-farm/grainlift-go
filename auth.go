// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"errors"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"net/http"
)

// Authentication domains used by HTTPAuthenticator. Handles and continuation
// tokens belong to a domain and principal, so anonymous state can never be
// resumed by a token principal of the same name.
const (
	tokenDomain     = "grainlift"
	anonymousDomain = "grainlift.anonymous"
)

// HTTPAuthenticator returns an authenticator for Service.HTTPHandler that
// accepts bearer tokens, mapped to their principals, and optionally anonymous
// requests.
//
// Anonymous access is for services that are safe to expose without
// credentials, such as read-only data. When anonymousPrincipal is not empty,
// requests without an Authorization header act as that principal; all
// anonymous clients share it, so grant it only public, read-only capabilities.
// A request that presents credentials which do not match a token is rejected,
// never downgraded to anonymous. The anonymous principal must differ from
// every token principal. Pass an empty anonymousPrincipal to require a token.
func HTTPAuthenticator(tokens map[string]string, anonymousPrincipal string) (vgirpc.AuthenticateFunc, error) {
	if len(tokens) == 0 && anonymousPrincipal == "" {
		return nil, errors.New("grainlift: configure bearer tokens, anonymous access, or both")
	}
	if !validText(anonymousPrincipal) || len(anonymousPrincipal) > 1024 {
		return nil, errors.New("grainlift: invalid anonymous principal")
	}
	contexts := make(map[string]*vgirpc.AuthContext, len(tokens))
	for token, principal := range tokens {
		if !validKey(token) || !validKey(principal) || len(principal) > 1024 {
			return nil, errors.New("grainlift: invalid bearer token or principal")
		}
		if principal == anonymousPrincipal {
			return nil, errors.New("grainlift: the anonymous principal must differ from every token principal")
		}
		contexts[token] = &vgirpc.AuthContext{Domain: tokenDomain, Authenticated: true, Principal: principal}
	}
	bearer := vgirpc.BearerAuthenticateStatic(contexts)
	return func(r *http.Request) (*vgirpc.AuthContext, error) {
		if anonymousPrincipal != "" && len(r.Header.Values("Authorization")) == 0 {
			return &vgirpc.AuthContext{Domain: anonymousDomain, Authenticated: true, Principal: anonymousPrincipal}, nil
		}
		if len(contexts) == 0 {
			return nil, &vgirpc.RpcError{Type: "ValueError", Message: "Authentication required"}
		}
		matched, e := bearer(r)
		if e != nil {
			return nil, e
		}
		authenticated := *matched
		return &authenticated, nil
	}, nil
}
