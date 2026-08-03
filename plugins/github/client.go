package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the one netpolicy.Policy this process builds. Every
// request this plugin makes - minting an installation token, and every
// go-github call - goes through the *http.Client it produces, which is what
// makes the response-byte cap and the (denied by default) egress rules
// actual enforcement rather than a comment: netpolicy caps a response body
// in its RoundTripper itself, on every response regardless of status code,
// covering exactly the gap CLAUDE.md's own connect-go lesson names - a
// library's non-2xx error path that forgets to apply a caller-configured
// read limit.
//
// It is built once, at process start, the same reasoning
// flowstate-plugin-vcs's installEgressPolicy gives for doing the same.
var egressPolicy *netpolicy.Policy

func installEgressPolicy() error {
	policy, err := netpolicy.New(
		netpolicy.WithMaxResponseBytes(maxResponseBytes),
		netpolicy.WithTimeout(requestTimeout*time.Second),
	)
	if err != nil {
		return fmt.Errorf("building the egress policy: %w", err)
	}
	egressPolicy = policy
	return nil
}

// egressClient returns the governed client every network call in this
// plugin uses - the App JWT token-minting request in auth.go, and every
// go-github call built by newClient below.
func egressClient() *http.Client {
	return egressPolicy.Client()
}

// newClient builds a go-github client authenticated with token (which may be
// empty, for an unauthenticated request) against baseURL (which may be
// empty, meaning github.com).
//
// GitHub Enterprise Server support is exactly this: a different base URL.
// It stays governed by the same egress policy as github.com - a workflow
// author naming a GHES host is still naming a network destination this
// worker will connect to, and CLAUDE.md is explicit that a configurable
// base URL must not become a hole in egress governance the way it would if
// this plugin built a bare *http.Client for it instead of reusing
// egressClient.
func newClient(token, baseURL string) (*github.Client, error) {
	client := github.NewClient(egressClient())

	if token != "" {
		client = client.WithAuthToken(token)
	}

	if baseURL != "" {
		u := strings.TrimSuffix(baseURL, "/") + "/"
		var err error
		client, err = client.WithEnterpriseURLs(u, u)
		if err != nil {
			return nil, sdk.InvalidInput("base_url %q is not a valid API base: %v", baseURL, err)
		}
	}

	return client, nil
}

// tokenFromValue extracts a credential from a task's `token` input. See
// plugins/vcs/secrets.go's tokenFromValue for the fuller explanation this
// mirrors exactly: a literal is refused, an unset value means an
// unauthenticated request, and a secret reference must name this plugin's
// own scheme because that is the only one a plugin task can resolve without
// an RPC this repository's plugin protocol does not have.
func tokenFromValue(ctx context.Context, v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", nil

	case *flowstatev1.Value_SecretRef:
		ref := kind.SecretRef
		if ref.GetScheme() != secretScheme {
			return "", sdk.InvalidInput("token must be a %q secret reference; got scheme %q", secretScheme, ref.GetScheme())
		}
		resp, err := resolveSecret(ctx, sdk.SecretRequest{Scheme: ref.GetScheme(), Name: ref.GetName()})
		if err != nil {
			return "", err
		}
		return string(resp.Value), nil

	case *flowstatev1.Value_Literal:
		return "", sdk.InvalidInput(
			"token must be a secret reference (${secret('github:token')}), never a literal value; " +
				"a literal here would put a credential in the Flowfile and in workflow history")

	default:
		return "", sdk.InvalidInput("token cannot be a %T", kind)
	}
}
