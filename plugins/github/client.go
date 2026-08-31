package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/google/go-github/v75/github"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

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

// effectiveAPIBase resolves the API base a call actually reaches, after
// every piece of defaulting and operator configuration this plugin applies
// to a task's own base_url input - canonicalized by canonicalAPIBase, so
// two spellings of one endpoint ("https://x", "https://x/", and for
// github.com the empty string) come back as one string.
//
// It is deliberately its own function rather than a few lines inside
// newClient, because two callers need the same answer and must not each
// derive it: newClient, to point the client at it, and each paginated list
// task, to fold it into its cursor fingerprint (#694). A cursor names a
// (page, skip) position, which means nothing against a different server -
// and since #663 the effective base for an authenticated call is something
// this plugin derives from GITHUB_API_BASE_URL rather than something the
// caller passed, so a fingerprint over the *input* base_url describes the
// wrong thing: that input is empty for every authenticated call, whichever
// instance the call went to.
//
// A credential belongs only to the API origin the operator selected. In
// particular it is never attached to a destination selected solely by a
// workflow author, which is why an authenticated call naming any other
// base is refused here rather than quietly redirected.
func effectiveAPIBase(token, baseURL string) (string, error) {
	if token == "" {
		return canonicalAPIBase(baseURL), nil
	}

	configuredBaseURL := canonicalAPIBase(os.Getenv(envAPIBaseURL))
	if baseURL != "" && canonicalAPIBase(baseURL) != configuredBaseURL {
		return "", sdk.InvalidInput(
			"base_url %q cannot receive this plugin's credential; the operator configured %q with %s",
			baseURL, configuredBaseURL, envAPIBaseURL)
	}
	return configuredBaseURL, nil
}

// newClient builds a go-github client authenticated with token (which may be
// empty, for an unauthenticated request) against baseURL (which may be
// empty, meaning github.com), and reports the effective API base it was
// pointed at - the same value effectiveAPIBase computes, returned rather
// than left to a caller to recompute, so that a cursor fingerprint cannot
// end up describing an endpoint other than the one the request went to.
//
// GitHub Enterprise Server support is exactly this: a different base URL.
// It stays governed by the same egress policy as github.com - a workflow
// author naming a GHES host is still naming a network destination this
// worker will connect to, and CLAUDE.md is explicit that a configurable
// base URL must not become a hole in egress governance the way it would if
// this plugin built a bare *http.Client for it instead of reusing
// egressClient.
func newClient(token, baseURL string) (*github.Client, string, error) {
	base, err := effectiveAPIBase(token, baseURL)
	if err != nil {
		return nil, "", err
	}

	client := github.NewClient(egressClient())

	if token != "" {
		client = client.WithAuthToken(token)
	}

	// Only for a base that is actually not github.com's. Forcing the base above
	// makes baseURL non-empty for every authenticated call, including the
	// github.com ones that previously left it empty, and WithEnterpriseURLs sets
	// the upload endpoint to whatever it is handed — so routing github.com
	// through it would silently move uploads from uploads.github.com to
	// api.github.com. Nothing here uploads today, which is exactly why it would
	// have gone unnoticed until something did.
	if base != defaultAPIBaseURL {
		u := base + "/"
		client, err = client.WithEnterpriseURLs(u, u)
		if err != nil {
			return nil, "", sdk.InvalidInput("base_url %q is not a valid API base: %v", baseURL, err)
		}
	}

	return client, base, nil
}

// tokenFromValue extracts the host-resolved credential from token. The
// manifest requires any supplied token to be a whole secret reference, but the
// host replaces it with a string before invoking this task. The reference may
// name github's compatibility provider or any other configured provider.
func tokenFromValue(_ context.Context, v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", nil
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			return "", sdk.InvalidInput("token must resolve to a string")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed("token reached this task still holding a secret reference; the host must resolve declared secret_inputs before invoking the plugin")
	default:
		return "", sdk.InvalidInput("token cannot be a %T", kind)
	}
}
