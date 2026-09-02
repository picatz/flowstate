package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/google/go-github/v75/github"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressPolicy is the deployment's egress policy, granted to this process at
// launch. Every request this plugin makes - minting an installation token, and
// every go-github call - goes through the *http.Client it produces, which is
// what makes the response-byte cap and the egress rules actual enforcement
// rather than a comment: netpolicy caps a response body in its RoundTripper
// itself, on every response regardless of status code, covering exactly the gap
// CLAUDE.md's own connect-go lesson names - a library's non-2xx error path that
// forgets to apply a caller-configured read limit.
//
// The rules are the deployment's, not this plugin's. Until #1323 this process
// built its own safe default, so a GitHub Enterprise operator could not
// authorize their private API network through --egress-policy, and a deny rule
// they wrote did not reach a github.* task at all. What stays this plugin's own
// are the two transport bounds: a paginated API response is not the shape
// max_response_bytes is sized for - see [sdk.EgressPolicyWithBounds].
//
// It is taken once, at process start, the same reasoning
// flowstate-plugin-vcs's installEgressPolicy gives for doing the same. Nil
// means the grant could not be used, and [egressRefusal] says why.
var egressClientOnce *http.Client

// egressRefusal is why there is no policy, kept so the task boundary can refuse
// with the SDK's message - which names the environment variable and the worker
// that sets it - rather than with a denial of its own invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant.
//
// An unusable grant does not stop the process. A plugin launched to be asked
// what it can do - `flow plugins`, `flow tasks`, a catalog build - has no use
// for a policy, and refusing to start would turn one bad policy file into a
// plugin the host cannot even describe. Every path that would reach the network
// goes through [egressClient], which refuses instead.
func installEgressPolicy() {
	// The SDK's client rather than policy.Client(): every request this plugin
	// makes carries a credential in an Authorization header (an App JWT, or a
	// token go-github attaches), and the SDK marks those before the policy is
	// evaluated - so an operator rule naming `credentials` decides a github.*
	// call the way it decides the built-in http task's. Composing a client out
	// of the policy alone would lose exactly that half.
	governed, err := sdk.HTTPClientWithBounds(maxResponseBytes, requestTimeout*time.Second)
	if err != nil {
		egressRefusal = err
		return
	}

	egressClientOnce = governed
}

// egressClient returns the governed client every network call in this
// plugin uses - the App JWT token-minting request in auth.go, and every
// go-github call built by newClient below.
//
// It returns an error rather than an ungoverned client when the grant could not
// be used, which is the whole of this plugin's fail-closed posture: there is no
// second client to fall back to, and a nil policy here would panic rather than
// silently reach GitHub, which is not a distinction worth relying on.
func egressClient() (*http.Client, error) {
	if egressClientOnce == nil {
		return nil, sdk.PermissionDenied("this plugin was launched without a usable egress policy: %v", egressRefusal)
	}

	// One client, shared: it is safe for concurrent use and its transport holds
	// the connection pool, which is where keep-alive lives for a plugin making
	// many API calls in one activity.
	return egressClientOnce, nil
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

	governed, err := egressClient()
	if err != nil {
		return nil, "", err
	}

	client := github.NewClient(governed)

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
