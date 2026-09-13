package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const (
	// maxManifestBytes bounds a manifest or an index. The distribution
	// specification's own registries cap manifests at four megabytes, and a
	// manifest is a few kilobytes of JSON: this is the specification's ceiling,
	// not a guess, and it is applied to bytes another party controls.
	maxManifestBytes = 4 << 20

	// maxTokenResponseBytes bounds a token endpoint's answer. A token is a
	// kilobyte of JWT at the outside; this leaves room for one and refuses a
	// body that is something else.
	maxTokenResponseBytes = 64 << 10

	// maxCredentialBytes bounds a resolved registry credential before it
	// becomes an Authorization header.
	maxCredentialBytes = 4096

	// maxErrorBytes bounds what a registry's own error text may contribute to
	// a failure this plugin reports.
	maxErrorBytes = 256
)

// acceptedManifestTypes is what this plugin tells a registry it understands, in
// the order the specification lists them. Both OCI media types and the two
// Docker ones: a registry serves what the Accept header asks for, and an
// image pushed by older tooling is a Docker manifest whether or not this
// plugin would have preferred otherwise.
var acceptedManifestTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

// credentials is one registry account, resolved per call from the caller's own
// secret provider. Nothing here persists between calls, and nothing here is
// this plugin's own: it holds no credential store and claims no secret scheme.
type credentials struct {
	username string
	password string
}

// set reports whether anything was supplied. Unset is an anonymous read, which
// is what a public repository wants and what a private one refuses.
func (c credentials) set() bool { return c.username != "" || c.password != "" }

// registryClient talks the distribution protocol to one registry.
type registryClient struct {
	http  *http.Client
	creds credentials

	// scheme is always "https" in a running plugin. It exists as a field so
	// this package's own tests can run against an httptest server on loopback;
	// there is no input, environment variable or operator setting that changes
	// it, because a registry read that can be downgraded to cleartext is a
	// credential that can be read off the wire.
	scheme string

	// tokens caches one bearer token per scope for the life of this client,
	// which is one task call. A registry issues a token per scope, and the
	// alternative to caching is re-running the exchange for every request in
	// the same call.
	tokens map[string]string
}

// newRegistryClient builds the client for one call, over the governed HTTP
// client: the deployment's egress policy is what decides which registries are
// reachable, evaluated on the real dial path rather than consulted here.
func newRegistryClient(creds credentials) (*registryClient, error) {
	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("no usable egress policy, so no registry is authorized: %v", err)
	}
	return &registryClient{http: governed, creds: creds, scheme: "https", tokens: map[string]string{}}, nil
}

// endpoint builds an absolute URL for one registry API path.
func (c *registryClient) endpoint(ref reference, path string) string {
	return c.scheme + "://" + ref.Endpoint + path
}

// get performs one authenticated GET, completing the registry's authentication
// challenge at most once.
//
// The caller closes the response body. Every caller bounds what it reads from
// it: a registry is another party, and its response length is its to choose
// until this process says otherwise.
func (c *registryClient) get(ctx context.Context, ref reference, path string, accept []string) (*http.Response, error) {
	scope := "repository:" + ref.Repository + ":pull"

	response, err := c.send(ctx, ref, path, accept, c.tokens[scope])
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusUnauthorized {
		return response, nil
	}

	// One challenge, one retry. A registry that answers the authenticated
	// request with another 401 is refusing the credential, not asking again.
	header := response.Header.Get("WWW-Authenticate")
	drainAndClose(response)

	token, err := c.authenticate(ctx, ref, header, scope)
	if err != nil {
		return nil, err
	}

	retried, err := c.send(ctx, ref, path, accept, token)
	if err != nil {
		return nil, err
	}
	if retried.StatusCode == http.StatusUnauthorized {
		drainAndClose(retried)
		return nil, sdk.PermissionDenied(
			"%s refused the credential for %s", ref.Registry, truncate(ref.Repository, 96))
	}
	return retried, nil
}

// send makes one request, with whatever authorization is already established.
func (c *registryClient) send(ctx context.Context, ref reference, path string, accept []string, token string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint(ref, path), nil)
	if err != nil {
		return nil, sdk.Failed("building the registry request: %v", err)
	}
	if len(accept) > 0 {
		request.Header.Set("Accept", strings.Join(accept, ", "))
	}
	switch {
	case token != "":
		request.Header.Set("Authorization", "Bearer "+token)
	case c.creds.set():
		// A registry that challenges with Basic, or one reached before any
		// challenge, takes the credential directly. It is the same credential
		// either way and it never leaves HTTPS.
		request.SetBasicAuth(c.creds.username, c.creds.password)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, classifyTransportError(ref, err)
	}
	return response, nil
}

// authenticate completes the challenge a registry answered with and returns the
// bearer token to retry with, caching it for this call's remaining requests.
//
// The realm is the registry's to name - Docker Hub's is on another host
// entirely - so this is the one place a credential travels somewhere this
// plugin did not choose. Two things bound that: the realm must be HTTPS, and
// the request to it goes through the same governed client as everything else,
// so a realm the deployment's egress policy does not permit is never dialed.
func (c *registryClient) authenticate(ctx context.Context, ref reference, header, scope string) (string, error) {
	parsed, ok := parseChallenge(header)
	if !ok {
		return "", sdk.PermissionDenied(
			"%s refused the request and named no authentication scheme this plugin speaks", ref.Registry)
	}
	if parsed.scheme == "basic" {
		if !c.creds.set() {
			return "", sdk.PermissionDenied(
				"%s requires a credential for %s and none was supplied", ref.Registry, truncate(ref.Repository, 96))
		}
		// Nothing to exchange: the retry carries the credential itself.
		return "", nil
	}

	realm, err := url.Parse(parsed.realm)
	if err != nil || realm.Scheme != c.scheme || realm.Host == "" {
		return "", sdk.PermissionDenied(
			"%s named the token endpoint %q, which is not an %s URL; a credential is not sent anywhere else",
			ref.Registry, truncate(parsed.realm, 96), strings.ToUpper(c.scheme))
	}

	query := realm.Query()
	if parsed.service != "" {
		query.Set("service", parsed.service)
	}
	// The registry's own scope when it named one, and the scope this call needs
	// when it did not. Never a wider one: a token minted for more than the
	// repository being read is authority this call has no use for.
	if parsed.scope != "" {
		query.Set("scope", parsed.scope)
	} else {
		query.Set("scope", scope)
	}
	realm.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, realm.String(), nil)
	if err != nil {
		return "", sdk.Failed("building the token request: %v", err)
	}
	if c.creds.set() {
		request.SetBasicAuth(c.creds.username, c.creds.password)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return "", classifyTransportError(ref, err)
	}
	defer drainAndClose(response)

	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return "", sdk.PermissionDenied("%s refused the credential at its token endpoint", ref.Registry)
	}
	if response.StatusCode != http.StatusOK {
		return "", classifyStatus(ref, response, "exchanging a credential for a token")
	}

	body, err := readBounded(response.Body, maxTokenResponseBytes)
	if err != nil {
		return "", sdk.Unavailable("reading the token response from %s: %v", ref.Registry, err)
	}

	var answer struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", sdk.Failed("%s returned a token response this plugin cannot read", ref.Registry)
	}

	token := answer.Token
	if token == "" {
		// The specification names `token`; the OAuth2 flow some registries
		// implement names `access_token`. Both mean the same thing here.
		token = answer.AccessToken
	}
	if token == "" || len(token) > maxCredentialBytes {
		return "", sdk.PermissionDenied("%s returned no usable token", ref.Registry)
	}

	c.tokens[scope] = token
	return token, nil
}

// challenge is a parsed WWW-Authenticate header.
type challenge struct {
	scheme  string
	realm   string
	service string
	scope   string
}

// parseChallenge reads the scheme and the parameters this plugin acts on. It is
// deliberately small: a header naming a scheme other than Bearer or Basic is
// not one this plugin can complete, and saying so is better than guessing.
func parseChallenge(header string) (challenge, bool) {
	scheme, params, found := strings.Cut(strings.TrimSpace(header), " ")
	parsed := challenge{scheme: strings.ToLower(scheme)}
	if parsed.scheme != "bearer" && parsed.scheme != "basic" {
		return challenge{}, false
	}
	if !found {
		return parsed, true
	}

	for _, part := range strings.Split(params, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		value = strings.Trim(value, `"`)
		switch strings.ToLower(key) {
		case "realm":
			parsed.realm = value
		case "service":
			parsed.service = value
		case "scope":
			parsed.scope = value
		}
	}
	return parsed, true
}

// readBounded reads at most limit bytes and reports a body that had more,
// rather than returning the first limit bytes of it. A truncated document read
// as a complete one is how a workflow decides on evidence it did not receive.
func readBounded(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("the response is longer than the %d-byte limit", limit)
	}
	return data, nil
}

// verifyDigest reports whether bytes hash to the digest that was asked for.
//
// This is the property that makes a content-addressed read worth making through
// this plugin rather than through a generic HTTP client: the caller knew what
// the bytes had to be before it asked, so bytes that are anything else are a
// failure rather than a result.
func verifyDigest(want string, data []byte) error {
	sum := sha256.Sum256(data)
	got := "sha256:" + hex.EncodeToString(sum[:])
	if got != want {
		return sdk.Failed(
			"the registry returned bytes that hash to %s, and %s was asked for; the content does not match its address",
			got, want)
	}
	return nil
}

// digestOf is what the bytes are, for a read that asked by tag and has to say
// what it got.
func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// drainAndClose releases a response this plugin is finished with, reading a
// bounded amount first so the connection can be reused rather than reset.
func drainAndClose(response *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	_ = response.Body.Close()
}

// boundedText returns the bytes as text when they are text, and empty when they
// are not: a workflow's string output is not where arbitrary binary belongs.
func boundedText(data []byte) string {
	if !utf8.Valid(data) {
		return ""
	}
	return string(data)
}
