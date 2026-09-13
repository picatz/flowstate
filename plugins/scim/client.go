package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const (
	// scimContentType is the media type RFC 7644 defines. Providers vary in
	// what they send back - some answer application/json - so it is sent and
	// never required.
	scimContentType = "application/scim+json"

	// maxResourceBytes bounds one user resource. A SCIM user with enterprise
	// extensions is a few kilobytes; this leaves room for an organization's own
	// custom attributes and still bounds what another party returns.
	maxResourceBytes = 1 << 20

	// maxListBytes bounds a page of them.
	maxListBytes = 4 << 20

	// maxErrorBytes bounds what a provider's own error text may contribute to a
	// failure this plugin reports.
	maxErrorBytes = 256

	// maxTokenBytes bounds a resolved credential before it becomes a header.
	maxTokenBytes = 8192

	// maxBaseURLBytes bounds the endpoint an input names.
	maxBaseURLBytes = 512

	// maxIdentifierBytes bounds a provider id or a user name.
	maxIdentifierBytes = 256

	// maxGroups bounds the group names carried out of one user, which is a
	// collection another party controls on its way into workflow history.
	maxGroups = 64
)

// client talks SCIM to one provider, for one task call.
type client struct {
	http  *http.Client
	base  *url.URL
	token string
}

// newClient validates the endpoint and builds the governed client.
//
// The base URL is an input rather than operator configuration because a
// deployment legitimately reviews more than one tenant, and the destination is
// governed where every other destination is: the deployment's egress policy, on
// the dial path.
func newClient(baseURL, token string) (*client, error) {
	if baseURL == "" {
		return nil, sdk.InvalidInput("base_url is required, and is the provider's SCIM base such as https://example.okta.com/scim/v2")
	}
	if len(baseURL) > maxBaseURLBytes {
		return nil, sdk.InvalidInput("base_url is %d bytes, over the %d-byte limit", len(baseURL), maxBaseURLBytes)
	}

	parsed, err := url.Parse(strings.TrimSuffix(baseURL, "/"))
	if err != nil {
		return nil, sdk.InvalidInput("base_url is not a URL: %v", err)
	}
	if parsed.Scheme != httpsScheme {
		return nil, sdk.InvalidInput(
			"base_url must be an %s URL; a directory credential is not sent in cleartext", strings.ToUpper(httpsScheme))
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, sdk.InvalidInput(
			"base_url must be scheme, host and path only - no credentials, query or fragment")
	}

	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("no usable egress policy, so no provider is authorized: %v", err)
	}

	return &client{http: governed, base: parsed, token: token}, nil
}

// httpsScheme is what a base URL must be. It is a variable rather than a
// constant only so this package's own tests can run against an httptest server
// on loopback; no input and no operator setting changes it, because a bearer
// credential sent in cleartext is a credential on the wire.
var httpsScheme = "https"

// do performs one request and returns the decoded body.
//
// Every call goes through here so that the header, the bounds, the
// classification and the credential handling are written once: a second request
// path is a second set of answers to keep correct.
func (c *client) do(ctx context.Context, method, path string, query url.Values, body any, limit int64, headers map[string]string) (*http.Response, []byte, error) {
	endpoint := *c.base
	endpoint.Path = c.base.Path + path
	if len(query) > 0 {
		endpoint.RawQuery = query.Encode()
	}

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, nil, sdk.Failed("encoding the SCIM request: %v", err)
		}
		payload = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), payload)
	if err != nil {
		return nil, nil, sdk.Failed("building the SCIM request: %v", err)
	}
	request.Header.Set("Accept", scimContentType+", application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		request.Header.Set("Content-Type", scimContentType)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, nil, classifyTransportError(c.base.Host, method, err)
	}
	defer response.Body.Close()

	raw, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if readErr != nil {
		return nil, nil, sdk.Unavailable("reading the response from %s: %v", c.base.Host, readErr)
	}
	if int64(len(raw)) > limit {
		return nil, nil, sdk.Failed("%s returned more than the %d-byte limit this task reads", c.base.Host, limit)
	}

	return response, raw, nil
}

// classifyTransportError turns a failure that happened before any response into
// the SDK's classification.
//
// Every request this plugin makes is either a read or an idempotent write -
// replacing `active` with false twice leaves one account in one state - so a
// lost connection is retryable rather than an unknown outcome. A task that
// created a user could not say this, which is one more reason there is not one.
func classifyTransportError(host, method string, err error) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited the %s to %s; retry after %s", method, host, delay)
	}

	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied(
			"deployment egress policy denied reaching %s; an identity provider this plugin may call is one the policy permits", host)
	}

	var tooLarge *netpolicy.BodyTooLargeError
	if errors.As(err, &tooLarge) {
		return sdk.Failed("%s returned more than the operator egress policy's %d-byte limit", host, tooLarge.Limit)
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.Unavailable("the %s to %s did not complete in time", method, host)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return sdk.Unavailable("%s could not be reached: %v", host, netErr)
	}
	return sdk.Unavailable("the %s to %s failed: %v", method, host, err)
}

// scimError is the error resource RFC 7644 section 3.12 defines.
type scimError struct {
	Detail   string `json:"detail"`
	Status   any    `json:"status"`
	SCIMType string `json:"scimType"`
}

// classifyStatus turns a provider's refusal into the SDK's classification,
// carrying the specification's own scimType where the provider sends one: it
// says what to fix far better than the status code does.
func classifyStatus(host, doing string, response *http.Response, body []byte) error {
	detail := scimDetail(body)

	switch response.StatusCode {
	case http.StatusNotFound:
		return sdk.NotFound("%s has no such user while %s%s", host, doing, detail)
	case http.StatusUnauthorized, http.StatusForbidden:
		return sdk.PermissionDenied("%s refused the credential while %s%s", host, doing, detail)
	case http.StatusPreconditionFailed:
		// If-Match lost: the user changed since the read this write was based
		// on. Its own classification, so a Flowfile can dispatch on "someone
		// else wrote here first" rather than on a failure.
		return sdk.Conflict(
			"%s reports the user changed since expected_version was read; re-read the user and decide again%s", host, detail)
	case http.StatusTooManyRequests:
		delay := retryAfter(response.Header.Get("Retry-After"))
		return sdk.UnavailableAfter(delay, "%s rate-limited the request while %s; retry after %s", host, doing, delay)
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return sdk.InvalidInput("%s refused the request while %s%s", host, doing, detail)
	}

	if response.StatusCode >= 500 {
		return sdk.Unavailable("%s returned HTTP %d while %s%s", host, response.StatusCode, doing, detail)
	}
	return sdk.Failed("%s returned HTTP %d while %s%s", host, response.StatusCode, doing, detail)
}

// scimDetail renders the provider's own error text, bounded.
func scimDetail(body []byte) string {
	var parsed scimError
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}

	parts := make([]string, 0, 2)
	if parsed.SCIMType != "" {
		parts = append(parts, parsed.SCIMType)
	}
	if parsed.Detail != "" {
		parts = append(parts, parsed.Detail)
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + truncate(strings.Join(parts, ": "), maxErrorBytes) + ")"
}

// retryAfter reads the header a provider asks with, bounded.
func retryAfter(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return boundedRetryAfter(time.Duration(seconds) * time.Second)
}

// boundedRetryAfter keeps another party's requested delay inside what a step is
// willing to wait.
func boundedRetryAfter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return time.Second
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

// truncate bounds a value before it is interpolated into a refusal.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}

// tokenFrom reads the resolved bearer credential this call acts as.
func tokenFrom(value *flowstatev1.Value) (string, error) {
	if value == nil {
		return "", sdk.InvalidInput("token is required")
	}

	switch kind := value.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		text, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok || text.StringValue == "" {
			return "", sdk.InvalidInput("token must resolve to a non-empty string")
		}
		if len(text.StringValue) > maxTokenBytes {
			return "", sdk.InvalidInput("token resolves to %d bytes, over the %d-byte limit", len(text.StringValue), maxTokenBytes)
		}
		return text.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed(
			"token reached this plugin as an unresolved secret reference; the host resolves required secret inputs before a plugin runs")
	default:
		return "", sdk.InvalidInput("token must resolve to a string")
	}
}

// identifier checks a provider id or user name before it becomes part of a path
// or a filter.
func identifier(value, field string) (string, error) {
	if value == "" {
		return "", sdk.InvalidInput("%s is required", field)
	}
	if len(value) > maxIdentifierBytes {
		return "", sdk.InvalidInput("%s is %d bytes, over the %d-byte limit", field, len(value), maxIdentifierBytes)
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		return "", sdk.InvalidInput("%s holds a control character", field)
	}
	return value, nil
}

// pathEscape renders an identifier as one path segment. A provider id is opaque
// - the specification says so - so it is escaped rather than assumed safe.
func pathEscape(value string) string {
	return url.PathEscape(value)
}

// fmtHeaders is a tiny helper so a caller reads as a sentence rather than a map
// literal at the call site.
func fmtHeaders(pairs ...string) map[string]string {
	if len(pairs)%2 != 0 {
		panic(fmt.Sprintf("fmtHeaders: odd number of arguments (%d)", len(pairs)))
	}
	headers := make(map[string]string, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		if pairs[i+1] == "" {
			continue
		}
		headers[pairs[i]] = pairs[i+1]
	}
	return headers
}
