package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Named values a [Credential] can carry. Which ones are present depends on the
// credential's [CredentialType].
const (
	// CredentialAccessToken is the bearer token of a [CredentialBearer].
	CredentialAccessToken = "access_token"

	// AWS session credentials, the three values of a [CredentialAWSSession].
	CredentialAccessKeyID     = "access_key_id"
	CredentialSecretAccessKey = "secret_access_key"
	CredentialSessionToken    = "session_token"
)

// CredentialType names the form of a credential, which determines how it is
// presented to the system that issued it.
type CredentialType string

const (
	// CredentialBearer is an access token presented in an Authorization header.
	// [Credential.Apply] can attach it to a request.
	CredentialBearer CredentialType = "bearer"

	// CredentialAWSSession is a set of temporary AWS session credentials, to be
	// handed to an AWS SDK. They cannot be attached to a request directly:
	// AWS authenticates with a request signature, not a header value.
	CredentialAWSSession CredentialType = "aws-session"
)

// Credential is a short-lived credential a workload obtained for one downstream
// system.
//
// # Never in workflow history
//
// The secret material is reachable only through [Credential.Value],
// [Credential.Bearer], and [Credential.Apply]. That is not only about logging: it
// means a Credential that goes through a serializer, such as the one a durable
// execution backend uses to record an activity's result, arrives with its metadata
// and no secret. Marshaling redacts, and a redacted credential fails closed with
// [ErrCredentialUnresolved] when it is used.
//
// So resolve credentials inside the activity that presents them, and never return
// one to workflow code. [Broker.Credential] is the intended entry point.
//
// The secret values are held in a [Material], which is what keeps them out of
// reach of fmt, log, and every serializer — including through an unexported field
// of some other struct, where a String method does not help.
type Credential struct {
	// material holds the secret values. See [Material] for why it is a closure and
	// not a map.
	material Material

	// Type is the credential's form.
	Type CredentialType `json:"type"`

	// Target is the operator's name for the system this credential is for.
	Target string `json:"target"`

	// Provider names the exchanger that obtained it, for audit.
	Provider string `json:"provider"`

	// ExpiresAt is when the credential stops working. It is always set: a
	// credential with no expiry would be a standing grant.
	ExpiresAt time.Time `json:"expires_at"`

	// Scopes are the scopes the issuer granted, when it said.
	Scopes []string `json:"scopes,omitempty"`

	// AssertionID is the "jti" of the assertion exchanged for this credential, so
	// a downstream audit record can be traced back to the workload that asked.
	AssertionID string `json:"assertion_id,omitempty"`
}

// NewCredential assembles a credential from what a relying party returned.
//
// This is how an [Exchanger] outside this package produces its result: the material
// is held in a closure with no other way in, so there is no field for a caller to
// set. The values map is copied, so a caller reusing or mutating it afterwards
// cannot change a credential already handed out.
//
// A credential must have a type, an expiry, and at least one non-empty value. A
// credential with no expiry would be a standing grant, and one with no material is
// not a credential.
func NewCredential(kind CredentialType, expiresAt time.Time, values map[string]string) (Credential, error) {
	switch {
	case kind == "":
		return Credential{}, fmt.Errorf("%w: a credential needs a type", ErrExchangeFailed)
	case expiresAt.IsZero():
		return Credential{}, fmt.Errorf("%w: a credential needs an expiry", ErrExchangeFailed)
	}

	if NewMaterial(values).IsZero() {
		return Credential{}, fmt.Errorf("%w: a credential needs at least one value", ErrExchangeFailed)
	}

	return Credential{
		material:  NewMaterial(values),
		Type:      kind,
		ExpiresAt: expiresAt,
	}, nil
}

// Value returns the named secret value, reporting false when the credential does
// not carry it.
//
// It reports false for every name once the credential has been serialized, since
// serializing strips the secrets.
func (c Credential) Value(name string) (string, bool) {
	return c.material.Value(name)
}

// Bearer returns the access token of a [CredentialBearer].
func (c Credential) Bearer() (string, bool) {
	if c.Type != CredentialBearer {
		return "", false
	}
	return c.Value(CredentialAccessToken)
}

// Apply attaches the credential to an outbound request.
//
// This is how a task should use a credential: the secret goes from the credential
// to the request header without the task ever holding it, so there is no value in
// scope to log or to return by mistake.
//
// It fails for a credential whose type cannot be presented as a header, such as
// [CredentialAWSSession], and for one that has been serialized.
func (c Credential) Apply(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("auth: cannot apply a credential to a nil request")
	}

	switch c.Type {
	case CredentialBearer:
		token, ok := c.Value(CredentialAccessToken)
		if !ok {
			return fmt.Errorf("%w: resolve credentials in the activity that uses them", ErrCredentialUnresolved)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	case CredentialAWSSession:
		return fmt.Errorf("auth: %s credentials must sign the request, not set a header; hand them to an AWS SDK", c.Type)
	default:
		return fmt.Errorf("auth: cannot apply a credential of type %q to a request", c.Type)
	}
}

// IsZero reports whether the credential is unset.
func (c Credential) IsZero() bool {
	return c.Type == "" && c.material.IsZero()
}

// Expired reports whether the credential has expired.
func (c Credential) Expired(now time.Time) bool {
	return !now.Before(c.ExpiresAt)
}

// ExpiresWithin reports whether the credential expires within d of now. A cache
// uses it to refresh a credential slightly before it stops working, rather than
// handing out one that expires mid-request.
func (c Credential) ExpiresWithin(d time.Duration, now time.Time) bool {
	return !now.Add(d).Before(c.ExpiresAt)
}

// String describes the credential without revealing any of it.
func (c Credential) String() string {
	if c.IsZero() {
		return "no credential"
	}
	return fmt.Sprintf("%s credential for %s, expires %s", c.Type, c.Target, c.ExpiresAt.UTC().Format(time.RFC3339))
}

// Format implements [fmt.Formatter], which is what closes the last gap a String
// method leaves: %#v ignores String and prints the struct's fields, and a
// Formatter is consulted before both String and GoString. Every verb renders the
// same redacted description, because there is no verb for which printing the
// material would be correct.
func (c Credential) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, c.String())
}

// LogValue implements [slog.LogValuer], recording which credential this is and
// never its contents.
func (c Credential) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", string(c.Type)),
		slog.String("target", c.Target),
		slog.String("provider", c.Provider),
		slog.Time("expires_at", c.ExpiresAt),
	)
}

// MarshalJSON writes the credential's metadata and none of its secret material.
//
// The redaction is deliberate and load-bearing rather than incidental: it is what
// makes a credential that is mistakenly returned from an activity useless instead
// of a secret written into durable history.
func (c Credential) MarshalJSON() ([]byte, error) {
	type metadata Credential // sheds this method, keeps the exported fields
	return json.Marshal(metadata(c))
}

// Requirement describes the assertion an [Exchanger] needs in order to obtain a
// credential.
//
// The exchanger states this rather than the caller, because what a relying party
// requires is a property of that relying party's protocol, and a caller guessing
// it wrong produces an assertion that is either rejected or, worse, accepted
// somewhere it was not meant for.
type Requirement struct {
	// Audience is the value the assertion's "aud" claim must carry. It is
	// required: an assertion minted without a specific relying party in mind is
	// one that any relying party would accept.
	Audience string

	// Subject, when set, replaces the derived workload subject.
	//
	// Some protocols dictate the subject. RFC 7523 client authentication requires
	// the issuer and subject to both be the client id, so an assertion used that
	// way cannot also name the workload in its subject. The workload is still
	// described by the assertion's other claims, and the assumption policy still
	// evaluates against the workload's real identity, so an override narrows what
	// the relying party can see, never what Flowstate will allow.
	Subject string
}

// An Exchanger turns a Flowstate identity assertion into a credential that a
// downstream system accepts.
//
// This is the extension point for outbound federation: supporting a new system is
// an implementation of this interface, not a change to the broker, the policy, or
// the identity model. Implementations must be safe for concurrent use, and must
// wrap [ErrExchangeFailed] in every error they return so a caller can tell a
// refused exchange from a policy denial.
type Exchanger interface {
	// Name identifies the exchanger in credentials, errors, and audit records,
	// such as "aws-sts" or "token-exchange".
	Name() string

	// Requirement returns the assertion this exchanger needs.
	Requirement() Requirement

	// Exchange presents the assertion and returns the resulting credential.
	Exchange(ctx context.Context, assertion Assertion) (Credential, error)
}

// Defaults for credential exchange and caching.
const (
	// DefaultExchangeTimeout bounds a single exchange with a relying party.
	DefaultExchangeTimeout = 20 * time.Second

	// DefaultRefreshMargin is how long before expiry a cached credential is
	// re-exchanged, so a credential handed to a caller has time left to be used.
	DefaultRefreshMargin = time.Minute

	// DefaultCredentialLifetime is assumed when a relying party returns a
	// credential without saying when it expires. It is short, because guessing
	// long about a credential's lifetime means using one after it has stopped
	// working, or worse, believing a long-lived one is short-lived.
	DefaultCredentialLifetime = 15 * time.Minute

	// DefaultMaxCachedCredentials bounds the credential cache. Every workflow and
	// step is a distinct identity, so an unbounded cache in a long-running worker
	// would grow without limit.
	DefaultMaxCachedCredentials = 1024

	// maxExchangeResponseBytes bounds a relying party's response body.
	maxExchangeResponseBytes = 1 << 20 // 1 MiB
)

// credentialCache holds credentials until shortly before they expire.
//
// Each entry has its own mutex, so two workloads asking for credentials for
// different systems do not wait for each other, while two asking for the same
// credential at the same time produce one exchange rather than two.
type credentialCache struct {
	clock  func() time.Time
	margin time.Duration
	limit  int

	mu      sync.Mutex
	entries map[string]*credentialEntry
}

// credentialEntry is one cached credential.
type credentialEntry struct {
	mu         sync.Mutex
	credential Credential
}

// newCredentialCache returns an empty cache.
func newCredentialCache(clock func() time.Time, margin time.Duration, limit int) *credentialCache {
	return &credentialCache{
		clock:   clock,
		margin:  margin,
		limit:   limit,
		entries: make(map[string]*credentialEntry),
	}
}

// get returns a usable cached credential for key, obtaining one with exchange if
// there is none or the cached one is close to expiring.
func (c *credentialCache) get(ctx context.Context, key string, exchange func(context.Context) (Credential, error)) (Credential, error) {
	entry := c.entry(key)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := c.clock()
	if !entry.credential.IsZero() && !entry.credential.ExpiresWithin(c.margin, now) {
		return entry.credential, nil
	}

	credential, err := exchange(ctx)
	if err != nil {
		return Credential{}, err
	}

	// A credential that arrives already inside the refresh margin is still
	// returned, because it is the best the relying party will give us, but it is
	// not cached: caching it would mean exchanging again on the very next call
	// anyway, and holding a credential we know is unusable serves nobody.
	if !credential.ExpiresWithin(c.margin, now) {
		entry.credential = credential
	}

	return credential, nil
}

// entry returns the cache entry for key, creating it if needed and pruning the
// cache when it has grown past its limit.
func (c *credentialCache) entry(key string) *credentialEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[key]; ok {
		return entry
	}

	if len(c.entries) >= c.limit {
		c.pruneLocked()
	}

	entry := &credentialEntry{}
	c.entries[key] = entry

	return entry
}

// pruneLocked drops expired entries, and if that is not enough, the entry closest
// to expiring. The caller must hold c.mu.
//
// Reading an entry's credential here without its own lock would race with an
// exchange in progress, so an entry that is locked is left alone: it is in use,
// which is the opposite of what pruning is looking for.
func (c *credentialCache) pruneLocked() {
	now := c.clock()

	var (
		oldestKey   string
		oldestUntil time.Time
	)

	for key, entry := range c.entries {
		if !entry.mu.TryLock() {
			continue
		}

		credential := entry.credential
		entry.mu.Unlock()

		switch {
		case credential.IsZero() || credential.Expired(now):
			delete(c.entries, key)
		case oldestKey == "" || credential.ExpiresAt.Before(oldestUntil):
			oldestKey, oldestUntil = key, credential.ExpiresAt
		}
	}

	if len(c.entries) >= c.limit && oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// credentialKey identifies a cached credential.
//
// Everything that shapes the assertion is part of the key, not only the subject.
// Two runs of the same step can act for different callers, and a relying party may
// well authorize on that: handing the credential obtained for one caller to a run
// acting for another would be a cross-tenant credential leak. Fields are
// length-prefixed so that no two different identities can produce the same key by
// having their values run together.
func credentialKey(target, subject string, identity WorkloadIdentity) string {
	digest := sha256.New()

	write := func(parts ...string) {
		for _, part := range parts {
			fmt.Fprintf(digest, "%d:%s|", len(part), part)
		}
	}

	write(target, subject, identity.Subject, identity.Issuer, identity.Namespace, identity.Deployment)

	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		write(name, identity.Claims[name])
	}

	return target + "|" + subject + "|" + hex.EncodeToString(digest.Sum(nil))
}

// tokenResponse is the OAuth 2.0 token endpoint response shared by RFC 8693 token
// exchange, RFC 6749 client credentials, and Google's security token service.
type tokenResponse struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int64  `json:"expires_in"`
	Scope           string `json:"scope"`

	// Error and ErrorDescription carry an OAuth error response, which arrives with
	// a 4xx status.
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// credential converts a token endpoint response into a bearer credential.
func (r tokenResponse) credential(provider, target string, assertion Assertion, now time.Time, fallbackLifetime time.Duration) (Credential, error) {
	if r.AccessToken == "" {
		return Credential{}, fmt.Errorf("%w: %s returned no access token", ErrExchangeFailed, provider)
	}

	// RFC 8693 section 2.2.1: a token_type of anything but Bearer means the token
	// is not usable as a bearer token, and presenting it as one would be wrong.
	if r.TokenType != "" && !strings.EqualFold(r.TokenType, "bearer") && !strings.EqualFold(r.TokenType, "n_a") {
		return Credential{}, fmt.Errorf("%w: %s issued a %q token, which cannot be used as a bearer token",
			ErrExchangeFailed, provider, truncate(r.TokenType, 32))
	}

	lifetime := fallbackLifetime
	if r.ExpiresIn > 0 {
		lifetime = time.Duration(r.ExpiresIn) * time.Second
	}

	credential, err := NewCredential(CredentialBearer, now.Add(lifetime), map[string]string{
		CredentialAccessToken: r.AccessToken,
	})
	if err != nil {
		return Credential{}, err
	}

	credential.Target = target
	credential.Provider = provider
	credential.Scopes = strings.Fields(r.Scope)
	credential.AssertionID = assertion.ID

	return credential, nil
}

// exchangeClient performs the HTTP half of an exchange, shared by every
// exchanger so that response limits, error reporting, and secret hygiene are
// implemented once.
type exchangeClient struct {
	client  *http.Client
	timeout time.Duration
}

// newExchangeClient returns a client for talking to a relying party. It follows
// no redirect at all, which is not the same rule the key fetcher needs and is the
// reason these are two clients.
func newExchangeClient(client *http.Client, timeout time.Duration) *exchangeClient {
	if timeout <= 0 {
		timeout = DefaultExchangeTimeout
	}
	return &exchangeClient{client: unredirectedClient(client), timeout: timeout}
}

// unredirectedClient returns a client that refuses to follow any redirect,
// applied to a copy so a caller's own client is never modified.
//
// An exchange carries the assertion in the request *body*, and that is the whole
// difference from fetching an issuer's keys, which shares almost all of this code.
// A key set is a GET carrying nothing, so following a redirect anywhere is
// ordinary and [transportProtectedClient]'s scheme check is the right rule for it.
//
// A body is not covered by anything. net/http drops the Authorization header when
// a redirect crosses to another host, and has no equivalent notion for a body —
// while 307 and 308 are defined to replay one. So a check on the redirect target's
// *scheme* let a configured endpoint name any other https host and have a signed
// assertion delivered to it. The assertion is audience-scoped, so what the
// recipient gains is the ability to act as that workload at the relying party it
// was minted for, which is the whole of what it was minted to do.
//
// Refused outright rather than pinned to the configured host, because a token
// endpoint does not redirect: RFC 8693 has no such step, and net/http turns 301,
// 302 and 303 on a POST into a bodyless GET — so an exchange meeting one of those
// was already failing. What was reachable was exactly the pair that leaks, and
// nothing that works today is being taken away.
func unredirectedClient(client *http.Client) *http.Client {
	unredirected := &http.Client{}
	if client != nil {
		unredirected = &http.Client{
			Transport: client.Transport,
			Jar:       client.Jar,
			Timeout:   client.Timeout,
		}
	}

	unredirected.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// Named so an operator can act on it: this is a misconfiguration — the
		// endpoint they wrote down is not the one serving the exchange — and the
		// fix is to configure the destination directly.
		return fmt.Errorf("endpoint redirected to %s: configure the endpoint that serves "+
			"the exchange, since following would send the assertion there",
			req.URL.Redacted())
	}

	return unredirected
}

// postForm posts form-encoded values and returns the response body.
func (e *exchangeClient) postForm(ctx context.Context, provider, endpoint string, form url.Values) ([]byte, error) {
	return e.post(ctx, provider, endpoint, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()), nil)
}

// postJSON posts a JSON body and returns the response body. bearer, when set,
// authenticates the request with a token obtained earlier in the exchange.
func (e *exchangeClient) postJSON(ctx context.Context, provider, endpoint string, body any, bearer string) ([]byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding %s request: %w", ErrExchangeFailed, provider, err)
	}

	var authorize func(*http.Request)
	if bearer != "" {
		authorize = func(req *http.Request) { req.Header.Set("Authorization", "Bearer "+bearer) }
	}

	return e.post(ctx, provider, endpoint, "application/json", strings.NewReader(string(encoded)), authorize)
}

// post performs the request and returns the response body, which the caller
// decodes: relying parties answer in JSON or in XML depending on whose protocol
// it is.
//
// The request body holds the assertion, and the response body holds a credential,
// so neither ever appears in an error. What an error does carry is the endpoint,
// the status, and the relying party's own error code and description, which is
// what an operator needs to tell a misconfigured trust relationship from an
// unreachable endpoint.
func (e *exchangeClient) post(ctx context.Context, provider, endpoint, contentType string, body io.Reader, authorize func(*http.Request)) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	if _, err := validateHTTPSURL(endpoint, "endpoint"); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, provider, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("%w: building %s request: %w", ErrExchangeFailed, provider, err)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Accept", "application/json")
	if authorize != nil {
		authorize(request)
	}

	response, err := e.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %s at %q: %w", ErrExchangeFailed, provider, endpoint, err)
	}
	defer response.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(response.Body, maxExchangeResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s response: %w", ErrExchangeFailed, provider, err)
	}
	if len(raw) > maxExchangeResponseBytes {
		return nil, fmt.Errorf("%w: %s returned more than %d bytes", ErrExchangeFailed, provider, maxExchangeResponseBytes)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s at %q returned %s%s",
			ErrExchangeFailed, provider, endpoint, response.Status, describeError(raw))
	}

	return raw, nil
}

// decodeJSON decodes a relying party's response, reporting the provider so a
// malformed answer is attributable.
func decodeJSON(provider string, raw []byte, out any) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%w: decoding %s response: %w", ErrExchangeFailed, provider, err)
	}
	return nil
}

// describeError extracts a relying party's own explanation from an error
// response, so the reason a trust relationship was refused reaches the operator.
// Only recognized error fields are reported, never the whole body, which for some
// providers echoes the request.
func describeError(raw []byte) string {
	var oauth struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
		// AWS reports errors as XML, and Google nests an OAuth error.
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &oauth); err == nil {
		switch {
		case oauth.Error != "" && oauth.ErrorDescription != "":
			return fmt.Sprintf(" (%s: %s)", truncate(oauth.Error, 64), truncate(oauth.ErrorDescription, 256))
		case oauth.Error != "":
			return fmt.Sprintf(" (%s)", truncate(oauth.Error, 64))
		case oauth.Message != "":
			return fmt.Sprintf(" (%s)", truncate(oauth.Message, 256))
		}
	}

	if code, message := xmlError(raw); code != "" {
		return fmt.Sprintf(" (%s: %s)", truncate(code, 64), truncate(message, 256))
	}

	return ""
}

// requiredEndpoint reports whether a configured endpoint is usable, so a
// misconfigured exchanger fails when it is built rather than at the first
// exchange.
func requiredEndpoint(name, field, endpoint string) error {
	if endpoint == "" {
		return fmt.Errorf("%w: %s exchanger needs %s", ErrInvalidPolicy, name, field)
	}
	if _, err := validateHTTPSURL(endpoint, field); err != nil {
		return fmt.Errorf("%w: %s exchanger: %w", ErrInvalidPolicy, name, err)
	}
	return nil
}
