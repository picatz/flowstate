package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// OAuth 2.0 URNs used when exchanging an assertion.
const (
	// grantTypeTokenExchange is the RFC 8693 token exchange grant.
	grantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"

	// grantTypeClientCredentials is the RFC 6749 section 4.4 client credentials
	// grant.
	grantTypeClientCredentials = "client_credentials"

	// tokenTypeJWT identifies the assertion as a JWT, per RFC 8693 section 3.
	tokenTypeJWT = "urn:ietf:params:oauth:token-type:jwt"

	// tokenTypeAccessToken requests an access token in return.
	tokenTypeAccessToken = "urn:ietf:params:oauth:token-type:access_token"

	// clientAssertionTypeJWT identifies the assertion as an RFC 7523 client
	// authentication assertion.
	clientAssertionTypeJWT = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

// DelegatorTokenFunc supplies the token of the party a workload is acting for,
// at the moment of an exchange.
//
// It is a function rather than a configured string because the value is a
// bearer credential belonging to somebody else: it is acquired per exchange,
// lives as long as the request, and is never part of a policy file. It returns
// [Material] for the same reason every other credential in this package does —
// a string in a struct field is printed in full by `%+v` on anything that
// happens to contain it, and a closure is not reachable by reflection at all.
//
// The token type is the RFC 8693 URN describing what was returned, such as
// [tokenTypeJWT]. Empty means a JWT, which is what an identity provider's own
// tokens are.
//
// Returning an error refuses the exchange. There is deliberately no fallback to
// an undelegated one: a credential minted without the delegation recorded in it
// carries more authority than the delegated request asked for, which is the
// failure this whole parameter exists to make impossible.
type DelegatorTokenFunc func(ctx context.Context) (token Material, tokenType string, err error)

// TokenExchangeConfig configures an RFC 8693 OAuth 2.0 Token Exchange.
//
// This is the standards-based path and the one to reach for first. Any
// authorization server that implements RFC 8693 can accept a Flowstate assertion
// and return a credential for a downstream service, with no Flowstate-specific
// support and no shared secret.
//
// # Delegation
//
// Set [TokenExchangeConfig.Delegator] and the exchange becomes RFC 8693's
// delegation case rather than its plain one. The mapping is the standard's own,
// and is worth stating explicitly because it is the opposite of the intuitive
// reading: **the subject token is the party being acted for, and the actor
// token is the party doing the acting**. RFC 8693 §2.1 defines actor_token as
// "the identity of the acting party … the party that is authorized to use the
// requested security token", and the requested credential is used by the
// Flowstate workload. So:
//
//   - subject_token is the delegator's token, from Delegator
//   - actor_token is the Flowstate assertion, always a JWT
//
// The authorization server records the pair as an "act" claim on the credential
// it returns, which is what makes the delegation checkable by the relying party
// rather than merely intended — the distinction #560 draws between an assertion
// that describes delegation and one that constrains it.
//
// # What this deliberately cannot express
//
// Impersonation. There is no way to present somebody else's token as the
// subject with no actor token beside it, which is RFC 8693's other case and is
// exactly the shape a delegated exchange must not be able to degrade into. And
// this is the client half only: Flowstate is an 8693 client and not a server,
// so nothing here mints an "act" claim, checks "may_act", or bounds a
// delegation chain. Those belong to the grant model, which is design-gated on
// #567's D1 and D2.
type TokenExchangeConfig struct {
	// Name identifies this exchanger in credentials and audit records. Defaults
	// to "token-exchange".
	Name string

	// TokenURL is the authorization server's token endpoint. Required.
	TokenURL string

	// Audience is the value the assertion's "aud" claim must carry: what the
	// authorization server expects to see in a token presented to it, usually its
	// own issuer identifier or token endpoint. Required.
	//
	// This is not the same as TargetAudience, and confusing the two is the usual
	// reason an exchange is refused. This one says who may accept the assertion;
	// TargetAudience says what the returned credential should be good for.
	Audience string

	// TargetAudience is the RFC 8693 "audience" parameter: the logical name of
	// the service the returned credential will be used against. Optional.
	TargetAudience string

	// Resource is the RFC 8693 "resource" parameter, a URI naming the target
	// service. Optional, and an alternative to TargetAudience.
	Resource string

	// Scopes are the scopes to request. Optional.
	Scopes []string

	// RequestedTokenType is the RFC 8693 "requested_token_type". Defaults to an
	// access token.
	RequestedTokenType string

	// Delegator, when set, makes this a delegated exchange rather than a plain
	// one. See [DelegatorTokenFunc], and [TokenExchangeConfig]'s own comment
	// for which token ends up in which parameter.
	Delegator DelegatorTokenFunc

	// HTTPClient talks to the token endpoint. Its redirect policy is replaced so
	// a redirect cannot move the exchange onto an unprotected connection, where
	// the assertion in the request body would be readable.
	HTTPClient *http.Client

	// Timeout bounds a single exchange. Defaults to [DefaultExchangeTimeout].
	Timeout time.Duration

	// MaxCredentialLifetime is the longest lifetime Flowstate will accept from
	// this target. Defaults to [DefaultMaxCredentialLifetime]. The provider must
	// still return expires_in; this is not a fallback expiry.
	MaxCredentialLifetime time.Duration

	// Clock is used for credential expiry. It exists for tests.
	Clock func() time.Time
}

// tokenExchanger implements RFC 8693 token exchange.
type tokenExchanger struct {
	name        string
	tokenURL    string
	audience    string
	target      string
	resource    string
	scopes      []string
	tokenType   string
	delegator   DelegatorTokenFunc
	client      *exchangeClient
	clock       func() time.Time
	maxLifetime time.Duration
}

// NewTokenExchanger returns an [Exchanger] that performs RFC 8693 token exchange.
func NewTokenExchanger(cfg TokenExchangeConfig) (Exchanger, error) {
	name := cfg.Name
	if name == "" {
		name = "token-exchange"
	}

	if err := requiredEndpoint(name, "token_url", cfg.TokenURL); err != nil {
		return nil, err
	}
	if cfg.Audience == "" {
		return nil, fmt.Errorf("%w: %s exchanger needs the audience the authorization server expects", ErrInvalidPolicy, name)
	}

	tokenType := cfg.RequestedTokenType
	if tokenType == "" {
		tokenType = tokenTypeAccessToken
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	maxLifetime, err := credentialLifetimeCeiling(name, cfg.MaxCredentialLifetime)
	if err != nil {
		return nil, err
	}

	return &tokenExchanger{
		name:        name,
		tokenURL:    cfg.TokenURL,
		audience:    cfg.Audience,
		target:      cfg.TargetAudience,
		resource:    cfg.Resource,
		scopes:      cfg.Scopes,
		tokenType:   tokenType,
		delegator:   cfg.Delegator,
		client:      newExchangeClient(cfg.HTTPClient, cfg.Timeout),
		clock:       clock,
		maxLifetime: maxLifetime,
	}, nil
}

// Name implements [Exchanger].
func (e *tokenExchanger) Name() string { return e.name }

// isDelegated implements delegatingExchanger, so that [Broker] does not serve
// one delegator's credential to another from a cache keyed on the workload
// alone. See the call site in Broker.Credential for why that key cannot tell
// two delegators apart.
func (e *tokenExchanger) isDelegated() bool { return e.delegator != nil }

// Requirement implements [Exchanger].
func (e *tokenExchanger) Requirement() Requirement {
	return Requirement{Audience: e.audience}
}

// Exchange implements [Exchanger], presenting the assertion as the subject token
// of an RFC 8693 exchange — or, when a delegator is configured, as its actor
// token beside the delegator's subject token. See [TokenExchangeConfig].
func (e *tokenExchanger) Exchange(ctx context.Context, assertion Assertion) (Credential, error) {
	token := assertion.Token()
	if token == "" {
		return Credential{}, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
	}

	form := url.Values{
		"grant_type":           {grantTypeTokenExchange},
		"subject_token":        {token},
		"subject_token_type":   {tokenTypeJWT},
		"requested_token_type": {e.tokenType},
	}

	if e.delegator != nil {
		if err := e.delegate(ctx, form, token); err != nil {
			return Credential{}, err
		}
	}

	if e.target != "" {
		form.Set("audience", e.target)
	}
	if e.resource != "" {
		form.Set("resource", e.resource)
	}
	if len(e.scopes) > 0 {
		form.Set("scope", strings.Join(e.scopes, " "))
	}

	raw, err := e.client.postForm(ctx, e.name, e.tokenURL, form)
	if err != nil {
		return Credential{}, err
	}

	var response tokenResponse
	if err := decodeJSON(e.name, raw, &response); err != nil {
		return Credential{}, err
	}

	return response.credential(e.name, e.name, assertion, e.clock(), e.maxLifetime)
}

// delegate rewrites form into RFC 8693's delegation shape: the delegator's
// token becomes the subject, and the Flowstate assertion — which arrived as the
// subject — moves to actor_token.
//
// Every failure here refuses the exchange. Falling back to the undelegated form
// would send the delegator's authority request without saying who was acting,
// and the credential that came back would be one the authorization server
// believed the delegator itself had asked for.
func (e *tokenExchanger) delegate(ctx context.Context, form url.Values, assertionToken string) error {
	material, tokenType, err := e.delegator(ctx)
	if err != nil {
		return fmt.Errorf("%w: %s: resolving the delegator's token: %w", ErrExchangeFailed, e.name, err)
	}

	delegatorToken, ok := material.Single()
	if !ok || delegatorToken == "" {
		// A delegated exchange with nobody to act for is not an undelegated
		// one, it is a misconfiguration.
		return fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
	}

	if tokenType == "" {
		tokenType = tokenTypeJWT
	}

	form.Set("subject_token", delegatorToken)
	form.Set("subject_token_type", tokenType)

	// The assertion names the party doing the acting, which is the workload
	// this credential is being obtained for. It is always one of ours, so its
	// type is not the caller's to choose.
	form.Set("actor_token", assertionToken)
	form.Set("actor_token_type", tokenTypeJWT)

	return nil
}

// ClientCredentialsConfig configures an OAuth 2.0 client credentials grant for
// plain service-to-service calls.
//
// By default the Flowstate assertion authenticates the client, using RFC 7523
// private key JWT client authentication, so no client secret is deployed
// anywhere. A ClientSecret may be supplied for an authorization server that does
// not support assertion-based client authentication, which reintroduces exactly
// the long-lived secret federation exists to remove.
//
// # A note on what this can express
//
// A client credentials grant identifies the OAuth client, which is the Flowstate
// deployment, not the individual workload: RFC 7523 requires the assertion's
// issuer and subject to both be the client id, so the workload's own subject
// cannot appear there. Flowstate still includes the workload's claims in the
// assertion, and the assumption policy still decides per workload which targets
// it may use, but the token the authorization server returns is scoped to the
// client. Where a relying party supports RFC 8693, prefer
// [NewTokenExchanger]: it can carry the workload identity all the way through.
type ClientCredentialsConfig struct {
	// Name identifies this exchanger in credentials and audit records. Defaults
	// to "client-credentials".
	Name string

	// TokenURL is the authorization server's token endpoint. Required.
	TokenURL string

	// ClientID is the client identifier the authorization server knows Flowstate
	// by. Required.
	ClientID string

	// Audience is the value the assertion's "aud" claim must carry. RFC 7523
	// section 3 requires the authorization server's token endpoint or issuer
	// identifier; it defaults to TokenURL.
	Audience string

	// ClientSecret authenticates the client with a shared secret instead of an
	// assertion. Leave it empty to use assertion-based authentication, which is
	// the point of federation.
	ClientSecret string

	// Scopes are the scopes to request. Optional.
	Scopes []string

	// MaxCredentialLifetime is the longest lifetime Flowstate will accept from
	// this target, with the same meaning it has in [TokenExchangeConfig]: a
	// ceiling on a reported expires_in, and never a stand-in for one that never
	// arrived.
	MaxCredentialLifetime time.Duration

	// HTTPClient, Timeout, and Clock behave as in [TokenExchangeConfig].
	HTTPClient *http.Client
	Timeout    time.Duration
	Clock      func() time.Time
}

// clientCredentialsExchanger implements the client credentials grant.
type clientCredentialsExchanger struct {
	name     string
	tokenURL string
	clientID string
	audience string

	// secret is [Material] rather than a string, and it is the only field here
	// that has to be. A string is reachable by reflection through the pointer a
	// caller holds, so `%+v` on this exchanger printed the client secret in
	// full — the leak class invariant 7 names, and the one this package's own
	// redacting types exist to close. Material holds the value in a closure,
	// which reflection cannot reach at all.
	//
	// Zero when no secret is configured, which is what NewSingleMaterial answers for
	// an empty value, so the tests below read exactly as the `== ""` they
	// replace.
	secret Material

	scopes      []string
	client      *exchangeClient
	clock       func() time.Time
	maxLifetime time.Duration
}

// NewClientCredentialsExchanger returns an [Exchanger] that performs an OAuth 2.0
// client credentials grant, authenticated by the Flowstate assertion unless a
// client secret is configured.
func NewClientCredentialsExchanger(cfg ClientCredentialsConfig) (Exchanger, error) {
	name := cfg.Name
	if name == "" {
		name = "client-credentials"
	}

	if err := requiredEndpoint(name, "token_url", cfg.TokenURL); err != nil {
		return nil, err
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("%w: %s exchanger needs a client id", ErrInvalidPolicy, name)
	}

	audience := cfg.Audience
	if audience == "" {
		audience = cfg.TokenURL
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	maxLifetime, err := credentialLifetimeCeiling(name, cfg.MaxCredentialLifetime)
	if err != nil {
		return nil, err
	}

	return &clientCredentialsExchanger{
		name:        name,
		tokenURL:    cfg.TokenURL,
		clientID:    cfg.ClientID,
		audience:    audience,
		secret:      NewSingleMaterial(cfg.ClientSecret),
		scopes:      cfg.Scopes,
		client:      newExchangeClient(cfg.HTTPClient, cfg.Timeout),
		clock:       clock,
		maxLifetime: maxLifetime,
	}, nil
}

// Name implements [Exchanger].
func (e *clientCredentialsExchanger) Name() string { return e.name }

// Requirement implements [Exchanger].
//
// When the assertion authenticates the client, RFC 7523 section 3 requires its
// subject to be the client id, so the subject is overridden. The workload is
// still described by the assertion's other claims.
func (e *clientCredentialsExchanger) Requirement() Requirement {
	requirement := Requirement{Audience: e.audience}
	if e.secret.IsZero() {
		requirement.Subject = e.clientID
	}
	return requirement
}

// Exchange implements [Exchanger].
func (e *clientCredentialsExchanger) Exchange(ctx context.Context, assertion Assertion) (Credential, error) {
	form := url.Values{
		"grant_type": {grantTypeClientCredentials},
		"client_id":  {e.clientID},
	}
	if len(e.scopes) > 0 {
		form.Set("scope", strings.Join(e.scopes, " "))
	}

	if secret, ok := e.secret.Single(); ok {
		form.Set("client_secret", secret)
	} else {
		token := assertion.Token()
		if token == "" {
			return Credential{}, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
		}
		form.Set("client_assertion_type", clientAssertionTypeJWT)
		form.Set("client_assertion", token)
	}

	raw, err := e.client.postForm(ctx, e.name, e.tokenURL, form)
	if err != nil {
		return Credential{}, err
	}

	var response tokenResponse
	if err := decodeJSON(e.name, raw, &response); err != nil {
		return Credential{}, err
	}

	return response.credential(e.name, e.name, assertion, e.clock(), e.maxLifetime)
}
