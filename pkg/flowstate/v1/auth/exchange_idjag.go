package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// ID-JAG draft-04 wire identifiers. They intentionally do not share the RFC
// 8693 exchanger's constants or implementation: the first leg uses Token
// Exchange only to obtain a JWT authorization grant, and the second leg is an
// RFC 7523 JWT bearer grant. Neither leg may degrade to another grant.
const (
	idjagRequestedTokenType         = "urn:ietf:params:oauth:token-type:id-jag"
	idjagGrantProfile               = "urn:ietf:params:oauth:grant-profile:id-jag"
	idjagJWTBearerGrant             = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	idjagJWTType                    = "oauth-id-jag+jwt"
	idjagMaximumAccessTokenLifetime = 24 * time.Hour
)

// IDJAGSubject is an activity-local identity assertion. Material is deliberately
// non-serializable; acquiring it in workflow code produces an unusable value.
type IDJAGSubject struct {
	Material      Material
	Type          string
	ID            string
	Subject       string
	Audience      string
	ClientID      string
	IdentityClass string
	ActorClass    string
	ActorChain    string
	TransactionID string
	ExpiresAt     time.Time
	Consent       bool
	ACR           string
}

// IDJAGClaims are authenticated claims returned by VerifyGrant. VerifyGrant
// must verify the signature and typ before returning; separating verification
// from parsing prevents this client from ever authorizing unverified JWT data.
type IDJAGClaims struct {
	ID, Issuer, Subject, Audience, ClientID, Resource string
	Tenant, AudienceTenant, ActorChain, TransactionID string
	IdentityClass, ActorClass, ACR, ProofThumbprint   string
	IssuedAt, ExpiresAt                               time.Time
	Scopes                                            []string
	AuthorizationDetails                              json.RawMessage
	Claims                                            map[string]any
}

// IDJAGDecision is passed to each CEL-backed policy boundary. Implementations
// must evaluate with Flowstate's bounded CEL evaluator and fail closed.
type IDJAGDecision struct {
	Stage, Target, Resource, ClientID, Subject, ActorChain, TransactionID string
	Scopes                                                                []string
	AuthorizationDetails                                                  json.RawMessage
}

type IDJAGAuthorizeFunc func(context.Context, IDJAGDecision) error
type IDJAGSubjectFunc func(context.Context) (IDJAGSubject, error)
type IDJAGVerifyFunc func(context.Context, Material) (IDJAGClaims, error)
type IDJAGAuthenticateFunc func(context.Context, *http.Request, string) error

// IDJAGRuntime contains secret-bearing and process-local behavior that cannot be
// protobuf configuration. All callbacks run synchronously in the activity that
// calls Exchange; none of their values survive serialization.
type IDJAGRuntime struct {
	Subject                               IDJAGSubjectFunc
	VerifyGrant                           IDJAGVerifyFunc
	AuthenticateIDP, AuthenticateResource IDJAGAuthenticateFunc
	Authorize                             IDJAGAuthorizeFunc
	HTTPClient                            *http.Client
	Clock                                 func() time.Time
	Timeout                               time.Duration
}

type idjagExchanger struct {
	name         string
	cfg          *IDJAGProfile
	runtime      IDJAGRuntime
	client       *exchangeClient
	clock        func() time.Time
	discoverOnce sync.Once
	discoverErr  error
	replayMu     sync.Mutex
	seenGrantIDs map[string]time.Time
}

// NewIDJAGExchanger constructs only draft-04. It requires explicit
// authentication, verification, subject acquisition, and three-stage CEL
// authorization; absence is a configuration error, never a reason to fall back.
func NewIDJAGExchanger(name string, cfg *IDJAGProfile, runtime IDJAGRuntime) (Exchanger, error) {
	if cfg == nil || cfg.GetRevision() != IDJAGProfile_DRAFT_IETF_OAUTH_IDENTITY_ASSERTION_AUTHZ_GRANT_04 {
		return nil, fmt.Errorf("%w: ID-JAG revision must be draft-ietf-oauth-identity-assertion-authz-grant-04", ErrInvalidPolicy)
	}
	if name == "" {
		name = "id-jag-draft-04"
	}
	if cfg.GetIdentityProviderIssuer() == "" || cfg.GetResourceAuthorizationServerIssuer() == "" ||
		cfg.GetIdentityProviderTokenEndpoint() == "" || cfg.GetResourceAuthorizationServerTokenEndpoint() == "" ||
		cfg.GetAssertionAudience() == "" || cfg.GetClientId() == "" || cfg.GetResourceApplication() == "" || cfg.GetTargetResource() == "" {
		return nil, fmt.Errorf("%w: %s requires both issuers/endpoints, assertion audience, client, resource application, and target resource", ErrInvalidPolicy, name)
	}
	if len(cfg.GetRequestedScopes()) == 0 == (len(cfg.GetAuthorizationDetailsJson()) == 0) {
		return nil, fmt.Errorf("%w: %s requires exactly one of requested scopes or authorization details", ErrInvalidPolicy, name)
	}
	if cfg.GetClientIdInterpretation() == IDJAGProfile_CLIENT_ID_INTERPRETATION_UNSPECIFIED ||
		cfg.GetClientAuthenticationMethod() == IDJAGProfile_CLIENT_AUTHENTICATION_METHOD_UNSPECIFIED ||
		cfg.GetProofRequirement() == IDJAGProfile_PROOF_REQUIREMENT_UNSPECIFIED {
		return nil, fmt.Errorf("%w: %s requires explicit client-ID, client-authentication, and proof semantics", ErrInvalidPolicy, name)
	}
	if cfg.GetIdentityProviderIssuer() != cfg.GetResourceAuthorizationServerIssuer() && cfg.GetTenantRelationship() == "" {
		return nil, fmt.Errorf("%w: %s requires an explicit cross-domain tenant relationship", ErrInvalidPolicy, name)
	}
	if runtime.Subject == nil || runtime.VerifyGrant == nil || runtime.AuthenticateIDP == nil || runtime.AuthenticateResource == nil || runtime.Authorize == nil {
		return nil, fmt.Errorf("%w: %s requires subject, verifier, both client authenticators, and CEL authorization", ErrInvalidPolicy, name)
	}
	clock := runtime.Clock
	if clock == nil {
		clock = time.Now
	}
	return &idjagExchanger{name: name, cfg: cfg, runtime: runtime, client: newExchangeClient(runtime.HTTPClient, runtime.Timeout), clock: clock}, nil
}

func (e *idjagExchanger) Name() string { return e.name }
func (e *idjagExchanger) Requirement() Requirement {
	return Requirement{Audience: e.cfg.GetAssertionAudience()}
}

func (e *idjagExchanger) Exchange(ctx context.Context, transaction Assertion) (Credential, error) {
	if transaction.ID == "" || transaction.Subject == "" || transaction.Audience != e.cfg.GetAssertionAudience() {
		return Credential{}, fmt.Errorf("%w: %s requires a unique transaction ID, effective subject, and exact assertion audience", ErrExchangeFailed, e.name)
	}
	e.discoverOnce.Do(func() { e.discoverErr = e.discover(ctx) })
	if e.discoverErr != nil {
		return Credential{}, e.discoverErr
	}

	subject, err := e.runtime.Subject(ctx)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %s identity assertion: %w", ErrExchangeFailed, e.name, err)
	}
	if err := e.validateSubject(subject, transaction); err != nil {
		return Credential{}, err
	}
	decision := e.decision("before_assertion", subject.Subject, subject.ActorChain, transaction.ID)
	if err := e.runtime.Authorize(ctx, decision); err != nil {
		return Credential{}, fmt.Errorf("%w: %s CEL before assertion: %w", ErrExchangeFailed, e.name, err)
	}

	grant, response, err := e.requestGrant(ctx, subject)
	if err != nil {
		return Credential{}, err
	}
	claims, err := e.runtime.VerifyGrant(ctx, grant)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %s verifying ID-JAG: %w", ErrExchangeFailed, e.name, err)
	}
	if err := e.validateGrant(claims, subject, transaction, response); err != nil {
		return Credential{}, err
	}
	if err := e.rememberGrant(claims.ID, claims.ExpiresAt); err != nil {
		return Credential{}, err
	}
	decision = e.decision("before_grant", claims.Subject, claims.ActorChain, transaction.ID)
	if err := e.runtime.Authorize(ctx, decision); err != nil {
		return Credential{}, fmt.Errorf("%w: %s CEL before grant: %w", ErrExchangeFailed, e.name, err)
	}

	credential, final, err := e.redeem(ctx, grant, transaction)
	if err != nil {
		return Credential{}, err
	}
	decision = e.decision("before_accept", claims.Subject, claims.ActorChain, transaction.ID)
	decision.Scopes = strings.Fields(final.Scope)
	decision.AuthorizationDetails = final.AuthorizationDetails
	if err := e.runtime.Authorize(ctx, decision); err != nil {
		return Credential{}, fmt.Errorf("%w: %s CEL before credential acceptance: %w", ErrExchangeFailed, e.name, err)
	}
	return credential, nil
}

func (e *idjagExchanger) rememberGrant(id string, expires time.Time) error {
	e.replayMu.Lock()
	defer e.replayMu.Unlock()
	if e.seenGrantIDs == nil {
		e.seenGrantIDs = make(map[string]time.Time)
	}
	now := e.clock()
	for seen, expiry := range e.seenGrantIDs {
		if !expiry.After(now) {
			delete(e.seenGrantIDs, seen)
		}
	}
	if _, exists := e.seenGrantIDs[id]; exists {
		return fmt.Errorf("%w: %s refused replayed ID-JAG %q", ErrExchangeFailed, e.name, id)
	}
	e.seenGrantIDs[id] = expires
	return nil
}

type idjagTokenResponse struct {
	AccessToken          string          `json:"access_token"`
	IssuedTokenType      string          `json:"issued_token_type"`
	TokenType            string          `json:"token_type"`
	Scope                string          `json:"scope"`
	Resource             string          `json:"resource"`
	RefreshToken         string          `json:"refresh_token"`
	ExpiresIn            int64           `json:"expires_in"`
	AuthorizationDetails json.RawMessage `json:"authorization_details"`
}

func (e *idjagExchanger) requestGrant(ctx context.Context, subject IDJAGSubject) (Material, idjagTokenResponse, error) {
	value, ok := subject.Material.Single()
	if !ok {
		return Material{}, idjagTokenResponse{}, fmt.Errorf("%w: unresolved identity assertion", ErrCredentialUnresolved)
	}
	form := url.Values{"grant_type": {grantTypeTokenExchange}, "requested_token_type": {idjagRequestedTokenType}, "audience": {e.cfg.GetResourceAuthorizationServerIssuer()}, "resource": {e.cfg.GetTargetResource()}, "subject_token": {value}, "subject_token_type": {subject.Type}}
	if len(e.cfg.GetRequestedScopes()) > 0 {
		form.Set("scope", strings.Join(e.cfg.GetRequestedScopes(), " "))
	} else {
		form.Set("authorization_details", string(e.cfg.GetAuthorizationDetailsJson()))
	}
	raw, err := e.postAuthenticated(ctx, e.cfg.GetIdentityProviderTokenEndpoint(), form, e.runtime.AuthenticateIDP)
	if err != nil {
		return Material{}, idjagTokenResponse{}, err
	}
	var response idjagTokenResponse
	if err := decodeJSON(e.name, raw, &response); err != nil {
		return Material{}, response, err
	}
	maximumGrantLifetime := int64(e.cfg.GetMaximumAssertionLifetimeSeconds())
	if maximumGrantLifetime == 0 {
		maximumGrantLifetime = 300
	}
	if response.AccessToken == "" || response.IssuedTokenType != idjagRequestedTokenType || !strings.EqualFold(response.TokenType, "N_A") || response.RefreshToken != "" || response.ExpiresIn <= 0 || response.ExpiresIn > maximumGrantLifetime {
		return Material{}, response, fmt.Errorf("%w: %s returned an invalid ID-JAG response", ErrExchangeFailed, e.name)
	}
	return NewSingleMaterial(response.AccessToken), response, nil
}

func (e *idjagExchanger) redeem(ctx context.Context, grant Material, transaction Assertion) (Credential, idjagTokenResponse, error) {
	value, ok := grant.Single()
	if !ok {
		return Credential{}, idjagTokenResponse{}, ErrCredentialUnresolved
	}
	form := url.Values{"grant_type": {idjagJWTBearerGrant}, "assertion": {value}}
	raw, err := e.postAuthenticated(ctx, e.cfg.GetResourceAuthorizationServerTokenEndpoint(), form, e.runtime.AuthenticateResource)
	if err != nil {
		return Credential{}, idjagTokenResponse{}, err
	}
	var r idjagTokenResponse
	if err := decodeJSON(e.name, raw, &r); err != nil {
		return Credential{}, r, err
	}
	if r.AccessToken == "" || !strings.EqualFold(r.TokenType, map[bool]string{true: "DPoP", false: "Bearer"}[e.cfg.GetProofRequirement() == IDJAGProfile_DPOP]) || r.ExpiresIn <= 0 || r.ExpiresIn > int64(idjagMaximumAccessTokenLifetime/time.Second) || r.RefreshToken != "" || (r.Resource != "" && r.Resource != e.cfg.GetTargetResource()) {
		return Credential{}, r, fmt.Errorf("%w: %s returned a broadened or improperly bound credential", ErrExchangeFailed, e.name)
	}
	if !subset(strings.Fields(r.Scope), e.cfg.GetRequestedScopes()) || !jsonSubset(r.AuthorizationDetails, e.cfg.GetAuthorizationDetailsJson()) {
		return Credential{}, r, fmt.Errorf("%w: %s returned excessive authorization", ErrExchangeFailed, e.name)
	}
	cred, err := NewCredential(CredentialBearer, e.clock().Add(time.Duration(r.ExpiresIn)*time.Second), map[string]string{CredentialAccessToken: r.AccessToken})
	if err != nil {
		return Credential{}, r, err
	}
	cred.Target = e.name
	cred.Provider = e.name
	cred.Scopes = strings.Fields(r.Scope)
	cred.AssertionID = transaction.ID
	return cred, r, nil
}

func (e *idjagExchanger) postAuthenticated(ctx context.Context, endpoint string, form url.Values, authenticate IDJAGAuthenticateFunc) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.client.timeout)
	defer cancel()
	if _, err := ValidateHTTPSURL(endpoint, "endpoint"); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("%w: building %s request: %w", ErrExchangeFailed, e.name, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if err := authenticate(ctx, req, endpoint); err != nil {
		return nil, fmt.Errorf("%w: %s client authentication: %w", ErrExchangeFailed, e.name, err)
	}
	resp, err := e.client.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w: %s request: %v", ErrExchangeFailed, ErrExchangeUnavailable, e.name, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxExchangeResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s response: %v", ErrExchangeFailed, e.name, err)
	}
	if len(raw) > maxExchangeResponseBytes {
		return nil, fmt.Errorf("%w: %s returned more than %d bytes", ErrExchangeFailed, e.name, maxExchangeResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned %s%s", ErrExchangeFailed, e.name, resp.Status, describeError(raw))
	}
	return raw, nil
}

func (e *idjagExchanger) validateSubject(s IDJAGSubject, tx Assertion) error {
	now := e.clock()
	max := time.Duration(e.cfg.GetMaximumAssertionLifetimeSeconds()) * time.Second
	if max == 0 {
		max = 5 * time.Minute
	}
	if s.ID == "" || s.TransactionID != tx.ID || s.Subject == "" || s.Subject != tx.Subject || s.Audience != e.cfg.GetClientId() || s.ClientID != e.cfg.GetClientId() || !s.ExpiresAt.After(now) || s.ExpiresAt.After(now.Add(max)) || s.ActorChain == "" || !slices.Contains(e.cfg.GetAcceptedIdentityClasses(), s.IdentityClass) || (s.ActorClass != "" && !slices.Contains(e.cfg.GetAcceptedActorClasses(), s.ActorClass)) || (e.cfg.GetRequireConsent() && !s.Consent) || (len(e.cfg.GetRequiredAcrValues()) > 0 && !slices.Contains(e.cfg.GetRequiredAcrValues(), s.ACR)) {
		return fmt.Errorf("%w: %s identity assertion violates binding, lifetime, identity-chain, consent, or step-up policy", ErrExchangeFailed, e.name)
	}
	return nil
}

func (e *idjagExchanger) validateGrant(c IDJAGClaims, s IDJAGSubject, tx Assertion, r idjagTokenResponse) error {
	now := e.clock()
	max := time.Duration(e.cfg.GetMaximumAssertionLifetimeSeconds()) * time.Second
	if max == 0 {
		max = 5 * time.Minute
	}
	if c.ID == "" || c.Issuer != e.cfg.GetIdentityProviderIssuer() || c.Audience != e.cfg.GetResourceAuthorizationServerIssuer() || c.ClientID != e.cfg.GetClientId() || c.Subject != s.Subject || c.Resource != e.cfg.GetTargetResource() || c.ActorChain != s.ActorChain || c.TransactionID != tx.ID || !c.ExpiresAt.After(now) || c.ExpiresAt.After(c.IssuedAt.Add(max)) || !subset(c.Scopes, e.cfg.GetRequestedScopes()) || !jsonSubset(c.AuthorizationDetails, e.cfg.GetAuthorizationDetailsJson()) || (e.cfg.GetProofRequirement() == IDJAGProfile_DPOP && c.ProofThumbprint == "") || !slices.Contains(e.cfg.GetAcceptedIdentityClasses(), c.IdentityClass) || (c.ActorClass != "" && !slices.Contains(e.cfg.GetAcceptedActorClasses(), c.ActorClass)) {
		return fmt.Errorf("%w: %s ID-JAG violates exact binding or authorization bounds", ErrExchangeFailed, e.name)
	}
	if (e.cfg.GetIdentityProviderTenant() != "" && c.Tenant != e.cfg.GetIdentityProviderTenant()) || (e.cfg.GetResourceAuthorizationServerTenant() != "" && c.AudienceTenant != e.cfg.GetResourceAuthorizationServerTenant()) {
		return fmt.Errorf("%w: %s ID-JAG tenant context does not match the explicit relationship", ErrExchangeFailed, e.name)
	}
	for claim := range c.Claims {
		if !slices.Contains(e.cfg.GetIdentityClaimAllowlist(), claim) {
			return fmt.Errorf("%w: %s ID-JAG contains non-allowlisted identity claim %q", ErrExchangeFailed, e.name, claim)
		}
	}
	if r.Scope != "" && !subset(strings.Fields(r.Scope), e.cfg.GetRequestedScopes()) {
		return fmt.Errorf("%w: excessive ID-JAG scope", ErrExchangeFailed)
	}
	return nil
}

func (e *idjagExchanger) decision(stage, subject, actor, transaction string) IDJAGDecision {
	return IDJAGDecision{Stage: stage, Target: e.cfg.GetResourceApplication(), Resource: e.cfg.GetTargetResource(), ClientID: e.cfg.GetClientId(), Subject: subject, ActorChain: actor, TransactionID: transaction, Scopes: append([]string(nil), e.cfg.GetRequestedScopes()...), AuthorizationDetails: append(json.RawMessage(nil), e.cfg.GetAuthorizationDetailsJson()...)}
}
func subset(got, want []string) bool {
	for _, v := range got {
		if !slices.Contains(want, v) {
			return false
		}
	}
	return true
}
func jsonSubset(got, want []byte) bool {
	if len(got) == 0 {
		return true
	}
	if len(want) == 0 {
		return false
	}
	var g, w []any
	return json.Unmarshal(got, &g) == nil && json.Unmarshal(want, &w) == nil && reflect.DeepEqual(g, w)
}

func (e *idjagExchanger) discover(ctx context.Context) error {
	for _, server := range []struct {
		issuer string
		idp    bool
	}{{e.cfg.GetIdentityProviderIssuer(), true}, {e.cfg.GetResourceAuthorizationServerIssuer(), false}} {
		u := strings.TrimSuffix(server.issuer, "/") + "/.well-known/oauth-authorization-server"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		resp, err := e.client.client.Do(req)
		if err != nil {
			return fmt.Errorf("%w: ID-JAG metadata: %v", ErrExchangeUnavailable, err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxExchangeResponseBytes+1))
		resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK || len(raw) > maxExchangeResponseBytes {
			return fmt.Errorf("%w: ID-JAG metadata unavailable", ErrExchangeFailed)
		}
		var m struct {
			Issuer        string   `json:"issuer"`
			TokenEndpoint string   `json:"token_endpoint"`
			Requested     []string `json:"identity_chaining_requested_token_types_supported"`
			Profiles      []string `json:"authorization_grant_profiles_supported"`
			Grants        []string `json:"grant_types_supported"`
		}
		if json.Unmarshal(raw, &m) != nil || m.Issuer != server.issuer {
			return fmt.Errorf("%w: invalid ID-JAG metadata", ErrExchangeFailed)
		}
		if server.idp {
			if !slices.Contains(m.Requested, idjagRequestedTokenType) || m.TokenEndpoint != e.cfg.GetIdentityProviderTokenEndpoint() {
				return fmt.Errorf("%w: IdP does not advertise ID-JAG draft-04 support", ErrExchangeFailed)
			}
		} else if !slices.Contains(m.Profiles, idjagGrantProfile) || !slices.Contains(m.Grants, idjagJWTBearerGrant) || m.TokenEndpoint != e.cfg.GetResourceAuthorizationServerTokenEndpoint() {
			return fmt.Errorf("%w: resource authorization server does not advertise ID-JAG draft-04 support", ErrExchangeFailed)
		}
	}
	return nil
}

var _ Exchanger = (*idjagExchanger)(nil)
var _ = idjagJWTType
