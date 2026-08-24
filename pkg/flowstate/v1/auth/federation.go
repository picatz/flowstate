package auth

import (
	"crypto"
	"fmt"
	"net/http"
	"time"

	"github.com/goccy/go-yaml"
)

// FederationPolicy is the outbound half of federation as data: the identity
// Flowstate presents to other systems, the systems its workloads may obtain
// credentials for, and the rules governing which workload may obtain which.
//
// It exists so that trusting Flowstate with a cloud role is a reviewable
// configuration change rather than a code change, and so that the simple cases are
// short. One AWS role is four lines:
//
//	issuer: https://flowstate.example.com
//	targets:
//	  - name: aws-prod
//	    aws:
//	      role_arn: arn:aws:iam::123456789012:role/flowstate
//
// The complicated cases use the same structure rather than a different mechanism:
// more targets, across more providers, with CEL rules deciding which workloads may
// use them.
//
// # No secrets
//
// Nothing here holds a secret, and there is deliberately no field that could. The
// whole point of federation is that a workload proves what it is instead of
// presenting a stored credential, so a policy file that needed protecting would
// defeat it. The one exception in the Go API, a client secret for an authorization
// server that supports nothing better, is reachable only by building
// [NewClientCredentialsExchanger] directly.
type FederationPolicy struct {
	// Issuer is the URL Flowstate publishes its identity at, and the "iss" claim
	// of every assertion it mints. It must be reachable by the relying parties
	// that will verify those assertions. Required.
	Issuer string `json:"issuer" yaml:"issuer"`

	// AssertionLifetime overrides [DefaultAssertionLifetime].
	AssertionLifetime time.Duration `json:"assertion_lifetime,omitempty" yaml:"assertion_lifetime,omitempty"`

	// JWKSPath overrides [DefaultJWKSPath].
	JWKSPath string `json:"jwks_path,omitempty" yaml:"jwks_path,omitempty"`

	// KeyRetention overrides [DefaultKeyRetention], how long a rotated-out key
	// stays published.
	KeyRetention time.Duration `json:"key_retention,omitempty" yaml:"key_retention,omitempty"`

	// SigningTimeout overrides [DefaultSigningTimeout], how long one signature
	// may take. It is the bound that matters to a deployment signing through a
	// KMS or an HSM — see [WithSigningTimeout] — and is here rather than only
	// in the Go API because the deployments with a remote signer are exactly
	// the ones configured from a policy file.
	SigningTimeout time.Duration `json:"signing_timeout,omitempty" yaml:"signing_timeout,omitempty"`

	// DeclaredClaims names the extension claims assertions minted here may
	// carry, beyond the ones every assertion has. The claim set is closed: a
	// carried claim absent from this list is refused at mint rather than
	// signed, with [ErrUndeclaredClaim].
	//
	//	federation:
	//	  issuer: https://flowstate.example.com
	//	  declared_claims: [repository, environment]
	//
	// Empty declares none, which is the fail-closed default: a deployment that
	// has not said which claims are part of its assertions' contract mints
	// assertions carrying only the claims the issuer sets itself.
	//
	// This is where a *deployment* declares a claim. Core claims are declared
	// in the schema instead, as the [ClaimNamespace] constants and the
	// reserved set built from them, so `buf breaking` covers them — a tenant
	// that needs a claim cannot edit our .proto, and a claim the issuer sets
	// itself is not a tenant's to redefine.
	//
	// See [WithDeclaredClaims] for why this is an allowlist and for its
	// relationship to the server's `--identity-claim`.
	DeclaredClaims []string `json:"declared_claims,omitempty" yaml:"declared_claims,omitempty"`

	// Allow are CEL rules gating credential assumption. When any are present, a
	// request must match one of them.
	//
	// The attributes are `target` and `audience`, plus two objects: `identity`,
	// the authenticated caller, carrying subject, issuer, namespace and claims —
	// the same four an egress or task-shape rule sees, meaning the same things —
	// and `workload`, the assertion this request would mint, carrying subject,
	// namespace, deployment, workflow, run, step, on_behalf_of,
	// on_behalf_of_issuer and claims.
	//
	// Grouped rather than bare because `namespace` is a reserved identifier in
	// CEL and cannot be a variable name, and a claim an operator carries could
	// collide with any other reserved word. Under an object every name is a
	// field, and no name is reserved.
	//
	//	# assumption policy
	//	allow:
	//	  - 'target == "aws-prod" && workload.on_behalf_of.startsWith("repo:acme/infra:")'
	//	  - 'target == "partner" && identity.namespace == "acme"'
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`

	// Deny are CEL rules refusing credential assumption. A request matching any of
	// them is refused, whatever Allow says.
	Deny []string `json:"deny,omitempty" yaml:"deny,omitempty"`

	// Targets are the systems workloads may obtain credentials for.
	Targets []FederationTarget `json:"targets,omitempty" yaml:"targets,omitempty"`
}

// FederationTarget is one system a workload may obtain a credential for. Exactly
// one of the provider fields must be set, which is what keeps a setting from
// silently belonging to no provider.
type FederationTarget struct {
	// Name is the operator's name for this system, matched by assumption rules and
	// recorded in audit. Required, and unique within the policy.
	Name string `json:"name" yaml:"name"`

	// TokenExchange configures RFC 8693 OAuth 2.0 Token Exchange, the
	// standards-based path that works with any authorization server implementing
	// it. Prefer it where it is available.
	TokenExchange *TokenExchangeTarget `json:"token_exchange,omitempty" yaml:"token_exchange,omitempty"`

	// AWS configures STS AssumeRoleWithWebIdentity.
	AWS *AWSTarget `json:"aws,omitempty" yaml:"aws,omitempty"`

	// GCP configures Google Cloud Workload Identity Federation.
	GCP *GCPTarget `json:"gcp,omitempty" yaml:"gcp,omitempty"`

	// ClientCredentials configures an OAuth 2.0 client credentials grant
	// authenticated by the Flowstate assertion.
	ClientCredentials *ClientCredentialsTarget `json:"client_credentials,omitempty" yaml:"client_credentials,omitempty"`

	// Assertion configures presenting the Flowstate assertion itself as the
	// bearer credential, for a relying party that verifies OIDC and needs no
	// exchange. See [AssertionConfig] for what that costs.
	Assertion *AssertionTarget `json:"assertion,omitempty" yaml:"assertion,omitempty"`
}

// TokenExchangeTarget is the file form of [TokenExchangeConfig].
type TokenExchangeTarget struct {
	TokenURL              string        `json:"token_url" yaml:"token_url"`
	Audience              string        `json:"audience" yaml:"audience"`
	TargetAudience        string        `json:"target_audience,omitempty" yaml:"target_audience,omitempty"`
	Resource              string        `json:"resource,omitempty" yaml:"resource,omitempty"`
	Scopes                []string      `json:"scopes,omitempty" yaml:"scopes,omitempty"`
	RequestedTokenType    string        `json:"requested_token_type,omitempty" yaml:"requested_token_type,omitempty"`
	MaxCredentialLifetime time.Duration `json:"max_credential_lifetime,omitempty" yaml:"max_credential_lifetime,omitempty"`
}

// AWSTarget is the file form of [AWSConfig].
type AWSTarget struct {
	RoleARN           string        `json:"role_arn" yaml:"role_arn"`
	Audience          string        `json:"audience,omitempty" yaml:"audience,omitempty"`
	Region            string        `json:"region,omitempty" yaml:"region,omitempty"`
	Endpoint          string        `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Duration          time.Duration `json:"duration,omitempty" yaml:"duration,omitempty"`
	SessionPolicy     string        `json:"session_policy,omitempty" yaml:"session_policy,omitempty"`
	SessionPolicyARNs []string      `json:"session_policy_arns,omitempty" yaml:"session_policy_arns,omitempty"`
}

// GCPTarget is the file form of [GCPConfig].
type GCPTarget struct {
	Audience            string        `json:"audience" yaml:"audience"`
	AssertionAudience   string        `json:"assertion_audience,omitempty" yaml:"assertion_audience,omitempty"`
	ServiceAccountEmail string        `json:"service_account_email,omitempty" yaml:"service_account_email,omitempty"`
	Scopes              []string      `json:"scopes,omitempty" yaml:"scopes,omitempty"`
	Lifetime            time.Duration `json:"lifetime,omitempty" yaml:"lifetime,omitempty"`
	Endpoint            string        `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	IAMEndpoint         string        `json:"iam_endpoint,omitempty" yaml:"iam_endpoint,omitempty"`
}

// ClientCredentialsTarget is the file form of [ClientCredentialsConfig]. It has no
// client secret field by design; see [FederationPolicy].
type ClientCredentialsTarget struct {
	TokenURL              string        `json:"token_url" yaml:"token_url"`
	ClientID              string        `json:"client_id" yaml:"client_id"`
	Audience              string        `json:"audience,omitempty" yaml:"audience,omitempty"`
	Scopes                []string      `json:"scopes,omitempty" yaml:"scopes,omitempty"`
	MaxCredentialLifetime time.Duration `json:"max_credential_lifetime,omitempty" yaml:"max_credential_lifetime,omitempty"`
}

// AssertionTarget is the file form of [AssertionConfig]. It carries only an
// audience, because there is nothing to exchange with: the credential is the
// assertion, and its lifetime is the issuer's `assertion_lifetime` above rather
// than a second one written per target.
type AssertionTarget struct {
	Audience string `json:"audience" yaml:"audience"`
}

// ParseFederationPolicy decodes an outbound federation policy from YAML or JSON.
// Unknown and duplicate fields are errors, so a misspelled key fails at startup
// rather than silently dropping a restriction.
func ParseFederationPolicy(data []byte) (FederationPolicy, error) {
	var policy FederationPolicy

	if err := yaml.UnmarshalWithOptions(data, &policy, yaml.Strict()); err != nil {
		return FederationPolicy{}, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	if err := policy.Validate(); err != nil {
		return FederationPolicy{}, err
	}

	return policy, nil
}

// Validate reports whether the policy is usable. [FederationPolicy.Broker] calls
// it, so configuration mistakes surface at startup.
func (p FederationPolicy) Validate() error {
	if err := validateIssuerURL(p.Issuer); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	// Checked here as well as in [NewIssuer], so that a declaration that can
	// never apply is a parse error rather than something an operator learns
	// about the first time a workload asks for a credential.
	if _, err := validateDeclaredClaims(p.DeclaredClaims); err != nil {
		return err
	}

	names := make(map[string]struct{}, len(p.Targets))
	for i, target := range p.Targets {
		if target.Name == "" {
			return fmt.Errorf("%w: targets[%d]: name is required", ErrInvalidPolicy, i)
		}
		if _, duplicate := names[target.Name]; duplicate {
			return fmt.Errorf("%w: targets[%d]: duplicate name %q", ErrInvalidPolicy, i, target.Name)
		}
		names[target.Name] = struct{}{}

		configured := 0
		for _, set := range []bool{
			target.TokenExchange != nil,
			target.AWS != nil,
			target.GCP != nil,
			target.ClientCredentials != nil,
			target.Assertion != nil,
		} {
			if set {
				configured++
			}
		}
		switch configured {
		case 1:
		case 0:
			return fmt.Errorf("%w: targets[%d] %q: needs one of token_exchange, aws, gcp, client_credentials, or assertion",
				ErrInvalidPolicy, i, target.Name)
		default:
			return fmt.Errorf("%w: targets[%d] %q: configures %d providers, and a target names exactly one system",
				ErrInvalidPolicy, i, target.Name, configured)
		}
	}

	// Building the exchangers is the real validation: it applies every provider's
	// own rules about required and well-formed fields, so a policy that validates
	// is one that produces a working broker.
	if _, err := p.exchangers(federationConfig{}); err != nil {
		return err
	}

	if _, err := compileAssumeRules(p.Allow, p.Deny, DefaultAssumeRuleCostLimit); err != nil {
		return err
	}

	return nil
}

// federationConfig collects the options for building from a policy.
type federationConfig struct {
	client *http.Client
	clock  func() time.Time

	// verifyOnly are extra public keys to publish beside the signing key. They
	// are issuer options rather than a second list of key material here,
	// because the issuer is the thing that publishes a key set and this type
	// only has to carry them to it.
	verifyOnly []IssuerOption
}

// A FederationOption configures how a [FederationPolicy] is built into a [Broker].
type FederationOption func(*federationConfig)

// WithFederationHTTPClient sets the HTTP client used to reach every relying party.
// Use it to supply a client with a proxy, custom roots, or instrumentation.
func WithFederationHTTPClient(client *http.Client) FederationOption {
	return func(c *federationConfig) {
		if client != nil {
			c.client = client
		}
	}
}

// WithFederationClock sets the clock used for assertion and credential lifetimes.
// It exists for tests.
func WithFederationClock(clock func() time.Time) FederationOption {
	return func(c *federationConfig) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithFederationVerifyOnlyKey publishes one more public key in the issuer's key
// set, without a private half, so assertions a previous process signed keep
// verifying across a restart. It is [WithVerifyOnlyKey] reached from a policy,
// and takes the same (id, public key) pair for the same reasons; repeat it for
// each key. See that option for what rotation across a restart needs and why
// this is not revocation.
func WithFederationVerifyOnlyKey(id string, public crypto.PublicKey) FederationOption {
	return func(c *federationConfig) {
		c.verifyOnly = append(c.verifyOnly, WithVerifyOnlyKey(id, public))
	}
}

// Broker builds the issuer, exchangers, and assumption rules this policy
// describes, signing assertions with the given key.
//
// Everything is validated and compiled here, so a bad policy fails at startup
// rather than the first time a workload asks for a credential. Reach the issuer,
// to publish its discovery document and key set, with [Broker.Issuer].
func (p FederationPolicy) Broker(key SigningKey, opts ...FederationOption) (*Broker, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	var cfg federationConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	issuerOpts := []IssuerOption{}
	if p.AssertionLifetime > 0 {
		issuerOpts = append(issuerOpts, WithAssertionLifetime(p.AssertionLifetime))
	}
	if p.JWKSPath != "" {
		issuerOpts = append(issuerOpts, WithJWKSPath(p.JWKSPath))
	}
	if p.KeyRetention > 0 {
		issuerOpts = append(issuerOpts, WithKeyRetention(p.KeyRetention))
	}
	if p.SigningTimeout > 0 {
		issuerOpts = append(issuerOpts, WithSigningTimeout(p.SigningTimeout))
	}
	if len(p.DeclaredClaims) > 0 {
		issuerOpts = append(issuerOpts, WithDeclaredClaims(p.DeclaredClaims...))
	}
	if cfg.clock != nil {
		issuerOpts = append(issuerOpts, WithIssuerClock(cfg.clock))
	}
	// Last, so the retention and clock the policy configured are the ones the
	// published keys are measured against: [NewIssuer] installs them once every
	// option has been applied, but ordering them here keeps that from being a
	// property a reader has to go and check.
	issuerOpts = append(issuerOpts, cfg.verifyOnly...)

	issuer, err := NewIssuer(p.Issuer, key, issuerOpts...)
	if err != nil {
		return nil, err
	}

	exchangers, err := p.exchangers(cfg)
	if err != nil {
		return nil, err
	}

	brokerOpts := []BrokerOption{
		WithAssumeAllowRules(p.Allow...),
		WithAssumeDenyRules(p.Deny...),
	}
	for name, exchanger := range exchangers {
		brokerOpts = append(brokerOpts, WithTarget(name, exchanger))
	}
	if cfg.clock != nil {
		brokerOpts = append(brokerOpts, WithBrokerClock(cfg.clock))
	}

	return NewBroker(issuer, brokerOpts...)
}

// exchangers builds an exchanger for every configured target.
func (p FederationPolicy) exchangers(cfg federationConfig) (map[string]Exchanger, error) {
	exchangers := make(map[string]Exchanger, len(p.Targets))

	for _, target := range p.Targets {
		var (
			exchanger Exchanger
			err       error
		)

		switch {
		case target.TokenExchange != nil:
			exchanger, err = NewTokenExchanger(TokenExchangeConfig{
				Name:                  target.Name,
				TokenURL:              target.TokenExchange.TokenURL,
				Audience:              target.TokenExchange.Audience,
				TargetAudience:        target.TokenExchange.TargetAudience,
				Resource:              target.TokenExchange.Resource,
				Scopes:                target.TokenExchange.Scopes,
				RequestedTokenType:    target.TokenExchange.RequestedTokenType,
				MaxCredentialLifetime: target.TokenExchange.MaxCredentialLifetime,
				HTTPClient:            cfg.client,
				Clock:                 cfg.clock,
			})
		case target.AWS != nil:
			exchanger, err = NewAWSExchanger(AWSConfig{
				Name:              target.Name,
				RoleARN:           target.AWS.RoleARN,
				Audience:          target.AWS.Audience,
				Region:            target.AWS.Region,
				Endpoint:          target.AWS.Endpoint,
				Duration:          target.AWS.Duration,
				SessionPolicy:     target.AWS.SessionPolicy,
				SessionPolicyARNs: target.AWS.SessionPolicyARNs,
				HTTPClient:        cfg.client,
				Clock:             cfg.clock,
			})
		case target.GCP != nil:
			exchanger, err = NewGCPExchanger(GCPConfig{
				Name:                target.Name,
				Audience:            target.GCP.Audience,
				AssertionAudience:   target.GCP.AssertionAudience,
				ServiceAccountEmail: target.GCP.ServiceAccountEmail,
				Scopes:              target.GCP.Scopes,
				Lifetime:            target.GCP.Lifetime,
				Endpoint:            target.GCP.Endpoint,
				IAMEndpoint:         target.GCP.IAMEndpoint,
				HTTPClient:          cfg.client,
				Clock:               cfg.clock,
			})
		case target.ClientCredentials != nil:
			exchanger, err = NewClientCredentialsExchanger(ClientCredentialsConfig{
				Name:                  target.Name,
				TokenURL:              target.ClientCredentials.TokenURL,
				ClientID:              target.ClientCredentials.ClientID,
				Audience:              target.ClientCredentials.Audience,
				Scopes:                target.ClientCredentials.Scopes,
				MaxCredentialLifetime: target.ClientCredentials.MaxCredentialLifetime,
				HTTPClient:            cfg.client,
				Clock:                 cfg.clock,
			})
		case target.Assertion != nil:
			// No HTTP client and no clock: this exchanger reaches nothing and
			// takes its expiry from the assertion the issuer minted.
			exchanger, err = NewAssertionExchanger(AssertionConfig{
				Name:     target.Name,
				Audience: target.Assertion.Audience,
			})
		default:
			// Validate rejects this, and reaching it would mean a target with no
			// provider had been built into a broker.
			return nil, fmt.Errorf("%w: target %q configures no provider", ErrInvalidPolicy, target.Name)
		}
		if err != nil {
			return nil, err
		}

		exchangers[target.Name] = exchanger
	}

	return exchangers, nil
}
