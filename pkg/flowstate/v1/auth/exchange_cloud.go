package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// AWS session credential limits, from the AssumeRoleWithWebIdentity API.
const (
	// awsMinDuration is the shortest session AWS will issue.
	awsMinDuration = 15 * time.Minute

	// awsMaxDuration is the longest session AWS will issue without a raised
	// role limit.
	awsMaxDuration = 12 * time.Hour

	// awsDefaultAudience is the audience AWS's OIDC identity providers expect,
	// and what an operator configures on the provider unless they choose
	// otherwise.
	awsDefaultAudience = "sts.amazonaws.com"

	// awsGlobalEndpoint is the global STS endpoint, used when no region is set.
	awsGlobalEndpoint = "https://sts.amazonaws.com/"

	// awsAPIVersion is the STS API version the query protocol requires.
	awsAPIVersion = "2011-06-15"

	// awsMaxSessionNameLength is the longest role session name AWS accepts.
	awsMaxSessionNameLength = 64

	// awsSessionDigestLength is how much of the subject digest a session name
	// carries, in base32 characters at five bits each — so twenty is a hundred
	// bits.
	//
	// The number that matters is second-preimage resistance, not birthday
	// resistance: the attack this closes is a workload aiming at one *particular*
	// other workload's name, which it does by choosing the workflow and step in
	// its own Flowfile. A hundred bits leaves it nothing to grind, and leaves
	// forty-three characters for the part a person reads.
	awsSessionDigestLength = 20
)

// AWSConfig configures exchanging a Flowstate assertion for temporary AWS
// credentials with STS AssumeRoleWithWebIdentity.
//
// On the AWS side an operator registers Flowstate's issuer URL as an IAM OIDC
// identity provider, then writes a role trust policy that conditions on the
// assertion's subject. The subject a workload will present is exactly what
// [WorkloadIdentity.SubjectFor] returns, and it is hierarchical, so a trust
// policy can be as broad or as narrow as the operator wants:
//
//	"Condition": {"StringLike": {
//	  "flowstate.example.com:sub": "flowstate:acme/prod/deploy-service/*"
//	}}
type AWSConfig struct {
	// Name identifies this exchanger in credentials and audit records. Defaults
	// to "aws-sts".
	Name string

	// RoleARN is the role to assume. Required.
	RoleARN string

	// Audience is the value the assertion's "aud" claim must carry, which is the
	// audience configured on the IAM identity provider. Defaults to
	// "sts.amazonaws.com".
	Audience string

	// Region selects a regional STS endpoint, such as "us-east-1". Regional
	// endpoints are preferable: they are lower latency and they keep the request
	// inside one region's failure domain. Empty uses the global endpoint.
	Region string

	// Endpoint overrides the STS endpoint entirely. Mostly useful for tests and
	// for private endpoints.
	Endpoint string

	// Duration is how long the session lasts. AWS allows 15 minutes to 12 hours,
	// subject to the role's own maximum. Defaults to the AWS minimum, because a
	// workflow step needs a credential for the length of one step.
	Duration time.Duration

	// SessionPolicy is an inline IAM policy that further restricts the session,
	// so a role can be reused by several workloads with different scope. Optional.
	SessionPolicy string

	// SessionPolicyARNs are managed policies that further restrict the session.
	// Optional.
	SessionPolicyARNs []string

	// HTTPClient, Timeout, and Clock behave as in [TokenExchangeConfig].
	HTTPClient *http.Client
	Timeout    time.Duration
	Clock      func() time.Time
}

// awsExchanger implements AWS STS AssumeRoleWithWebIdentity.
type awsExchanger struct {
	name       string
	roleARN    string
	audience   string
	endpoint   string
	duration   time.Duration
	policy     string
	policyARNs []string
	client     *exchangeClient
	clock      func() time.Time
}

// NewAWSExchanger returns an [Exchanger] that trades a Flowstate assertion for
// temporary AWS session credentials.
func NewAWSExchanger(cfg AWSConfig) (Exchanger, error) {
	name := cfg.Name
	if name == "" {
		name = "aws-sts"
	}

	if cfg.RoleARN == "" {
		return nil, fmt.Errorf("%w: %s exchanger needs a role ARN", ErrInvalidPolicy, name)
	}
	if !strings.HasPrefix(cfg.RoleARN, "arn:") {
		return nil, fmt.Errorf("%w: %s exchanger role %q is not an ARN", ErrInvalidPolicy, name, truncate(cfg.RoleARN, 64))
	}

	endpoint := cfg.Endpoint
	switch {
	case endpoint != "":
	case cfg.Region != "":
		if strings.ContainsAny(cfg.Region, "/?#@") {
			return nil, fmt.Errorf("%w: %s exchanger region %q is not a region name", ErrInvalidPolicy, name, truncate(cfg.Region, 32))
		}
		endpoint = fmt.Sprintf("https://sts.%s.amazonaws.com/", cfg.Region)
	default:
		endpoint = awsGlobalEndpoint
	}
	if err := requiredEndpoint(name, "endpoint", endpoint); err != nil {
		return nil, err
	}

	duration := cfg.Duration
	if duration == 0 {
		duration = awsMinDuration
	}
	if duration < awsMinDuration || duration > awsMaxDuration {
		return nil, fmt.Errorf("%w: %s exchanger duration %s is outside the %s to %s AWS allows",
			ErrInvalidPolicy, name, duration, awsMinDuration, awsMaxDuration)
	}

	audience := cfg.Audience
	if audience == "" {
		audience = awsDefaultAudience
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	return &awsExchanger{
		name:       name,
		roleARN:    cfg.RoleARN,
		audience:   audience,
		endpoint:   endpoint,
		duration:   duration,
		policy:     cfg.SessionPolicy,
		policyARNs: cfg.SessionPolicyARNs,
		client:     newExchangeClient(cfg.HTTPClient, cfg.Timeout),
		clock:      clock,
	}, nil
}

// Name implements [Exchanger].
func (e *awsExchanger) Name() string { return e.name }

// Requirement implements [Exchanger].
func (e *awsExchanger) Requirement() Requirement {
	return Requirement{Audience: e.audience}
}

// Exchange implements [Exchanger], calling AssumeRoleWithWebIdentity.
func (e *awsExchanger) Exchange(ctx context.Context, assertion Assertion) (Credential, error) {
	token := assertion.Token()
	if token == "" {
		return Credential{}, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
	}

	form := url.Values{
		"Action":           {"AssumeRoleWithWebIdentity"},
		"Version":          {awsAPIVersion},
		"RoleArn":          {e.roleARN},
		"RoleSessionName":  {awsSessionName(assertion.Subject)},
		"WebIdentityToken": {token},
		"DurationSeconds":  {strconv.Itoa(int(e.duration.Seconds()))},
	}
	if e.policy != "" {
		form.Set("Policy", e.policy)
	}
	for i, arn := range e.policyARNs {
		form.Set(fmt.Sprintf("PolicyArns.member.%d.arn", i+1), arn)
	}

	raw, err := e.client.postForm(ctx, e.name, e.endpoint, form)
	if err != nil {
		return Credential{}, err
	}

	var response struct {
		Result struct {
			Credentials struct {
				AccessKeyID     string    `xml:"AccessKeyId"`
				SecretAccessKey string    `xml:"SecretAccessKey"`
				SessionToken    string    `xml:"SessionToken"`
				Expiration      time.Time `xml:"Expiration"`
			} `xml:"Credentials"`
			AssumedRoleUser struct {
				ARN string `xml:"Arn"`
			} `xml:"AssumedRoleUser"`
		} `xml:"AssumeRoleWithWebIdentityResult"`
	}
	if err := xml.Unmarshal(raw, &response); err != nil {
		return Credential{}, fmt.Errorf("%w: decoding %s response: %w", ErrExchangeFailed, e.name, err)
	}

	credentials := response.Result.Credentials
	if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" || credentials.SessionToken == "" {
		return Credential{}, fmt.Errorf("%w: %s returned incomplete session credentials", ErrExchangeFailed, e.name)
	}

	// STS always reports an expiration, and the session it reports must be the
	// session that was asked for. Neither a missing one nor an implausible one is
	// papered over with a guess: a manufactured expiry is a credential Flowstate
	// would keep using after it stopped working, or keep believing is short-lived
	// after it stopped being so.
	now := e.clock()
	expiresAt := credentials.Expiration
	switch {
	case expiresAt.IsZero():
		return Credential{}, fmt.Errorf("%w: %s returned session credentials with no expiration", ErrExchangeFailed, e.name)
	case !withinRequestedLifetime(expiresAt, now, e.duration):
		return Credential{}, fmt.Errorf("%w: %s reported a session expiration of %s, outside the %s session that was requested",
			ErrExchangeFailed, e.name, expiresAt.UTC().Format(time.RFC3339), e.duration)
	}

	credential, err := NewCredential(CredentialAWSSession, expiresAt, map[string]string{
		CredentialAccessKeyID:     credentials.AccessKeyID,
		CredentialSecretAccessKey: credentials.SecretAccessKey,
		CredentialSessionToken:    credentials.SessionToken,
	})
	if err != nil {
		return Credential{}, err
	}

	credential.Target = e.name
	credential.Provider = e.name
	credential.AssertionID = assertion.ID

	return credential, nil
}

// awsSessionName derives a role session name from the workload subject.
//
// The session name is what appears in CloudTrail for everything the credential
// goes on to do, so deriving it from the subject is what connects an AWS audit
// trail back to the workload that caused the call. AWS accepts only
// [\w+=,.@-] and at most 64 characters, so the subject cannot simply be used —
// and the property that makes any substitute worth having is that two different
// subjects must not collapse into one session name.
//
// # Why a digest, and not a better rewrite
//
// Rewriting the separators was the first answer, and it gave that property away
// twice.
//
// AWS accepts `-`, which is also legal *inside* a subject component, so
// `.../c/d-e` and `.../c-d/e` — two different workloads, both valid — rewrote to
// one name. That is the failure CLAUDE.md already records for the secret
// providers, in the same words: no separator fixes it, because every character
// legal in a prefix is legal in a name.
//
// And truncation kept the tail, on the reasoning that the workflow and step are
// more distinguishing than the namespace they share. That holds inside one tenant
// and is backwards across tenants: the namespace is the only component a tenant
// does not choose, so dropping the head left a workload naming all 64 characters
// out of its own Flowfile, and CloudTrail attributing its calls to whichever
// other tenant it wanted.
//
// No rewriting can be injective into a 64-character alphabet from an unbounded
// subject, so the identity is carried by a digest of the whole subject, and the
// readable part sits beside it as a convenience. Distinctness then does not
// depend on what the readable part happens to keep, which is what both bugs did.
//
// # What this is not
//
// It is not the authorization boundary. A relying party restricts *which*
// workloads may assume a role through the `sub` condition on the role's own trust
// policy, matched against the full subject — which is why
// [WorkloadIdentity.SubjectFor] refuses a component containing a separator. This
// is attribution: it has to name one workload unambiguously and stay recognisable
// at a glance.
func awsSessionName(subject string) string {
	// Over the subject as given, before any rewriting. A digest of the rewritten
	// form would inherit exactly the collisions the rewriting introduces.
	sum := sha256.Sum256([]byte(subject))

	// Base32 rather than base64, whose alphabet includes `/` and `+` and only one
	// of those is legal here. Lowercased so the name reads as a word rather than
	// as shouting in the middle of a console.
	digest := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).
		EncodeToString(sum[:]))[:awsSessionDigestLength]

	var readable strings.Builder
	for _, r := range subject {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			readable.WriteRune(r)
		case strings.ContainsRune("+=,.@-_", r):
			readable.WriteRune(r)
		default:
			readable.WriteRune('-')
		}
	}

	name := readable.String()

	// One character for the separator between the two halves.
	if budget := awsMaxSessionNameLength - awsSessionDigestLength - 1; len(name) > budget {
		// Still the tail, and now that is only a readability choice: the step is
		// the most specific component and the one an operator scanning CloudTrail
		// is usually looking for. Nothing depends on it any more.
		//
		// Sliced by byte safely, because every rune outside the accepted alphabet
		// was replaced above and what is left to cut is ASCII.
		name = name[len(name)-budget:]
	}
	if name == "" {
		// A subject that rewrote to nothing at all. The digest alone is a legal
		// name and still a distinct one, which is more than the fixed placeholder
		// this used to return could say.
		return digest
	}

	return name + "-" + digest
}

// xmlError extracts the code and message from an AWS error response, which is
// XML rather than JSON.
func xmlError(raw []byte) (code, message string) {
	var response struct {
		Error struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	if err := xml.Unmarshal(raw, &response); err != nil {
		return "", ""
	}
	return response.Error.Code, response.Error.Message
}

// Google Cloud security token service defaults.
const (
	// gcpSTSEndpoint is Google's security token service token endpoint.
	gcpSTSEndpoint = "https://sts.googleapis.com/v1/token"

	// gcpIAMEndpoint is the IAM Credentials API, used to impersonate a service
	// account after federating.
	gcpIAMEndpoint = "https://iamcredentials.googleapis.com/v1"

	// gcpDefaultScope is the scope requested when none is configured.
	gcpDefaultScope = "https://www.googleapis.com/auth/cloud-platform"

	// gcpFederatedLifetime is how long the security token service's federated
	// access token lasts, and therefore the ceiling on the expires_in it may
	// report. It is not an operator setting: the federated token is either handed
	// straight back or spent immediately on impersonation, and Google names no
	// lifetime knob for it.
	gcpFederatedLifetime = time.Hour

	// gcpDefaultLifetime is the service account token lifetime Google issues when
	// generateAccessToken is called without one. It is written down because
	// impersonate has to bound a response to a request that named no lifetime,
	// and the bound has to be the number Google will actually have used.
	gcpDefaultLifetime = time.Hour

	// gcpMaxLifetime is the longest service account token Google will issue. An
	// hour is the ceiling by default, and up to twelve is available to a project
	// that has set constraints/iam.allowServiceAccountCredentialLifetimeExtension
	// — so refusing anything above an hour here would refuse a legitimately
	// configured deployment at startup, on the strength of a constraint that is
	// Google's to enforce and not ours to guess at.
	gcpMaxLifetime = 12 * time.Hour
)

// GCPConfig configures exchanging a Flowstate assertion for a Google Cloud access
// token through Workload Identity Federation.
//
// On the Google Cloud side an operator creates a workload identity pool with an
// OIDC provider whose issuer is Flowstate's issuer URL, and maps the assertion's
// subject to a principal:
//
//	attribute mapping: google.subject = assertion.sub
//	attribute condition: assertion.namespace == "acme"
//
// The pool provider's resource name is the Audience below, and is what the
// assertion must be minted for.
type GCPConfig struct {
	// Name identifies this exchanger in credentials and audit records. Defaults
	// to "gcp-sts".
	Name string

	// Audience is the workload identity pool provider, in the form
	//
	//	//iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>
	//
	// It is both the security token service audience and, by default, the
	// audience the assertion must carry. Required.
	Audience string

	// AssertionAudience overrides the audience the assertion is minted for, for a
	// provider configured to accept something other than its own resource name.
	// Optional.
	AssertionAudience string

	// ServiceAccountEmail, when set, impersonates that service account after
	// federating, which is how a federated identity acquires a service account's
	// permissions. Optional; without it the federated token itself is returned.
	ServiceAccountEmail string

	// Scopes are the scopes to request. Defaults to cloud-platform.
	Scopes []string

	// Lifetime is how long an impersonated service account token lasts. Google
	// allows up to an hour by default, and up to twelve for a project that has
	// set constraints/iam.allowServiceAccountCredentialLifetimeExtension. Zero
	// names no lifetime at all and takes Google's own one-hour default, which is
	// also what the response is then bounded against. Ignored, and therefore
	// unvalidated, without ServiceAccountEmail.
	Lifetime time.Duration

	// Endpoint overrides the security token service endpoint. Mostly useful for
	// tests.
	Endpoint string

	// IAMEndpoint overrides the IAM Credentials API base URL. Mostly useful for
	// tests.
	IAMEndpoint string

	// HTTPClient, Timeout, and Clock behave as in [TokenExchangeConfig].
	HTTPClient *http.Client
	Timeout    time.Duration
	Clock      func() time.Time
}

// gcpExchanger implements Google Cloud Workload Identity Federation.
type gcpExchanger struct {
	name           string
	audience       string
	assertionAud   string
	serviceAccount string
	scopes         []string
	lifetime       time.Duration
	endpoint       string
	iamEndpoint    string
	client         *exchangeClient
	clock          func() time.Time
}

// NewGCPExchanger returns an [Exchanger] that trades a Flowstate assertion for a
// Google Cloud access token.
func NewGCPExchanger(cfg GCPConfig) (Exchanger, error) {
	name := cfg.Name
	if name == "" {
		name = "gcp-sts"
	}

	if cfg.Audience == "" {
		return nil, fmt.Errorf("%w: %s exchanger needs the workload identity pool provider as its audience", ErrInvalidPolicy, name)
	}

	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = gcpSTSEndpoint
	}
	if err := requiredEndpoint(name, "endpoint", endpoint); err != nil {
		return nil, err
	}

	iamEndpoint := cfg.IAMEndpoint
	if iamEndpoint == "" {
		iamEndpoint = gcpIAMEndpoint
	}
	if cfg.ServiceAccountEmail != "" {
		if err := requiredEndpoint(name, "iam_endpoint", iamEndpoint); err != nil {
			return nil, err
		}
		if !strings.Contains(cfg.ServiceAccountEmail, "@") {
			return nil, fmt.Errorf("%w: %s exchanger service account %q is not an email address",
				ErrInvalidPolicy, name, truncate(cfg.ServiceAccountEmail, 64))
		}

		// Checked under impersonation for the same reason iam_endpoint above is:
		// Lifetime is documented as ignored without a service account, and a
		// field that does nothing must not be able to refuse a policy that loads
		// today.
		//
		// Zero means "name no lifetime and take Google's default". Anything else
		// is sent as whole seconds, which is why sub-second values are refused
		// rather than rounded: int(0.5) is "0s", a request Google reads as no
		// lifetime at all, and the hour-long token it answers with is then
		// refused as out of policy on every single exchange. That is a target
		// that can never succeed, and an operator should learn it here rather
		// than from a workflow.
		if cfg.Lifetime != 0 && (cfg.Lifetime < time.Second || cfg.Lifetime > gcpMaxLifetime) {
			return nil, fmt.Errorf("%w: %s exchanger service account lifetime %s is outside the %s to %s Google issues, and zero takes Google's %s default",
				ErrInvalidPolicy, name, cfg.Lifetime, time.Second, gcpMaxLifetime, gcpDefaultLifetime)
		}
	}

	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{gcpDefaultScope}
	}

	assertionAud := cfg.AssertionAudience
	if assertionAud == "" {
		assertionAud = cfg.Audience
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	return &gcpExchanger{
		name:           name,
		audience:       cfg.Audience,
		assertionAud:   assertionAud,
		serviceAccount: cfg.ServiceAccountEmail,
		scopes:         scopes,
		lifetime:       cfg.Lifetime,
		endpoint:       endpoint,
		iamEndpoint:    strings.TrimSuffix(iamEndpoint, "/"),
		client:         newExchangeClient(cfg.HTTPClient, cfg.Timeout),
		clock:          clock,
	}, nil
}

// Name implements [Exchanger].
func (e *gcpExchanger) Name() string { return e.name }

// Requirement implements [Exchanger].
func (e *gcpExchanger) Requirement() Requirement {
	return Requirement{Audience: e.assertionAud}
}

// Exchange implements [Exchanger], federating the assertion and optionally
// impersonating a service account with the result.
func (e *gcpExchanger) Exchange(ctx context.Context, assertion Assertion) (Credential, error) {
	token := assertion.Token()
	if token == "" {
		return Credential{}, fmt.Errorf("%w: %s: %w", ErrExchangeFailed, e.name, ErrCredentialUnresolved)
	}

	raw, err := e.client.postJSON(ctx, e.name, e.endpoint, map[string]string{
		"audience":           e.audience,
		"grantType":          grantTypeTokenExchange,
		"requestedTokenType": tokenTypeAccessToken,
		"scope":              strings.Join(e.scopes, " "),
		"subjectTokenType":   tokenTypeJWT,
		"subjectToken":       token,
	}, "")
	if err != nil {
		return Credential{}, err
	}

	var federated tokenResponse
	if err := decodeJSON(e.name, raw, &federated); err != nil {
		return Credential{}, err
	}

	credential, err := federated.credential(e.name, e.name, assertion, e.clock(), gcpFederatedLifetime)
	if err != nil {
		return Credential{}, err
	}

	if e.serviceAccount == "" {
		return credential, nil
	}

	return e.impersonate(ctx, assertion, federated.AccessToken)
}

// impersonate exchanges a federated token for a service account access token,
// which is how a federated identity acquires a service account's permissions.
func (e *gcpExchanger) impersonate(ctx context.Context, assertion Assertion, federatedToken string) (Credential, error) {
	endpoint := fmt.Sprintf("%s/projects/-/serviceAccounts/%s:generateAccessToken",
		e.iamEndpoint, url.PathEscape(e.serviceAccount))

	request := map[string]any{"scope": e.scopes}
	if e.lifetime > 0 {
		request["lifetime"] = strconv.Itoa(int(e.lifetime.Seconds())) + "s"
	}

	raw, err := e.client.postJSON(ctx, e.name, endpoint, request, federatedToken)
	if err != nil {
		return Credential{}, err
	}

	var response struct {
		AccessToken string `json:"accessToken"`
		ExpireTime  string `json:"expireTime"`
	}
	if err := decodeJSON(e.name, raw, &response); err != nil {
		return Credential{}, err
	}

	if response.AccessToken == "" {
		return Credential{}, fmt.Errorf("%w: %s returned no service account token", ErrExchangeFailed, e.name)
	}

	if response.ExpireTime == "" {
		return Credential{}, fmt.Errorf("%w: %s returned no service account token expiry", ErrExchangeFailed, e.name)
	}
	expiresAt, err := time.Parse(time.RFC3339, response.ExpireTime)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %s reported an unparseable expiry: %w", ErrExchangeFailed, e.name, err)
	}
	now := e.clock()
	lifetime := e.lifetime
	if lifetime == 0 {
		lifetime = gcpDefaultLifetime
	}
	if !withinRequestedLifetime(expiresAt, now, lifetime) {
		return Credential{}, fmt.Errorf("%w: %s reported a service account token expiry of %s, outside the %s lifetime that was requested",
			ErrExchangeFailed, e.name, expiresAt.UTC().Format(time.RFC3339), lifetime)
	}

	credential, err := NewCredential(CredentialBearer, expiresAt, map[string]string{
		CredentialAccessToken: response.AccessToken,
	})
	if err != nil {
		return Credential{}, err
	}

	credential.Target = e.name
	credential.Provider = e.name
	credential.Scopes = e.scopes
	credential.AssertionID = assertion.ID

	return credential, nil
}
