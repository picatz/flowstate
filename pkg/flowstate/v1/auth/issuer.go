package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
)

// Defaults applied by [NewIssuer] when the corresponding option is not given.
const (
	// DefaultAssertionLifetime is how long a minted assertion is valid. It is
	// short because an assertion only has to survive being presented to one
	// relying party, and a captured one should stop working quickly.
	DefaultAssertionLifetime = 5 * time.Minute

	// MaxAssertionLifetime is the longest lifetime that can be configured. An
	// assertion is a credential; one that lives for hours is a standing grant.
	MaxAssertionLifetime = time.Hour

	// DefaultJWKSPath is where an [Issuer] publishes its public keys, relative
	// to its issuer URL.
	DefaultJWKSPath = "/.well-known/jwks.json"

	// DefaultKeyRetention is how long a rotated-out key stays published for
	// verification. It has to outlast both the assertions signed with it and any
	// relying party's cache of the key set, or rotation would reject assertions
	// that are still valid.
	DefaultKeyRetention = 24 * time.Hour
)

// DiscoveryPath is where an [Issuer] publishes its OpenID Provider Metadata,
// relative to its issuer URL. It is fixed by OpenID Connect Discovery.
const DiscoveryPath = "/.well-known/openid-configuration"

// reservedClaims are the claims an [Issuer] sets itself. A carried claim may not
// use one of these names: a workload whose submitting token contained a claim
// called "sub" must not be able to choose the subject of the assertion Flowstate
// mints for it.
var reservedClaims = []string{
	jwt.Issuer, jwt.Subject, jwt.Audience,
	jwt.ExpirationTime, jwt.NotBefore, jwt.IssuedAt, jwt.JWTID,
	ClaimNamespace, ClaimDeployment, ClaimWorkflow, ClaimRun, ClaimStep,
	ClaimOnBehalfOf, ClaimOnBehalfOfIssuer, ClaimRunMode,
}

// Claims an [Issuer] adds to every assertion, beyond the registered JWT claims.
//
// They are flat strings rather than a nested object because that is what relying
// parties can match on: an AWS trust policy condition and a Google Cloud
// attribute mapping both address a top-level claim directly.
const (
	// ClaimNamespace is the tenant or environment the workload runs in.
	ClaimNamespace = "namespace"

	// ClaimDeployment names the Flowstate deployment running the workload.
	ClaimDeployment = "deployment"

	// ClaimWorkflow is the workload's name.
	ClaimWorkflow = "workflow"

	// ClaimRun identifies the individual execution, for audit.
	ClaimRun = "run"

	// ClaimStep is the step that asked for the credential.
	ClaimStep = "step"

	// ClaimOnBehalfOf is the subject of the caller the workload acts for. It is
	// what makes delegation visible to a relying party: the subject says which
	// workload is calling, and this says who caused it to run.
	ClaimOnBehalfOf = "on_behalf_of"

	// ClaimOnBehalfOfIssuer is the issuer that authenticated that caller, so a
	// relying party can tell two callers with the same subject from different
	// issuers apart.
	ClaimOnBehalfOfIssuer = "on_behalf_of_issuer"

	// ClaimRunMode is "local" for an assertion minted by `flow run local` and
	// "server" for a server-attested run. See [localComponent] for why the
	// subject, not this claim, is the actual enforcement point: AWS STS ignores
	// custom claims, so a relying party that can only see "sub" and "aud" needs
	// the distinction encoded there. This claim is belt and braces for relying
	// parties that can read it, such as a GCP attribute mapping or an
	// Anthropic or OpenAI assumption rule. Like every other reserved claim, an
	// identity cannot carry one of this name — [WorkloadIdentity.Validate] and
	// [Issuer.mintFor] both refuse it — so it cannot be set by `--as-claim` or
	// any other caller-supplied claim.
	ClaimRunMode = "run_mode"
)

// Values [ClaimRunMode] takes.
const (
	// RunModeServer marks an assertion minted for a server-attested run.
	RunModeServer = "server"

	// RunModeLocal marks an assertion minted for a `flow run local` rehearsal.
	RunModeLocal = "local"
)

// SigningKey is a key an [Issuer] signs assertions with.
//
// The private half is unexported and has no accessor, so it cannot be printed,
// logged, or serialized by accident: the whole value formats as a description of
// the key rather than as the key.
type SigningKey struct {
	id        string
	algorithm jwa.Algorithm

	// signer produces a signed token. The private key exists only inside this
	// closure: a func field has nothing structural for fmt to print, and fmt
	// cannot call a method on a value it reaches through an unexported field, so a
	// key held in a plain field would be printed in full by %v on any struct that
	// happened to contain it.
	signer func(claims jwt.ClaimsSet) (string, error)

	// published is the public half rendered as a JSON Web Key, built when the key
	// is created so that serving the key set cannot fail at request time.
	published jwk.Value
}

// NewSigningKey returns a signing key with the given key id, deriving the
// algorithm from the key's type.
//
// The key id is published alongside the public key and named in every assertion
// signed with it, so it should identify this key among an issuer's keys, past and
// present. A date, such as "2026-07", makes rotation self-documenting.
//
// Supported keys are RSA of at least 2048 bits (RS256), ECDSA on P-256 (ES256),
// and Ed25519 (EdDSA). P-384 and P-521 are refused: the underlying JOSE library
// cannot produce signatures for them, and discovering that at the first mint
// would be worse than refusing the key here.
func NewSigningKey(id string, private crypto.PrivateKey) (SigningKey, error) {
	if id == "" {
		return SigningKey{}, fmt.Errorf("%w: signing key needs an id", ErrInvalidPolicy)
	}
	if strings.ContainsAny(id, " \t\n\r") {
		return SigningKey{}, fmt.Errorf("%w: signing key id %q must not contain whitespace", ErrInvalidPolicy, truncate(id, 64))
	}

	var (
		key    = SigningKey{id: id}
		public crypto.PublicKey
	)

	switch typed := private.(type) {
	case *rsa.PrivateKey:
		if typed.N.BitLen() < minRSAKeyBits {
			return SigningKey{}, fmt.Errorf("%w: RSA signing key is %d bits, want at least %d",
				ErrInvalidPolicy, typed.N.BitLen(), minRSAKeyBits)
		}
		key.algorithm, public = jwa.RS256, &typed.PublicKey
	case *ecdsa.PrivateKey:
		if typed.Curve != elliptic.P256() {
			return SigningKey{}, fmt.Errorf("%w: ECDSA signing keys must use P-256, got %s",
				ErrInvalidPolicy, typed.Curve.Params().Name)
		}
		key.algorithm, public = jwa.ES256, &typed.PublicKey
	case ed25519.PrivateKey:
		key.algorithm, public = jwa.EdDSA, typed.Public()
	default:
		return SigningKey{}, fmt.Errorf("%w: %T cannot sign assertions, want an RSA, P-256 ECDSA, or Ed25519 private key",
			ErrInvalidPolicy, private)
	}

	key.signer = signerFor(key.id, key.algorithm, private)

	published, err := jwk.ValueFromPublicKey(public)
	if err != nil {
		return SigningKey{}, fmt.Errorf("%w: rendering public key %q: %w", ErrInvalidPolicy, id, err)
	}
	published[jwk.KeyID] = id
	published[jwk.Algorithm] = key.algorithm
	published[jwk.PublicKeyUse] = "sig"
	key.published = published

	return key, nil
}

// GenerateSigningKey generates a new signing key for the given algorithm.
//
// ES256 is the smallest and fastest option and is accepted by every major relying
// party. RS256 is the most conservative choice, and some cloud providers require
// it. EdDSA is the most modern and the least widely accepted.
func GenerateSigningKey(id string, algorithm jwa.Algorithm) (SigningKey, error) {
	var (
		private crypto.PrivateKey
		err     error
	)

	switch algorithm {
	case jwa.RS256:
		private, err = rsa.GenerateKey(rand.Reader, minRSAKeyBits)
	case jwa.ES256:
		private, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case jwa.EdDSA:
		_, private, err = ed25519.GenerateKey(rand.Reader)
	default:
		return SigningKey{}, fmt.Errorf("%w: cannot generate a %q signing key, want %q, %q, or %q",
			ErrInvalidPolicy, algorithm, jwa.ES256, jwa.RS256, jwa.EdDSA)
	}
	if err != nil {
		return SigningKey{}, fmt.Errorf("generating %s signing key: %w", algorithm, err)
	}

	return NewSigningKey(id, private)
}

// ID returns the key's identifier, which appears in the "kid" header of every
// assertion signed with it.
func (k SigningKey) ID() string { return k.id }

// Algorithm returns the signature algorithm the key is used with.
func (k SigningKey) Algorithm() jwa.Algorithm { return k.algorithm }

// IsZero reports whether the key is unset.
func (k SigningKey) IsZero() bool { return k.signer == nil }

// String describes the key without revealing any part of it.
func (k SigningKey) String() string {
	if k.IsZero() {
		return "no signing key"
	}
	return fmt.Sprintf("signing key %s (%s)", k.id, k.algorithm)
}

// Format implements [fmt.Formatter], which is what closes the last gap a String
// method leaves: %#v ignores String and prints the struct's fields, and a
// Formatter is consulted before both String and GoString. Every verb renders the
// same redacted description, because there is no verb for which printing the
// material would be correct.
func (k SigningKey) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, k.String())
}

// LogValue implements [slog.LogValuer], so logging a key records which key it is
// and never any key material.
func (k SigningKey) LogValue() slog.Value {
	if k.IsZero() {
		return slog.StringValue("none")
	}
	return slog.GroupValue(slog.String("kid", k.id), slog.String("alg", k.algorithm))
}

// sign signs the given claims with this key.
func (k SigningKey) sign(claims jwt.ClaimsSet) (string, error) {
	if k.signer == nil {
		return "", ErrNoSigningKey
	}
	return k.signer(claims)
}

// signerFor returns a closure that signs claims with the given private key.
//
// The key is captured here and nowhere else, so it is reachable only by signing.
// The header is rebuilt on every call because jwt.New writes to the map it is
// given.
func signerFor(id string, algorithm jwa.Algorithm, private crypto.PrivateKey) func(jwt.ClaimsSet) (string, error) {
	return func(claims jwt.ClaimsSet) (string, error) {
		params := header.Parameters{
			header.Type:      jwt.Type,
			header.Algorithm: algorithm,
			header.KeyID:     id,
		}

		var (
			token *jwt.Token
			err   error
		)
		switch typed := private.(type) {
		case *rsa.PrivateKey:
			token, err = jwt.New(params, claims, typed)
		case *ecdsa.PrivateKey:
			token, err = jwt.New(params, claims, typed)
		case ed25519.PrivateKey:
			token, err = jwt.New(params, claims, typed)
		default:
			// Unreachable: NewSigningKey admits no other type.
			return "", fmt.Errorf("%w: cannot sign with %T", ErrNoSigningKey, private)
		}
		if err != nil {
			return "", fmt.Errorf("signing assertion with key %q: %w", id, err)
		}

		return token.String(), nil
	}
}

// Assertion is a short-lived signed statement of a workload's identity, minted by
// an [Issuer] for one relying party.
//
// It is a bearer credential. The token itself is unexported and reachable only
// through [Assertion.Token], so the value cannot be logged, printed, or
// serialized by accident, and an Assertion that has been through a serializer
// arrives with no token rather than with a leaked one.
type Assertion struct {
	// token holds the compact JWS. It is a [Material] for the same reason a
	// credential's values are: a string in a plain field is printed by %v on any
	// struct that contains one, whatever this type's String method says.
	token Material

	// Subject is the workload the assertion names.
	Subject string

	// Audience is the relying party the assertion was minted for. Presenting it
	// anywhere else fails, which is what keeps one relying party from replaying
	// it at another.
	Audience string

	// Issuer is the Flowstate issuer that minted it.
	Issuer string

	// KeyID is the key it was signed with.
	KeyID string

	// IssuedAt and ExpiresAt bound its validity.
	IssuedAt  time.Time
	ExpiresAt time.Time

	// ID is the "jti" claim, so a relying party can detect replay and an operator
	// can correlate a downstream audit record with this assertion.
	ID string
}

// Token returns the compact JWS to present to the relying party.
//
// It is empty when the assertion has been through a serializer, since the token
// is deliberately not serialized. Callers should treat an empty token as an
// error; the exchangers in this package do.
func (a Assertion) Token() string {
	token, _ := a.token.Single()
	return token
}

// IsZero reports whether the assertion is unset.
func (a Assertion) IsZero() bool { return a.token.IsZero() && a.Subject == "" }

// String describes the assertion without revealing the token.
func (a Assertion) String() string {
	if a.Subject == "" {
		return "no assertion"
	}
	return fmt.Sprintf("assertion for %s to %s, expires %s", a.Subject, a.Audience, a.ExpiresAt.UTC().Format(time.RFC3339))
}

// Format implements [fmt.Formatter], which is what closes the last gap a String
// method leaves: %#v ignores String and prints the struct's fields, and a
// Formatter is consulted before both String and GoString. Every verb renders the
// same redacted description, because there is no verb for which printing the
// material would be correct.
func (a Assertion) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, a.String())
}

// LogValue implements [slog.LogValuer], recording which assertion this is and
// never the token.
func (a Assertion) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("jti", a.ID),
		slog.String("subject", a.Subject),
		slog.String("audience", a.Audience),
		slog.Time("expires_at", a.ExpiresAt),
	)
}

// Issuer mints Flowstate's own identity assertions and publishes the keys needed
// to verify them.
//
// This is the half of federation that makes Flowstate an identity rather than
// only a relying party: a workload proves what it is with a signed assertion, and
// the system it is calling verifies that assertion against keys it fetches from
// [Issuer.Handler]. Neither side needs a shared secret, and nothing long-lived is
// deployed anywhere.
//
// An Issuer is safe for concurrent use, and its keys can be rotated while it is
// serving.
type Issuer struct {
	url          string
	jwksPath     string
	lifetime     time.Duration
	keyRetention time.Duration
	clock        func() time.Time

	// mu guards the keys. Minting takes a read lock, rotation a write lock, so
	// signing is not serialized.
	mu      sync.RWMutex
	active  SigningKey
	retired []retiredKey
}

// retiredKey is a rotated-out key, kept published so assertions signed with it
// can still be verified. Only the public half is kept: a retired key must not be
// able to sign again.
type retiredKey struct {
	algorithm jwa.Algorithm
	published jwk.Value
	expiresAt time.Time
}

// An IssuerOption configures an [Issuer].
type IssuerOption func(*Issuer)

// WithAssertionLifetime sets how long minted assertions are valid. It must be
// positive and no more than [MaxAssertionLifetime].
func WithAssertionLifetime(lifetime time.Duration) IssuerOption {
	return func(i *Issuer) { i.lifetime = lifetime }
}

// WithJWKSPath sets the path, relative to the issuer URL, where public keys are
// published. It must begin with a slash.
func WithJWKSPath(path string) IssuerOption {
	return func(i *Issuer) { i.jwksPath = path }
}

// WithKeyRetention sets how long a rotated-out key stays published for
// verification.
func WithKeyRetention(retention time.Duration) IssuerOption {
	return func(i *Issuer) { i.keyRetention = retention }
}

// WithIssuerClock sets the clock used for assertion lifetimes and key retention.
// It exists for tests.
func WithIssuerClock(clock func() time.Time) IssuerOption {
	return func(i *Issuer) {
		if clock != nil {
			i.clock = clock
		}
	}
}

// NewIssuer returns an issuer that mints assertions signed with the given key.
//
// The issuer URL is the identity Flowstate presents to the world: it goes in the
// "iss" claim of every assertion, and it is where relying parties fetch the
// discovery document and key set from. It must therefore be the URL at which
// [Issuer.Handler] is actually reachable by those relying parties, and must be
// https outside of local development.
func NewIssuer(issuerURL string, key SigningKey, opts ...IssuerOption) (*Issuer, error) {
	if key.IsZero() {
		return nil, fmt.Errorf("%w: an issuer needs a signing key", ErrNoSigningKey)
	}

	issuer := &Issuer{
		url:          strings.TrimSuffix(issuerURL, "/"),
		jwksPath:     DefaultJWKSPath,
		lifetime:     DefaultAssertionLifetime,
		keyRetention: DefaultKeyRetention,
		clock:        time.Now,
		active:       key,
	}

	for _, opt := range opts {
		opt(issuer)
	}

	if err := validateIssuerURL(issuer.url); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	switch {
	case issuer.lifetime <= 0:
		return nil, fmt.Errorf("%w: assertion lifetime must be positive", ErrInvalidPolicy)
	case issuer.lifetime > MaxAssertionLifetime:
		return nil, fmt.Errorf("%w: assertion lifetime %s is longer than the %s maximum",
			ErrInvalidPolicy, issuer.lifetime, MaxAssertionLifetime)
	case !strings.HasPrefix(issuer.jwksPath, "/"):
		return nil, fmt.Errorf("%w: key set path %q must begin with %q", ErrInvalidPolicy, issuer.jwksPath, "/")
	case issuer.jwksPath == DiscoveryPath:
		return nil, fmt.Errorf("%w: key set path must not be the discovery path %q", ErrInvalidPolicy, DiscoveryPath)
	case issuer.keyRetention < 0:
		return nil, fmt.Errorf("%w: key retention must not be negative", ErrInvalidPolicy)
	}

	return issuer, nil
}

// URL returns the issuer identifier, which is the "iss" claim of every assertion
// it mints.
func (i *Issuer) URL() string { return i.url }

// JWKSPath returns the path, relative to the issuer URL, where public keys are
// published. Mount [Issuer.Handler] there and at [DiscoveryPath].
func (i *Issuer) JWKSPath() string { return i.jwksPath }

// JWKSURL returns the absolute URL of the key set, as advertised in the discovery
// document.
func (i *Issuer) JWKSURL() string { return i.url + i.jwksPath }

// AssertionLifetime returns how long minted assertions are valid.
func (i *Issuer) AssertionLifetime() time.Duration { return i.lifetime }

// Rotate installs a new signing key.
//
// Assertions minted from this point are signed with the new key. The previous
// key's public half stays published for the configured retention period, so
// assertions already in flight, and relying parties holding a cached key set,
// keep working; its private half is dropped, so it cannot sign again.
func (i *Issuer) Rotate(key SigningKey) error {
	if key.IsZero() {
		return fmt.Errorf("%w: cannot rotate to an unset key", ErrNoSigningKey)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if key.id == i.active.id {
		return fmt.Errorf("%w: key id %q is already active; a new key needs a new id, or verifiers cannot tell them apart",
			ErrInvalidPolicy, key.id)
	}

	now := i.clock()

	i.retired = append(i.retired, retiredKey{
		algorithm: i.active.algorithm,
		published: i.active.published,
		expiresAt: now.Add(i.keyRetention),
	})
	i.active = key

	i.pruneLocked(now)

	return nil
}

// pruneLocked drops retired keys past their retention. The caller must hold i.mu
// for writing.
func (i *Issuer) pruneLocked(now time.Time) {
	i.retired = slices.DeleteFunc(i.retired, func(key retiredKey) bool {
		return now.After(key.expiresAt)
	})
}

// ActiveKeyID returns the id of the key assertions are currently signed with.
func (i *Issuer) ActiveKeyID() string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.active.id
}

// Mint returns an assertion for the given workload, step, and relying party.
//
// # This must run in an activity
//
// Minting reads the clock and produces a value that differs every time, so it can
// never be part of workflow code: a replay would produce a different assertion
// and break determinism. Mint in the activity that presents the assertion, and do
// not return the result to the workflow. [Broker.Credential] is the intended
// entry point, and holds to the same rule.
func (i *Issuer) Mint(ctx context.Context, identity WorkloadIdentity, ref StepRef, audience string) (Assertion, error) {
	if err := ctx.Err(); err != nil {
		return Assertion{}, err
	}

	if err := identity.Validate(); err != nil {
		return Assertion{}, err
	}

	if audience == "" {
		// An assertion with no audience is one any relying party would accept,
		// which is the whole problem audience scoping exists to prevent.
		return Assertion{}, fmt.Errorf("%w: an assertion needs an audience naming the relying party it is for", ErrInvalidIdentity)
	}

	subject, err := identity.SubjectFor(ref)
	if err != nil {
		return Assertion{}, err
	}

	return i.mintFor(ctx, identity, ref, subject, audience)
}

// mintFor builds and signs an assertion for an already-derived subject, which lets
// a protocol that dictates its own subject, such as RFC 7523 client
// authentication, use the same claim set and signing path.
func (i *Issuer) mintFor(ctx context.Context, identity WorkloadIdentity, ref StepRef, subject, audience string) (Assertion, error) {
	if err := ctx.Err(); err != nil {
		return Assertion{}, err
	}

	if subject == "" {
		return Assertion{}, fmt.Errorf("%w: an assertion needs a subject", ErrInvalidIdentity)
	}
	if audience == "" {
		return Assertion{}, fmt.Errorf("%w: an assertion needs an audience naming the relying party it is for", ErrInvalidIdentity)
	}

	id, err := newAssertionID()
	if err != nil {
		return Assertion{}, err
	}

	i.mu.RLock()
	key := i.active
	i.mu.RUnlock()

	if key.IsZero() {
		return Assertion{}, ErrNoSigningKey
	}

	var (
		now       = i.clock()
		expiresAt = now.Add(i.lifetime)
	)

	claims := jwt.ClaimsSet{
		jwt.Issuer:         i.url,
		jwt.Subject:        subject,
		jwt.Audience:       audience,
		jwt.IssuedAt:       now.Unix(),
		jwt.NotBefore:      now.Unix(),
		jwt.ExpirationTime: expiresAt.Unix(),
		jwt.JWTID:          id,

		ClaimNamespace:  orDefault(identity.Namespace),
		ClaimDeployment: orDefault(identity.Deployment),
		ClaimWorkflow:   ref.Workflow,
		ClaimStep:       ref.Step,

		ClaimOnBehalfOf:       identity.Subject,
		ClaimOnBehalfOfIssuer: identity.Issuer,

		ClaimRunMode: runModeFor(identity),
	}

	if ref.Run != "" {
		claims[ClaimRun] = ref.Run
	}

	// Validate rejects a carried claim that shadows a reserved one, so this
	// cannot overwrite anything above. Checked again here because that check is
	// what keeps the guarantee, and it belongs next to the code it protects.
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		if slices.Contains(reservedClaims, name) {
			return Assertion{}, fmt.Errorf("%w: carried claim %q collides with a reserved claim", ErrInvalidIdentity, name)
		}
		claims[name] = identity.Claims[name]
	}

	token, err := key.sign(claims)
	if err != nil {
		return Assertion{}, err
	}

	return Assertion{
		token:     NewSingleMaterial(token),
		Subject:   subject,
		Audience:  audience,
		Issuer:    i.url,
		KeyID:     key.id,
		IssuedAt:  now,
		ExpiresAt: expiresAt,
		ID:        id,
	}, nil
}

// runModeFor reports the [ClaimRunMode] value for identity, driven entirely by
// which constructor built it. [NewLocalWorkloadIdentity] is the only thing
// that can set the identity's unexported local field, so this cannot be swayed
// by anything a flag or a caller supplies — see [WorkloadIdentity].
func runModeFor(identity WorkloadIdentity) string {
	if identity.local {
		return RunModeLocal
	}
	return RunModeServer
}

// newAssertionID returns a random identifier for the "jti" claim, so a relying
// party can detect a replayed assertion.
func newAssertionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generating assertion id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
