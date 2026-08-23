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

// builtInClaimNames is the single declaration of the claims an [Issuer] mints
// itself and advertises through discovery. A carried claim may not use one of
// these names: a workload whose submitting token contained a claim called "sub"
// must not be able to choose the subject of the assertion Flowstate mints for it.
//
// ClaimRun is included even though an assertion without a run reference omits it:
// claims_supported describes claims the issuer can return, not claims every token
// is required to contain.
var builtInClaimNames = []string{
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
	signer func(context.Context, jwt.ClaimsSet) (string, error)

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
		public = &typed.PublicKey
	case *ecdsa.PrivateKey:
		if typed.Curve != elliptic.P256() {
			return SigningKey{}, fmt.Errorf("%w: ECDSA signing keys must use P-256, got %s",
				ErrInvalidPolicy, typed.Curve.Params().Name)
		}
		public = &typed.PublicKey
	case ed25519.PrivateKey:
		public = typed.Public()
	default:
		return SigningKey{}, fmt.Errorf("%w: %T cannot sign assertions, want an RSA, P-256 ECDSA, or Ed25519 private key",
			ErrInvalidPolicy, private)
	}

	// The algorithm and the published key come from the public half, through
	// the same function a verify-only key goes through: a signing key and a
	// key published for verification only differ in whether this process can
	// sign with them, and must not differ in how they appear in the key set.
	algorithm, published, err := publishValue(key.id, public)
	if err != nil {
		return SigningKey{}, err
	}
	key.algorithm, key.published = algorithm, published

	local := signerFor(key.id, key.algorithm, private)
	key.signer = func(_ context.Context, claims jwt.ClaimsSet) (string, error) { return local(claims) }

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
func (k SigningKey) sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	if k.signer == nil {
		return "", ErrNoSigningKey
	}
	return k.signer(ctx, claims)
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

	// declared is the closed set of extension claim names an assertion minted
	// here may carry, sorted. Nil means none: a deployment that declares
	// nothing mints nothing beyond the claims the issuer sets itself. See
	// [WithDeclaredClaims].
	declared []string

	// verifyOnly holds the keys [WithVerifyOnlyKey] named, in the order they
	// were given, until [NewIssuer] can turn them into retired keys. They
	// cannot be installed by the option itself because an option cannot fail
	// and cannot see the retention or the clock it needs, both of which other
	// options may still change.
	verifyOnly []verifyOnlyKey

	// mu guards the keys. Minting takes a read lock, rotation a write lock, so
	// signing is not serialized.
	mu      sync.RWMutex
	active  SigningKey
	retired []retiredKey
}

// verifyOnlyKey is a public key an operator named at start-up so that
// assertions a previous process signed keep verifying. See [WithVerifyOnlyKey].
type verifyOnlyKey struct {
	id     string
	public crypto.PublicKey
}

// retiredKey is a rotated-out key, kept published so assertions signed with it
// can still be verified. Only the public half is kept: a retired key must not be
// able to sign again.
type retiredKey struct {
	// id is the key id assertions signed with this key name in their "kid"
	// header, and what [Issuer.RevokeKey] addresses. It is also inside
	// published, and is kept here as well so that finding a key by id is not a
	// map lookup into a value whose shape belongs to the JOSE library.
	id        string
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

// WithDeclaredClaims declares the extension claims assertions from this issuer
// may carry, beyond the ones the issuer sets itself.
//
// # Why this is an allowlist
//
// The claim set an assertion carries is a public contract: it is signed, it is
// cached by relying parties, and a mistake in it is a breaking change to every
// verifier in the world rather than a refactor. Keeping it closed is what turns
// "the claim set is fixed" from a convention into a property.
//
// Minting used to be a denylist — every carried claim was copied and only a
// collision with a reserved name was refused (#560). That protected the
// assertion from being shadowed and did nothing about growth: whatever an
// operator carried into a [WorkloadIdentity] was signed, whether anyone had
// decided it should be part of the contract or not. So the mint now refuses a
// claim it was not told about, with [ErrUndeclaredClaim].
//
// # Where a deployment declares one
//
// In the same trust policy the rest of federation is configured in, as
// `federation.declared_claims` — see [FederationPolicy.DeclaredClaims]. It is
// the outbound counterpart to the server's own `--identity-claim`, which names
// the caller-token claims carried *into* a run's identity: that flag decides
// what is available to carry, and this decides what may be signed. They are two
// processes' configuration and so cannot be one declaration, which is a drift
// risk worth knowing about — an operator carrying a claim the issuer does not
// declare finds out at the first mint, loudly, by name.
//
// Names are validated here rather than at the first mint, so a policy that
// loads is one whose declarations mean something: a reserved name, an empty
// one, or one longer than [MaxCarriedClaimNameBytes] is a startup error.
// Declaring a reserved claim is refused rather than ignored, because a
// declaration that silently never applies is worse than no declaration.
func WithDeclaredClaims(names ...string) IssuerOption {
	return func(i *Issuer) {
		i.declared = append(i.declared, names...)
	}
}

// WithVerifyOnlyKey publishes one more public key in the key set, without a
// private half, so that assertions signed by a *previous process* keep
// verifying while they live.
//
// # Why this exists beside [Issuer.Rotate]
//
// Rotate is an in-process rotation: it moves the outgoing key into the
// published-but-not-signing set and installs a new one, with no restart. It is
// therefore no help at all with the rotation an operator actually performs,
// which is to change what a process is started with and restart it. A fresh
// process builds a fresh issuer whose retired set is empty, so the key set it
// publishes from its first request onward names the new key and nothing else —
// and every assertion the old process signed, still valid for its remaining
// lifetime, stops verifying against any relying party that refetches. This
// option is how a deployment carries the old key across that boundary
// (picatz/flowstate#891).
//
// The key is installed exactly where a rotated-out key goes, so there is one
// set of published keys and one retention rule rather than two: it is dropped
// from the key set [Issuer.KeySet] serves once the issuer's retention has
// elapsed, measured from when the issuer was built, and it can be withdrawn
// early by [Issuer.RevokeKey] like any other retired key.
//
// It is a public key, and deliberately not a [SigningKey]: a key this process
// will never sign with has no business holding private material, and a
// verify-only key that could sign would make the distinction a convention
// rather than a property.
//
// Rotating is still not revoking. Publishing a previous key is what keeps
// rotation from rejecting assertions that are perfectly valid; an operator
// responding to a suspected compromise wants [Issuer.RevokeKey], or simply not
// to name the key here.
//
// The id must be the one the old key published under, since it is what the
// "kid" header of those assertions names, and it must differ from every other
// key this issuer publishes — [NewIssuer] refuses a collision rather than
// publishing two keys a verifier cannot tell apart.
func WithVerifyOnlyKey(id string, public crypto.PublicKey) IssuerOption {
	return func(i *Issuer) {
		i.verifyOnly = append(i.verifyOnly, verifyOnlyKey{id: id, public: public})
	}
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

	declared, err := validateDeclaredClaims(issuer.declared)
	if err != nil {
		return nil, err
	}
	issuer.declared = declared

	if err := issuer.installVerifyOnlyKeys(); err != nil {
		return nil, err
	}

	return issuer, nil
}

// installVerifyOnlyKeys turns what [WithVerifyOnlyKey] collected into retired
// keys, once every option has been applied and the retention and clock are
// settled.
//
// Every failure here is a start-up error rather than a key quietly left out.
// A deployment that named a key it cannot publish is a deployment whose
// rotation is half-performed: the process would come up serving a key set that
// silently rejects assertions the operator believes are covered, which is the
// failure this option exists to prevent, arriving without a message.
func (i *Issuer) installVerifyOnlyKeys() error {
	now := i.clock()

	for _, key := range i.verifyOnly {
		switch {
		case key.id == "":
			return fmt.Errorf("%w: a verify-only key needs the id it was published under", ErrInvalidPolicy)
		case strings.ContainsAny(key.id, " \t\n\r"):
			return fmt.Errorf("%w: verify-only key id %q must not contain whitespace",
				ErrInvalidPolicy, truncate(key.id, 64))
		case key.id == i.active.id:
			return fmt.Errorf("%w: verify-only key id %q is the active signing key's id; a key needs its own id, or verifiers cannot tell them apart",
				ErrInvalidPolicy, truncate(key.id, 64))
		}

		if slices.ContainsFunc(i.retired, func(other retiredKey) bool { return other.id == key.id }) {
			return fmt.Errorf("%w: verify-only key id %q was given twice; a key needs its own id, or verifiers cannot tell them apart",
				ErrInvalidPolicy, truncate(key.id, 64))
		}

		algorithm, published, err := publishValue(key.id, key.public)
		if err != nil {
			return err
		}

		i.retired = append(i.retired, retiredKey{
			id:        key.id,
			algorithm: algorithm,
			published: published,
			// Measured from start-up, because that is when this key stopped
			// signing as far as this process can tell. An operator whose
			// retention has to outlast a longer gap configures a longer
			// key_retention rather than getting an unbounded one by default.
			expiresAt: now.Add(i.keyRetention),
		})
	}

	i.verifyOnly = nil

	return nil
}

// publishValue renders a public key as the JSON Web Key an issuer publishes,
// and reports the algorithm it is used with.
//
// The accepted key types are exactly [NewSigningKey]'s, seen from the public
// side: a key that could not have signed for this issuer cannot verify for it
// either, and admitting one here would publish a key set entry no assertion
// this deployment ever minted can match.
func publishValue(id string, public crypto.PublicKey) (jwa.Algorithm, jwk.Value, error) {
	var algorithm jwa.Algorithm

	switch typed := public.(type) {
	case *rsa.PublicKey:
		if typed.N.BitLen() < minRSAKeyBits {
			return "", nil, fmt.Errorf("%w: RSA key %q is %d bits, want at least %d",
				ErrInvalidPolicy, truncate(id, 64), typed.N.BitLen(), minRSAKeyBits)
		}
		algorithm = jwa.RS256
	case *ecdsa.PublicKey:
		if typed.Curve != elliptic.P256() {
			return "", nil, fmt.Errorf("%w: ECDSA key %q must use P-256, got %s",
				ErrInvalidPolicy, truncate(id, 64), typed.Curve.Params().Name)
		}
		algorithm = jwa.ES256
	case ed25519.PublicKey:
		algorithm = jwa.EdDSA
	default:
		return "", nil, fmt.Errorf("%w: %T cannot verify assertions, want an RSA, P-256 ECDSA, or Ed25519 public key",
			ErrInvalidPolicy, public)
	}

	published, err := jwk.ValueFromPublicKey(public)
	if err != nil {
		return "", nil, fmt.Errorf("%w: rendering public key %q: %w", ErrInvalidPolicy, truncate(id, 64), err)
	}
	published[jwk.KeyID] = id
	published[jwk.Algorithm] = algorithm
	published[jwk.PublicKeyUse] = "sig"

	return algorithm, published, nil
}

// validateDeclaredClaims checks a declaration and returns it sorted and
// deduplicated, so that the mint's membership test and the discovery document's
// claims list read the same set in the same order.
//
// A declaration is configuration, so every one of these is a startup error
// rather than something discovered at the first mint.
func validateDeclaredClaims(names []string) ([]string, error) {
	declared := slices.Sorted(slices.Values(names))
	declared = slices.Compact(declared)

	// Deliberately no bound on how many names may be declared. The declaration
	// is a deployment's own configuration, read once at startup, and it is a
	// vocabulary rather than a claim set: an assertion still carries at most
	// [MaxCarriedClaims] of them, which is the bound that faces the caller.
	for _, name := range declared {
		switch {
		case name == "":
			return nil, fmt.Errorf("%w: a declared claim needs a name", ErrInvalidPolicy)
		case len(name) > MaxCarriedClaimNameBytes:
			return nil, fmt.Errorf("%w: declared claim name %q is %d bytes, and at most %d are allowed",
				ErrInvalidPolicy, truncate(name, 64), len(name), MaxCarriedClaimNameBytes)
		case slices.Contains(builtInClaimNames, name):
			// Refused rather than ignored: a carried claim of this name can
			// never be minted, whatever the declaration says, and a
			// declaration that silently never applies is worse than none.
			return nil, fmt.Errorf("%w: claim %q is reserved and set by the issuer itself, so it cannot be declared as one an identity carries",
				ErrInvalidPolicy, name)
		}
	}

	return declared, nil
}

// DeclaredClaims returns the extension claims assertions from this issuer may
// carry, sorted. It is what [WithDeclaredClaims] was given, and what the
// discovery document advertises beyond the claims every assertion has.
func (i *Issuer) DeclaredClaims() []string { return slices.Clone(i.declared) }

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
//
// # Rotation is not revocation
//
// Rotating invalidates nothing that has already been signed, and that is
// deliberate: keeping the retired key published for [DefaultKeyRetention] is
// what stops rotation from rejecting assertions that are still perfectly
// valid. An operator who rotates in response to a suspected key compromise has
// therefore done nothing about the assertions already in flight, and the key
// they are worried about keeps verifying for another day.
//
// [Issuer.RevokeKey] is the verb for that, and it is a different verb because
// it has a different consequence.
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
		id:        i.active.id,
		algorithm: i.active.algorithm,
		published: i.active.published,
		expiresAt: now.Add(i.keyRetention),
	})
	i.active = key

	i.pruneLocked(now)

	return nil
}

// RevokeKey withdraws a retired key from the published key set immediately,
// rather than when its retention lapses.
//
// This is the verb [Issuer.Rotate] is not. Rotating drops a key's private half
// and keeps its public half published for [DefaultKeyRetention], precisely so
// that assertions already signed with it keep verifying — which means rotating
// invalidates nothing already issued. Revoking is the operator saying that is
// no longer acceptable: **every assertion signed with this key stops verifying
// as soon as each relying party refreshes its cached key set**, and there is no
// way to make the ones in flight work again.
//
// So this is the response to a key believed compromised, and it is not the
// response to a scheduled rotation. Reach for [Issuer.Rotate] on a schedule and
// this only when something has gone wrong.
//
// Two things it will not do, both because the alternative is an issuer that
// silently stops working:
//
//   - The active key cannot be revoked. Withdrawing the key assertions are
//     currently signed with would publish a set that verifies nothing this
//     issuer is about to mint. Rotate to a new key first, then revoke the one
//     it replaced.
//   - An unknown key id is an error rather than a no-op, because "revoked" and
//     "misspelled the key id and revoked nothing" must not look the same to
//     whoever is handling an incident.
//
// The revocation is this process's. An issuer's key set lives in memory, so a
// deployment running several of them revokes on each, and a relying party stops
// accepting the key when its own cache of the key set next refreshes — which is
// its [DefaultKeyCacheTTL], not ours.
func (i *Issuer) RevokeKey(keyID string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if keyID == "" {
		return fmt.Errorf("%w: revoking a key needs its id", ErrInvalidPolicy)
	}

	if keyID == i.active.id {
		return fmt.Errorf("%w: key %q is the active signing key; rotate to a new key first, then revoke this one",
			ErrInvalidPolicy, truncate(keyID, 64))
	}

	before := len(i.retired)
	i.retired = slices.DeleteFunc(i.retired, func(key retiredKey) bool {
		return key.id == keyID
	})

	if len(i.retired) == before {
		return fmt.Errorf("%w: no retired key with id %q is published by this issuer",
			ErrUnknownKey, truncate(keyID, 64))
	}

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

	// Validate applies the same bound, and [Broker] reaches this function by a
	// path that does not go through Mint. Checked again here because this is
	// where a claim set becomes a signed token: an oversized one is refused,
	// never trimmed, since a truncated claim set is an assertion that says
	// something other than what was authorized.
	if err := validateCarriedClaims(identity.Claims); err != nil {
		return Assertion{}, err
	}

	// The claim set is closed: a name the issuer does not declare is refused,
	// not signed. This is an allowlist rather than the denylist it began as,
	// which is the difference between an assertion whose claim set is fixed by
	// convention and one whose claim set is fixed by the code that signs it.
	//
	// Validate rejects a carried claim that shadows a reserved one, and the
	// reserved names can never be declared, so neither loop can overwrite
	// anything set above. Both checks are stated here because this is the line
	// where a claim becomes a signed statement.
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		switch {
		case slices.Contains(builtInClaimNames, name):
			return Assertion{}, fmt.Errorf("%w: carried claim %q collides with a reserved claim", ErrInvalidIdentity, name)
		case !slices.Contains(i.declared, name):
			// The name, never the value: this error travels wherever the
			// refusal does. See [validateCarriedClaims].
			return Assertion{}, fmt.Errorf("%w: %q; declare it in the issuer's federation.declared_claims to carry it",
				ErrUndeclaredClaim, truncate(name, 64))
		}
		claims[name] = identity.Claims[name]
	}

	// Choosing the key and signing with it happen under one hold of the read
	// lock, and that is a correctness requirement rather than tidiness.
	//
	// Copying i.active and releasing the lock before signing leaves a window:
	// [Issuer.Rotate] can retire that key and [Issuer.RevokeKey] can withdraw
	// it, both completing while this signature is still in flight, and the
	// assertion is then minted with a key the issuer has already told the world
	// is revoked. A relying party holding a cached key set accepts it, so
	// revocation would report success while still emitting new assertions
	// signed by the compromised key — the one thing RevokeKey exists to stop.
	//
	// A read lock is the right one: concurrent mints still sign in parallel,
	// since they share it. Only Rotate and RevokeKey take the write lock, and
	// they now wait for in-flight signatures to finish — which is exactly the
	// guarantee RevokeKey's doc claims, that no assertion signed with the key
	// survives its return.
	i.mu.RLock()
	key := i.active
	if key.IsZero() {
		i.mu.RUnlock()
		return Assertion{}, ErrNoSigningKey
	}

	token, err := key.sign(ctx, claims)
	keyID := key.id
	i.mu.RUnlock()

	if err != nil {
		return Assertion{}, err
	}

	return Assertion{
		token:     NewSingleMaterial(token),
		Subject:   subject,
		Audience:  audience,
		Issuer:    i.url,
		KeyID:     keyID,
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
