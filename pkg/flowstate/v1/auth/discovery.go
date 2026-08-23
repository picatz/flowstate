package auth

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
)

// discoveryCacheMaxAge is how long a relying party is told it may cache the
// discovery document and key set.
//
// It is a compromise: long enough that relying parties are not refetching keys on
// every call, short enough that a rotated key is picked up in minutes. The
// retention period for rotated keys is what actually makes rotation safe, and it
// is far longer than this.
const discoveryCacheMaxAge = 5 * time.Minute

// DiscoveryDocument is the OpenID Provider Metadata a relying party reads to learn
// how to verify Flowstate's assertions.
//
// It is exported so a deployment can serve it from somewhere other than
// [Issuer.Handler], such as a static file behind a CDN, or embed it in a
// configuration bundle for a relying party that does not fetch discovery documents
// at all.
type DiscoveryDocument struct {
	// Issuer is the issuer identifier, which must exactly equal the "iss" claim of
	// every assertion and the URL this document is served from. Relying parties
	// check this, and so does this package's own [OIDCVerifier].
	Issuer string `json:"issuer"`

	// JWKSURI is where the public keys are published.
	JWKSURI string `json:"jwks_uri"`

	// ResponseTypesSupported and SubjectTypesSupported are required by OpenID
	// Connect Discovery. Flowstate is not an interactive provider: it issues
	// assertions about workloads, so these describe the minimum a strict consumer
	// needs to accept the document.
	ResponseTypesSupported []string `json:"response_types_supported"`
	SubjectTypesSupported  []string `json:"subject_types_supported"`

	// IDTokenSigningAlgValuesSupported lists the algorithms of every published
	// key, including keys retained from a rotation.
	IDTokenSigningAlgValuesSupported []jwa.Algorithm `json:"id_token_signing_alg_values_supported"`

	// ClaimsSupported names the claims an assertion carries, so an operator
	// configuring an attribute mapping can see what there is to map.
	ClaimsSupported []string `json:"claims_supported"`

	// ScopesSupported is present because some consumers require the field.
	ScopesSupported []string `json:"scopes_supported"`
}

// Discovery returns the metadata document for this issuer.
func (i *Issuer) Discovery() DiscoveryDocument {
	return DiscoveryDocument{
		Issuer:                           i.url,
		JWKSURI:                          i.JWKSURL(),
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		IDTokenSigningAlgValuesSupported: i.algorithms(),
		ClaimsSupported:                  i.supportedClaims(),
		ScopesSupported:                  []string{"openid"},
	}
}

// WorkloadAssertionProfile is the versioned identifier of the assertions an
// [Issuer] mints, advertised by [WorkloadIssuerMetadata].
//
// It exists so a relying party can key its compatibility on a value this
// repository controls and versions, rather than inferring what an assertion
// contains from the well-known path the document arrived on. The path is a
// convention several cloud products hard-code; the profile is a statement.
const WorkloadAssertionProfile = "https://flowstate.dev/profiles/workload-assertion/v1"

// WorkloadIssuerMetadata describes exactly what an [Issuer] mints: signed,
// audience-bound, short-lived workload assertions, and nothing else.
//
// It is served from [WorkloadIssuerMetadataPath], alongside — not instead of —
// the OpenID Provider Metadata at [DiscoveryPath]. The two answer different
// questions and have different audiences. [DiscoveryDocument] is what a
// consumer that speaks OpenID Connect Discovery requires in order to accept
// this issuer at all: AWS IAM OIDC providers, Google Cloud Workload Identity
// Federation configured by discovery, and Vault/OpenBao's `oidc_discovery_url`
// all read it and all require its fields. This document is for a consumer that
// wants to know what the assertions actually are, and can say so in fields
// OpenID Connect has no spelling for: the assertion profile and the key types.
//
// It is deliberately a distinct type rather than a rename or a type alias of
// [DiscoveryDocument]. The two documents carry different field names, so one
// name for both would be a compile break for callers outside this repository
// wearing an alias's clothes — and their contents are allowed to diverge,
// because one of them is fixed by a specification this project does not own.
type WorkloadIssuerMetadata struct {
	// Issuer is the issuer identifier, exactly the value minted as "iss", and
	// exactly the [DiscoveryDocument.Issuer] served at [DiscoveryPath].
	Issuer string `json:"issuer"`

	// JWKSURI is where the public keys are published. It is the same key set
	// [DiscoveryDocument.JWKSURI] names; there is only one.
	JWKSURI string `json:"jwks_uri"`

	// AssertionProfilesSupported names every assertion profile this issuer can
	// mint. Today that is exactly [WorkloadAssertionProfile]; it is a list so
	// that a second profile can be added without a consumer that reads this
	// field having to change how it reads it.
	AssertionProfilesSupported []string `json:"assertion_profiles_supported"`

	// SigningAlgValuesSupported lists the algorithms of every published key,
	// including keys retained from a rotation.
	//
	// It is not spelled "id_token_signing_alg_values_supported", because an
	// assertion is not an ID token. That name belongs to the OpenID Provider
	// Metadata at [DiscoveryPath], where it means what OpenID Connect says it
	// means.
	SigningAlgValuesSupported []jwa.Algorithm `json:"signing_alg_values_supported"`

	// ClaimsSupported names the claims an assertion carries, so an operator
	// configuring an attribute mapping can see what there is to map.
	ClaimsSupported []string `json:"claims_supported"`

	// KeyTypesSupported lists the "kty" of every published key. OpenID Provider
	// Metadata has no field for this, and it is the one thing an operator most
	// often has to know before a consumer will accept the issuer: a federation
	// target that supports RSA and not EC fails at verification time, with an
	// error from the far side, rather than at configuration time.
	KeyTypesSupported []string `json:"key_types_supported"`
}

// WorkloadMetadata returns this issuer's native contract: what it mints, with
// which keys, carrying which claims.
func (i *Issuer) WorkloadMetadata() WorkloadIssuerMetadata {
	algorithms, keyTypes := i.signingProfile()

	return WorkloadIssuerMetadata{
		Issuer:                     i.url,
		JWKSURI:                    i.JWKSURL(),
		AssertionProfilesSupported: []string{WorkloadAssertionProfile},
		SigningAlgValuesSupported:  algorithms,
		ClaimsSupported:            i.supportedClaims(),
		KeyTypesSupported:          keyTypes,
	}
}

// supportedClaims is the claim vocabulary both documents advertise.
//
// Derived from the same declaration the mint enforces, rather than listed a
// second time: an issuer that advertises a claim it would refuse to sign, or
// signs one it never advertised, is describing an assertion that does not
// exist. Written once here for the same reason: two documents naming the same
// vocabulary from two expressions is that drift with an extra step.
func (i *Issuer) supportedClaims() []string {
	return append(slices.Clone(builtInClaimNames), i.DeclaredClaims()...)
}

// KeySet returns the public keys relying parties verify assertions with: the
// active signing key, and any rotated-out key still within its retention period.
func (i *Issuer) KeySet() jwk.Set {
	i.mu.RLock()
	defer i.mu.RUnlock()

	now := i.clock()

	set := jwk.Set{Keys: make([]jwk.Value, 0, len(i.retired)+1)}
	set.Keys = append(set.Keys, i.active.published)

	for _, key := range i.retired {
		if now.After(key.expiresAt) {
			continue
		}
		set.Keys = append(set.Keys, key.published)
	}

	return set
}

// algorithms returns the distinct algorithms of every published key.
func (i *Issuer) algorithms() []jwa.Algorithm {
	algorithms, _ := i.signingProfile()
	return algorithms
}

// signingProfile returns the distinct algorithms and key types of every
// published key, read from one snapshot of the key set.
//
// Both answers describe the same set of keys, so they are taken under one lock
// and one reading of the clock. Asking for them separately lets a [Issuer.Rotate]
// or [Issuer.RevokeKey] land between the two — or, with no concurrency at all,
// lets a retained key's retention expire between two clock readings — and
// publishes a document whose algorithms and key types describe two different key
// sets. A relying party caches that document for discoveryCacheMaxAge and then
// refuses assertions signed by a key it was half told about.
func (i *Issuer) signingProfile() ([]jwa.Algorithm, []string) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	now := i.clock()

	algorithms := []jwa.Algorithm{i.active.algorithm}
	keyTypes := []string{}
	if keyType, ok := i.active.published[jwk.KeyType].(string); ok {
		keyTypes = append(keyTypes, keyType)
	}

	for _, key := range i.retired {
		if now.After(key.expiresAt) {
			continue
		}
		if !slices.Contains(algorithms, key.algorithm) {
			algorithms = append(algorithms, key.algorithm)
		}
		if keyType, ok := key.published[jwk.KeyType].(string); ok && !slices.Contains(keyTypes, keyType) {
			keyTypes = append(keyTypes, keyType)
		}
	}

	return algorithms, keyTypes
}

// Handler serves the issuer's two metadata documents and its key set.
//
// This is the surface that makes Flowstate verifiable by other systems. Mount it
// at every path, on the host the issuer URL names:
//
//	mux.Handle(auth.DiscoveryPath, issuer.Handler())
//	mux.Handle(auth.WorkloadIssuerMetadataPath, issuer.Handler())
//	mux.Handle(issuer.JWKSPath(), issuer.Handler())
//
// Every endpoint is public and must stay public: a relying party fetches them
// before it has any credential to present, and they contain only public keys.
// Putting them behind the API's own authentication is the usual reason a working
// federation setup suddenly stops verifying.
func (i *Issuer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var document any
		switch r.URL.Path {
		case DiscoveryPath:
			document = i.Discovery()
		case WorkloadIssuerMetadataPath:
			document = i.WorkloadMetadata()
		case i.jwksPath:
			document = i.KeySet()
		default:
			http.NotFound(w, r)
			return
		}

		body, err := json.Marshal(document)
		if err != nil {
			// Unreachable: both documents are plain strings and maps built from
			// keys validated when they were created.
			http.Error(w, "cannot render document", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(int(discoveryCacheMaxAge.Seconds())))
		_, _ = w.Write(body)
	})
}
