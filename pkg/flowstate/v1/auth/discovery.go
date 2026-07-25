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
		ClaimsSupported: []string{
			"iss", "sub", "aud", "exp", "nbf", "iat", "jti",
			ClaimNamespace, ClaimDeployment, ClaimWorkflow, ClaimRun, ClaimStep,
			ClaimOnBehalfOf, ClaimOnBehalfOfIssuer,
		},
		ScopesSupported: []string{"openid"},
	}
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
	i.mu.RLock()
	defer i.mu.RUnlock()

	now := i.clock()

	algorithms := []jwa.Algorithm{i.active.algorithm}
	for _, key := range i.retired {
		if now.After(key.expiresAt) {
			continue
		}
		if !slices.Contains(algorithms, key.algorithm) {
			algorithms = append(algorithms, key.algorithm)
		}
	}

	return algorithms
}

// Handler serves the issuer's discovery document and key set.
//
// This is the surface that makes Flowstate verifiable by other systems. Mount it
// at both paths, on the host the issuer URL names:
//
//	mux.Handle(auth.DiscoveryPath, issuer.Handler())
//	mux.Handle(issuer.JWKSPath(), issuer.Handler())
//
// Both endpoints are public and must stay public: a relying party fetches them
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
