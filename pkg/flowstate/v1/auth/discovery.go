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

const discoveryCacheMaxAge = 5 * time.Minute

// WorkloadIssuerMetadataPath is Flowstate's protocol-accurate discovery
// endpoint.  It describes signed workload assertions, not an OAuth
// authorization server or an OpenID Provider.
const WorkloadIssuerMetadataPath = "/.well-known/workload-identity-configuration"

// WorkloadAssertionProfile is the versioned contract of assertions minted by
// Issuer.  Consumers should key compatibility on this value, not infer OIDC
// authorization semantics from the well-known URL used by some cloud products.
const WorkloadAssertionProfile = "https://flowstate.dev/profiles/workload-assertion/v1"

// OIDCCompatibilityProfile identifies the deliberately small, OIDC-shaped
// compatibility document. It is not OpenID Provider Metadata: Flowstate has no
// authorization endpoint, token endpoint, openid scope, authorization response,
// ID token, or client protocol.
const OIDCCompatibilityProfile = "https://flowstate.dev/profiles/oidc-shaped-workload-issuer/v1"

// WorkloadIssuerMetadata describes exactly the JWT workload assertion surface.
// Algorithms and claims are derived from the declarations used by signing.
type WorkloadIssuerMetadata struct {
	Issuer                     string          `json:"issuer"`
	JWKSURI                    string          `json:"jwks_uri"`
	AssertionProfilesSupported []string        `json:"assertion_profiles_supported"`
	SigningAlgValuesSupported  []jwa.Algorithm `json:"signing_alg_values_supported"`
	ClaimsSupported            []string        `json:"claims_supported"`
	KeyTypesSupported          []string        `json:"key_types_supported"`
}

// OIDCCompatibilityDocument is served at the path hard-coded by OIDC-shaped
// WIF consumers. Its profile marker makes the semantics explicit. In particular
// it intentionally omits every field that would claim OpenID Provider or OAuth
// authorization-server behavior.
type OIDCCompatibilityDocument struct {
	Issuer                    string          `json:"issuer"`
	JWKSURI                   string          `json:"jwks_uri"`
	WorkloadIssuerProfile     string          `json:"workload_issuer_profile"`
	SigningAlgValuesSupported []jwa.Algorithm `json:"signing_alg_values_supported"`
	ClaimsSupported           []string        `json:"claims_supported"`
}

// DiscoveryDocument is retained as a source-compatible name for the workload
// issuer contract. It no longer denotes OpenID Provider Metadata.
type DiscoveryDocument = WorkloadIssuerMetadata

func (i *Issuer) supportedClaims() []string {
	return append(slices.Clone(builtInAssertionClaims), i.DeclaredClaims()...)
}

// WorkloadMetadata returns Flowstate's native workload-issuer contract.
func (i *Issuer) WorkloadMetadata() WorkloadIssuerMetadata {
	return WorkloadIssuerMetadata{
		Issuer: i.url, JWKSURI: i.JWKSURL(),
		AssertionProfilesSupported: []string{WorkloadAssertionProfile},
		SigningAlgValuesSupported:  i.algorithms(), ClaimsSupported: i.supportedClaims(),
		KeyTypesSupported: i.keyTypes(),
	}
}

// Discovery returns the native workload metadata. Use OIDCCompatibility when a
// consumer insists on fetching the OIDC Discovery well-known path.
func (i *Issuer) Discovery() DiscoveryDocument { return i.WorkloadMetadata() }

func (i *Issuer) OIDCCompatibility() OIDCCompatibilityDocument {
	m := i.WorkloadMetadata()
	return OIDCCompatibilityDocument{Issuer: m.Issuer, JWKSURI: m.JWKSURI,
		WorkloadIssuerProfile:     OIDCCompatibilityProfile,
		SigningAlgValuesSupported: m.SigningAlgValuesSupported, ClaimsSupported: m.ClaimsSupported}
}

// KeySet returns all currently verifiable public keys.
func (i *Issuer) KeySet() jwk.Set {
	i.mu.RLock()
	defer i.mu.RUnlock()
	now := i.clock()
	set := jwk.Set{Keys: make([]jwk.Value, 0, len(i.retired)+1)}
	set.Keys = append(set.Keys, i.active.published)
	for _, key := range i.retired {
		if !now.After(key.expiresAt) {
			set.Keys = append(set.Keys, key.published)
		}
	}
	return set
}

func (i *Issuer) algorithms() []jwa.Algorithm {
	i.mu.RLock()
	defer i.mu.RUnlock()
	now := i.clock()
	result := []jwa.Algorithm{i.active.algorithm}
	for _, key := range i.retired {
		if !now.After(key.expiresAt) && !slices.Contains(result, key.algorithm) {
			result = append(result, key.algorithm)
		}
	}
	return result
}

func (i *Issuer) keyTypes() []string {
	result := []string{}
	for _, key := range i.KeySet().Keys {
		if kty, ok := key[jwk.KeyType].(string); ok && !slices.Contains(result, kty) {
			result = append(result, kty)
		}
	}
	return result
}

// Handler serves native metadata, the explicitly labelled OIDC-shaped
// compatibility document, and JWKS. All are public bootstrap information.
func (i *Issuer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var document any
		switch r.URL.Path {
		case WorkloadIssuerMetadataPath:
			document = i.WorkloadMetadata()
		case DiscoveryPath:
			document = i.OIDCCompatibility()
		case i.jwksPath:
			document = i.KeySet()
		default:
			http.NotFound(w, r)
			return
		}
		body, err := json.Marshal(document)
		if err != nil {
			http.Error(w, "cannot render document", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(int(discoveryCacheMaxAge.Seconds())))
		_, _ = w.Write(body)
	})
}
