package auth_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// testIdentity is the workload identity most outbound tests mint for: a run
// submitted by a CI pipeline, acting on its behalf.
func testIdentity() auth.WorkloadIdentity {
	return auth.WorkloadIdentity{
		Subject:    "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:     "https://token.actions.githubusercontent.com",
		Namespace:  "acme",
		Deployment: "prod",
		Claims:     map[string]string{"repository": "picatz/flowstate"},
	}
}

// testStepRef is the unit of work most outbound tests mint for.
func testStepRef() auth.StepRef {
	return auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "push-image"}
}

// newIssuer returns an issuer serving its discovery document and key set from an
// httptest server, along with that server's URL as the issuer identifier.
//
// The server is created before the issuer, because the issuer has to be told the
// URL it is reachable at, and dispatches to whatever handler the issuer later
// installs.
func newIssuer(t *testing.T, clock *authtest.Clock, opts ...auth.IssuerOption) (*auth.Issuer, *httptest.Server) {
	t.Helper()

	var (
		mu      sync.RWMutex
		handler http.Handler
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		current := handler
		mu.RUnlock()

		if current == nil {
			http.Error(w, "issuer not ready", http.StatusServiceUnavailable)
			return
		}
		current.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	key, err := auth.GenerateSigningKey("test-key", jwa.ES256)
	require.NoError(t, err)

	// The claim set is closed, so an issuer these tests mint through has to
	// declare what [testIdentity] carries. Derived from that identity rather
	// than spelled again, so adding a claim there does not silently start
	// failing every mint in the package. A test about the allowlist itself
	// declares its own set — see TestMintRefusesAnUndeclaredClaim.
	declared := slices.Sorted(maps.Keys(testIdentity().Claims))

	issuer, err := auth.NewIssuer(server.URL, key,
		append([]auth.IssuerOption{
			auth.WithIssuerClock(clock.Now),
			auth.WithDeclaredClaims(declared...),
		}, opts...)...)
	require.NoError(t, err)

	mu.Lock()
	handler = issuer.Handler()
	mu.Unlock()

	return issuer, server
}

// TestWorkloadIdentitySubject covers the subject a relying party writes its policy
// against. Its shape is a promise: it has to be stable, hierarchical so a prefix
// rule can match at any level, and impossible for one component to forge another.
func TestWorkloadIdentitySubject(t *testing.T) {
	tests := []struct {
		name     string
		identity auth.WorkloadIdentity
		ref      auth.StepRef
		want     string
		wantErr  bool
	}{
		{
			name:     "a fully named workload",
			identity: testIdentity(),
			ref:      testStepRef(),
			want:     "flowstate:acme/prod/deploy-service/push-image",
		},
		{
			name:     "namespace and deployment default when unset",
			identity: auth.WorkloadIdentity{Subject: "someone", Issuer: "https://idp.example.com"},
			ref:      testStepRef(),
			// _default, not default: "default" is a legal namespace, so the
			// placeholder for "no namespace at all" must be spelled with
			// something no namespace can spell. See the defaultComponent doc
			// comment in identity.go.
			want: "flowstate:_default/_default/deploy-service/push-image",
		},
		{
			name:     "the run is not part of the subject",
			identity: testIdentity(),
			ref:      auth.StepRef{Workflow: "deploy-service", Run: "another-run", Step: "push-image"},
			want:     "flowstate:acme/prod/deploy-service/push-image",
		},
		{
			name:     "no workflow",
			identity: testIdentity(),
			ref:      auth.StepRef{Step: "push-image"},
			wantErr:  true,
		},
		{
			name:     "no step",
			identity: testIdentity(),
			ref:      auth.StepRef{Workflow: "deploy-service"},
			wantErr:  true,
		},
		{
			name: "a namespace that would spell out two components",
			identity: auth.WorkloadIdentity{
				Subject: "someone", Issuer: "https://idp.example.com",
				Namespace: "acme/prod", Deployment: "prod",
			},
			ref:     testStepRef(),
			wantErr: true,
		},
		{
			name:     "a workflow name containing the prefix separator",
			identity: testIdentity(),
			ref: auth.StepRef{
				Workflow: "deploy:service", Step: "push-image",
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject, err := test.identity.SubjectFor(test.ref)

			if test.wantErr {
				require.ErrorIs(t, err, auth.ErrInvalidIdentity)
				require.Empty(t, subject)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, subject)
		})
	}
}

// TestIssuerMintRejects covers what must never be minted. An assertion is a
// credential Flowstate signs, so refusing to make one is always safer than making
// one that says less than it appears to.
func TestIssuerMintRejects(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	tests := []struct {
		name     string
		identity auth.WorkloadIdentity
		ref      auth.StepRef
		audience string
		wantErr  error
	}{
		{
			name:     "no identity at all",
			identity: auth.WorkloadIdentity{},
			ref:      testStepRef(),
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name:     "an identity with no subject",
			identity: auth.WorkloadIdentity{Issuer: "https://idp.example.com", Namespace: "acme"},
			ref:      testStepRef(),
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name:     "an identity with no issuer",
			identity: auth.WorkloadIdentity{Subject: "someone", Namespace: "acme"},
			ref:      testStepRef(),
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name:     "no audience, which any relying party would accept",
			identity: testIdentity(),
			ref:      testStepRef(),
			audience: "",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name:     "no step reference to name the work",
			identity: testIdentity(),
			ref:      auth.StepRef{},
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name: "a carried claim shadowing the subject",
			identity: auth.WorkloadIdentity{
				Subject: "someone", Issuer: "https://idp.example.com",
				Claims: map[string]string{"sub": "flowstate:acme/prod/other/step"},
			},
			ref:      testStepRef(),
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name: "a carried claim shadowing the delegation",
			identity: auth.WorkloadIdentity{
				Subject: "someone", Issuer: "https://idp.example.com",
				Claims: map[string]string{auth.ClaimOnBehalfOf: "someone-else"},
			},
			ref:      testStepRef(),
			audience: "sts.amazonaws.com",
			wantErr:  auth.ErrInvalidIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertion, err := issuer.Mint(t.Context(), test.identity, test.ref, test.audience)

			require.ErrorIs(t, err, test.wantErr)
			require.True(t, assertion.IsZero())
			require.Empty(t, assertion.Token())
		})
	}
}

// TestIssuerRoundTrip is the end-to-end proof that both halves of federation agree:
// an assertion minted by the issuer, verified by this package's own relying-party
// verifier, using only the discovery document and key set the issuer publishes.
//
// If Flowstate cannot verify its own identity through its own published metadata,
// no other relying party will either.
func TestIssuerRoundTrip(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock)

	const audience = "sts.amazonaws.com"

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	require.Equal(t, server.URL, assertion.Issuer)
	require.Equal(t, audience, assertion.Audience)
	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", assertion.Subject)
	require.Equal(t, "test-key", assertion.KeyID)
	require.NotEmpty(t, assertion.ID)
	require.Equal(t, referenceTime.Add(auth.DefaultAssertionLifetime).Unix(), assertion.ExpiresAt.Unix())

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "flowstate-self",
				Issuer:    server.URL,
				Audiences: []string{audience},
			}},
		},
		auth.WithClock(clock.Now),
	)

	principal, err := verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "Flowstate must be able to verify its own assertions from its published keys")

	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", principal.Subject)
	require.Equal(t, server.URL, principal.Issuer)
	require.True(t, principal.HasAudience(audience))

	// The claims a relying party authorizes on, including the delegation: the
	// subject says which workload is calling, and on_behalf_of says who caused it
	// to run.
	for claim, want := range map[string]string{
		auth.ClaimNamespace:        "acme",
		auth.ClaimDeployment:       "prod",
		auth.ClaimWorkflow:         "deploy-service",
		auth.ClaimStep:             "push-image",
		auth.ClaimRun:              "run-1",
		auth.ClaimOnBehalfOf:       "repo:picatz/flowstate:ref:refs/heads/main",
		auth.ClaimOnBehalfOfIssuer: "https://token.actions.githubusercontent.com",
		"repository":               "picatz/flowstate",
	} {
		got, ok := principal.StringClaim(claim)
		require.True(t, ok, "assertion must carry the %q claim", claim)
		require.Equal(t, want, got, "claim %q", claim)
	}
}

// TestIssuerAssertionIsAudienceScoped is the replay test: an assertion minted for
// one relying party must be useless at another. Without this, one compromised or
// merely careless relying party could present a Flowstate assertion to every other
// system that trusts Flowstate.
func TestIssuerAssertionIsAudienceScoped(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock)

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "sts.amazonaws.com")
	require.NoError(t, err)

	// A relying party that expects a different audience, verifying correctly.
	other := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "someone-else",
				Issuer:    server.URL,
				Audiences: []string{"https://partner.example.com"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	_, err = other.Verify(t.Context(), assertion.Token())
	require.ErrorIs(t, err, auth.ErrInvalidAudience)
}

// TestIssuerAssertionExpires checks that an assertion stops working, and that the
// lifetime cannot be configured long enough to be a standing grant.
func TestIssuerAssertionExpires(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock, auth.WithAssertionLifetime(2*time.Minute))

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "flowstate-test")
	require.NoError(t, err)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "flowstate-self",
				Issuer:    server.URL,
				Audiences: []string{"flowstate-test"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithClockSkew(0),
	)

	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err)

	clock.Advance(3 * time.Minute)

	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.ErrorIs(t, err, auth.ErrTokenExpired)
}

// TestNewIssuerRejectsBadConfiguration checks that an unusable issuer is refused
// when it is built.
func TestNewIssuerRejectsBadConfiguration(t *testing.T) {
	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)

	tests := []struct {
		name string
		url  string
		opts []auth.IssuerOption
	}{
		{name: "no issuer URL", url: ""},
		{name: "an issuer URL that is not a URL", url: "flowstate.example.com"},
		{name: "an issuer URL served over plain http", url: "http://flowstate.example.com"},
		{name: "an issuer URL with a query string", url: "https://flowstate.example.com?tenant=a"},
		{
			name: "a lifetime long enough to be a standing grant",
			url:  "https://flowstate.example.com",
			opts: []auth.IssuerOption{auth.WithAssertionLifetime(24 * time.Hour)},
		},
		{
			name: "a lifetime of zero",
			url:  "https://flowstate.example.com",
			opts: []auth.IssuerOption{auth.WithAssertionLifetime(0)},
		},
		{
			name: "a key set path that is not a path",
			url:  "https://flowstate.example.com",
			opts: []auth.IssuerOption{auth.WithJWKSPath("keys.json")},
		},
		{
			name: "a key set path that shadows the discovery document",
			url:  "https://flowstate.example.com",
			opts: []auth.IssuerOption{auth.WithJWKSPath(auth.DiscoveryPath)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issuer, err := auth.NewIssuer(test.url, key, test.opts...)
			require.Error(t, err)
			require.Nil(t, issuer)
		})
	}

	t.Run("no signing key", func(t *testing.T) {
		issuer, err := auth.NewIssuer("https://flowstate.example.com", auth.SigningKey{})
		require.ErrorIs(t, err, auth.ErrNoSigningKey)
		require.Nil(t, issuer)
	})
}

// TestSigningKey covers which keys may sign assertions, and that a key never
// reveals itself.
func TestSigningKey(t *testing.T) {
	t.Run("supported algorithms", func(t *testing.T) {
		for _, algorithm := range []jwa.Algorithm{jwa.ES256, jwa.RS256, jwa.EdDSA} {
			t.Run(algorithm, func(t *testing.T) {
				key, err := auth.GenerateSigningKey("k", algorithm)
				require.NoError(t, err)
				require.Equal(t, algorithm, key.Algorithm())
				require.Equal(t, "k", key.ID())
				require.False(t, key.IsZero())
			})
		}
	})

	t.Run("algorithms that cannot sign", func(t *testing.T) {
		// ES384 and ES512 are refused because the JOSE library cannot produce
		// those signatures; finding that out at the first mint would be worse.
		for _, algorithm := range []jwa.Algorithm{jwa.ES384, jwa.ES512, jwa.HS256, jwa.None, "nonsense"} {
			t.Run(algorithm, func(t *testing.T) {
				_, err := auth.GenerateSigningKey("k", algorithm)
				require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			})
		}
	})

	t.Run("a curve that cannot be signed with", func(t *testing.T) {
		private, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		require.NoError(t, err)

		_, err = auth.NewSigningKey("k", private)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("a key with no id", func(t *testing.T) {
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		_, err = auth.NewSigningKey("", private)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("something that is not a key", func(t *testing.T) {
		_, err := auth.NewSigningKey("k", "hunter2")
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("never reveals key material", func(t *testing.T) {
		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)

		// A signing key is the one secret that must never be printable, and Go
		// will happily format a struct's unexported fields with %v unless the
		// type says otherwise.
		for _, rendered := range []string{
			key.String(),
			fmt.Sprint(key),
			fmt.Sprintf("%v", key),
			fmt.Sprintf("%s", key),
		} {
			require.Equal(t, "signing key k (ES256)", rendered)
		}

		// A type with no exported fields and no custom marshaling is usually a
		// mistake, and staticcheck says so. Here it is the property under test:
		// the key holds its material unexported precisely so that encoding it
		// yields nothing, and the day someone exports a field the assertion
		// below is what catches it.
		//lint:ignore SA9005 the empty encoding is the assertion, not an oversight
		encoded, err := json.Marshal(key)
		require.NoError(t, err)
		require.Equal(t, "{}", string(encoded), "a signing key must serialize to nothing")
	})
}

// TestIssuerRotation checks that keys can be replaced while serving: assertions
// signed with the old key keep verifying for as long as the operator configured,
// and stop afterwards.
func TestIssuerRotation(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock, auth.WithKeyRetention(time.Hour))

	const audience = "flowstate-test"

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "flowstate-self",
				Issuer:    server.URL,
				Audiences: []string{audience},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithKeyCacheTTL(time.Minute),
		auth.WithMinKeyRefreshInterval(time.Second),
	)

	before, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, "test-key", issuer.ActiveKeyID())

	rotated, err := auth.GenerateSigningKey("test-key-2", jwa.RS256)
	require.NoError(t, err)
	require.NoError(t, issuer.Rotate(rotated))
	require.Equal(t, "test-key-2", issuer.ActiveKeyID())

	after, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, "test-key-2", after.KeyID)

	// Both keys are published, so both assertions verify. The relying party
	// refetches because it has never seen the new key id.
	clock.Advance(2 * time.Second)

	_, err = verifier.Verify(t.Context(), after.Token())
	require.NoError(t, err, "an assertion signed with the new key must verify")

	_, err = verifier.Verify(t.Context(), before.Token())
	require.NoError(t, err, "an assertion signed with the retired key must still verify")

	require.Len(t, issuer.KeySet().Keys, 2)

	// Past the retention period the old key is withdrawn.
	clock.Advance(2 * time.Hour)
	require.Len(t, issuer.KeySet().Keys, 1)

	fresh, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), fresh.Token())
	require.NoError(t, err)

	t.Run("rotating to the same key id is refused", func(t *testing.T) {
		same, err := auth.GenerateSigningKey("test-key-2", jwa.ES256)
		require.NoError(t, err)
		require.Error(t, issuer.Rotate(same))
	})

	t.Run("rotating to nothing is refused", func(t *testing.T) {
		require.ErrorIs(t, issuer.Rotate(auth.SigningKey{}), auth.ErrNoSigningKey)
	})
}

// TestIssuerHandler checks the surface other systems read to establish trust.
func TestIssuerHandler(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock)

	t.Run("discovery document", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + auth.DiscoveryPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		require.Contains(t, response.Header.Get("Cache-Control"), "max-age=")

		var document auth.DiscoveryDocument
		require.NoError(t, json.NewDecoder(response.Body).Decode(&document))

		// The issuer field must match exactly, or a relying party performing
		// discovery correctly will refuse the document.
		require.Equal(t, server.URL, document.Issuer)
		require.Equal(t, server.URL+auth.DefaultJWKSPath, document.JWKSURI)
		require.Equal(t, []jwa.Algorithm{jwa.ES256}, document.IDTokenSigningAlgValuesSupported)
		require.Contains(t, document.ClaimsSupported, auth.ClaimOnBehalfOf)
	})

	t.Run("key set", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + issuer.JWKSPath())
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })

		require.Equal(t, http.StatusOK, response.StatusCode)

		var set struct {
			Keys []map[string]any `json:"keys"`
		}
		require.NoError(t, json.NewDecoder(response.Body).Decode(&set))
		require.Len(t, set.Keys, 1)

		key := set.Keys[0]
		require.Equal(t, "test-key", key["kid"])
		require.Equal(t, "EC", key["kty"])
		require.Equal(t, "sig", key["use"])
		require.Equal(t, jwa.ES256, key["alg"])
		require.NotContains(t, key, "d", "the key set must never contain private key material")
	})

	t.Run("anything else", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + "/secrets")
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })
		require.Equal(t, http.StatusNotFound, response.StatusCode)
	})

	t.Run("a method other than GET", func(t *testing.T) {
		response, err := server.Client().Post(server.URL+auth.DiscoveryPath, "application/json", strings.NewReader("{}"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })

		require.Equal(t, http.StatusMethodNotAllowed, response.StatusCode)
		require.Equal(t, "GET, HEAD", response.Header.Get("Allow"))
	})
}

// TestAssertionNeverRevealsItsToken checks that the one value in an assertion that
// is a credential cannot escape through printing or serialization.
func TestAssertionNeverRevealsItsToken(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "flowstate-test")
	require.NoError(t, err)

	token := assertion.Token()
	require.NotEmpty(t, token)

	for _, rendered := range []string{
		assertion.String(),
		fmt.Sprint(assertion),
		fmt.Sprintf("%v", assertion),
	} {
		require.NotContains(t, rendered, token)
		require.Contains(t, rendered, assertion.Subject)
	}

	encoded, err := json.Marshal(assertion)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), token, "serializing an assertion must not carry the token")

	// Which is what makes the durable-history rule enforceable rather than
	// advisory: an assertion that has been through a serializer has no token.
	var restored auth.Assertion
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Empty(t, restored.Token())
}
