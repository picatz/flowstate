package vaulttransit_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth/signers/vaulttransit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

const keyName = "identity"

// backend starts a fake Transit holding one key of the given type.
func backend(t *testing.T, typ string) *authtest.Transit {
	t.Helper()

	transit := authtest.NewTransit()
	t.Cleanup(func() { _ = transit.Close() })
	transit.CreateKey(keyName, typ)

	return transit
}

// config reaches the fake the way a deployment with a loopback Vault would:
// through a named egress policy, with a token.
func config(transit *authtest.Transit, opts ...vault.Option) vaulttransit.Config {
	return vaulttransit.Config{
		Address:      transit.URL(),
		Key:          keyName,
		EgressPolicy: authtest.EgressPolicy(),
		Vault:        append([]vault.Option{vault.WithToken(authtest.TransitToken)}, opts...),
	}
}

func claims() jwt.ClaimsSet { return jwt.ClaimsSet{"sub": "workload", "aud": "https://as.example"} }

// verify checks a compact JWS against the public key the fake holds for the
// version its "kid" names, which is what a relying party does with the key set.
func verify(t *testing.T, transit *authtest.Transit, raw string, version int) *jwt.Token {
	t.Helper()

	token, err := jwt.Parse(raw)
	require.NoError(t, err)

	kid, err := token.Header.Get(header.KeyID)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%s-v%d", keyName, version), kid)

	alg, err := token.Header.Get(header.Algorithm)
	require.NoError(t, err)

	require.NoError(t, token.VerifySignature([]jwa.Algorithm{jwa.Algorithm(alg.(string))},
		map[string]any{kid.(string): transit.PublicKey(keyName, version)}))

	return token
}

func signRequests(transit *authtest.Transit) []authtest.TransitRequest {
	var requests []authtest.TransitRequest
	for _, request := range transit.Requests() {
		if strings.Contains(request.Path, "/sign/") {
			requests = append(requests, request)
		}
	}
	return requests
}

func TestSignerSignsWhatTheBackendPublishes(t *testing.T) {
	for _, tc := range []struct {
		typ       string
		algorithm jwa.Algorithm
		jws       bool
	}{
		{authtest.TransitECDSAP256, jwa.ES256, true},
		{authtest.TransitEd25519, jwa.EdDSA, false},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			transit := backend(t, tc.typ)

			signer, err := vaulttransit.New(t.Context(), config(transit))
			require.NoError(t, err)

			require.Equal(t, keyName+"-v1", signer.KeyID(), "the id is derived from the key version")
			require.Equal(t, tc.algorithm, signer.Algorithm(), "the algorithm is negotiated from the key type")
			require.Equal(t, transit.PublicKey(keyName, 1), signer.Public(), "the public half comes from the backend")

			key, err := signer.SigningKey(t.Context())
			require.NoError(t, err)
			require.Equal(t, keyName+"-v1", key.ID())

			raw, err := signer.Sign(t.Context(), claims())
			require.NoError(t, err)
			verify(t, transit, raw, 1)

			// The request, as the backend saw it: the version is pinned, the
			// format is the JWS one for ECDSA and left alone for Ed25519, and
			// the input is sent whole rather than prehashed.
			requests := signRequests(transit)
			require.NotEmpty(t, requests)
			for _, request := range requests {
				require.True(t, request.HadToken)
				require.Equal(t, 1, request.KeyVersion, "every signature names the version its kid names")
				require.False(t, request.Prehashed, "Transit hashes the input; it is not sent a digest")
				if tc.jws {
					require.Equal(t, "jws", request.Marshaling)
					require.Equal(t, "sha2-256", request.HashAlgorithm)
				} else {
					require.Empty(t, request.Marshaling)
					require.Empty(t, request.HashAlgorithm)
				}
			}
		})
	}
}

// TestSignerRefusesASignatureInTheWrongFormat is the format conversion gotcha
// from both directions: a backend that ignores the JWS marshaling answers ASN.1
// DER, which taken for r||s is a signature nobody verifies, and a signer that
// never asked for the JWS form would get exactly that.
func TestSignerRefusesASignatureInTheWrongFormat(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)

	transit.IgnoreMarshaling()

	_, err = signer.Sign(t.Context(), claims())
	require.ErrorIs(t, err, vaulttransit.ErrBackendMismatch)
	require.ErrorContains(t, err, "ASN.1 DER")

	// And the proof of possession a deployment runs at load refuses it too,
	// rather than the first mint finding out.
	_, err = signer.SigningKey(t.Context())
	require.Error(t, err)

	// The other half: the request did ask for the format that works.
	for _, request := range signRequests(transit) {
		require.Equal(t, "jws", request.Marshaling)
	}
}

// TestSignerSignsWithTheVersionItPublishes is the rotation race: Transit signs
// with its latest version unless told otherwise, so a signer that stamped its
// kid at start-up and let Transit choose would name one key and be signed by
// another the moment an operator rotated.
func TestSignerSignsWithTheVersionItPublishes(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)

	transit.RotateKey(keyName)

	raw, err := signer.Sign(t.Context(), claims())
	require.NoError(t, err)
	verify(t, transit, raw, 1)

	// A backend that does not honour the version it was asked for says which it
	// used, and the signer refuses the answer.
	transit.IgnoreKeyVersion()

	_, err = signer.Sign(t.Context(), claims())
	require.ErrorIs(t, err, vaulttransit.ErrBackendMismatch)
}

func TestSignerRefusesKeyTypesTheIssuerDoesNotPublish(t *testing.T) {
	for _, typ := range []string{authtest.TransitECDSAP384, authtest.TransitRSA2048} {
		t.Run(typ, func(t *testing.T) {
			transit := backend(t, typ)

			_, err := vaulttransit.New(t.Context(), config(transit))
			require.ErrorIs(t, err, vaulttransit.ErrUnsupportedKey)

			_, err = vaulttransit.Read(t.Context(), config(transit))
			require.ErrorIs(t, err, vaulttransit.ErrUnsupportedKey)

			require.Empty(t, signRequests(transit), "nothing is signed before the type is accepted")
		})
	}
}

func TestRotationPublishesPreviousVersions(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)
	require.Empty(t, signer.Previous())

	same, rotated, err := signer.Next(t.Context())
	require.NoError(t, err)
	require.False(t, rotated)
	require.Same(t, signer, same)

	transit.RotateKey(keyName)
	transit.RotateKey(keyName)

	next, rotated, err := signer.Next(t.Context())
	require.NoError(t, err)
	require.True(t, rotated)
	require.Equal(t, keyName+"-v3", next.KeyID())
	require.Equal(t, uint32(3), next.Version())

	var previous []string
	for _, key := range next.Previous() {
		previous = append(previous, key.ID)
		require.Equal(t, transit.PublicKey(keyName, int(key.Version)), key.Key)
	}
	require.Equal(t, []string{keyName + "-v2", keyName + "-v1"}, previous, "newest first")

	// The old signer still signs as the version it was built for.
	raw, err := signer.Sign(t.Context(), claims())
	require.NoError(t, err)
	verify(t, transit, raw, 1)

	// Raising the minimum version withdraws the versions below it, which is how
	// an operator ends the overlap; trimming drops what the backend no longer holds.
	transit.SetMinDecryptionVersion(keyName, 3)
	set, err := vaulttransit.Read(t.Context(), config(transit))
	require.NoError(t, err)
	require.Empty(t, set.Previous)
	require.Equal(t, keyName+"-v3", set.Current.ID)

	transit.SetMinDecryptionVersion(keyName, 2)
	transit.TrimKey(keyName, 2)
	set, err = vaulttransit.Read(t.Context(), config(transit))
	require.NoError(t, err)
	require.Len(t, set.Previous, 1)
	require.Equal(t, keyName+"-v2", set.Previous[0].ID)
}

// TestSignerRefusesAPublicKeyThatIsNotTheSigningKey is the "wrong public key"
// failure the Signer contract describes, reached the one way this signer can
// reach it: the backend reporting a key it does not sign with.
func TestSignerRefusesAPublicKeyThatIsNotTheSigningKey(t *testing.T) {
	for _, typ := range []string{authtest.TransitECDSAP256, authtest.TransitEd25519} {
		t.Run(typ, func(t *testing.T) {
			transit := backend(t, typ)
			transit.CreateKey("other", typ)
			transit.ServePublicKeyOf(keyName, 1, "other", 1)

			signer, err := vaulttransit.New(t.Context(), config(transit))
			require.NoError(t, err)

			_, err = signer.SigningKey(t.Context())
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.ErrorContains(t, err, "does not verify what that signer signs")
		})
	}
}

// TestSignerRefusesAPublicKeyOfAnotherTypeThanTheKey: a listing that says P-256
// and carries another curve or algorithm is not published.
func TestSignerRefusesAPublicKeyOfAnotherTypeThanTheKey(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	transit.CreateKey("p384", authtest.TransitECDSAP384)
	transit.CreateKey("ed", authtest.TransitEd25519)

	for _, other := range []string{"p384", "ed"} {
		transit.ServePublicKeyOf(keyName, 1, other, 1)

		_, err := vaulttransit.New(t.Context(), config(transit))
		require.ErrorIs(t, err, vaulttransit.ErrBackendMismatch, other)
	}
}

func TestSignerFailsClosedAndNeverEchoesTheBackend(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*authtest.Transit)
		is     error
	}{
		{"permission denied", func(tr *authtest.Transit) { tr.RevokeToken() }, secrets.ErrPermission},
		{"forbidden status", func(tr *authtest.Transit) { tr.SetStatus(http.StatusForbidden) }, secrets.ErrPermission},
		{"missing key", func(tr *authtest.Transit) { tr.SetStatus(http.StatusNotFound) }, secrets.ErrNotFound},
		{"sealed", func(tr *authtest.Transit) { tr.SetStatus(http.StatusServiceUnavailable) }, secrets.ErrUnavailable},
		{"server error", func(tr *authtest.Transit) { tr.SetStatus(http.StatusInternalServerError) }, secrets.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transit := backend(t, authtest.TransitECDSAP256)
			signer, err := vaulttransit.New(t.Context(), config(transit))
			require.NoError(t, err)

			tc.break_(transit)

			// Signing fails; there is no local key to fall back to.
			raw, err := signer.Sign(t.Context(), claims())
			require.ErrorIs(t, err, tc.is)
			require.Empty(t, raw)
			requireClean(t, err)

			_, err = signer.SigningKey(t.Context())
			require.Error(t, err)
			requireClean(t, err)

			_, err = vaulttransit.New(t.Context(), config(transit))
			require.ErrorIs(t, err, tc.is)
			requireClean(t, err)

			_, err = vaulttransit.Read(t.Context(), config(transit))
			require.ErrorIs(t, err, tc.is)
			requireClean(t, err)
		})
	}
}

// requireClean asserts an error carries neither the token nor anything the
// backend said.
func requireClean(t *testing.T, err error) {
	t.Helper()

	for _, secret := range []string{authtest.TransitToken, authtest.TransitErrorMarker} {
		require.NotContains(t, err.Error(), secret)
	}
}

func TestSignerBoundsResponses(t *testing.T) {
	t.Run("a configured cap", func(t *testing.T) {
		transit := backend(t, authtest.TransitECDSAP256)

		cfg := config(transit)
		cfg.MaxResponseBytes = 4096
		signer, err := vaulttransit.New(t.Context(), cfg)
		require.NoError(t, err)

		transit.Oversize(64 << 10)

		_, err = signer.Sign(t.Context(), claims())
		require.ErrorIs(t, err, secrets.ErrTooLarge)

		_, err = vaulttransit.Read(t.Context(), cfg)
		require.ErrorIs(t, err, secrets.ErrTooLarge)
	})

	t.Run("the default cap", func(t *testing.T) {
		transit := backend(t, authtest.TransitEd25519)
		transit.Oversize(int(vaulttransit.DefaultMaxResponseBytes) + 1)

		_, err := vaulttransit.New(t.Context(), config(transit))
		// Whichever bound reads the body first refuses it: the egress policy's
		// own cap and the Vault client's are both 1 MiB.
		require.True(t, errors.Is(err, secrets.ErrTooLarge) || errors.Is(err, netpolicy.ErrBodyTooLarge), "%v", err)
	})

	t.Run("a request past the issuer's token limit is never sent", func(t *testing.T) {
		transit := backend(t, authtest.TransitECDSAP256)
		signer, err := vaulttransit.New(t.Context(), config(transit))
		require.NoError(t, err)

		_, err = signer.Sign(t.Context(), jwt.ClaimsSet{"sub": strings.Repeat("x", auth.MaxSignatureBytes)})
		require.ErrorContains(t, err, "over the")
		require.Empty(t, signRequests(transit))
	})
}

func TestSignerTimesOut(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)

	cfg := config(transit)
	cfg.Timeout = 150 * time.Millisecond
	signer, err := vaulttransit.New(t.Context(), cfg)
	require.NoError(t, err)

	transit.Hang(true)

	start := time.Now()
	_, err = signer.Sign(t.Context(), claims())
	require.ErrorIs(t, err, secrets.ErrUnavailable)
	require.Less(t, time.Since(start), 5*time.Second, "the configured timeout bounds the request")
}

// TestSignerHonoursTheCallersDeadline: the mint's request context reaches the
// backend, and the earlier of it and the configured timeout wins.
func TestSignerHonoursTheCallersDeadline(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)

	transit.Hang(true)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = signer.Sign(ctx, claims())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestSignerIsBoundByTheEgressPolicy(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)

	// No policy: the default denies loopback, which is where the fake is.
	cfg := config(transit)
	cfg.EgressPolicy = nil

	_, err := vaulttransit.New(t.Context(), cfg)
	require.ErrorIs(t, err, netpolicy.ErrDenied)
	require.Empty(t, transit.Requests(), "a denied request never reaches the backend")

	// A policy that does not admit the scheme.
	httpsOnly, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithSchemes("https"))
	require.NoError(t, err)

	cfg.EgressPolicy = httpsOnly
	_, err = vaulttransit.New(t.Context(), cfg)
	require.ErrorIs(t, err, netpolicy.ErrDenied)
	require.Empty(t, transit.Requests())
}

func TestSignerDoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	t.Cleanup(elsewhere.Close)

	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirecting.Close)

	_, err := vaulttransit.New(t.Context(), vaulttransit.Config{
		Address:      redirecting.URL,
		Key:          keyName,
		EgressPolicy: authtest.EgressPolicy(),
		Vault:        []vault.Option{vault.WithToken(authtest.TransitToken)},
	})
	require.Error(t, err)
	require.Zero(t, reached.Load(), "the token must not follow a redirect off-origin")
	requireClean(t, err)
}

func TestSignerIsSafeForConcurrentUse(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)

	transit.DelaySign(100 * time.Millisecond)

	const calls = 8

	var wg sync.WaitGroup
	tokens := make([]string, calls)
	errs := make([]error, calls)
	for i := range calls {
		wg.Go(func() {
			tokens[i], errs[i] = signer.Sign(t.Context(), jwt.ClaimsSet{"sub": fmt.Sprint(i)})
		})
	}
	wg.Wait()

	for i := range calls {
		require.NoError(t, errs[i])
		verify(t, transit, tokens[i], 1)
	}
	require.Greater(t, transit.PeakInFlight(), 1, "concurrent signs are concurrent round trips, not serialized")
}

func TestConfigRefusals(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)

	for name, mutate := range map[string]func(*vaulttransit.Config){
		"no authentication":    func(c *vaulttransit.Config) { c.Vault = nil },
		"a bad key name":       func(c *vaulttransit.Config) { c.Key = "a/b" },
		"no key name":          func(c *vaulttransit.Config) { c.Key = "" },
		"a cleartext address":  func(c *vaulttransit.Config) { c.Address = "http://vault.example.com:8200" },
		"credentials in a URL": func(c *vaulttransit.Config) { c.Address = "https://user:pw@vault.example.com" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config(transit)
			mutate(&cfg)

			_, err := vaulttransit.New(t.Context(), cfg)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "pw@")
		})
	}
}

// TestSignerHoldsNoPrivateKey pins the claim the package exists for: nothing a
// Signer exposes, by field or by method, can be or return private key material.
// The key is in the backend, so the only way this could fail is someone adding
// a way to hold one.
func TestSignerHoldsNoPrivateKey(t *testing.T) {
	forbidden := []reflect.Type{
		reflect.TypeFor[crypto.PrivateKey](),
		reflect.TypeFor[crypto.Signer](),
		reflect.TypeFor[*ecdsa.PrivateKey](),
		reflect.TypeFor[*rsa.PrivateKey](),
		reflect.TypeFor[ed25519.PrivateKey](),
	}

	holds := func(typ reflect.Type) bool {
		for _, bad := range forbidden {
			if typ == bad {
				return true
			}
			// Anything with the private key's shape: it can sign. A public key
			// type never can.
			if typ.Kind() != reflect.Interface && typ.Implements(reflect.TypeFor[crypto.Signer]()) {
				return true
			}
		}
		return false
	}

	// Walk every type reachable from a Signer's methods and fields, including
	// unexported fields, which is where a stored key would hide.
	seen := map[reflect.Type]bool{}
	var walk func(reflect.Type)
	walk = func(typ reflect.Type) {
		if seen[typ] {
			return
		}
		seen[typ] = true

		require.False(t, holds(typ), "%s can hold or return a private key", typ)

		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(typ.Elem())
		case reflect.Map:
			walk(typ.Key())
			walk(typ.Elem())
		case reflect.Struct:
			// Another package's internals (the Vault client, a token cache) are
			// opaque here: the claim is about this package's types and what
			// they expose.
			if typ.PkgPath() != reflect.TypeFor[vaulttransit.Signer]().PkgPath() {
				return
			}
			for field := range typ.Fields() {
				walk(field.Type)
			}
		}
	}

	for _, typ := range []reflect.Type{
		reflect.TypeFor[*vaulttransit.Signer](),
		reflect.TypeFor[vaulttransit.PublicKey](),
		reflect.TypeFor[vaulttransit.KeySet](),
	} {
		walk(typ)
		for method := range typ.Methods() {
			for out := range method.Type.Outs() {
				walk(out)
			}
		}
	}

	// And the one value it does expose as a key is the public half.
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)
	require.IsType(t, &ecdsa.PublicKey{}, signer.Public())
}

// TestSignerRendersWithoutTheToken: whatever a log line or a %v makes of the
// signer, the Vault token is not in it.
func TestSignerRendersWithoutTheToken(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), config(transit))
	require.NoError(t, err)

	key, err := signer.SigningKey(t.Context())
	require.NoError(t, err)

	for _, rendered := range []string{
		fmt.Sprintf("%v %+v %#v", signer, signer, signer),
		fmt.Sprintf("%v %+v %#v", *signer, *signer, *signer),
		fmt.Sprintf("%v %+v %#v", key, key, key),
	} {
		require.NotContains(t, rendered, authtest.TransitToken)
	}
}

// TestCallerOptionsCannotReplaceTheEgressPolicy: a Vault option that brings its
// own HTTP client does not get to dial where the policy forbids.
func TestCallerOptionsCannotReplaceTheEgressPolicy(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)

	cfg := config(transit, vault.WithHTTPClient(&http.Client{}))
	cfg.EgressPolicy = nil // the default, which denies loopback

	_, err := vaulttransit.New(t.Context(), cfg)
	require.ErrorIs(t, err, netpolicy.ErrDenied)
	require.Empty(t, transit.Requests(), "the unrestricted client was never used")
}

// TestVersionsAreBoundedByTheListingNotTheirNumbers: one entry at the top of the
// version range costs one entry.
func TestVersionsAreBoundedByTheListingNotTheirNumbers(t *testing.T) {
	public := base64.StdEncoding.EncodeToString(make([]byte, 32))
	body := `{"data":{"type":"ed25519","latest_version":4294967295,"min_decryption_version":1,"keys":{` +
		`"4294967295":{"name":"ed25519","public_key":"` + public + `"},` +
		`"4294967290":{"name":"ed25519","public_key":"` + public + `"}}}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)

	cfg := vaulttransit.Config{
		Address:      server.URL,
		Key:          keyName,
		EgressPolicy: authtest.EgressPolicy(),
		Vault:        []vault.Option{vault.WithToken("t")},
	}

	start := time.Now()
	set, err := vaulttransit.Read(t.Context(), cfg)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, keyName+"-v4294967295", set.Current.ID)
	require.Len(t, set.Previous, 1)
	require.Equal(t, uint32(4294967290), set.Previous[0].Version)
}

// TestTokenFileIsReReadWhenVaultRejectsTheToken is the Vault Agent sink: the
// token rotates under the process, and the next signature picks it up.
func TestTokenFileIsReReadWhenVaultRejectsTheToken(t *testing.T) {
	transit := backend(t, authtest.TransitECDSAP256)

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(authtest.TransitToken+"\n"), 0o600))

	cfg := config(transit)
	cfg.Vault = []vault.Option{vault.WithTokenFile(path)}

	signer, err := vaulttransit.New(t.Context(), cfg)
	require.NoError(t, err)

	_, err = signer.Sign(t.Context(), claims())
	require.NoError(t, err)

	const rotated = "rotated-token-value"
	transit.AcceptToken(rotated)

	// The file still holds the rejected token: refused, and the error is clean.
	_, err = signer.Sign(t.Context(), claims())
	require.ErrorIs(t, err, secrets.ErrPermission)
	require.NotContains(t, err.Error(), authtest.TransitToken)

	// The agent writes the new one; the next call is rejected once, re-reads, and succeeds.
	require.NoError(t, os.WriteFile(path, []byte(rotated+"\n"), 0o600))

	raw, err := signer.Sign(t.Context(), claims())
	require.NoError(t, err)
	verify(t, transit, raw, 1)

	// An unreadable file is a failure that names the path and not the token.
	require.NoError(t, os.Remove(path))
	transit.AcceptToken("another")

	_, err = signer.Sign(t.Context(), claims())
	require.Error(t, err)
	require.NotContains(t, err.Error(), rotated)
}
