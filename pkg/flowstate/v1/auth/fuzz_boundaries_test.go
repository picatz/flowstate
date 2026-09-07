package auth_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The parsers here take bytes across a boundary THREAT_MODEL.md names — an
// operator's trust policy, a tenant map, a bearer token from any network
// client — and none had a fuzz target while fourteen covered the Flowfile an
// author feeds (#1721). A panic in one of these is a denial of service against
// a shared server rather than a bad exit code on a laptop, so each asserts the
// two properties that matter at a boundary: no panic, and an error or a value,
// never both.

// FuzzParsePolicy fuzzes the trust-policy and federation-policy decoders.
func FuzzParsePolicy(f *testing.F) {
	f.Add([]byte("issuers:\n  - name: idp\n    issuer: https://issuer.example.com\n    audiences: [flowstate]\n    require:\n      - claim: sub\n        any_of: [runner]\n"))
	f.Add([]byte("issuers:\n  - name: idp\n    issuer: https://issuer.example.com\n"))
	f.Add([]byte(`{"issuers":[{"name":"idp","issuer":"https://issuer.example.com","audiences":["flowstate"]}]}`))
	f.Add([]byte("issuers: []\n"))
	f.Add([]byte("audience: flowstate\n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		policy, err := auth.ParsePolicy(data)
		if err != nil && !reflect.DeepEqual(policy, auth.Policy{}) {
			t.Fatalf("ParsePolicy returned both an error and a policy: %v", err)
		}

		federation, err := auth.ParseFederationPolicy(data)
		if err != nil && !reflect.DeepEqual(federation, auth.FederationPolicy{}) {
			t.Fatalf("ParseFederationPolicy returned both an error and a policy: %v", err)
		}
	})
}

// FuzzNamespaceMap fuzzes the tenant map's YAML and JSON decoders, with the
// round-trip property on top: what decodes must encode, and decode again to
// the same map. [auth.Material]'s decoder, which discards whatever it is
// given, is fed the same bytes so it can never be surprised by them.
func FuzzNamespaceMap(f *testing.F) {
	f.Add([]byte("team-a\n"))
	f.Add([]byte("{\"issuer\": \"team-a\", \"*\": \"shared\"}\n"))
	f.Add([]byte("issuer: team-a\n\"*\": shared\n"))
	f.Add([]byte("[]"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		var fromYAML auth.NamespaceMap
		if err := fromYAML.UnmarshalYAML(data); err == nil {
			encoded, err := fromYAML.MarshalYAML()
			if err != nil {
				t.Fatalf("a map that decoded from YAML did not encode: %v", err)
			}
			var again auth.NamespaceMap
			if err := again.UnmarshalYAML(encoded); err != nil {
				t.Fatalf("the YAML a map encoded to did not decode: %v\n%s", err, encoded)
			}
			if !reflect.DeepEqual(fromYAML, again) {
				t.Fatalf("YAML round trip changed the map:\n%#v\n%#v", fromYAML, again)
			}
		}

		var fromJSON auth.NamespaceMap
		if err := fromJSON.UnmarshalJSON(data); err == nil {
			encoded, err := fromJSON.MarshalJSON()
			if err != nil {
				t.Fatalf("a map that decoded from JSON did not encode: %v", err)
			}
			if !json.Valid(encoded) {
				t.Fatalf("MarshalJSON produced invalid JSON: %s", encoded)
			}
			var again auth.NamespaceMap
			if err := again.UnmarshalJSON(encoded); err != nil {
				t.Fatalf("the JSON a map encoded to did not decode: %v\n%s", err, encoded)
			}
			if !reflect.DeepEqual(fromJSON, again) {
				t.Fatalf("JSON round trip changed the map:\n%#v\n%#v", fromJSON, again)
			}
		}

		var material auth.Material
		if err := material.UnmarshalJSON(data); err != nil {
			t.Fatalf("Material.UnmarshalJSON, which accepts and discards anything, refused: %v", err)
		}
	})
}

// FuzzVerifyRefusesAMutatedToken fuzzes the bytes a bearer token arrives as,
// against a verifier trusting one test issuer with one key. The property is
// the one that matters at this boundary: the token the issuer minted is
// admitted, and nothing else is — a mutation of it, a truncation, a token
// with the same claims and a different signature, or any other string.
//
// Deep-tier only: the issuer is an httptest server and the verifier fetches
// its key set, which is a real HTTP round trip per key refresh.
func FuzzVerifyRefusesAMutatedToken(f *testing.F) {
	issuer := authtest.NewIssuer()
	f.Cleanup(func() { _ = issuer.Close() })

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "fuzz-idp",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
		}},
	}, auth.WithEgressPolicy(authtest.EgressPolicy()))
	if err != nil {
		f.Fatalf("NewOIDCVerifier: %v", err)
	}

	minted := issuer.MintToken(map[string]any{"sub": "fuzz"}, authtest.WithAudience("flowstate"))
	f.Add(minted)
	f.Add(minted[:len(minted)-1])
	f.Add(strings.Replace(minted, ".", "..", 1))
	f.Add("")
	f.Add("not.a.token")
	if dot := strings.LastIndex(minted, "."); dot > 0 {
		// The signature is what stands between the claims and admission:
		// the same header and claims under a signature of the wrong bytes.
		f.Add(minted[:dot+1] + strings.Repeat("A", len(minted)-dot-1))
	}

	f.Fuzz(func(t *testing.T, raw string) {
		principal, err := verifier.Verify(context.Background(), raw)
		if raw == minted {
			if err != nil {
				t.Fatalf("the token the issuer minted was refused: %v", err)
			}
			return
		}
		if err == nil {
			t.Fatalf("a token the issuer never minted was admitted as %+v:\n%q", principal, raw)
		}
	})
}
