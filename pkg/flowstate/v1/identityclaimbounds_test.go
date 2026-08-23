package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestCarriedClaimBoundsAgreeAcrossSchemaAndMint is the test that has to exist
// because the bound on a carried claim is written down twice: once as a
// protovalidate rule on flowstate.v1.WorkloadIdentity.claims, and once in
// auth.validateCarriedClaims, which the mint enforces.
//
// Two spellings of one limit is the shape CLAUDE.md legislates against, and it
// bit here in the unit rather than the number. protovalidate's `max_len` is
// `this.size()` — Unicode code points — while Go's `len` on a string counts
// bytes. Under `max_len: 1024` a value of 700 two-byte runes passes the schema
// at 700 and is refused at the mint at 1400 bytes: an identity that validates
// and then cannot obtain a credential, with nothing to tell an operator why.
// The schema says `max_bytes` now, so both layers count the same unit.
//
// This lives in this package rather than in auth because it is the only one
// that can see both: auth deliberately imports no Flowstate package, and the
// package holding the generated types imports auth.
func TestCarriedClaimBoundsAgreeAcrossSchemaAndMint(t *testing.T) {
	// A rune that is two bytes in UTF-8 and one code point, which is the whole
	// difference between the two units.
	const twoByteRune = "é"
	require.Len(t, twoByteRune, 2, "the fixture has to actually be two bytes")

	// schemaRefuses reports what the protovalidate rules say about an identity
	// carrying this one claim.
	schemaRefuses := func(t *testing.T, value string) bool {
		t.Helper()
		return v1.Validate(&v1.WorkloadIdentity{
			Subject: "repo:picatz/flowstate:ref:refs/heads/main",
			Issuer:  "https://token.actions.githubusercontent.com",
			Claims:  map[string]string{"repository": value},
		}) != nil
	}

	// mintRefuses reports what the mint's own check says about the same claim,
	// reached through the same conversion the server uses.
	mintRefuses := func(t *testing.T, value string) bool {
		t.Helper()
		identity := auth.IdentityFrom(&v1.WorkloadIdentity{
			Subject: "repo:picatz/flowstate:ref:refs/heads/main",
			Issuer:  "https://token.actions.githubusercontent.com",
			Claims:  map[string]string{"repository": value},
		})
		return identity.Validate() != nil
	}

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{
			// The case the two units disagreed about: 700 code points, 1400
			// bytes. Both layers must refuse it.
			name:  "700 two-byte runes is over the byte bound",
			value: strings.Repeat(twoByteRune, 700),
			want:  true,
		},
		{
			// The bound reached exactly, in the awkward unit: 512 code points
			// is 1024 bytes. Both layers must admit it, which is what stops
			// this from being fixed by simply refusing more.
			name:  "512 two-byte runes is exactly at the byte bound",
			value: strings.Repeat(twoByteRune, auth.MaxCarriedClaimValueBytes/2),
			want:  false,
		},
		{
			name:  "one two-byte rune past the byte bound",
			value: strings.Repeat(twoByteRune, auth.MaxCarriedClaimValueBytes/2) + "a",
			want:  true,
		},
		{
			// The same boundary in the easy unit, so the ASCII case is pinned
			// too and a future edit cannot fix one unit by breaking the other.
			name:  "1024 ASCII bytes is exactly at the bound",
			value: strings.Repeat("a", auth.MaxCarriedClaimValueBytes),
			want:  false,
		},
		{
			name:  "1025 ASCII bytes is over it",
			value: strings.Repeat("a", auth.MaxCarriedClaimValueBytes+1),
			want:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, mint := schemaRefuses(t, test.value), mintRefuses(t, test.value)

			require.Equal(t, test.want, schema, "the schema's verdict")
			require.Equal(t, test.want, mint, "the mint's verdict")

			// Stated as its own assertion, because agreement is the property
			// under test: a future change that moves one layer's bound without
			// the other fails here by name rather than as a puzzling mint
			// failure in production.
			require.Equal(t, schema, mint,
				"the schema and the mint must agree about which identities are valid")
		})
	}
}

// TestCarriedClaimNameBoundsAgreeAcrossSchemaAndMint is the same property for
// the claim's name, which carries the same unit hazard and the same fix.
func TestCarriedClaimNameBoundsAgreeAcrossSchemaAndMint(t *testing.T) {
	for _, test := range []struct {
		name  string
		claim string
		want  bool
	}{
		{
			name:  "a name at the byte bound in two-byte runes",
			claim: strings.Repeat("é", auth.MaxCarriedClaimNameBytes/2),
			want:  false,
		},
		{
			name:  "a name one two-byte rune past it",
			claim: strings.Repeat("é", auth.MaxCarriedClaimNameBytes/2+1),
			want:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := &v1.WorkloadIdentity{
				Subject: "repo:picatz/flowstate:ref:refs/heads/main",
				Issuer:  "https://token.actions.githubusercontent.com",
				Claims:  map[string]string{test.claim: "value"},
			}

			schema := v1.Validate(identity) != nil
			mint := auth.IdentityFrom(identity).Validate() != nil

			require.Equal(t, test.want, schema, "the schema's verdict")
			require.Equal(t, test.want, mint, "the mint's verdict")
			require.Equal(t, schema, mint,
				"the schema and the mint must agree about which identities are valid")
		})
	}
}
