package auth_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestMapClaims is the one mapping from a verified token's raw claims to the
// claims a principal carries: what an entry names is carried with its type, and
// anything else, absent, or ill-typed is left out rather than coerced.
func TestMapClaims(t *testing.T) {
	t.Parallel()

	raw := map[string]any{
		"sub":                          "alice",
		"team":                         "platform",
		"email":                        "alice@example.com",
		"admin":                        true,
		"level":                        float64(3),
		"scopes":                       []any{"a", "b"},
		"mixed":                        []any{"a", float64(1)},
		"https://example.com/tenant":   "acme",
		"realm_access":                 map[string]any{"roles": []any{"dev", "sre", "dev"}},
		"nested":                       map[string]any{"deep": map[string]any{"er": "value"}},
		"shadow.name":                  "literal",
		"shadow":                       map[string]any{"name": "path"},
		"too.many.path.segments.hence": "x",
	}

	tests := []struct {
		name  string
		entry auth.TrustedIssuer
		want  map[string]any
	}{
		{
			name: "nothing named, nothing carried",
			want: map[string]any{},
		},
		{
			name: "each type is carried as the token had it",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "team", Type: auth.ClaimTypeString},
				{Claim: "admin", Type: auth.ClaimTypeBool},
				{Claim: "level", Type: auth.ClaimTypeNumber},
				{Claim: "scopes", Type: auth.ClaimTypeStringList},
			}},
			want: map[string]any{"team": "platform", "admin": true, "level": float64(3), "scopes": []any{"a", "b"}},
		},
		{
			name: "a claim the entry does not name stays behind",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "team", Type: auth.ClaimTypeString},
			}},
			want: map[string]any{"team": "platform"},
		},
		{
			name: "a claim of another type is left out, never coerced",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "team", Type: auth.ClaimTypeBool},
				{Claim: "level", Type: auth.ClaimTypeString},
				{Claim: "scopes", Type: auth.ClaimTypeString},
				{Claim: "team", As: "team_list", Type: auth.ClaimTypeStringList},
				{Claim: "mixed", Type: auth.ClaimTypeStringList},
			}},
			want: map[string]any{},
		},
		{
			name: "an absent claim is left out",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "missing", Type: auth.ClaimTypeString},
				{Claim: "nested.nope", Type: auth.ClaimTypeString},
				{Claim: "team.deeper", Type: auth.ClaimTypeString},
			}},
			want: map[string]any{},
		},
		{
			name: "a dotted path reads nested objects and a rename is what rules see",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "nested.deep.er", As: "far", Type: auth.ClaimTypeString},
				{Claim: "realm_access.roles", Type: auth.ClaimTypeStringList},
			}},
			want: map[string]any{"far": "value", "realm_access.roles": []any{"dev", "sre", "dev"}},
		},
		{
			name: "a claim whose own name has dots is read by that name first",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "https://example.com/tenant", As: "tenant", Type: auth.ClaimTypeString},
				{Claim: "shadow.name", Type: auth.ClaimTypeString},
			}},
			want: map[string]any{"tenant": "acme", "shadow.name": "literal"},
		},
		{
			name: "a path deeper than the bound is not walked",
			entry: auth.TrustedIssuer{CarryClaims: []auth.CarryClaim{
				{Claim: "nested.deep.er.x.y", Type: auth.ClaimTypeString},
			}},
			want: map[string]any{},
		},
		{
			name:  "groups are carried deduplicated under the fixed name",
			entry: auth.TrustedIssuer{GroupsClaim: "realm_access.roles"},
			want:  map[string]any{"groups": []any{"dev", "sre"}},
		},
		{
			name: "group_map renames and is the allowlist of carried groups",
			entry: auth.TrustedIssuer{
				GroupsClaim: "realm_access.roles",
				GroupMap:    map[string]string{"sre": "oncall", "unseen": "other"},
			},
			want: map[string]any{"groups": []any{"oncall"}},
		},
		{
			name:  "a groups claim that is not a list of strings carries nothing",
			entry: auth.TrustedIssuer{GroupsClaim: "mixed"},
			want:  map[string]any{},
		},
		{
			name:  "an absent groups claim carries nothing",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			want:  map[string]any{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.entry.Name = "entry"
			got, err := auth.MapClaims(test.entry, raw)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

// TestMapClaimsRefusesAnIncompleteGroupList is the fail-closed half of groups:
// a token that says its groups are not all here, or whose list is over bound,
// refuses the caller by naming the entry. It never carries a prefix, because a
// rule over membership would decide on a membership the caller does not have.
func TestMapClaimsRefusesAnIncompleteGroupList(t *testing.T) {
	t.Parallel()

	many := func(n int) []any {
		list := make([]any, n)
		for i := range list {
			list[i] = fmt.Sprintf("group-%03d", i)
		}

		return list
	}

	longGroups := func(n int) []any {
		list := make([]any, n)
		for i := range list {
			list[i] = fmt.Sprintf("%0*d", auth.MaxGroupBytes, i)
		}

		return list
	}

	tests := []struct {
		name  string
		entry auth.TrustedIssuer
		raw   map[string]any
		want  string
	}{
		{
			name:  "an Entra distributed claim for groups",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw: map[string]any{
				"_claim_names":   map[string]any{"groups": "src1"},
				"_claim_sources": map[string]any{"src1": map[string]any{"endpoint": "https://graph.example/users/x/getMemberObjects"}},
			},
			want: `"_claim_names"`,
		},
		{
			name:  "an Entra distributed claim even when a groups claim is also present",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw:   map[string]any{"groups": many(3), "_claim_names": map[string]any{"groups": "src1"}},
			want:  `"_claim_names"`,
		},
		{
			name:  "hasgroups",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw:   map[string]any{"hasgroups": true},
			want:  `"hasgroups"`,
		},
		{
			name:  "a truncation marker",
			entry: auth.TrustedIssuer{GroupsClaim: "realm_access.roles"},
			raw:   map[string]any{"realm_access": map[string]any{"roles": many(2)}, "groups_truncated": true},
			want:  `"groups_truncated"`,
		},
		{
			name:  "more groups than an identity carries",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw:   map[string]any{"groups": many(auth.MaxGroups + 1)},
			want:  "more than the 64 groups",
		},
		{
			name:  "a group name over the bound",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw:   map[string]any{"groups": []any{strings.Repeat("g", auth.MaxGroupBytes+1)}},
			want:  "257 byte group",
		},
		{
			name:  "a list within the count and over the structured byte bound",
			entry: auth.TrustedIssuer{GroupsClaim: "groups"},
			raw:   map[string]any{"groups": longGroups(auth.MaxGroups)},
			want:  "bytes of list or object",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.entry.Name = "entra-prod"
			got, err := auth.MapClaims(test.entry, test.raw)
			require.ErrorIs(t, err, auth.ErrGroupsOverage)
			assert.Contains(t, err.Error(), `"entra-prod"`, "the refusal names the entry to fix")
			assert.Contains(t, err.Error(), test.want)
			assert.Nil(t, got, "a refused caller carries no claims at all")
		})
	}

	t.Run("a distributed claim for something else is not an overage", func(t *testing.T) {
		t.Parallel()

		_, err := auth.MapClaims(auth.TrustedIssuer{Name: "e", GroupsClaim: "groups"}, map[string]any{
			"groups": []any{"a"}, "_claim_names": map[string]any{"photo": "src1"}, "hasgroups": false,
		})
		require.NoError(t, err)
	})

	t.Run("a group_map keeps a long raw list under the bound", func(t *testing.T) {
		t.Parallel()

		got, err := auth.MapClaims(auth.TrustedIssuer{
			Name: "e", GroupsClaim: "groups", GroupMap: map[string]string{"group-007": "sre"},
		}, map[string]any{"groups": many(200)})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"groups": []any{"sre"}}, got)
	})
}

// TestMapClaimsOwnsWhatItReturns: a later change to the token's claim set must
// not change what a principal says.
func TestMapClaimsOwnsWhatItReturns(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"scopes": []any{"a"}, "groups": []any{"g"}}
	got, err := auth.MapClaims(auth.TrustedIssuer{
		Name:        "e",
		CarryClaims: []auth.CarryClaim{{Claim: "scopes", Type: auth.ClaimTypeStringList}},
		GroupsClaim: "groups",
	}, raw)
	require.NoError(t, err)

	got["scopes"].([]any)[0] = "changed"
	got["groups"].([]any)[0] = "changed"
	assert.Equal(t, []any{"a"}, raw["scopes"])
	assert.Equal(t, []any{"g"}, raw["groups"])
}

// TestClaimCarriagePolicyValidation holds the policy file to what MapClaims can
// carry, at load, so a typo is a start-up failure rather than a token that
// quietly carries nothing.
func TestClaimCarriagePolicyValidation(t *testing.T) {
	t.Parallel()

	entry := func(change func(*auth.TrustedIssuer)) auth.Policy {
		issuer := auth.TrustedIssuer{
			Name: "idp", Issuer: "https://idp.example.com", Audiences: []string{"flowstate"}, Actions: []string{},
		}
		change(&issuer)

		return auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}
	}

	tests := []struct {
		name    string
		change  func(*auth.TrustedIssuer)
		wantErr string
	}{
		{name: "a typed claim, a rename and groups with a map", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "team", Type: auth.ClaimTypeString}, {Claim: "realm_access.roles", As: "roles", Type: auth.ClaimTypeStringList}}
			i.GroupsClaim = "groups"
			i.GroupMap = map[string]string{"idp-value": "flowstate-group"}
		}},
		{name: "a URL-style claim name", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "https://example.com/tenant", As: "tenant", Type: auth.ClaimTypeString}}
		}},
		{name: "a missing type", wantErr: `type ""`, change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "team"}}
		}},
		{name: "an unknown type", wantErr: `type "int"`, change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "team", Type: "int"}}
		}},
		{name: "an empty claim", wantErr: "carry_claims[0]: claim is required", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Type: auth.ClaimTypeString}}
		}},
		{name: "an empty path segment", wantErr: "empty path segment", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "a..b", Type: auth.ClaimTypeString}}
		}},
		{name: "a path over the depth bound", wantErr: "path segments", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "a.b.c.d.e", Type: auth.ClaimTypeString}}
		}},
		{name: "the same name carried twice", wantErr: "carried twice", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "team", Type: auth.ClaimTypeString}, {Claim: "org.team", As: "team", Type: auth.ClaimTypeString}}
		}},
		{name: "carry_claims and groups_claim both carrying groups", wantErr: `carries "groups", which groups_claim already carries`, change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "groups", Type: auth.ClaimTypeStringList}}
			i.GroupsClaim = "realm_access.roles"
		}},
		{name: "a rename onto groups conflicts the same way", wantErr: `which groups_claim already carries`, change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "roles", As: "groups", Type: auth.ClaimTypeStringList}}
			i.GroupsClaim = "groups"
		}},
		{name: "groups carried by carry_claims alone is fine", change: func(i *auth.TrustedIssuer) {
			i.CarryClaims = []auth.CarryClaim{{Claim: "groups", Type: auth.ClaimTypeStringList}}
		}},
		{name: "group_map without groups_claim", wantErr: "group_map requires groups_claim", change: func(i *auth.TrustedIssuer) {
			i.GroupMap = map[string]string{"a": "b"}
		}},
		{name: "an empty group_map", wantErr: "group_map is present but empty", change: func(i *auth.TrustedIssuer) {
			i.GroupsClaim = "groups"
			i.GroupMap = map[string]string{}
		}},
		{name: "a group_map that maps to nothing", wantErr: "maps to an empty group", change: func(i *auth.TrustedIssuer) {
			i.GroupsClaim = "groups"
			i.GroupMap = map[string]string{"a": ""}
		}},
		{name: "an over-long group_map target", wantErr: "over the 256 allowed", change: func(i *auth.TrustedIssuer) {
			i.GroupsClaim = "groups"
			i.GroupMap = map[string]string{"a": strings.Repeat("g", 257)}
		}},
		{name: "more carried claims than an identity holds", wantErr: "over the 32", change: func(i *auth.TrustedIssuer) {
			for n := range 33 {
				i.CarryClaims = append(i.CarryClaims, auth.CarryClaim{Claim: fmt.Sprintf("c%d", n), Type: auth.ClaimTypeString})
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := entry(test.change).Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

// TestClaimCarriageParsesFromYAML pins the file spelling the docs teach.
func TestClaimCarriageParsesFromYAML(t *testing.T) {
	t.Parallel()

	policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: keycloak
    issuer: https://idp.example.com/realms/acme
    audiences: [flowstate]
    actions: []
    carry_claims:
      - {claim: team, type: string}
      - {claim: realm_access.roles, as: roles, type: string_list}
    groups_claim: groups
    group_map:
      "11111111-2222-3333-4444-555555555555": platform
`))
	require.NoError(t, err)
	require.Len(t, policy.Issuers, 1)
	assert.Equal(t, []string{"groups", "roles", "team"}, policy.Issuers[0].ClaimNames())
	assert.Equal(t, "platform", policy.Issuers[0].GroupMap["11111111-2222-3333-4444-555555555555"])

	_, err = auth.ParsePolicy([]byte(`
issuers:
  - name: keycloak
    issuer: https://idp.example.com/realms/acme
    audiences: [flowstate]
    actions: []
    carry_claims: [{claim: team, type: strng}]
`))
	require.Error(t, err)
}

// TestVerifierCarriesOnlyWhatTheEntryNames is the end-to-end property: a token
// holding far more than the entry carries reaches policy with only what was
// named, groups included, and a token with an overage marker is refused with
// the entry named server-side and nothing configured in the public reason.
func TestVerifierCarriesOnlyWhatTheEntryNames(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now))

	verifier := newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "keycloak", Issuer: issuer.URL(), Audiences: []string{"flowstate"}, Actions: []string{},
			CarryClaims: []auth.CarryClaim{{Claim: "team", Type: auth.ClaimTypeString}},
			GroupsClaim: "realm_access.roles",
			GroupMap:    map[string]string{"platform-admins": "admins"},
		}}},
		auth.WithClock(clock.Now),
	)

	mint := func(extra map[string]any) string {
		claims := issuer.Claims(authtest.WithSubject("alice"), authtest.WithAudience("flowstate"))
		for name, value := range extra {
			claims[name] = value
		}

		return issuer.MintToken(claims)
	}

	t.Run("carried and mapped claims reach the principal, nothing else", func(t *testing.T) {
		principal, err := verifier.Verify(t.Context(), mint(map[string]any{
			"team":         "platform",
			"email":        "alice@example.com",
			"realm_access": map[string]any{"roles": []string{"platform-admins", "uma_authorization"}},
		}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"team": "platform", "groups": []any{"admins"}}, principal.Claims)
	})

	t.Run("a token with no carried claim carries none", func(t *testing.T) {
		principal, err := verifier.Verify(t.Context(), mint(map[string]any{"email": "alice@example.com"}))
		require.NoError(t, err)
		assert.Empty(t, principal.Claims)
	})

	t.Run("an overage refuses the caller and names the entry in the error, not the public reason", func(t *testing.T) {
		_, err := verifier.Verify(t.Context(), mint(map[string]any{
			"_claim_names": map[string]any{"realm_access": "src1"},
		}))
		require.ErrorIs(t, err, auth.ErrGroupsOverage)
		assert.Contains(t, err.Error(), `"keycloak"`)
		assert.NotContains(t, auth.PublicReason(err), "keycloak")
		assert.Contains(t, auth.PublicReason(err), "group")
	})
}

// TestWithClaimMapperReplacesTheMapperAndIsBounded: an embedder's mapper is the
// one the verifier calls, and what it returns is still held to the carried-claim
// bounds so it cannot carry more than policy surfaces will read.
func TestWithClaimMapperReplacesTheMapperAndIsBounded(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now))
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: issuer.URL(), Audiences: []string{"flowstate"}, Actions: []string{},
	}}}
	token := issuer.MintToken(issuer.Claims(authtest.WithSubject("alice"), authtest.WithAudience("flowstate")))

	t.Run("replaced", func(t *testing.T) {
		verifier := newVerifier(t, policy, auth.WithClock(clock.Now),
			auth.WithClaimMapper(func(entry auth.TrustedIssuer, raw map[string]any) (map[string]any, error) {
				return map[string]any{"entry": entry.Name, "sub": raw["sub"]}, nil
			}))
		principal, err := verifier.Verify(t.Context(), token)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"entry": "idp", "sub": "alice"}, principal.Claims)
	})

	t.Run("over the bounds is refused", func(t *testing.T) {
		verifier := newVerifier(t, policy, auth.WithClock(clock.Now),
			auth.WithClaimMapper(func(auth.TrustedIssuer, map[string]any) (map[string]any, error) {
				return map[string]any{"big": strings.Repeat("x", auth.MaxCarriedClaimValueBytes+1)}, nil
			}))
		_, err := verifier.Verify(t.Context(), token)
		require.ErrorIs(t, err, auth.ErrClaimMismatch)
	})

	t.Run("an error refuses the caller under one sentinel", func(t *testing.T) {
		verifier := newVerifier(t, policy, auth.WithClock(clock.Now),
			auth.WithClaimMapper(func(auth.TrustedIssuer, map[string]any) (map[string]any, error) {
				return nil, fmt.Errorf("directory down")
			}))
		_, err := verifier.Verify(t.Context(), token)
		require.ErrorIs(t, err, auth.ErrClaimMismatch)
	})
}

// FuzzMapClaims fuzzes the mapper with arbitrary JSON claims and arbitrary
// claim paths, group maps and renames. Whatever a token holds, the mapper must
// not panic, must carry nothing the entry did not name, must stay inside the
// carried-claim bounds, must return either claims or an error, and must not
// hand back anything that aliases the token's claims.
func FuzzMapClaims(f *testing.F) {
	f.Add([]byte(`{"team":"a","realm_access":{"roles":["x","y"]},"groups":["g"]}`), "realm_access.roles", "team", "as", "x", "mapped")
	f.Add([]byte(`{"_claim_names":{"groups":"src1"},"groups":["g"]}`), "groups", "groups", "", "g", "h")
	f.Add([]byte(`{"hasgroups":true}`), "groups", "a.b.c.d.e", "n", "", "")
	f.Add([]byte(`{"a":{"b":{"c":{"d":[1,2,{"e":null}]}}}}`), "a.b.c.d", "a..b", "", "1", "2")
	f.Add([]byte(`[]`), "", "", "", "", "")

	f.Fuzz(func(t *testing.T, data []byte, groupsPath, claimPath, as, mapFrom, mapTo string) {
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			return
		}
		original, err := json.Marshal(raw)
		require.NoError(t, err)

		entry := auth.TrustedIssuer{
			Name:        "fuzz",
			GroupsClaim: groupsPath,
			CarryClaims: []auth.CarryClaim{
				{Claim: claimPath, As: as, Type: auth.ClaimTypeString},
				{Claim: claimPath, As: as + "_list", Type: auth.ClaimTypeStringList},
				{Claim: claimPath, As: as + "_bool", Type: auth.ClaimTypeBool},
			},
		}
		if mapFrom != "" {
			entry.GroupMap = map[string]string{mapFrom: mapTo}
		}

		claims, err := auth.MapClaims(entry, raw)
		if err != nil {
			if claims != nil {
				t.Fatalf("MapClaims returned both claims and an error: %v", err)
			}
			if !strings.Contains(err.Error(), `"fuzz"`) {
				t.Fatalf("a refusal must name the entry: %v", err)
			}

			return
		}

		named := map[string]bool{auth.GroupsClaim: true}
		for _, carried := range entry.CarryClaims {
			named[cmp.Or(carried.As, carried.Claim)] = true
		}
		for name := range claims {
			if !named[name] {
				t.Fatalf("carried a claim the entry did not name: %q", name)
			}
		}
		if groups, ok := claims[auth.GroupsClaim].([]any); ok && len(groups) > auth.MaxGroups {
			t.Fatalf("carried %d groups, over the bound of %d", len(groups), auth.MaxGroups)
		}
		identity := auth.WorkloadIdentity{Subject: "s", Issuer: "i", Claims: claims}
		if err := identity.Validate(); err != nil {
			t.Fatalf("a mapped claim set is over the carried-claim bounds: %v", err)
		}

		// Mutating what was returned must not reach the token's own claims.
		for _, value := range claims {
			if list, ok := value.([]any); ok && len(list) > 0 {
				list[0] = "changed"
			}
		}
		after, err := json.Marshal(raw)
		require.NoError(t, err)
		if string(after) != string(original) {
			t.Fatalf("the mapper's result aliases the token's claims:\n%s\n%s", original, after)
		}
	})
}
