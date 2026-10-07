package auth_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// The inbound `act` chain (RFC 8693 section 4.1). Negative direction first, as
// everywhere else: what is refused, then what is admitted, then that admission
// can only narrow.

const (
	delegationAudience = "flowstate"
	botIssuer          = "https://agents.example.com"
	botSubject         = "triage-bot"
	orchestratorIssuer = "https://platform.example.com"
	orchestrator       = "orchestrator"
)

// link is one `act` link as an identity provider writes it.
func link(issuer, subject string, nested map[string]any) map[string]any {
	out := map[string]any{"iss": issuer, "sub": subject}
	if nested != nil {
		out["act"] = nested
	}

	return out
}

// delegationEntry is an entry that admits tokens from issuerURL and accepts the
// given stanza, granting three actions so a narrowing has something to remove.
func delegationEntry(issuerURL string, stanza *auth.Delegation) auth.TrustedIssuer {
	return auth.TrustedIssuer{
		Name:          "agents-idp",
		Issuer:        issuerURL,
		Audiences:     []string{delegationAudience},
		Role:          "operator",
		PrincipalKind: auth.PrincipalKindHuman,
		Namespace:     "team-a",
		Actions:       []string{"run.start", "run.read", "run.delete"},
		Delegation:    stanza,
	}
}

func delegationVerifier(t *testing.T, build func(issuerURL string) auth.TrustedIssuer) (*authtest.Issuer, *auth.OIDCVerifier) {
	t.Helper()

	issuer := newTestIssuer(t)

	return issuer, newVerifier(t, auth.Policy{Issuers: []auth.TrustedIssuer{build(issuer.URL())}})
}

func botRow(actions ...string) auth.DelegationActor {
	if actions == nil {
		actions = []string{}
	}

	return auth.DelegationActor{Issuer: botIssuer, Subject: botSubject, Actions: actions}
}

func orchestratorRow(actions ...string) auth.DelegationActor {
	if actions == nil {
		actions = []string{}
	}

	return auth.DelegationActor{Issuer: orchestratorIssuer, Subject: orchestrator, Actions: actions}
}

func verifyDelegated(t *testing.T, issuer *authtest.Issuer, verifier *auth.OIDCVerifier, claims map[string]any, options ...authtest.TokenOption) (auth.Principal, error) {
	t.Helper()

	options = append([]authtest.TokenOption{authtest.WithSubject("alice"), authtest.WithAudience(delegationAudience)}, options...)

	return verifier.Verify(t.Context(), issuer.MintToken(claims, options...))
}

// TestActorChainRefusesWholeWhatItCannotReadWhole is the parser's negative
// direction: every shape that is not a clean one or two link chain is refused
// with the delegation class of error, never truncated to its readable prefix.
func TestActorChainRefusesWholeWhatItCannotReadWhole(t *testing.T) {
	t.Parallel()

	wide := map[string]any{"iss": botIssuer, "sub": botSubject}
	for i := range 17 {
		wide["extra"+strings.Repeat("x", i)] = "v"
	}

	for _, test := range []struct {
		name string
		act  any
		want string
	}{
		{"depth 3", link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, link("https://third.example", "third", nil))), "nests deeper"},
		{"depth 4", link(botIssuer, botSubject, link(botIssuer, "a", link(botIssuer, "b", link(botIssuer, "c", nil)))), "nests deeper"},
		{"a string", "triage-bot", "not a JSON object"},
		{"a list of objects", []any{link(botIssuer, botSubject, nil)}, "not a JSON object"},
		{"null", nil, "not a JSON object"},
		{"a number", float64(1), "not a JSON object"},
		{"a bool", true, "not a JSON object"},
		{"an empty object", map[string]any{}, `no "iss"`},
		{"no sub", map[string]any{"iss": botIssuer}, `no "sub"`},
		{"no iss", map[string]any{"sub": botSubject}, `no "iss"`},
		{"an empty sub", link(botIssuer, "", nil), "is empty"},
		{"a non-string sub", map[string]any{"iss": botIssuer, "sub": float64(7)}, "not a string"},
		{"a list sub", map[string]any{"iss": botIssuer, "sub": []any{botSubject}}, "not a string"},
		{"an oversized sub", link(botIssuer, strings.Repeat("s", auth.MaxActorFieldBytes+1), nil), "over 1024 bytes"},
		{"an oversized iss", link(strings.Repeat("i", auth.MaxActorFieldBytes+1), botSubject, nil), "over 1024 bytes"},
		{"too many members", wide, "more than"},
		{"a non-object nested link", map[string]any{"iss": botIssuer, "sub": botSubject, "act": "orchestrator"}, "not a JSON object"},
		{"a null nested link", map[string]any{"iss": botIssuer, "sub": botSubject, "act": nil}, "not a JSON object"},
		{"a nested link missing its sub", map[string]any{"iss": botIssuer, "sub": botSubject, "act": map[string]any{"iss": orchestratorIssuer}}, `no "sub"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			actors, err := auth.ActorChain(map[string]any{"act": test.act})

			require.ErrorIs(t, err, auth.ErrDelegatedToken)
			require.Nil(t, actors, "a refused chain is refused whole, never cut to what parsed")
			require.ErrorContains(t, err, test.want)

			delegation, ok := errors.AsType[*auth.DelegationClaimError](err)
			require.True(t, ok)
			require.Equal(t, auth.ClaimActor, delegation.Claim)

			// The reason is this package's own words: nothing the token held.
			for _, value := range []string{botSubject, orchestrator, "https://third.example"} {
				require.NotContains(t, err.Error(), value)
			}
		})
	}
}

// TestActorChainReadsCleanChains is the positive direction that keeps the
// refusals above honest: a reader that refused everything would pass them all.
func TestActorChainReadsCleanChains(t *testing.T) {
	t.Parallel()

	none, err := auth.ActorChain(map[string]any{"sub": "alice"})
	require.NoError(t, err)
	require.Nil(t, none, "no act claim is no chain, not an empty one that reads as delegated")

	one, err := auth.ActorChain(map[string]any{"act": link(botIssuer, botSubject, nil)})
	require.NoError(t, err)
	require.Equal(t, []principal.Actor{{Issuer: botIssuer, Subject: botSubject}}, one)

	two, err := auth.ActorChain(map[string]any{"act": link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, nil))})
	require.NoError(t, err)
	require.Equal(t, []principal.Actor{
		{Issuer: botIssuer, Subject: botSubject},
		{Issuer: orchestratorIssuer, Subject: orchestrator},
	}, two, "the current actor is first")

	// RFC 8693 lets a link carry other members; they mean nothing here.
	extra, err := auth.ActorChain(map[string]any{"act": map[string]any{
		"iss": botIssuer, "sub": botSubject, "exp": float64(1), "aud": "x", "kind": "human", "actions": []any{"run.delete"},
	}})
	require.NoError(t, err)
	require.Equal(t, []principal.Actor{{Issuer: botIssuer, Subject: botSubject}}, extra)

	atLimit, err := auth.ActorChain(map[string]any{"act": link(
		strings.Repeat("i", auth.MaxActorFieldBytes), strings.Repeat("s", auth.MaxActorFieldBytes), nil)})
	require.NoError(t, err, "a value exactly at the bound is within it")
	require.Len(t, atLimit, 1)
}

// TestVerifierRefusesAnActChainTheEntryDoesNotAccept covers every way an entry
// can decline a well-formed chain: no stanza, an actor it does not list, a
// chain deeper than its max_depth, and one deeper than anything can be.
func TestVerifierRefusesAnActChainTheEntryDoesNotAccept(t *testing.T) {
	t.Parallel()

	allowed := &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}}
	twoDeep := &auth.Delegation{MaxDepth: 2, Actors: []auth.DelegationActor{botRow("run.read"), orchestratorRow("run.read")}}

	for _, test := range []struct {
		name   string
		stanza *auth.Delegation
		act    map[string]any
	}{
		{"an entry with no delegation stanza", nil, link(botIssuer, botSubject, nil)},
		{"an actor under another issuer", allowed, link("https://elsewhere.example", botSubject, nil)},
		{"an actor with another subject", allowed, link(botIssuer, "other-bot", nil)},
		{"a subject that differs only by case", allowed, link(botIssuer, "Triage-Bot", nil)},
		{"a subject that is a prefix of a listed one", allowed, link(botIssuer, "triage", nil)},
		{"a listed actor with a trailing slash on its issuer", allowed, link(botIssuer+"/", botSubject, nil)},
		{"a chain deeper than the default max_depth of one", &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read"), orchestratorRow("run.read")}},
			link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, nil))},
		{"a second actor the stanza does not list", allowed, link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, nil))},
		{"a first actor listed only as the second", twoDeep, link(orchestratorIssuer, orchestrator, link("https://elsewhere.example", "x", nil))},
		{"depth three under max_depth two", twoDeep, link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, link(botIssuer, botSubject, nil)))},
		{"a malformed chain", twoDeep, map[string]any{"iss": botIssuer}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer { return delegationEntry(url, test.stanza) })

			principal, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": test.act})

			require.ErrorIs(t, err, auth.ErrDelegatedToken)
			require.True(t, principal.IsZero(), "a refused token vouches for nobody")
			require.Contains(t, auth.PublicReason(err), `"act"`, "the caller is told which claim to stop sending")
			// Neither the allowlist nor the chain is echoed into the refusal.
			for _, value := range []string{botSubject, orchestrator, "other-bot", "elsewhere.example"} {
				require.NotContains(t, err.Error(), value)
				require.NotContains(t, auth.PublicReason(err), value)
			}
		})
	}
}

// TestVerifierStillRefusesMayAct: `may_act` is permission to delegate in a
// token that does not exist yet, and no stanza makes it a chain.
func TestVerifierStillRefusesMayAct(t *testing.T) {
	t.Parallel()

	issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
		return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
	})

	principal, err := verifyDelegated(t, issuer, verifier, nil, authtest.WithMayAct(map[string]any{"sub": botSubject}))

	require.ErrorIs(t, err, auth.ErrDelegatedToken)
	require.True(t, principal.IsZero())
	delegation, ok := errors.AsType[*auth.DelegationClaimError](err)
	require.True(t, ok)
	require.Equal(t, auth.ClaimMayAct, delegation.Claim)
}

// TestVerifierAdmitsAListedChain is the positive case: the chain arrives on the
// principal, current actor first, and nothing about the admitting entry moves.
func TestVerifierAdmitsAListedChain(t *testing.T) {
	t.Parallel()

	issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
		return delegationEntry(url, &auth.Delegation{
			MaxDepth: 2,
			Actors:   []auth.DelegationActor{botRow("run.read", "run.start"), orchestratorRow("run.read", "run.start", "run.delete")},
		})
	})

	t.Run("one actor", func(t *testing.T) {
		principal, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
		require.NoError(t, err)

		require.True(t, principal.Delegated())
		require.Equal(t, []principalActor{{Issuer: botIssuer, Subject: botSubject}}, principal.Actors)
		require.Equal(t, "alice", principal.Subject, "the subject stays the one the token names")
		require.Equal(t, "agents-idp", principal.IssuerName)
		require.Equal(t, auth.PrincipalKindHuman, principal.Kind)
		require.Equal(t, "operator", principal.Role)
		require.Equal(t, "team-a", principal.Namespace)
		require.Equal(t, auth.ActionScopes{"run.start", "run.read"}, principal.Actions)
		require.Equal(t, principal.ID()+", acting via "+botIssuer+"#"+botSubject+" (operator)", principal.String())
		require.Equal(t, "acting via "+botIssuer+"#"+botSubject, principal.ActingVia())
	})

	t.Run("two actors", func(t *testing.T) {
		principal, err := verifyDelegated(t, issuer, verifier, map[string]any{
			"act": link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, nil)),
		})
		require.NoError(t, err)

		require.Equal(t, []principalActor{
			{Issuer: botIssuer, Subject: botSubject},
			{Issuer: orchestratorIssuer, Subject: orchestrator},
		}, principal.Actors)
		require.Equal(t, "acting via "+botIssuer+"#"+botSubject+", via "+orchestratorIssuer+"#"+orchestrator, principal.ActingVia())
	})

	t.Run("no act claim is not delegated", func(t *testing.T) {
		principal, err := verifyDelegated(t, issuer, verifier, nil)
		require.NoError(t, err)

		require.False(t, principal.Delegated())
		require.Nil(t, principal.Actors)
		require.Empty(t, principal.ActingVia())
		require.Equal(t, auth.ActionScopes{"run.start", "run.read", "run.delete"}, principal.Actions,
			"an undelegated caller holds the entry's actions whole")
	})
}

// principalActor spells the leaf package's type where this file also uses a
// local named principal.
type principalActor = principal.Actor

// TestDelegationOnlyNarrows is the property the stanza exists to keep: a
// delegated caller's actions are the intersection of the subject's and every
// actor's, never a union, so an actor can subtract and cannot add.
func TestDelegationOnlyNarrows(t *testing.T) {
	t.Parallel()

	t.Run("an actor can remove actions", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
		})

		delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
		require.NoError(t, err)
		require.Equal(t, auth.ActionScopes{"run.read"}, delegated.Actions)

		alone, err := verifyDelegated(t, issuer, verifier, nil)
		require.NoError(t, err)
		require.Equal(t, auth.ActionScopes{"run.start", "run.read", "run.delete"}, alone.Actions)
		for _, action := range delegated.Actions {
			require.Contains(t, alone.Actions, action, "a delegated caller never holds what the subject alone does not")
		}
	})

	t.Run("an actor that leaves nothing leaves nothing", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow()}})
		})

		delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
		require.NoError(t, err)
		require.NotNil(t, delegated.Actions, "none is an empty allowlist, not the unrestricted nil")
		require.Empty(t, delegated.Actions)
	})

	t.Run("the token's own scopes narrow too and the two intersect", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read", "run.start")}})
		})

		delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{
			"scope": "run.start run.delete", "act": link(botIssuer, botSubject, nil),
		})
		require.NoError(t, err)
		require.Equal(t, auth.ActionScopes{"run.start"}, delegated.Actions,
			"run.read is the actor's but not the token's, run.delete the token's but not the actor's")
	})

	t.Run("every actor in a chain narrows", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{
				MaxDepth: 2,
				Actors:   []auth.DelegationActor{botRow("run.read", "run.start"), orchestratorRow("run.start", "run.delete")},
			})
		})

		delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{
			"act": link(botIssuer, botSubject, link(orchestratorIssuer, orchestrator, nil)),
		})
		require.NoError(t, err)
		require.Equal(t, auth.ActionScopes{"run.start"}, delegated.Actions, "the intersection, not the union of the two actors' lists")
	})

	t.Run("an actor cannot be granted what the entry does not grant", func(t *testing.T) {
		t.Parallel()

		policy := auth.Policy{Issuers: []auth.TrustedIssuer{
			delegationEntry("https://idp.example.com", &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read", "workflow.delete")}}),
		}}

		err := policy.Validate()
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "can only narrow")
		require.ErrorContains(t, err, `"workflow.delete"`)
	})

	t.Run("a chain's own members cannot widen, set a kind, or name an entry", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
		})

		hostile := map[string]any{
			"iss": botIssuer, "sub": botSubject,
			"kind": "human", "principal_kind": "agent", "issuer_entry": "root", "actions": []any{"run.delete", "admin"},
			"scope": "admin", "role": "admin", "namespace": "team-b", "claims": map[string]any{"groups": []any{"admin"}},
		}
		delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": hostile})
		require.NoError(t, err)

		require.Equal(t, auth.PrincipalKindHuman, delegated.Kind, "the kind is the entry's, never the chain's")
		require.Equal(t, "agents-idp", delegated.IssuerName)
		require.Equal(t, "operator", delegated.Role)
		require.Equal(t, "team-a", delegated.Namespace)
		require.Equal(t, auth.ActionScopes{"run.read"}, delegated.Actions)
		require.Empty(t, delegated.Claims, "nothing in the chain is carried as a claim")
		require.NotContains(t, delegated.Claims, "act")
		require.Equal(t, []principalActor{{Issuer: botIssuer, Subject: botSubject}}, delegated.Actors,
			"an actor is its issuer and subject and nothing else")
	})

	t.Run("an actor is matched exactly and a wildcard is just a string", func(t *testing.T) {
		t.Parallel()

		issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
			return delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{
				{Issuer: botIssuer, Subject: "*", Actions: []string{"run.read"}},
			}})
		})

		_, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
		require.ErrorIs(t, err, auth.ErrDelegatedToken, "* does not match triage-bot")

		literal, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, "*", nil)})
		require.NoError(t, err)
		require.Equal(t, "*", literal.Actors[0].Subject)
	})
}

// TestTwoEntriesForOneIssuerChooseByStanza: whether an entry accepts a chain is
// part of whether it admits the token, so a delegated token reaches the entry
// that lists its actor and is not ambiguous with the one that lists none, and
// an undelegated one still matches both and is refused as ambiguous.
func TestTwoEntriesForOneIssuerChooseByStanza(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	plain := delegationEntry(issuer.URL(), nil)
	plain.Name = "plain"
	delegating := delegationEntry(issuer.URL(), &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
	delegating.Name = "delegating"
	verifier := newVerifier(t, auth.Policy{Issuers: []auth.TrustedIssuer{plain, delegating}})

	principal, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
	require.NoError(t, err)
	require.Equal(t, "delegating", principal.IssuerName)

	_, err = verifyDelegated(t, issuer, verifier, nil)
	require.ErrorIs(t, err, auth.ErrAmbiguousIdentity, "an undelegated token still matches both entries")
}

// TestSurfacesAdmitADelegatedTokenAndBindItsSession: both bearer surfaces take
// the chain through, and a delegated session is not the subject's own.
func TestSurfacesAdmitADelegatedTokenAndBindItsSession(t *testing.T) {
	t.Parallel()

	issuer, verifier := delegationVerifier(t, func(url string) auth.TrustedIssuer {
		entry := delegationEntry(url, &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
		entry.Audiences = []string{mcpResource}

		return entry
	})
	mint := func(options ...authtest.TokenOption) string {
		return issuer.MintToken(nil, append([]authtest.TokenOption{authtest.WithSubject("alice"), authtest.WithAudience(mcpResource)}, options...)...)
	}
	delegatedToken := mint(authtest.WithDelegation(link(botIssuer, botSubject, nil)))
	plainToken := mint()

	delegatedInfo, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), delegatedToken, nil)
	require.NoError(t, err)
	plainInfo, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), plainToken, nil)
	require.NoError(t, err)
	require.NotEqual(t, plainInfo.UserID, delegatedInfo.UserID,
		"the subject acting alone could resume a session pinned by the same subject acting through an agent")

	identity, err := auth.NewAuthenticator(verifier).Authenticate(t.Context(), rpcRequest(t, delegatedToken))
	require.NoError(t, err)
	admitted, ok := identity.(auth.Principal)
	require.True(t, ok, "the Authenticator admits the Principal itself, got %T", identity)
	require.True(t, admitted.Delegated())
	require.Equal(t, []principalActor{{Issuer: botIssuer, Subject: botSubject}}, admitted.Actors)
}

// TestIdentityCarriesTheChain: the principal's chain reaches the workload
// identity, its CEL rendering and its bounds.
func TestIdentityCarriesTheChain(t *testing.T) {
	t.Parallel()

	p := auth.Principal{
		Issuer: "https://idp.example.com", Subject: "alice", Namespace: "team-a",
		Actors: []principalActor{{Issuer: botIssuer, Subject: botSubject}},
	}
	identity := auth.IdentityFromPrincipal(p, "", "prod")

	require.Equal(t, p.Actors, identity.Actors)
	p.Actors[0].Subject = "mutated"
	require.Equal(t, botSubject, identity.Actors[0].Subject, "the identity does not alias the principal's chain")

	caller := identity.Caller()
	require.True(t, caller.Delegated)
	require.Equal(t, []principalActor{{Issuer: botIssuer, Subject: botSubject}}, caller.Actors)

	require.NoError(t, identity.Validate())

	identity.Actors = []principalActor{{Issuer: "a", Subject: "1"}, {Issuer: "b", Subject: "2"}, {Issuer: "c", Subject: "3"}}
	require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity, "a third actor is over the depth bound")

	identity.Actors = []principalActor{{Issuer: "a"}}
	require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity, "an actor with no subject is no actor")

	identity.Actors = []principalActor{{Issuer: strings.Repeat("a", auth.MaxActorFieldBytes+1), Subject: "s"}}
	require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity)
}

// TestPolicyParsesAndValidatesTheStanza covers the file form and the load-time
// refusals.
func TestPolicyParsesAndValidatesTheStanza(t *testing.T) {
	t.Parallel()

	const entry = `
issuers:
  - name: agents-idp
    issuer: https://idp.example.com
    audiences: [flowstate]
    actions: [run.start, run.read]
`
	parse := func(delegation string) (auth.Policy, error) {
		return auth.ParsePolicy([]byte(entry + delegation))
	}

	policy, err := parse(`    delegation:
      max_depth: 2
      actors:
        - issuer: https://agents.example.com
          subject: triage-bot
          actions: [run.read]
        - issuer: https://platform.example.com
          subject: orchestrator
          actions: []
`)
	require.NoError(t, err)
	require.Equal(t, &auth.Delegation{MaxDepth: 2, Actors: []auth.DelegationActor{
		{Issuer: botIssuer, Subject: botSubject, Actions: auth.ActionScopes{"run.read"}},
		{Issuer: orchestratorIssuer, Subject: orchestrator, Actions: auth.ActionScopes{}},
	}}, policy.Issuers[0].Delegation)

	// And it survives the JSON round trip the policy is also written as.
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	roundTripped, err := auth.ParsePolicy(encoded)
	require.NoError(t, err)
	require.Equal(t, policy.Issuers[0].Delegation, roundTripped.Issuers[0].Delegation)

	for name, test := range map[string]struct{ stanza, want string }{
		"no actors":        {"    delegation:\n      max_depth: 1\n", "delegation.actors is required"},
		"empty actors":     {"    delegation:\n      actors: []\n", "delegation.actors is required"},
		"depth three":      {"    delegation:\n      max_depth: 3\n      actors:\n        - {issuer: a, subject: b, actions: []}\n", "max_depth 3 is not supported"},
		"negative depth":   {"    delegation:\n      max_depth: -1\n      actors:\n        - {issuer: a, subject: b, actions: []}\n", "max_depth -1"},
		"no actions":       {"    delegation:\n      actors:\n        - {issuer: a, subject: b}\n", "actions is required"},
		"no subject":       {"    delegation:\n      actors:\n        - {issuer: a, actions: []}\n", "needs both issuer and subject"},
		"no issuer":        {"    delegation:\n      actors:\n        - {subject: b, actions: []}\n", "needs both issuer and subject"},
		"a hash in issuer": {"    delegation:\n      actors:\n        - {issuer: 'a#b', subject: b, actions: []}\n", "contains '#'"},
		"a duplicate":      {"    delegation:\n      actors:\n        - {issuer: a, subject: b, actions: []}\n        - {issuer: a, subject: b, actions: []}\n", "listed twice"},
		"an unknown field": {"    delegation:\n      actors:\n        - {issuer: a, subject: b, actions: [], kind: human}\n", "kind"},
		"a pattern field":  {"    delegation:\n      subject_pattern: 'agent-.*'\n      actors:\n        - {issuer: a, subject: b, actions: []}\n", "subject_pattern"},
		"a widening actor": {"    delegation:\n      actors:\n        - {issuer: a, subject: b, actions: [run.delete]}\n", "can only narrow"},
		"a repeated scope": {"    delegation:\n      actors:\n        - {issuer: a, subject: b, actions: [run.read, run.read]}\n", "duplicate action"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := parse(test.stanza)
			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("an mTLS entry has no act claim to accept", func(t *testing.T) {
		t.Parallel()

		mesh := auth.TrustedIssuer{
			Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "mesh-ca", ClientCAFile: "/ca.pem", SubjectFrom: auth.SubjectFromURISAN,
			Actions:    []string{},
			Delegation: &auth.Delegation{Actors: []auth.DelegationActor{{Issuer: "a", Subject: "b", Actions: []string{}}}},
		}
		err := auth.Policy{Issuers: []auth.TrustedIssuer{mesh}}.Validate()
		require.ErrorContains(t, err, "delegation is not meaningful")
	})
}

// TestDelegationCloneSharesNothing: a verifier built from a policy is not
// affected by a later edit to the caller's copy, as for every other slice in an
// entry.
func TestDelegationCloneSharesNothing(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	entry := delegationEntry(issuer.URL(), &auth.Delegation{Actors: []auth.DelegationActor{botRow("run.read")}})
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{entry}}
	verifier := newVerifier(t, policy)

	// Widen the caller's own copy after the verifier was built.
	entry.Delegation.Actors[0].Actions[0] = "run.delete"
	entry.Delegation.Actors[0].Subject = "someone-else"

	delegated, err := verifyDelegated(t, issuer, verifier, map[string]any{"act": link(botIssuer, botSubject, nil)})
	require.NoError(t, err)
	require.Equal(t, auth.ActionScopes{"run.read"}, delegated.Actions)
}

// FuzzActorChain fuzzes the `act` reader with arbitrary JSON as the claim. It
// must never panic, never return both a chain and an error, and never hand back
// a chain outside the bounds the verifier admits one under, whatever the
// nesting, width or string lengths of what it was given.
func FuzzActorChain(f *testing.F) {
	f.Add([]byte(`{"iss":"https://agents.example.com","sub":"triage-bot"}`))
	f.Add([]byte(`{"iss":"a","sub":"b","act":{"iss":"c","sub":"d"}}`))
	f.Add([]byte(`{"iss":"a","sub":"b","act":{"iss":"c","sub":"d","act":{"iss":"e","sub":"f"}}}`))
	f.Add([]byte(`{"sub":"b"}`))
	f.Add([]byte(`{"iss":"a","sub":"b","act":null}`))
	f.Add([]byte(`{"iss":"a","sub":["b"]}`))
	f.Add([]byte(`"a string"`))
	f.Add([]byte(`[{"iss":"a","sub":"b"}]`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"iss":"a","sub":"b","a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"j":10,"k":11,"l":12,"m":13,"n":14,"o":15,"p":16}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var act any
		if err := json.Unmarshal(data, &act); err != nil {
			return
		}

		actors, err := auth.ActorChain(map[string]any{"act": act})
		if err != nil {
			if actors != nil {
				t.Fatalf("ActorChain returned both a chain and an error: %v", err)
			}
			if _, ok := errors.AsType[*auth.DelegationClaimError](err); !ok {
				t.Fatalf("a refusal is a DelegationClaimError, got %T: %v", err, err)
			}

			return
		}

		if len(actors) == 0 || len(actors) > auth.MaxActorDepth {
			t.Fatalf("a chain of %d actors was returned; want 1 to %d", len(actors), auth.MaxActorDepth)
		}
		for _, actor := range actors {
			if actor.Issuer == "" || actor.Subject == "" || len(actor.Issuer) > auth.MaxActorFieldBytes || len(actor.Subject) > auth.MaxActorFieldBytes {
				t.Fatalf("an actor outside the bounds was returned: %d, %d bytes", len(actor.Issuer), len(actor.Subject))
			}
		}
	})
}
