package secretstest

import (
	"context"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// naiveEnvProvider reproduces, on purpose, the exact historical bug CLAUDE.md
// documents for the env provider: it derives a tenant's variable prefix by
// concatenating a base prefix, an env-var-safe rendering of the namespace,
// and an underscore, rather than requiring a disjoint prefix be configured
// for each namespace. It exists only so
// Test_checkNoCollision_CatchesTheHistoricalCollision below can prove the
// collision check actually fails against it — every provider exercised by
// secretstest_test.go is already fixed, so none of those tests can show
// that.
type naiveEnvProvider struct {
	values map[string]string // variable name -> value, standing in for the process environment
}

func (p *naiveEnvProvider) Scheme() string { return "env" }

func (p *naiveEnvProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	prefix := "FLOWSTATE_SECRET_"
	if req.Namespace != "" {
		// Env-var-safe rendering of the namespace, the way a real
		// implementation would have to produce one — uppercased, with
		// separators folded to underscores. This is exactly what makes the
		// derivation collide: the same folding a namespace goes through
		// also happens to be a valid name a sibling reference could spell
		// directly.
		envNamespace := strings.ToUpper(strings.ReplaceAll(req.Namespace, "-", "_"))
		prefix = "FLOWSTATE_SECRET_" + envNamespace + "_"
	}

	variable := prefix + req.Ref.GetName()

	value, ok := p.values[variable]
	if !ok {
		return secrets.Secret{}, &secrets.ResolveError{Ref: req.Ref, Err: secrets.ErrNotFound}
	}

	return secrets.NewSecret(req.Ref, value), nil
}

// Test_checkNoCollision_CatchesTheHistoricalCollision is the meta regression
// the P1 review comment on this package asked for: proof that the collision
// check [VerifyNamespaceIsolation] runs actually has teeth, by running it
// directly against a provider that reproduces the exact historical bug shape
// and confirming it reports a leak.
//
// It calls checkNoCollision rather than VerifyNamespaceIsolation itself,
// and lives in this internal test file (package secretstest, not
// secretstest_test) rather than alongside the public conformance tests, for
// the same reason: a failing t.Run subtest propagates t.Fail() to every
// ancestor *testing.T unconditionally, including one running under a parent
// that only wants to *observe* the failure — there is no way to run
// VerifyNamespaceIsolation against a deliberately broken provider and then
// assert "the right subtest failed" without also failing this test itself.
// Calling the underlying check directly sidesteps that: it returns a plain
// error, so failure is data instead of test-framework control flow.
//
// Without this, nothing in this package demonstrates that the collision
// check catches anything — every provider secretstest_test.go exercises is
// already fixed, so all of those tests would stay green even if the
// collision check silently did nothing.
func Test_checkNoCollision_CatchesTheHistoricalCollision(t *testing.T) {
	provider := &naiveEnvProvider{
		values: map[string]string{
			// team-a's own variable, as the naive scheme derives it: base +
			// namespace + "_" + name.
			"FLOWSTATE_SECRET_TEAM_A_API_KEY": "team-a-value",
		},
	}

	def := NamespaceFixture{
		Namespace: "",
		Ref:       secrets.NewRef("env", "OWN_KEY"),
		Value:     "default-value",
	}
	teamA := NamespaceFixture{
		Namespace: "team-a",
		Ref:       secrets.NewRef("env", "API_KEY"),
		Value:     "team-a-value",
	}

	// Sanity check: team-a's own reference genuinely resolves to its own
	// value under this naive provider, so the collision failure below isn't
	// an artifact of a broken fixture.
	if err := checkOwnFixture(t.Context(), provider, teamA); err != nil {
		t.Fatalf("fixture setup is broken, not the thing under test: %v", err)
	}

	// The default tenant (no namespace, bare base prefix) naming
	// "TEAM_A_API_KEY" collides with team-a's own variable under the naive
	// scheme: "FLOWSTATE_SECRET_" + "TEAM_A_API_KEY" ==
	// "FLOWSTATE_SECRET_TEAM_A_" + "API_KEY". This is the exact shape
	// CLAUDE.md documents as the historical bug.
	collision := secrets.NewRef("env", "TEAM_A_API_KEY")

	err := checkNoCollision(t.Context(), provider, def.Namespace, teamA, collision)
	if err == nil {
		t.Fatal("checkNoCollision did not catch the exact historical collision shape it exists to prevent: " +
			"a naive prefix+NAMESPACE+\"_\"+name provider let the default namespace read team-a's secret " +
			"through reference \"TEAM_A_API_KEY\"")
	}

	// The other half of the second-round P1 finding this test now also
	// covers: the collision above only collides when tried from the empty
	// namespace. Trying the identical reference from some *other* namespace
	// present in a fixture list — say, team-b's — is not the attack at all,
	// and a naive, genuinely vulnerable provider correctly (if
	// coincidentally) fails closed against it. A check that iterated over
	// every other fixture's namespace instead of the collision's own
	// declared requester would have exercised exactly this non-attack and
	// reported the naive provider clean — which is precisely how the second
	// round of review found the first fix's loop unsound. Confirm that
	// shape stays a miss, so nobody "fixes" checkNoCollision back into
	// iterating over every fixture and has this pass by accident.
	teamB := NamespaceFixture{
		Namespace: "team-b",
		Ref:       secrets.NewRef("env", "API_KEY"),
		Value:     "team-b-value",
	}
	if err := checkNoCollision(t.Context(), provider, teamB.Namespace, teamA, collision); err != nil {
		t.Fatalf("checkNoCollision reported a collision from namespace %q, which was never the attack "+
			"(\"TEAM_A_API_KEY\" only collides from the empty namespace under this derivation scheme): %v",
			teamB.Namespace, err)
	}
}

// fallbackToTeamAProvider reproduces, on purpose, a different bug shape than
// naiveEnvProvider: it separates its two *configured* tenants correctly —
// team-a and team-b each get their own storage slot — but silently falls
// back to team-a's storage for any namespace it was never told about,
// instead of failing closed. This is exactly the gap the third-round P1
// review comment on this package described: a fixture list that only ever
// probes as a requester a namespace that owns a fixture can never construct
// an unconfigured requester, so it can never observe this fallback.
type fallbackToTeamAProvider struct {
	values map[string]string // namespace+"/"+name -> value
}

func (p *fallbackToTeamAProvider) Scheme() string { return "env" }

func (p *fallbackToTeamAProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	namespace := req.Namespace
	switch namespace {
	case "team-a", "team-b":
		// configured; use as given
	default:
		// unconfigured requester: silently fall back to team-a's storage
		// rather than failing closed.
		namespace = "team-a"
	}

	value, ok := p.values[namespace+"/"+req.Ref.GetName()]
	if !ok {
		return secrets.Secret{}, &secrets.ResolveError{Ref: req.Ref, Err: secrets.ErrNotFound}
	}

	return secrets.NewSecret(req.Ref, value), nil
}

// Test_checkNoCollision_CatchesTheUnconfiguredNamespaceFallback is the meta
// regression for the fourth-round P1 finding: proof that probing an
// unconfigured, sentinel requester namespace against each owner's own plain
// Ref — the check [VerifyNamespaceIsolation] now runs in addition to the
// owner-fixture loop — actually catches a provider that correctly separates
// its configured tenants but falls back to one of them for anyone else.
//
// Without this probe, nothing in [VerifyNamespaceIsolation]'s fixture-driven
// loops can construct an unconfigured requester at all: every requester the
// existing loops try is itself a fixture's own Namespace, and a namespace
// with no fixture representing it can't be added to the fixture list without
// either failing the ownership subtest (if the provider genuinely rejects
// it) or tautologically becoming "just another configured tenant" (if it
// doesn't) — see [NamespaceFixture] and the unconfigured-namespace probe in
// [VerifyNamespaceIsolation].
func Test_checkNoCollision_CatchesTheUnconfiguredNamespaceFallback(t *testing.T) {
	provider := &fallbackToTeamAProvider{
		values: map[string]string{
			"team-a/api-key": "team-a-value",
			"team-b/api-key": "team-b-value",
		},
	}

	teamA := NamespaceFixture{
		Namespace: "team-a",
		Ref:       secrets.NewRef("env", "api-key"),
		Value:     "team-a-value",
	}
	teamB := NamespaceFixture{
		Namespace: "team-b",
		Ref:       secrets.NewRef("env", "api-key"),
		Value:     "team-b-value",
	}

	// Sanity check: both tenants genuinely reach their own value, so the
	// probe below isn't an artifact of a broken fixture — this provider does
	// separate its configured tenants correctly.
	if err := checkOwnFixture(t.Context(), provider, teamA); err != nil {
		t.Fatalf("fixture setup is broken, not the thing under test: %v", err)
	}
	if err := checkOwnFixture(t.Context(), provider, teamB); err != nil {
		t.Fatalf("fixture setup is broken, not the thing under test: %v", err)
	}

	// The existing cross-namespace check (team-a asking for team-b's ref and
	// vice versa) also passes clean, because the fallback only ever fires
	// for a namespace neither fixture names — confirming this provider would
	// slip through the pre-existing loops undetected.
	if err := checkNoCollision(t.Context(), provider, teamB.Namespace, teamA, teamA.Ref); err != nil {
		t.Fatalf("checkNoCollision reported a collision between two correctly-separated configured tenants, "+
			"which is not the bug this test targets: %v", err)
	}

	// The unconfigured-namespace probe: a sentinel requester namespace that
	// owns no fixture at all must not reach team-a's value through team-a's
	// own, ordinary reference.
	const unconfiguredNamespace = "secretstest-unconfigured-tenant"
	if err := checkNoCollision(t.Context(), provider, unconfiguredNamespace, teamA, teamA.Ref); err == nil {
		t.Fatal("checkNoCollision did not catch a provider that falls back to team-a's storage for an " +
			"unconfigured namespace — the exact gap the unconfigured-requester probe exists to close")
	}
}

// Test_validateFixtures_RejectsDuplicateNamespaces is the meta regression for
// the other half of the fourth-round review: proof that the precondition
// [VerifyNamespaceIsolation] runs actually rejects a fixture list whose
// entries share a Namespace value, rather than merely counting entries.
//
// It calls validateFixtures directly rather than VerifyNamespaceIsolation,
// for the same reason the other tests in this file call checkOwnFixture and
// checkNoCollision directly: VerifyNamespaceIsolation reports this failure
// with t.Fatalf on the *testing.T it's given, and that has no way to be
// observed without also failing whatever test called it.
func Test_validateFixtures_RejectsDuplicateNamespaces(t *testing.T) {
	same := []NamespaceFixture{
		{Namespace: "team-a", Ref: secrets.NewRef("env", "api-key"), Value: "team-a-value"},
		{Namespace: "team-a", Ref: secrets.NewRef("env", "other-key"), Value: "team-a-other-value"},
	}
	if err := validateFixtures(same); err == nil {
		t.Fatal("validateFixtures accepted two fixtures sharing the same Namespace — a provider that ignores " +
			"Request.Namespace entirely would pass VerifyNamespaceIsolation undetected with a fixture list " +
			"shaped like this, since every ownership subtest trivially passes and the negative-direction loop " +
			"skips the only pair there is")
	}

	// Sanity check: the same count, but genuinely distinct namespaces, is
	// accepted — this test is about duplication, not fixture count.
	distinct := []NamespaceFixture{
		{Namespace: "team-a", Ref: secrets.NewRef("env", "api-key"), Value: "team-a-value"},
		{Namespace: "team-b", Ref: secrets.NewRef("env", "api-key"), Value: "team-b-value"},
	}
	if err := validateFixtures(distinct); err != nil {
		t.Fatalf("validateFixtures rejected a fixture list with two genuinely distinct namespaces: %v", err)
	}
}
