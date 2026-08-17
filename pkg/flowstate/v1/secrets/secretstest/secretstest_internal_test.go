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

	err := checkNoCollision(t.Context(), provider, def, teamA, collision)
	if err == nil {
		t.Fatal("checkNoCollision did not catch the exact historical collision shape it exists to prevent: " +
			"a naive prefix+NAMESPACE+\"_\"+name provider let the default namespace read team-a's secret " +
			"through reference \"TEAM_A_API_KEY\"")
	}
}
