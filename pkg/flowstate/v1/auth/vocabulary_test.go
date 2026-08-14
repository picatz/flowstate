package auth

import (
	"testing"
)

// One vocabulary, checked at the only place it can be: the compiled rule.
//
// Egress rules and task-shape rules call the caller `identity`; this package
// called the same thing `workload`. That is one meaning written down twice, and
// it was user-facing — an expression correct in one policy file was a compile
// error in the next. #548's first slice makes `identity` the spelling to write
// here too, with `workload` bound to the same value so a deployment's existing
// policy keeps compiling across the upgrade.
//
// The assertion that matters is not that each name compiles on its own. It is
// that they are the *same object*: a rule comparing the two fields must be true
// for every field, or the alias is a second vocabulary wearing the first one's
// name.

func TestBothSpellingsNameTheSameCaller(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		// The spelling to write.
		`identity.namespace == "team-a"`,
		// The retired spelling, still compiling.
		`workload.namespace == "team-a"`,
		// And the proof they are one value rather than two that happen to agree
		// on the field a test picked.
		`identity.subject == workload.subject`,
		`identity.namespace == workload.namespace`,
		`identity.deployment == workload.deployment`,
		`identity.workflow == workload.workflow`,
		`identity.run == workload.run`,
		`identity.step == workload.step`,
		`identity.on_behalf_of == workload.on_behalf_of`,
		`identity.claims == workload.claims`,
	} {
		t.Run(rule, func(t *testing.T) {
			t.Parallel()

			rules, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
			if err != nil {
				t.Fatalf("compiling %q: %v", rule, err)
			}

			vars := assumeVars("aws-prod", "spiffe://acme/run", "https://as.example.com",
				WorkloadIdentity{Namespace: "team-a", Subject: "caller", Issuer: "https://idp"},
				StepRef{Workflow: "deploy", Run: "run-1", Step: "push"})

			matched, err := rules.allow[0].eval(t.Context(), vars)
			if err != nil {
				t.Fatalf("evaluating %q: %v", rule, err)
			}
			if !matched {
				t.Errorf("%q did not match; the two spellings are not the same value", rule)
			}
		})
	}
}

// TestASecretRuleTakesEitherSpelling covers the other surface that shares this
// environment, because a vocabulary unified in one of the two is not unified.
func TestASecretRuleTakesEitherSpelling(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`identity.namespace == "team-a" && secret.scheme == "env"`,
		`workload.namespace == "team-a" && secret.scheme == "env"`,
	} {
		if _, err := compileSecretRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err != nil {
			t.Errorf("compiling %q: %v", rule, err)
		}
	}
}

// TestAnInventedSpellingIsStillRefused is the negative direction. Adding a name
// must not turn the environment into one that accepts anything: a misspelled
// attribute has to stay a startup error, which is what makes a policy file
// trustworthy at all.
func TestAnInventedSpellingIsStillRefused(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`principal.namespace == "team-a"`,
		`identity.tenant == "team-a"`,
		`workload.tenant == "team-a"`,
	} {
		if _, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err == nil {
			t.Errorf("compiling %q succeeded; an invented name must fail at startup", rule)
		}
	}
}
