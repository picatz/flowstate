package auth

import (
	"errors"
	"testing"
)

type vocabularySecretRef struct{}

func (vocabularySecretRef) GetScheme() string { return "env" }
func (vocabularySecretRef) GetName() string   { return "DEPLOY_TOKEN" }

// One vocabulary, and the trap on the way to it.
//
// Egress rules and task-shape rules call the caller `identity` and mean the
// authenticated caller's own subject. This package had no such object: it
// exposed `workload`, whose subject is the assertion Flowstate is about to
// *mint*. Those are different principals.
//
// The first attempt at #548 bound `identity` as an alias for `workload`, which
// looked like unification and was worse than the split: a rule copied from a
// task-shape policy would compile, run, and decide something other than what it
// says, with nothing to warn anybody. Two names meaning two things is at least
// visible; one name meaning two things is not.
//
// So `identity` is the caller, with exactly netpolicy's and taskpolicy's four
// fields, and `workload` keeps its own name for the minted assertion and its run
// context. The tests below are the ones that can tell those two apart.

// callerFixture is a caller whose every field differs from what the minted
// assertion would carry, so a test cannot pass by the two happening to agree.
func callerFixture() (WorkloadIdentity, StepRef) {
	return WorkloadIdentity{
			Subject:    "spiffe://acme/ci-runner",
			Issuer:     "https://token.actions.githubusercontent.com",
			Namespace:  "team-a",
			Deployment: "prod",
		}, StepRef{
			Workflow: "deploy",
			Run:      "run-1",
			Step:     "push",
		}
}

func TestIdentityIsTheCallerAndWorkloadIsTheAssertion(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()

	minted, err := identity.SubjectFor(ref)
	if err != nil {
		t.Fatalf("SubjectFor: %v", err)
	}
	if minted == identity.Subject {
		t.Fatal("fixture is useless: the minted subject equals the caller's")
	}

	for _, test := range []struct {
		rule string
		want bool
	}{
		// The caller, which is what every other policy surface means by this.
		{rule: `identity.subject == "spiffe://acme/ci-runner"`, want: true},
		{rule: `identity.issuer == "https://token.actions.githubusercontent.com"`, want: true},
		{rule: `identity.namespace == "team-a"`, want: true},

		// The assertion about to be minted, which is a different principal.
		{rule: `workload.subject == "` + minted + `"`, want: true},
		{rule: `workload.workflow == "deploy" && workload.step == "push"`, want: true},

		// And the assertion that would have caught the first attempt: the two
		// subjects are not the same value, so a rule written about one does not
		// silently decide the other.
		{rule: `identity.subject != workload.subject`, want: true},
	} {
		t.Run(test.rule, func(t *testing.T) {
			t.Parallel()

			rules, err := compileAssumeRules([]string{test.rule}, nil, DefaultAssumeRuleCostLimit)
			if err != nil {
				t.Fatalf("compiling %q: %v", test.rule, err)
			}

			vars := assumeVars("aws-prod", minted, "https://as.example.com", identity, ref)

			matched, err := rules.allow[0].eval(t.Context(), vars)
			if err != nil {
				t.Fatalf("evaluating %q: %v", test.rule, err)
			}
			if matched != test.want {
				t.Errorf("%q = %v, want %v", test.rule, matched, test.want)
			}
		})
	}
}

// TestCallerSubjectRulesDecideOnTheCaller exercises both authorization paths,
// rather than only inspecting the activation assembled for CEL. In particular,
// the deny case guards the security-sensitive direction: a deny rule naming the
// authenticated caller must still win when a separate allow rule names the
// assertion Flowstate is about to mint.
func TestCallerSubjectRulesDecideOnTheCaller(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()
	minted, err := identity.SubjectFor(ref)
	if err != nil {
		t.Fatalf("SubjectFor: %v", err)
	}

	t.Run("assumption allow", func(t *testing.T) {
		rules, err := compileAssumeRules(
			[]string{`identity.subject == "spiffe://acme/ci-runner"`}, nil, DefaultAssumeRuleCostLimit,
		)
		if err != nil {
			t.Fatalf("compileAssumeRules: %v", err)
		}
		if err := rules.evaluate(t.Context(), "aws-prod", minted,
			assumeVars("aws-prod", minted, "https://as.example.com", identity, ref)); err != nil {
			t.Fatalf("caller allow rule denied assumption: %v", err)
		}
	})

	t.Run("secret deny wins over workload allow", func(t *testing.T) {
		policy, err := (SecretAccessPolicy{
			Allow: []string{`workload.subject == "` + minted + `"`},
			Deny:  []string{`identity.subject == "spiffe://acme/ci-runner"`},
		}).Compile()
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}

		err = policy.Authorize(t.Context(), identity, ref, vocabularySecretRef{})
		if !errors.Is(err, ErrSecretDenied) {
			t.Fatalf("caller deny rule returned %v, want ErrSecretDenied", err)
		}
		var denied *SecretDeniedError
		if !errors.As(err, &denied) || denied.Reason != ReasonSecretDenyRule {
			t.Fatalf("caller deny rule returned %v, want reason %s", err, ReasonSecretDenyRule)
		}
	})
}

// TestIdentityCarriesTheSameFieldsAsEveryOtherSurface is the portability claim
// stated as a test rather than as prose in a doc comment.
//
// netpolicy and taskpolicy both expose subject, issuer, namespace and claims. A
// clause about the caller has to compile here too, or "one vocabulary" is a
// slogan. `issuer` in particular is the field the first attempt could not
// provide at all.
func TestIdentityCarriesTheSameFieldsAsEveryOtherSurface(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`identity.subject == "x"`,
		`identity.issuer == "x"`,
		`identity.namespace == "x"`,
		`"repository" in identity.claims && identity.claims["repository"] == "x"`,
	} {
		if _, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err != nil {
			t.Errorf("compiling %q: %v", rule, err)
		}
		if _, err := compileSecretRules([]string{rule + ` && secret.scheme == "env"`}, nil, DefaultAssumeRuleCostLimit); err != nil {
			t.Errorf("compiling %q against the secret surface: %v", rule, err)
		}
	}
}

// TestTheCallerHasNoRunContext is the negative direction, and it is what keeps
// the two objects from drifting back together.
//
// A field that belongs to the minted assertion must not appear on the caller: if
// `identity.workflow` compiled, the next person would reasonably write it, and
// the same expression would then mean nothing on the surfaces this is supposed
// to match.
func TestTheCallerHasNoRunContext(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`identity.workflow == "deploy"`,
		`identity.run == "run-1"`,
		`identity.step == "push"`,
		`identity.deployment == "prod"`,
		`identity.on_behalf_of == "x"`,
		// And an invented name is still refused, because adding a variable must
		// not turn a type-checked environment into one that accepts anything.
		`identity.tenant == "team-a"`,
		`principal.namespace == "team-a"`,
	} {
		if _, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err == nil {
			t.Errorf("compiling %q succeeded; the caller must not carry it", rule)
		}
	}
}

// TestTheWorkloadSpellingStillCompiles pins that no existing policy broke. The
// minted-assertion object keeps every field it had, under the name it had.
func TestTheWorkloadSpellingStillCompiles(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`workload.subject == "x"`,
		`workload.namespace == "team-a"`,
		`workload.deployment == "prod"`,
		`workload.workflow == "deploy"`,
		`workload.run == "run-1"`,
		`workload.step == "push"`,
		`workload.on_behalf_of == "spiffe://acme/ci-runner"`,
		`workload.on_behalf_of_issuer == "https://idp"`,
		`"repository" in workload.claims`,
	} {
		if _, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err != nil {
			t.Errorf("compiling %q: %v", rule, err)
		}
	}
}
