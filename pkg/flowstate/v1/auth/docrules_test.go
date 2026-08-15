package auth

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every CEL rule this package documents has to compile, on the surface it is
// documented for.
//
// federation.go carried an example using bare attribute names —
// `on_behalf_of.startsWith(...)` — and it does not compile. It has not compiled
// for as long as the attributes have been grouped under objects, which is since
// long before #565: git says that file was last touched in #48. doc.go and
// secretpolicy.go were both updated when the grouping landed and this one was
// not, which is the shape of every doc-rot bug — a change applied everywhere
// except one place, with nothing that could see the difference.
//
// Worth a test rather than a one-line fix, because copying a rule out of a doc
// comment is the first thing an operator does, and a documented rule that fails
// at startup teaches them the policy surface is unreliable at exactly the moment
// they have no other evidence about it.
//
// # Why the surface is annotated rather than inferred
//
// The first version of this test decided which environment to compile against by
// looking at the rule text: a rule naming `secret.` went to the secret surface
// and everything else to the assumption surface. That is the same test failing
// the way the doc did, and Codex caught it on the review.
//
// The two environments are not nested. The secret environment has `identity`,
// `workload` and `secret`; the assumption environment has `identity`, `workload`,
// `target` and `audience`. So a *secret* policy example written as
// `target == "aws-prod" && secret.scheme == "env"` — plausible, and wrong —
// carries `secret.` and would be compiled correctly, while one written as
// `target == "aws-prod"` carries no marker, gets sent to the assumption
// environment, compiles, and passes. The operator who copies it gets the startup
// error the test was written to prevent.
//
// Text cannot answer this, because the failing case is a rule whose text is legal
// on the surface it does not belong to. So each documented block says which
// surface it is, and a rule with no marker above it is a failure rather than a
// guess: an unannotated example is exactly the one nobody has checked.

// docRule matches the rules this package writes in its doc comments, which are
// YAML list entries in single quotes:
//
//	//	  - 'target == "aws-prod" && workload.namespace == "acme"'
//
// Deliberately narrow. It matches the shape the examples are written in rather
// than anything quoted anywhere, so prose mentioning a fragment is not dragged
// in and then expected to be a whole boolean rule.
var docRule = regexp.MustCompile(`^\s*//\s*-\s*'(.+)'\s*$`)

// docSurface matches the marker introducing a documented policy block. It is a
// YAML comment inside the example, so it reads as part of the snippet an
// operator is copying rather than as scaffolding for this test:
//
//	//	# secret access policy
//	//	secrets:
//	//	  allow:
//	//	    - 'secret.scheme == "env"'
var docSurface = regexp.MustCompile(`^\s*//\s*#\s*(assumption|secret access) policy\s*$`)

// compileFor returns the compiler for a marked surface, which is how a
// documented rule is checked against the environment it will actually meet.
func compileFor(surface string) func(rule string) error {
	switch surface {
	case "assumption":
		return func(rule string) error {
			_, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
			return err
		}
	case "secret access":
		return func(rule string) error {
			_, err := compileSecretRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
			return err
		}
	default:
		return nil
	}
}

func TestEveryDocumentedRuleCompiles(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}

	// Counted per surface rather than in total, so a marker that stops matching
	// cannot be hidden by the other surface's examples still being read.
	found := map[string]int{}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		contents, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}

		// The marker governs the rules below it and is reset at each file, so a
		// block at the end of one file cannot claim the rules at the top of the
		// next.
		surface := ""

		for number, line := range strings.Split(string(contents), "\n") {
			if match := docSurface.FindStringSubmatch(line); match != nil {
				surface = match[1]
				continue
			}

			match := docRule.FindStringSubmatch(line)
			if match == nil {
				continue
			}

			rule := match[1]

			compile := compileFor(surface)
			if compile == nil {
				t.Errorf("%s:%d documents a rule under no policy surface:\n\t%s\n\t"+
					"introduce the block with `# assumption policy` or `# secret access policy`; "+
					"an example nobody has placed on a surface is one nobody has checked",
					name, number+1, rule)
				continue
			}

			found[surface]++

			if err := compile(rule); err != nil {
				t.Errorf("%s:%d documents a rule that does not compile on the %s surface:\n\t%s\n\t%v",
					name, number+1, surface, rule, err)
			}
		}
	}

	// The anti-vacuity guard, which this file needs more than most: a regex that
	// stops matching turns this into a test that passes by examining nothing, and
	// the failure it exists to catch is precisely a doc nobody looked at.
	for _, want := range []struct {
		surface string
		least   int
	}{
		{surface: "assumption", least: 4},
		{surface: "secret access", least: 3},
	} {
		if found[want.surface] < want.least {
			t.Errorf("found only %d documented %s rules, want at least %d; the walk, the "+
				"comment style or a surface marker changed, and this test is no longer "+
				"reading what it claims to", found[want.surface], want.surface, want.least)
		}
	}
}

// TestTheTwoPolicySurfacesAreNotInterchangeable is what makes the annotation
// worth its noise, and it is the review finding stated as an assertion.
//
// If every rule legal on one surface were legal on the other, guessing would be
// harmless and the marker would be ceremony. They are not nested in either
// direction: the assumption surface has `target` and `audience` and no `secret`,
// the secret surface has `secret` and neither of the other two. So a rule sent to
// the wrong environment either fails here or — the dangerous half — compiles
// while meaning something the documented surface cannot express.
func TestTheTwoPolicySurfacesAreNotInterchangeable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		rule    string
		surface string
	}{
		// The exact shape the text heuristic approved: a secret-policy example
		// naming an assumption-only attribute. It compiles as an assumption rule,
		// which is why guessing passed it, and fails at an operator's startup.
		{rule: `target == "aws-prod"`, surface: "secret access"},
		{rule: `audience == "https://as.example.com"`, surface: "secret access"},

		// And the other direction, so this cannot be satisfied by one surface
		// simply being a subset of the other.
		{rule: `secret.scheme == "env"`, surface: "assumption"},
	} {
		t.Run(test.surface+": "+test.rule, func(t *testing.T) {
			t.Parallel()

			if err := compileFor(test.surface)(test.rule); err == nil {
				t.Errorf("%q compiled on the %s surface; the two environments have "+
					"converged, and a documented rule can now be checked against the "+
					"wrong one without anything noticing", test.rule, test.surface)
			}
		})
	}
}
