package auth

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every CEL rule this package documents has to compile.
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

// docRule matches the rules this package writes in its doc comments, which are
// YAML list entries in single quotes:
//
//	//	  - 'target == "aws-prod" && workload.namespace == "acme"'
//
// Deliberately narrow. It matches the shape the examples are written in rather
// than anything quoted anywhere, so prose mentioning a fragment is not dragged
// in and then expected to be a whole boolean rule.
var docRule = regexp.MustCompile(`^\s*//\s*-\s*'(.+)'\s*$`)

func TestEveryDocumentedRuleCompiles(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}

	var found int

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		contents, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}

		for number, line := range strings.Split(string(contents), "\n") {
			match := docRule.FindStringSubmatch(line)
			if match == nil {
				continue
			}

			rule := match[1]
			found++

			// Which surface a rule belongs to is decided by what it names: the
			// secret environment is the assumption environment plus `secret`, so a
			// rule mentioning it is documented against that one and everything else
			// against the other. Trying both and accepting either would let a rule
			// belonging to neither pass by matching the wrong one.
			var compileErr error
			if strings.Contains(rule, "secret.") {
				_, compileErr = compileSecretRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
			} else {
				_, compileErr = compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
			}

			if compileErr != nil {
				t.Errorf("%s:%d documents a rule that does not compile:\n\t%s\n\t%v",
					name, number+1, rule, compileErr)
			}
		}
	}

	// The anti-vacuity guard, which this file needs more than most: a regex that
	// stops matching turns this into a test that passes by examining nothing, and
	// the failure it exists to catch is precisely a doc nobody looked at.
	if found < 4 {
		t.Errorf("found only %d documented rules; the walk or the comment style changed, "+
			"and this test is no longer reading what it claims to", found)
	}
}
