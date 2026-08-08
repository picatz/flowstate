package server

// The invariant these tests hold is structural, and it is the codec seam's one
// hard exception: a search attribute can never be codec-covered, because the
// cluster has to index it, so the SDK always encodes it with the default
// converter no matter what a deployment configured. Payloads, memos, and
// failures encrypt; search attributes do not, ever. The only way "encrypted at
// rest" stays true on a deployment that asked for it is therefore a rule about
// what may be projected into one at all: values derived from the deployment or
// from the authenticated identity, and never anything derived from a run's
// inputs, outputs, or payloads.
//
// The current set satisfies the rule: the namespace is the authenticated
// caller's own, and the workflow name is the specification's protovalidate-
// constrained `name`. These tests pin that set, so widening it is a deliberate
// act that meets this comment rather than a convenient line in a diff. See
// the payloadcodec package doc, which states the rule from the codec's side.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSearchAttributesCarryExactlyTheTwoPinnedValues holds the value half:
// what one submission projects into the visibility store is the namespace and
// the workflow name under the two Flowstate-prefixed keys, and nothing else.
func TestSearchAttributesCarryExactlyTheTwoPinnedValues(t *testing.T) {
	t.Parallel()

	attrs := runSearchAttributes("team-a", "payments")

	untyped := attrs.GetUntypedValues()
	require.Len(t, untyped, 2,
		"a third search attribute appeared; read the package note in this file before widening the set")

	values := make(map[string]any, len(untyped))
	for key, value := range untyped {
		values[key.GetName()] = value
	}

	require.Equal(t, map[string]any{
		"FlowstateNamespace":    "team-a",
		"FlowstateWorkflowName": "payments",
	}, values,
		"the projected values are not the identity's namespace and the spec's name; whatever else "+
			"this carries is indexed by the cluster in plaintext on every deployment, codec or not")
}

// TestNoSearchAttributeIsBuiltOutsideTheOneConstructor holds the shape half,
// the way TestNoReadSiteBuildsItsOwnDefaultDataConverter holds the converter
// rule: every search-attribute key in this package is declared in the one var
// block beside runSearchAttributes, so there is exactly one place the set can
// grow and it is the place carrying the invariant's doc comment.
func TestNoSearchAttributeIsBuiltOutsideTheOneConstructor(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	// Declaring a key, building an attribute set, and binding a value to a key
	// are the three spellings a projection can start with; a guard that watched
	// only declarations would stay green while an existing key was reused with
	// a run-input-derived value somewhere else.
	tokens := []string{"NewSearchAttributeKey", "NewSearchAttributes(", ".ValueSet("}

	permitted := 0
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		source, err := os.ReadFile(filepath.Clean(name))
		require.NoError(t, err)
		checked++

		for _, line := range strings.Split(string(source), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			matched := false
			for _, token := range tokens {
				if strings.Contains(trimmed, token) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			if name == "server.go" {
				permitted++
				continue
			}
			t.Errorf("%s: %s\n\nbuild the projection in server.go, in or beside runSearchAttributes, "+
				"where the rule is written: deployment- or identity-derived values only, never "+
				"anything from inputs, outputs, or payloads. A construction site elsewhere is a "+
				"projection nothing reviews against that rule.", name, trimmed)
		}
	}

	require.Greater(t, checked, 0, "this guard read no source files, so it proves nothing")

	// The permitted sites have to still be there, and there are exactly five:
	// the two key declarations, one NewSearchAttributes call, and one ValueSet
	// per key inside runSearchAttributes. A guard that passes because they all
	// moved out of server.go would prove the opposite of what it says, and a
	// sixth site in server.go is a new projection that has to be reviewed
	// against the rule and then counted here, deliberately.
	require.Equal(t, 5, permitted,
		"server.go's search-attribute surface changed; review the new site against the projection "+
			"rule, then update this count")
}
