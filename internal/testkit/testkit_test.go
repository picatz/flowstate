package testkit_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/testkit"
)

// TestNamespaceNameForIsLegalAndUnique pins the two properties a namespace
// name has to have: only the characters Temporal accepts, and a different
// name on every call, since two subtests of one parent sanitize to the same
// string.
func TestNamespaceNameForIsLegalAndUnique(t *testing.T) {
	t.Parallel()

	legal := regexp.MustCompile(`^[A-Za-z0-9-]+-[0-9a-f]{6}-[0-9]+$`)

	t.Run("a subtest with spaces and slashes", func(t *testing.T) {
		t.Parallel()

		first := testkit.NamespaceNameFor(t)
		second := testkit.NamespaceNameFor(t)
		assert.Regexp(t, legal, first)
		assert.NotEqual(t, first, second, "two names from one test collided")
		// One token per process: the two names share it, and a second
		// process would draw its own, so a server that outlives this one
		// never sees the same name twice.
		token := regexp.MustCompile(`-([0-9a-f]{6})-[0-9]+$`)
		assert.Equal(t, token.FindStringSubmatch(first)[1], token.FindStringSubmatch(second)[1],
			"two names from one process carry different tokens")
		assert.LessOrEqual(t, len(first), 48+1+6+1+20, "the sanitized half is bounded, so a log line stays readable")
	})
}

// TestRepoRootHoldsTheModule pins that the walk lands on this module's
// go.mod and not on some other file by that name up the tree.
func TestRepoRootHoldsTheModule(t *testing.T) {
	t.Parallel()

	root := testkit.RepoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "module github.com/picatz/flowstate\n")
}
