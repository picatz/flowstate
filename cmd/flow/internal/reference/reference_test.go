package reference

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repoRoot is where the originals live, relative to this package's directory —
// which is where `go test` runs.
const repoRoot = "../../../.."

// TestTheMirrorMatchesTheRepository is the pin that makes the copy honest.
//
// The mirror exists because go:embed cannot reach out of a package directory,
// and a copy nobody checks is the defect this repository keeps refinding: a
// value written down twice, diverging quietly, discovered by whoever reads the
// wrong one. So it is held to the originals byte for byte, in both directions —
// a document edited without regenerating fails here, and a mirrored file whose
// original is gone fails here too.
//
// Skipped rather than failed when the originals are absent, since this package
// is also compiled from a module cache where docs/ was never shipped. The
// checkout is where the check has to bite, and the checkout is where CI runs.
func TestTheMirrorMatchesTheRepository(t *testing.T) {
	t.Parallel()

	if _, err := os.Stat(filepath.Join(repoRoot, "docs", "DSL.md")); err != nil {
		t.Skip("not running from a checkout; nothing to compare the mirror against")
	}

	const regenerate = "run `go generate ./cmd/flow/internal/reference` and commit the result"

	original, err := os.ReadFile(filepath.Join(repoRoot, "docs", "DSL.md"))
	require.NoError(t, err)
	assert.Equal(t, string(original), DSL(),
		"the embedded copy of docs/DSL.md is stale: %s", regenerate)

	sources, err := filepath.Glob(filepath.Join(repoRoot, "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, sources, "no examples found; the glob is wrong")

	onDisk := map[string]string{}
	for _, source := range sources {
		data, err := os.ReadFile(source)
		require.NoError(t, err)

		onDisk[filepath.Base(filepath.Dir(source))] = string(data)
	}

	embedded := map[string]string{}
	for _, name := range ExampleNames() {
		content, ok := Example(name)
		require.True(t, ok, "ExampleNames lists %q and Example cannot read it", name)

		embedded[name] = content
	}

	for name, want := range onDisk {
		got, ok := embedded[name]
		if assert.True(t, ok, "examples/%s/workflow.yaml is not embedded: %s", name, regenerate) {
			assert.Equal(t, want, got,
				"the embedded copy of examples/%s/workflow.yaml is stale: %s", name, regenerate)
		}
	}
	for name := range embedded {
		assert.Contains(t, onDisk, name,
			"the mirror holds %q, which examples/ no longer has: %s", name, regenerate)
	}
}

// TestExampleRefusesANameThatIsAPath.
//
// The name comes off a URI an agent composed, so the negative direction is the
// one worth asserting: not that a real example reads, but that a name shaped
// like a traversal does not reach anything. Reading is from an embedded
// filesystem today, which is why this is cheap to keep true rather than a thing
// to add after the first time it matters.
func TestExampleRefusesANameThatIsAPath(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"",
		".",
		"..",
		"../DSL.md",
		"..\\DSL.md",
		"nested/workflow",
		"/hello-world",
	} {
		_, ok := Example(name)
		assert.False(t, ok, "Example(%q) resolved to something", name)
	}

	content, ok := Example("hello-world")
	require.True(t, ok, "the hello-world example is not embedded")
	assert.Contains(t, content, "edition:")
}
