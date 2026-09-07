package taskexample

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestBuildValidates is the mirror test [Build] owes.
//
// A step somebody is invited to copy has to compile, and the way to know that
// is to compile it rather than to read it: `flow validate`'s own compiler,
// run over the bytes this package prints. Asserting that an example was
// produced proves nothing, which is the lesson `flow fix`'s two corruptions
// left behind.
//
// It runs for every registered task, so a task added with a required input
// this cannot write a value for fails here rather than handing an author
// (or docs/reference/tasks.md) a file the validator then rejects.
func TestBuildValidates(t *testing.T) {
	t.Parallel()

	for _, def := range v1.DefaultRegistry().All() {
		t.Run(def.Name, func(t *testing.T) {
			t.Parallel()

			example, err := Build(def)
			require.NoError(t, err, "no example could be built for %s", def.Name)

			// Written with two spaces of indent for the terminal, which is not a
			// document. What a reader copies is the block; what compiles is the
			// block with that indent removed.
			var source strings.Builder
			for _, line := range strings.Split(example, "\n") {
				source.WriteString(strings.TrimPrefix(line, "  ") + "\n")
			}

			diagnostics, err := flowfile.ValidateSource([]byte(source.String()))
			require.NoError(t, err, "the example for %s does not parse:\n%s", def.Name, source.String())
			assert.Empty(t, diagnostics, "the example for %s does not validate:\n%s", def.Name, source.String())
		})
	}
}

// TestAPluginsTaskIsPinned is #1676: a plugin's task is written under the
// `plugins:` block that makes the file safe to submit, at the version the
// catalog reports, in the grammar's own spelling — and a task with no pin, or
// a pin the grammar would refuse, is written exactly as before.
func TestAPluginsTaskIsPinned(t *testing.T) {
	t.Parallel()

	def, ok := v1.DefaultRegistry().Lookup("log")
	require.True(t, ok)

	pinned, err := BuildPinned(def, Pin{Plugin: "example", Version: "0.1.0"})
	require.NoError(t, err)
	assert.Contains(t, pinned, "  plugins:\n    example: v0.1.0\n  steps:\n",
		"the block is missing, misplaced, or spelled without the v the grammar requires:\n%s", pinned)

	// The pinned file compiles: the block is one the grammar reads.
	var source strings.Builder
	for _, line := range strings.Split(pinned, "\n") {
		source.WriteString(strings.TrimPrefix(line, "  ") + "\n")
	}
	diagnostics, err := flowfile.ValidateSource([]byte(source.String()))
	require.NoError(t, err)
	assert.Empty(t, diagnostics, "the pinned example does not validate:\n%s", source.String())

	unpinned, err := Build(def)
	require.NoError(t, err)
	assert.NotContains(t, unpinned, "plugins:", "a task this build provides was given a pin")

	for name, pin := range map[string]Pin{
		"no plugin":     {Version: "0.1.0"},
		"no version":    {Plugin: "example"},
		"not a version": {Plugin: "example", Version: "latest"},
		"leading zeros": {Plugin: "example", Version: "1.02.0"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := BuildPinned(def, pin)
			require.NoError(t, err)
			assert.Equal(t, unpinned, got, "a pin the grammar would refuse was written anyway")
		})
	}
}
