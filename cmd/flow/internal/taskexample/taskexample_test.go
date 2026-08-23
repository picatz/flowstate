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
