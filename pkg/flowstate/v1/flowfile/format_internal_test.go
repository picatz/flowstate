package flowfile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestACommentOnAContainerItselfIsNeverSilentlyDeleted is the review finding:
// prose above an inline `{}` hangs off the container node, and a collector
// that only read entry comments never saw it, so the formatter deleted it
// without even the refusal. The contract this pins is preserved-or-refused;
// which of the two the renderer can manage may change, silence may not.
func TestACommentOnAContainerItselfIsNeverSilentlyDeleted(t *testing.T) {
	t.Parallel()

	source := []byte(`edition: v2026.3
name: container-comment
vars:
  # deliberately empty, and the reason lives in this comment
  {}
steps:
  - id: greet
    log:
      message: hello
`)

	wf, _, err := Parse(source)
	require.NoError(t, err)

	out, err := Format(source, wf)
	if err != nil {
		var diagnostics Diagnostics
		require.ErrorAs(t, err, &diagnostics,
			"a container comment that cannot be carried must refuse with a position, not fail opaquely")
		return
	}

	assert.Contains(t, string(out), "deliberately empty",
		"the formatter neither carried the container's comment nor refused; it was silently deleted")
}

// TestFormatRefusesSourceBeyondTheByteBound pins the other review finding: the
// exported entry point parses bytes an outside party wrote, so it holds the
// same byte bound Parse and Fix do rather than assuming its caller compiled
// the source first.
func TestFormatRefusesSourceBeyondTheByteBound(t *testing.T) {
	t.Parallel()

	oversized := bytes.Repeat([]byte{'#'}, maxBytes+1)

	_, err := Format(oversized, &v1.Workflow{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bytes",
		"the refusal should say what bound was exceeded")
}
