package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A Flowfile's top-level keys are unordered, and everything written below `steps:`
// belonged to the last step.
//
// `assignStepRanges` ended the last step at the document, having no next step to end
// at, so a `vars:` or an `edition:` at the bottom of the file fell inside it. Both
// features that ask "which step is this position in" then answered a step, for a
// position that is not in one:
//
//   - Completion handed out the last step's scope, so a trailing `vars:` block was
//     offered step ids and a loop iterator — names the validator refuses on that
//     exact line, which is the one thing this package exists not to do.
//   - Hover stopped answering, because `hoverAt` takes the step branch and never
//     reaches the document keys. `edition:` documented itself written first and said
//     nothing written last. The package's own fixtures write it last.
//
// These assert the placement makes no difference, which is the property the format
// actually has, rather than asserting a range — a range is an implementation detail
// and the two answers above are what an author sees.

// TestATrailingTopLevelKeyIsNotInsideTheLastStep is the hover half.
func TestATrailingTopLevelKeyIsNotInsideTheLastStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const leading = `edition: v2026.2
name: placement
vars:
  greeting: hi
steps:
  - id: only
    log:
      message: hello
`

	const trailing = `name: placement
steps:
  - id: only
    log:
      message: hello
vars:
  greeting: hi
edition: v2026.2
`

	for _, key := range []string{"vars", "edition"} {
		t.Run(key+" documents itself wherever it is written", func(t *testing.T) {
			above := hoverTextAtKey(t, c, "file:///placement-above-"+key+".yaml", leading, key)
			below := hoverTextAtKey(t, c, "file:///placement-below-"+key+".yaml", trailing, key)

			require.NotEmpty(t, above,
				"the key does not document itself even above `steps:`, so this test is "+
					"measuring the wrong thing")
			assert.Equal(t, above, below,
				"`%s:` documents itself above `steps:` and says nothing below it; a Flowfile's "+
					"top-level keys are unordered, so where it is written cannot decide whether "+
					"the editor explains it", key)
		})
	}
}

// TestCompletionInATrailingBlockDoesNotOfferAStepsScope is the completion half, and
// the one with teeth: what was offered there is rejected by the validator.
func TestCompletionInATrailingBlockDoesNotOfferAStepsScope(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	// A loop last, so both kinds of leaked name are reachable: its body's step ids
	// and the iterator it binds.
	const src = `name: trailing-vars
steps:
  - id: earlier
    log:
      message: hi
  - id: loop
    for_each:
      items: ${['a']}
      as: each
      steps:
        - id: body
          log:
            message: hi
vars:
  greeting: ${|}
edition: v2026.2
`

	clean, pos := splitCursor(t, src)
	c.open("file:///trailing-vars.yaml", clean)
	got := labels(c.complete("file:///trailing-vars.yaml", pos.Line, pos.Character).Items)

	for name, why := range map[string]string{
		// Not `first`: that is also a function the profile provides, so a fixture
		// using it would assert against a name legitimately offered here.
		"earlier": "a var is evaluated once before the first step runs, so no step has produced anything",
		"body":    "a loop body's step, from a position outside the loop",
		"loop":    "a var may not read a step at all",
		"each":    "the loop's iterator, which exists only inside its body",
	} {
		assert.NotContains(t, got, name, "completion offered %q in a top-level `vars:`: %s", name, why)
	}

	// The other direction, so that a fix which simply stopped offering anything
	// anywhere does not pass. What the validator rejects is what must not be
	// offered, so it is asked about the same file.
	refers := strings.Replace(src, "${|}", "${steps.earlier.result}", 1)

	diags, err := flowfile.ValidateSource([]byte(refers))
	require.NoError(t, err)
	require.NotEmpty(t, diags,
		"the validator accepted a step reference in a top-level `vars:`, so there is nothing "+
			"for completion to be wrong about")
}

// hoverTextAtKey opens src and returns the hover text over the given top-level key.
func hoverTextAtKey(t *testing.T, c *client, uri, src, key string) string {
	t.Helper()

	line := -1
	for i, text := range strings.Split(src, "\n") {
		if strings.HasPrefix(text, key+":") {
			line = i
			break
		}
	}
	require.GreaterOrEqual(t, line, 0, "the fixture has no top-level %q key", key)

	c.open(uri, src)

	h := c.hover(uri, line, 1)
	if h == nil {
		return ""
	}

	var out strings.Builder
	for _, part := range h.Contents {
		out.WriteString(part.Value)
	}

	return out.String()
}
