package flowdebug_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestAHoldInsideACalleeWithholdsItsSensitiveInputs is #2208: a value only a
// callee declares `sensitive: true` is withheld at a hold inside that callee,
// by the typed contract's inspection and by the prompt's, as the durable
// driver withholds it — although the session was given no redactor of its
// own, and the root declares nothing sensitive.
const calleeSecret = "hunter2-callee-only-secret"

// calleeSensitiveFiles is a root that passes a literal to a callee declaring
// it sensitive, and itself declares nothing sensitive.
func calleeSensitiveFiles() map[string]string {
	child := `edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: ${"using " + inputs.api_key}
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
`

	return map[string]string{"main.yaml": root, "child.yaml": child}
}

func TestAHoldInsideACalleeWithholdsItsSensitiveInputs(t *testing.T) {
	t.Parallel()

	const secret = calleeSecret
	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	require.Equal(t, "first", at.GetOccurrence().GetSite().GetPath()[0])

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")
	require.Equal(t, "nested(child)/use", at.GetOccurrence().GetAddress())

	for _, expression := range []string{"inputs.api_key", "inputs", `"key: " + inputs.api_key`} {
		inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{
			Revision: at.GetRevision(), Expression: expression, Children: true,
		})
		require.NoError(t, err, expression)
		encoded, err := protojson.Marshal(inspected)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), secret, "%s showed the callee's sensitive input", expression)
	}

	result, err := flowdebug.NewDriver(run.session).Do(t.Context(), "inspect inputs.api_key")
	require.NoError(t, err)
	assert.NotContains(t, result.Text, secret, "the prompt's inspect showed the callee's sensitive input")
	assert.True(t, strings.Contains(result.Text, "[redacted]") || strings.Contains(result.Text, "withheld"),
		"the prompt's inspect did not say the value was withheld: %s", result.Text)

	final := move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, final.GetState())
	require.NoError(t, <-run.done)
}

// TestARevealedSessionShowsACalleesSensitiveInputs: a front that was
// authorized to show sensitive values (`--reveal-sensitive`, embed's
// RevealSensitive) says so with [flowdebug.Options.RevealSensitive], and a
// hold inside the callee then shows what it withholds otherwise.
func TestARevealedSessionShowsACalleesSensitiveInputs(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), func(opts *flowdebug.Options) { opts.RevealSensitive = true })
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.api_key"})
	require.NoError(t, err)
	assert.Contains(t, inspected.GetValue().GetRendered(), calleeSecret, "an authorized reveal withheld the value")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}

// TestAHoldInsideACalleeWithholdsTheRootsSensitiveInputs: sensitivity belongs
// to a value's origin, so a root's sensitive input passed to a callee under a
// name the callee does not declare sensitive is still withheld at a hold
// there, as the durable driver withholds it.
func TestAHoldInsideACalleeWithholdsTheRootsSensitiveInputs(t *testing.T) {
	t.Parallel()

	const secret = "root-only-sensitive-value"
	root := `edition: v2026.3
name: parent
inputs:
  token:
    type: string
    sensitive: true
    default: ` + secret + `
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      who: ${inputs.token}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/greet")
	require.Equal(t, "nested(child)/greet", at.GetOccurrence().GetAddress())

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.who"})
	require.NoError(t, err)
	encoded, err := protojson.Marshal(inspected)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), secret, "the root's sensitive input was shown inside the callee")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}

// TestAHoldInsideACalleeWithholdsAShortSensitiveValue: a sensitive container
// with a short descendant — `[7]` — has nothing a substring redactor can
// safely replace, so it is withheld by equality at the value seam, as
// flowtest's own value redactor withholds a root's.
func TestAHoldInsideACalleeWithholdsAShortSensitiveValue(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.3
name: child
inputs:
  codes:
    type: list
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: hi
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      codes: ${[7]}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.codes"})
	require.NoError(t, err)
	assert.NotEqual(t, "[7]", inspected.GetValue().GetRendered(), "a short sensitive value was shown inside the callee")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}
