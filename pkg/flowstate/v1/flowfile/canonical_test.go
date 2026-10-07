package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const canonicalBase = `edition: v2026.4
name: canon
steps:
  - id: first
    log:
      message: hello
  - id: second
    log:
      message: ${steps.first.logged + "!"}
`

func parseCanonical(t *testing.T, src string) *v1.Workflow {
	t.Helper()
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	return wf
}

// The same expression written at different offsets, as a frontend with its own
// source text would emit it, is the same program: the canonical digest says so,
// while the debugger's position-bound digest rightly does not.
func TestCanonicalDigestIgnoresExpressionPositions(t *testing.T) {
	a := parseCanonical(t, canonicalBase)
	b := proto.CloneOf(a)
	shifted := 0
	for _, in := range b.GetSteps()[1].GetTask().GetInputs() {
		info := in.GetExpr().GetSourceInfo()
		for id := range info.Positions {
			info.Positions[id] += 7
		}
		info.LineOffsets = append([]int32{0}, info.LineOffsets...)
		info.Location = "other.src"
		shifted++
	}
	require.NotZero(t, shifted)

	require.NotEqual(t, v1.WorkflowIRDigest(a), v1.WorkflowIRDigest(b), "positions must still reach the debugger's digest")
	require.Equal(t, v1.CanonicalDigest(a), v1.CanonicalDigest(b))

	ja, err := v1.MarshalCanonicalJSON(a)
	require.NoError(t, err)
	jb, err := v1.MarshalCanonicalJSON(b)
	require.NoError(t, err)
	require.Equal(t, string(ja), string(jb))
	require.NotContains(t, string(ja), "positions")
}

// A different program must not collapse: changing the operator changes the digest.
func TestCanonicalDigestDistinguishesPrograms(t *testing.T) {
	a := parseCanonical(t, canonicalBase)
	b := parseCanonical(t, strings.Replace(canonicalBase, `+ "!"`, `+ "?"`, 1))

	require.NotEqual(t, v1.CanonicalDigest(a), v1.CanonicalDigest(b))
}

// Canonicalizing leaves the caller's workflow untouched, and is idempotent.
func TestCanonicalWorkflowDoesNotMutateAndIsIdempotent(t *testing.T) {
	wf := parseCanonical(t, canonicalBase)
	before := v1.WorkflowIRDigest(wf)

	once := v1.CanonicalWorkflow(wf)
	require.Equal(t, before, v1.WorkflowIRDigest(wf))
	require.Equal(t, v1.CanonicalDigest(once), v1.CanonicalDigest(v1.CanonicalWorkflow(once)))
	require.Equal(t, v1.CanonicalDigest(wf), v1.CanonicalDigest(once))
}

func TestCanonicalWorkflowNil(t *testing.T) {
	require.Nil(t, v1.CanonicalWorkflow(nil))
}
