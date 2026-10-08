package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

// shiftIDs renumbers every expression id in parsed by delta, as a producer with
// its own numbering would, including the ids that key the macro-call table.
func shiftIDs(parsed *exprpb.ParsedExpr, delta int64) {
	var walk func(*exprpb.Expr)
	walk = func(e *exprpb.Expr) {
		if e == nil {
			return
		}
		e.Id += delta
		switch k := e.ExprKind.(type) {
		case *exprpb.Expr_SelectExpr:
			walk(k.SelectExpr.GetOperand())
		case *exprpb.Expr_CallExpr:
			walk(k.CallExpr.GetTarget())
			for _, a := range k.CallExpr.GetArgs() {
				walk(a)
			}
		case *exprpb.Expr_ListExpr:
			for _, el := range k.ListExpr.GetElements() {
				walk(el)
			}
		case *exprpb.Expr_ComprehensionExpr:
			c := k.ComprehensionExpr
			for _, sub := range []*exprpb.Expr{c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult()} {
				walk(sub)
			}
		}
	}
	walk(parsed.GetExpr())
	macros := map[int64]*exprpb.Expr{}
	for id, call := range parsed.GetSourceInfo().GetMacroCalls() {
		walk(call)
		macros[id+delta] = call
	}
	if parsed.GetSourceInfo() != nil {
		parsed.SourceInfo.MacroCalls = macros
	}
}

// A producer that numbers the same tree differently, macro-call table included,
// still names the same program.
func TestCanonicalDigestIgnoresExpressionNumbering(t *testing.T) {
	a := parseCanonical(t, strings.Replace(canonicalBase, `${steps.first.logged + "!"}`, `${[1, 2].map(x, x + 1)}`, 1))
	b := proto.CloneOf(a)
	macros := 0
	for _, in := range b.GetSteps()[1].GetTask().GetInputs() {
		macros += len(in.GetExpr().GetSourceInfo().GetMacroCalls())
		shiftIDs(in.GetExpr(), 100)
	}
	require.NotZero(t, macros, "the fixture must exercise the macro-call table")

	require.NotEqual(t, v1.WorkflowIRDigest(a), v1.WorkflowIRDigest(b))
	require.Equal(t, v1.CanonicalDigest(a), v1.CanonicalDigest(b))
}

// The digest of the file a workflow came from names bytes, not a program: the
// same logic read from a file and inlined as a callee is one program.
func TestCanonicalDigestIgnoresSourceDigest(t *testing.T) {
	a := parseCanonical(t, canonicalBase)
	b := proto.CloneOf(a)
	b.SourceDigest = "sha256:" + strings.Repeat("ab", 32)

	require.NotEqual(t, v1.WorkflowIRDigest(a), v1.WorkflowIRDigest(b))
	require.Equal(t, v1.CanonicalDigest(a), v1.CanonicalDigest(b))
}
