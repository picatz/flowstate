package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// #241's P2 gives each Diagnostic a stable Code beside its Message, chosen from
// what the diagnostic *is* rather than from what it currently says — so the two
// things worth pinning are that every registered code is actually reachable
// (nothing declared and never assigned) and that Code does not move when Message
// is reworded (the whole reason it exists).

// diagnosticCodeCase is one workflow engineered to trigger exactly one
// registered [v1.DiagnosticCode], plus the step the diagnostic is expected to
// land on so the assertion does not have to guess which of several
// diagnostics is the one under test.
type diagnosticCodeCase struct {
	name     string
	code     v1.DiagnosticCode
	workflow *v1.Workflow
	step     string
}

func diagnosticCodeCases() []diagnosticCodeCase {
	return []diagnosticCodeCase{
		{
			name: "unknown task",
			code: v1.DiagnosticCodeUnknownTask,
			step: "bad",
			workflow: &v1.Workflow{
				Name: "unknown-task",
				Steps: []*v1.Node{{
					Id:   "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}},
				}},
			},
		},
		{
			name: "unresolved reference",
			code: v1.DiagnosticCodeUnresolvedReference,
			step: "bad",
			workflow: &v1.Workflow{
				Name: "unresolved-reference",
				Steps: []*v1.Node{{
					Id: "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewExpr("vars.nope")},
					}},
				}},
			},
		},
		{
			name: "type mismatch",
			code: v1.DiagnosticCodeTypeMismatch,
			step: "bad",
			workflow: &v1.Workflow{
				Name:    "type-mismatch",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{{
					Id: "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewExpr(`1 + "a"`)},
					}},
				}},
			},
		},
		{
			name: "constraint violation",
			code: v1.DiagnosticCodeConstraintViolation,
			step: "bad",
			workflow: &v1.Workflow{
				Name: "constraint-violation",
				Steps: []*v1.Node{{
					Id: "bad",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("not a uri at all"),
							"method": v1.NewLiteral("GET"),
						},
					}},
				}},
			},
		},
		{
			name: "placement refusal",
			code: v1.DiagnosticCodePlacementRefusal,
			step: "outer",
			workflow: &v1.Workflow{
				Name: "placement-refusal",
				Steps: []*v1.Node{{
					Id: "outer",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						Until: v1.NewExpr("true"),
						Body: []*v1.Node{{
							Id: "inner",
							Kind: &v1.Node_Loop{Loop: &v1.Loop{
								Until: v1.NewExpr("true"),
								Body: []*v1.Node{{
									Id:   "leaf",
									Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
								}},
							}},
						}},
					}},
				}},
			},
		},
		{
			name: "retired key",
			code: v1.DiagnosticCodeRetiredKey,
			step: "b",
			workflow: &v1.Workflow{
				Name: "retired-key",
				Steps: []*v1.Node{
					{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
					{
						Id: "b",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("a")},
						}},
					},
				},
			},
		},
	}
}

// TestDiagnosticCodesAreAssigned pins that every non-general registered code is
// actually produced by some real diagnostic, and that each case's workflow
// produces exactly the code it was engineered to.
//
// This is the other direction of docs/reference/diagnostics.md's own drift
// guard: cmd/flow/docsgen.go renders the registry, so a code declared there and
// never assigned would still generate cleanly — nothing about `flow docs
// generate` can tell a real code from an aspirational one. Only running the
// validator and checking what comes out can.
func TestDiagnosticCodesAreAssigned(t *testing.T) {
	t.Parallel()

	seen := map[v1.DiagnosticCode]bool{}

	for _, tc := range diagnosticCodeCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diagnostics := flowfile.Validate(tc.workflow)
			require.NotEmpty(t, diagnostics, "case must produce at least one diagnostic")

			var found bool
			for _, d := range diagnostics {
				if d.Step != tc.step {
					continue
				}
				proto := d.Proto()
				if v1.DiagnosticCode(proto.GetCode()) == tc.code {
					found = true
				}
			}
			assert.True(t, found, "step %q must carry a diagnostic coded %q; got %+v",
				tc.step, tc.code, diagnostics)
		})
	}

	for _, tc := range diagnosticCodeCases() {
		for _, d := range flowfile.Validate(tc.workflow) {
			seen[v1.DiagnosticCode(d.Proto().GetCode())] = true
		}
	}

	for _, info := range v1.DiagnosticCodes() {
		if info.Code == v1.DiagnosticCodeGeneral {
			// The fallback, not a class any case is built to trigger on purpose —
			// every case above that is not deliberately one of the other six still
			// exercises other, unrelated diagnostics that fall back to it, but that
			// is incidental rather than the property under test here.
			continue
		}
		assert.True(t, seen[info.Code], "registered code %q is never produced by %s; "+
			"either a case here is missing or the code is unused and should be removed",
			info.Code, "diagnosticCodeCases")
	}
}

// TestDiagnosticCodeStableAcrossRewording pins the reason Code exists: the
// same class of mistake reported with two different sentences must carry the
// same Code, because a program branching on it must not care which sentence
// this build happened to choose today.
//
// The unknown-task diagnostic already renders two different messages for the
// same mistake — a plain "unknown task" sentence, and a different one for a
// dotted name that names a plugin task this build has not loaded — which is
// exactly a pre-existing rewording pair rather than one invented for this
// test. See [validateTaskStep]'s own comment for why the two sentences differ.
func TestDiagnosticCodeStableAcrossRewording(t *testing.T) {
	t.Parallel()

	plain := &v1.Workflow{
		Name: "unknown-task-plain",
		Steps: []*v1.Node{{
			Id:   "bad",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}},
		}},
	}
	dotted := &v1.Workflow{
		Name: "unknown-task-dotted",
		Steps: []*v1.Node{{
			Id:   "bad",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "slack.post"}},
		}},
	}

	plainDiagnostic := diagnosticFor(t, flowfile.Validate(plain), "bad")
	dottedDiagnostic := diagnosticFor(t, flowfile.Validate(dotted), "bad")

	require.NotEqual(t, plainDiagnostic.Message, dottedDiagnostic.Message,
		"the two cases must actually be worded differently, or this test proves nothing")

	assert.Equal(t, v1.DiagnosticCodeUnknownTask, v1.DiagnosticCode(plainDiagnostic.Proto().GetCode()))
	assert.Equal(t, v1.DiagnosticCodeUnknownTask, v1.DiagnosticCode(dottedDiagnostic.Proto().GetCode()))
	assert.Equal(t, plainDiagnostic.Proto().GetCode(), dottedDiagnostic.Proto().GetCode(),
		"Code must not move when Message is reworded")
}

// diagnosticFor finds the one diagnostic reported against a step, failing the
// test if there is not exactly one.
func diagnosticFor(t *testing.T, diagnostics flowfile.Diagnostics, step string) flowfile.Diagnostic {
	t.Helper()

	var matches []flowfile.Diagnostic
	for _, d := range diagnostics {
		if d.Step == step {
			matches = append(matches, d)
		}
	}
	require.Len(t, matches, 1, "expected exactly one diagnostic against step %q, got %+v", step, diagnostics)

	return matches[0]
}
