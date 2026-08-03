package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Continue-As-New carries forward only the step outputs the remaining steps still
// need, and [collectNodeRefs] decides what those are. Its own doc names the sites
// it walks — "a step's condition, a loop's item list and everything in its body,
// every branch of a parallel block, and a wait's own expression" — and says what is
// at stake:
//
//	Dropping an output one of those needs is a correctness failure — the resumed
//	run fails on an unresolved reference.
//
// A step's `vars:` was missing from the list and from the switch. It is an
// expression site like any other, and it is the one the language most encourages:
// `examples/http-json` is exactly this shape, and its comment teaches it —
// "a step's own `vars:` gives that value a name so the parse is written once rather
// than at every use".
//
// The example passes CI because `examples_run_test.go` runs the local driver, which
// never continues as new. The failure needs a run long enough to hand over, and then
// it is permanent: the segment resumes without the output and fails on a reference
// to a step that has already succeeded.
//
// These are table-driven over *every* site rather than a case for `vars:`, because a
// list that omitted one is what this was.

// TestEveryExpressionSiteKeepsWhatItReferences walks one reference through each
// place a node can hold an expression.
func TestEveryExpressionSiteKeepsWhatItReferences(t *testing.T) {
	t.Parallel()

	// The output a resumed segment must still be able to reach. One field, so a
	// site that keeps nothing and a site that keeps everything are distinguishable.
	produced := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"src": {NamedValues: map[string]*v1.Value{"said": v1.NewLiteral("payload")}},
	}}

	reference := v1.NewExpr("src.said")

	// A step that needs nothing, so a site's own reference is the only thing that
	// could have kept the output.
	inert := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("nothing to see")},
		}}}
	}

	for _, test := range []struct {
		name string
		node *v1.Node
	}{
		{
			name: "a task input",
			node: &v1.Node{Id: "n", Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "log", Inputs: map[string]*v1.Value{"message": reference},
			}}},
		},
		{
			name: "a condition",
			node: func() *v1.Node {
				n := inert("n")
				n.Condition = v1.NewExpr("src.said != ''")

				return n
			}(),
		},
		{
			// The one that was missing, and the shape `examples/http-json` ships.
			name: "a step's own vars",
			node: func() *v1.Node {
				n := inert("n")
				n.Vars = map[string]*v1.Value{"carried": reference}

				return n
			}(),
		},
		{
			name: "a loop's items",
			node: &v1.Node{Id: "n", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewExpr("[src.said]"),
				Iterator: "item",
				Body:     []*v1.Node{inert("body")},
			}}},
		},
		{
			name: "a loop body's task input",
			node: &v1.Node{Id: "n", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteral("one"),
				Iterator: "item",
				Body: []*v1.Node{{Id: "body", Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "log", Inputs: map[string]*v1.Value{"message": reference},
				}}}},
			}}},
		},
		{
			// A body step's vars is the same gap one level down, which is why the
			// fix belongs in the recursive function rather than at the top.
			name: "a loop body step's vars",
			node: &v1.Node{Id: "n", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteral("one"),
				Iterator: "item",
				Body: []*v1.Node{func() *v1.Node {
					body := inert("body")
					body.Vars = map[string]*v1.Value{"carried": reference}

					return body
				}()},
			}}},
		},
		{
			name: "a parallel branch's task input",
			node: &v1.Node{Id: "n", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{{Steps: []*v1.Node{{
					Id: "branch", Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "log", Inputs: map[string]*v1.Value{"message": reference},
					}},
				}}}},
			}}},
		},
		{
			name: "a parallel branch step's vars",
			node: &v1.Node{Id: "n", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{{Steps: []*v1.Node{func() *v1.Node {
					branch := inert("branch")
					branch.Vars = map[string]*v1.Value{"carried": reference}

					return branch
				}()}}},
			}}},
		},
		{
			name: "a wait's until",
			node: &v1.Node{Id: "n", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Until{Until: v1.NewExpr("src.said != ''")},
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			carried := compactOutputsForRemainingSteps([]*v1.Node{test.node}, 0, produced, nil)
			require.NotNil(t, carried)

			assert.Contains(t, carried.GetStepValues(), "src",
				"a reference written in %s was not seen, so the output it needs is dropped at "+
					"Continue-As-New and the resumed segment fails on it permanently", test.name)
		})
	}
}

// TestASiteThatReferencesNothingKeepsNothing is the control, and it is what makes
// the assertions above mean something.
//
// [collectNodeRefs] is deliberately generous — "when in doubt this keeps more" —
// and a version that gave up and carried everything would pass every case above
// while defeating the entire point of compaction, which is that a long run's
// payload does not grow without bound.
func TestASiteThatReferencesNothingKeepsNothing(t *testing.T) {
	t.Parallel()

	produced := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"src": {NamedValues: map[string]*v1.Value{"said": v1.NewLiteral("payload")}},
	}}

	node := &v1.Node{
		Id:   "n",
		Vars: map[string]*v1.Value{"carried": v1.NewLiteral("a literal names nothing")},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("nor does this")},
		}},
	}

	carried := compactOutputsForRemainingSteps([]*v1.Node{node}, 0, produced, nil)
	require.NotNil(t, carried)

	assert.Empty(t, carried.GetStepValues(),
		"an output nothing references was carried forward, so compaction is not compacting")
}

// TestAWholeStepReferenceSurvivesAFieldReference is the encoding, and it is the
// half that could not survive company.
//
// "Every output of this step is needed" used to be spelled as an *empty* field set,
// which any sibling reference then filled in. `${steps.a}` in one place and
// `${steps.a.foo}` in another recorded the empty set and then put `foo` into it,
// after which "everything" and "just foo" were the same value — and the resumed
// segment got `foo` alone, then failed on the next field the whole reference
// reached for.
//
// Both orders and both collectors, because the bug is order-independent and lives
// in the shared encoding rather than in either caller. It predates the `vars:`
// gap this file is otherwise about: the last case reaches it through two ordinary
// task inputs.
func TestAWholeStepReferenceSurvivesAFieldReference(t *testing.T) {
	t.Parallel()

	produced := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": {NamedValues: map[string]*v1.Value{
			"foo": v1.NewLiteral("F"),
			"bar": v1.NewLiteral("B"),
		}},
	}}

	logging := func(inputs map[string]*v1.Value) *v1.Task {
		return &v1.Task{Name: "log", Inputs: inputs}
	}

	for _, test := range []struct {
		name string
		node *v1.Node
	}{
		{
			name: "whole in vars, a field in the input",
			node: &v1.Node{
				Id:   "n",
				Vars: map[string]*v1.Value{"everything": v1.NewExpr("steps.a")},
				Kind: &v1.Node_Task{Task: logging(map[string]*v1.Value{
					"message": v1.NewExpr("steps.a.foo"),
				})},
			},
		},
		{
			name: "a field in vars, whole in the input",
			node: &v1.Node{
				Id:   "n",
				Vars: map[string]*v1.Value{"one": v1.NewExpr("steps.a.foo")},
				Kind: &v1.Node_Task{Task: logging(map[string]*v1.Value{
					"message": v1.NewExpr("string(steps.a)"),
				})},
			},
		},
		{
			// Neither reference is in `vars:`, which is what makes this older than
			// everything else in this file.
			name: "both in ordinary task inputs",
			node: &v1.Node{Id: "n", Kind: &v1.Node_Task{Task: logging(map[string]*v1.Value{
				"message": v1.NewExpr("steps.a.foo"),
				"fields":  v1.NewExpr("steps.a"),
			})}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			carried := compactOutputsForRemainingSteps([]*v1.Node{test.node}, 0, produced, nil)
			require.NotNil(t, carried)

			assert.Len(t, carried.GetStepValues()["a"].GetNamedValues(), 2,
				"a step referenced whole was trimmed to the fields another expression "+
					"happened to name, so the resumed run fails on the first one it did not")
		})
	}
}

// TestAReferenceInAMapKeySurvivesCompaction covers the other half of a map
// literal.
//
// The tests above walk every place a *node* can hold an expression. This one walks
// a place an *expression* can hold an expression, and the walker knew about only
// one of the two: `Expr_CreateStruct_Entry` has a key_kind oneof — `field_key`,
// a bare string naming a message field, or `map_key`, a full expression — and
// [collectRefsFromExpr] read `GetValue()` alone.
//
// So `${ {steps.src.said: 'v'} }` recorded nothing. The map is built from an
// output the resumed segment no longer has, and the step that already succeeded
// fails after a Continue-As-New. It is reachable from a Flowfile exactly as
// written here: the parser stores the key as a `map_key` select chain and
// `flow validate` resolves it, because every other CEL walker in the repo
// (`flowfile`'s validate, celcheck, secret and fixexpr passes) walks both halves.
// Only the one deciding what survives a handover did not.
//
// Both compaction sites, because they share the walker and neither is fail-safe:
// [compactOutputsForRemainingSteps] prunes the handover payload, and
// [compactPrevOutputsForTask] prunes what an activity is handed.
func TestAReferenceInAMapKeySurvivesCompaction(t *testing.T) {
	t.Parallel()

	// Two outputs, so "kept the right one" is distinguishable from "kept them all".
	produced := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"src": {NamedValues: map[string]*v1.Value{
			"said":  v1.NewLiteral("payload"),
			"other": v1.NewLiteral("unreferenced"),
		}},
	}}

	for _, test := range []struct {
		name string
		expr string
	}{
		{
			// The reported shape, rooted under `steps` — the reference exists
			// nowhere but in key position.
			name: "rooted, key position only",
			expr: "{steps.src.said: 'v'}",
		},
		{
			// The same in the legacy unrooted spelling, which reaches the walker's
			// ident arm rather than [rootedStepRef].
			name: "unrooted, key position only",
			expr: "{src.said: 'v'}",
		},
		{
			// A map literal is an ordinary sub-expression, so the gap also hides
			// under everything that can contain one. A macro expands to a
			// comprehension whose iter_range is the map — the walker reaches the
			// map and then loses the key inside it.
			name: "key position inside a macro's iteration range",
			expr: "{steps.src.said: 'v'}.map(k, k)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reference := v1.NewExpr(test.expr)
			require.NotNil(t, reference.GetExpr(), "expression did not parse: %s", test.expr)

			node := &v1.Node{Id: "n", Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": reference},
			}}}

			carried := compactOutputsForRemainingSteps([]*v1.Node{node}, 0, produced, nil)
			require.NotNil(t, carried)
			require.Contains(t, carried.GetStepValues(), "src",
				"a reference in map-key position was not seen, so the output it needs is "+
					"dropped at Continue-As-New and the resumed segment fails on a step "+
					"that already succeeded")
			assert.Contains(t, carried.GetStepValues()["src"].GetNamedValues(), "said")

			handed := compactPrevOutputsForTask(node.GetTask(), produced)
			require.NotNil(t, handed)
			require.Contains(t, handed.GetStepValues(), "src",
				"the same gap at the other compaction site: the activity is handed a "+
					"map missing the output its own key names")
			assert.Contains(t, handed.GetStepValues()["src"].GetNamedValues(), "said")
		})
	}
}
