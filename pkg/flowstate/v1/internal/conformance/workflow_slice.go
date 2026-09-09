package conformance

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// WorkflowSliceCases exercise consecutive pure workflow-side work through both
// drivers. The expressions are individually inside every CEL bound; what makes
// these cases distinct is how many of them run before any activity, timer, or
// signal receive would otherwise return control to Temporal's scheduler.
func WorkflowSliceCases() []Case {
	const heavy = "lists.range(10000).map(i, i + 1).size()"

	values := make([]*v1.Node, 100)
	wantValues := make(map[string]*v1.Node_Outputs, len(values))
	refs := make([]string, len(values))
	wantRunValues := make([]any, len(values))
	for i := range values {
		id := fmt.Sprintf("value-%03d", i)
		values[i] = &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr(heavy)}}
		wantValues[id] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			v1.ValueOutput: v1.NewLiteral(int64(10000)),
		}}
		refs[i] = `steps["` + id + `"].` + v1.ValueOutput
		wantRunValues[i] = int64(10000)
	}

	skipped := make([]*v1.Node, 30)
	for i := range skipped {
		skipped[i] = &v1.Node{
			Id:        fmt.Sprintf("skipped-%02d", i),
			Condition: v1.NewExpr(heavy + " == 0"),
			Kind:      &v1.Node_Value{Value: v1.NewLiteral(int64(1))},
		}
	}

	return []Case{
		{
			Name: "one hundred bounded value steps complete as separate scheduler slices",
			Workflow: &v1.Workflow{
				Name:    "bounded-value-slices",
				Profile: v1.CurrentProfile,
				Steps:   values,
				DeclaredOutputs: []*v1.OutputDeclaration{{
					Name:  "values",
					Value: v1.NewExpr("[" + strings.Join(refs, ",") + "]"),
				}},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: wantValues,
				RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
					"values": v1.NewLiteralList(wantRunValues...),
				}},
			},
		},
		{
			Name: "costly false conditions continue at step boundaries",
			Workflow: &v1.Workflow{
				Name:    "bounded-skipped-condition-slices",
				Profile: v1.CurrentProfile,
				Steps: append(skipped, &v1.Node{
					Id:   "observed",
					Kind: &v1.Node_Value{Value: v1.NewLiteral(int64(30))},
				}),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"observed": {NamedValues: map[string]*v1.Value{
						v1.ValueOutput: v1.NewLiteral(int64(30)),
					}},
				},
			},
		},
		{
			Name: "one thousand pure loop iterations complete as separate scheduler slices",
			Workflow: &v1.Workflow{
				Name:    "bounded-loop-slices",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{{
					Id: "count",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						State:         "n",
						Initial:       v1.NewLiteral(int64(0)),
						Update:        v1.NewExpr("n + 1"),
						Until:         v1.NewExpr("n >= 999"),
						MaxIterations: 1000,
						Body: []*v1.Node{{
							Id:   "work",
							Kind: &v1.Node_Value{Value: v1.NewExpr("lists.range(100).map(i, i + 1).size()")},
						}},
					}},
				}, {
					// Keeping results reachable makes both drivers report the
					// complete iteration list even if a test forces Continue-As-New.
					Id:   "observed",
					Kind: &v1.Node_Value{Value: v1.NewExpr("steps.count.results.size()")},
				}},
			},
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				count := out.GetStepValues()["count"].GetNamedValues()
				results := count[v1.LoopResultsField].GetLiteral().GetListValue().GetValues()
				observed := out.GetStepValues()["observed"].GetNamedValues()[v1.ValueOutput]
				if len(results) != 1000 || count[v1.LoopStateField].GetLiteral().GetInt64Value() != 999 ||
					observed.GetLiteral().GetInt64Value() != 1000 {
					return false
				}
				for _, result := range results {
					iteration := mapEntry(result, "work")
					if mapEntry(iteration, v1.ValueOutput).GetInt64Value() != 100 {
						return false
					}
				}
				return true
			},
		},
	}
}

func mapEntry(value *expr.Value, key string) *expr.Value {
	for _, entry := range value.GetMapValue().GetEntries() {
		if entry.GetKey().GetStringValue() == key {
			return entry.GetValue()
		}
	}
	return nil
}
