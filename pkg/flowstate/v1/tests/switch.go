package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// SwitchCases are the shared cases that hold both drivers to one behaviour for
// `switch:`, a dispatch on one value.
//
// Run by both the local driver ([flowstatev1] eval_test.go) and the durable
// driver (engine workflow_test.go). One [v1.SelectSwitchCase] is what keeps the
// two together; these are what prove it is what both of them reach. Which branch
// a value takes, what the record says, and what an unresolvable discriminant
// does are the whole observable surface of the construct, and every one of them
// is the exact shape invariant 3 protects: a rehearsal that took a different
// branch than production would be wrong about the one thing it exists to show.
//
// The record is asserted literally — `value` and `case` spelled out beside the
// constants that produce them — for the reason ValueCases spells
// `steps.over.value` out: the constant keeps the drivers agreeing, and only a
// literal can pin what the constant has to be.
func SwitchCases() []Case {
	return []Case{
		{
			// The plain match: first case in written order takes the value, its
			// body runs and its step outputs merge into the enclosing scope (the
			// body's `taken` appears at the top level, exactly as a parallel
			// branch step's would), and the record holds the observed value and
			// the case that took it.
			Name: "a matched case runs its body and the record names the case",
			Workflow: declares("switch-matched",
				[]*v1.InputDeclaration{input("env", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				append([]*v1.Node{{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("inputs.env"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral("prod")}, Steps: []*v1.Node{says("taken", "prod path")}},
							{Values: []*v1.Value{v1.NewLiteral("dev")}, Steps: []*v1.Node{says("not_taken", "dev path")}},
						},
					}},
				}}, pins("show", `steps.route.value == "prod" && steps.route.case == "prod"`)...)...,
			),
			Inputs: map[string]*v1.Value{"env": v1.NewLiteral("prod")},
			ExpectedOutputs: withStep(held("taken", "show"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral("prod"),
				v1.SwitchCaseOutput:  v1.NewLiteral("prod"),
			}),
		},
		{
			// A list case, flattened into one membership check: the record names
			// the *member* that matched rather than the list, which is what makes
			// `steps.route.case` a single scalar a later step can dispatch on.
			Name: "a list case matches any member and the record names the member",
			Workflow: declares("switch-list-matched",
				[]*v1.InputDeclaration{input("outcome", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				&v1.Node{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("inputs.outcome"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral("deployed")}, Steps: []*v1.Node{says("deployed", "deployed")}},
							{
								Values: []*v1.Value{v1.NewLiteral("rejected"), v1.NewLiteral("withdrawn")},
								Steps:  []*v1.Node{says("declined", "declined")},
							},
						},
					}},
				},
			),
			Inputs: map[string]*v1.Value{"outcome": v1.NewLiteral("withdrawn")},
			ExpectedOutputs: withStep(held("declined"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral("withdrawn"),
				v1.SwitchCaseOutput:  v1.NewLiteral("withdrawn"),
			}),
		},
		{
			// The default: a value nobody enumerated runs the `default:` body,
			// and the record says no case took it — `case` is null, which is how
			// "default ran" and "case matched" stay distinguishable downstream.
			Name: "an unenumerated value runs the default and the record holds null",
			Workflow: declares("switch-default",
				[]*v1.InputDeclaration{input("action", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				append([]*v1.Node{{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("inputs.action"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral("opened")}, Steps: []*v1.Node{says("triage", "triage")}},
						},
						Default: &v1.Switch_Default{Steps: []*v1.Node{says("unhandled", "unhandled")}},
					}},
				}}, pins("show", "steps.route.case == null")...)...,
			),
			Inputs: map[string]*v1.Value{"action": v1.NewLiteral("labeled")},
			ExpectedOutputs: withStep(held("unhandled", "show"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral("labeled"),
				v1.SwitchCaseOutput:  v1.NewValue(nil),
			}),
		},
		{
			// An empty body is legal and load-bearing: `case: ignore` with
			// `steps: []` runs nothing, *matches*, and the record says so — the
			// written-down ignoring the design promises, distinct from the
			// unmatched record below by exactly the `case` output.
			Name: "an empty case body matches, runs nothing, and is recorded as taken",
			Workflow: declares("switch-empty-body",
				[]*v1.InputDeclaration{input("severity", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				append([]*v1.Node{{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("inputs.severity"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral("page")}, Steps: []*v1.Node{says("page_oncall", "paging")}},
							{Values: []*v1.Value{v1.NewLiteral("ignore")}, Steps: []*v1.Node{}},
						},
					}},
				}}, pins("show", `steps.route.case == "ignore"`)...)...,
			),
			Inputs: map[string]*v1.Value{"severity": v1.NewLiteral("ignore")},
			ExpectedOutputs: withStep(held("show"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral("ignore"),
				v1.SwitchCaseOutput:  v1.NewLiteral("ignore"),
			}),
		},
		{
			// Unmatched with no default: not a failure, not silence — nothing
			// runs, and the record carries the observed value with a null `case`,
			// which is the runtime half of the silent-nothing fix. A downstream
			// step dispatches on the record itself.
			Name: "an unmatched value with no default runs nothing and is recorded",
			Workflow: declares("switch-unmatched",
				[]*v1.InputDeclaration{input("outcome", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				append([]*v1.Node{{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("inputs.outcome"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral("deployed")}, Steps: []*v1.Node{says("deploy", "deploying")}},
						},
					}},
				}}, pins("show", `steps.route.case == null && steps.route.value == "expired"`)...)...,
			),
			Inputs: map[string]*v1.Value{"outcome": v1.NewLiteral("expired")},
			ExpectedOutputs: withStep(held("show"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral("expired"),
				v1.SwitchCaseOutput:  v1.NewValue(nil),
			}),
		},
		{
			// Fail closed: a discriminant referencing a skipped step's output
			// fails the step, and never flows into `default:`. Both drivers must
			// fail it — a driver that let it reach the default would make
			// `default:` mean "I couldn't compute the value", which is the exact
			// reading the design forbids.
			Name: "an unresolvable discriminant fails the step rather than taking the default",
			Workflow: &v1.Workflow{
				Name:    "switch-unresolvable",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:        "absent",
						Condition: v1.NewExpr("false"),
						Kind:      &v1.Node_Value{Value: v1.NewExpr(`"deployed"`)},
					},
					{
						Id: "route",
						Kind: &v1.Node_Switch{Switch: &v1.Switch{
							Value: v1.NewExpr("steps.absent." + v1.ValueOutput),
							Cases: []*v1.Switch_Case{
								{Values: []*v1.Value{v1.NewLiteral("deployed")}, Steps: []*v1.Node{says("deploy", "deploying")}},
							},
							Default: &v1.Switch_Default{Steps: []*v1.Node{says("swallowed", "must never run")}},
						}},
					},
				},
			},
			ExpectFailure: true,
		},
		{
			// The equality pin from the design's open question, answered: literal
			// equality is CEL's, so numbers compare numerically across int and
			// double — `case: 1` takes a discriminant of `1.0`, because that is
			// what `x == 1` in the `if:` it replaces would say.
			Name: "case matching is CEL equality, numeric across int and double",
			Workflow: &v1.Workflow{
				Name:    "switch-numeric",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{{
					Id: "route",
					Kind: &v1.Node_Switch{Switch: &v1.Switch{
						Value: v1.NewExpr("1.0"),
						Cases: []*v1.Switch_Case{
							{Values: []*v1.Value{v1.NewLiteral(int64(1))}, Steps: []*v1.Node{says("one", "one")}},
						},
					}},
				}}, pins("show", "steps.route.case == 1")...),
			},
			ExpectedOutputs: withStep(held("one", "show"), "route", map[string]*v1.Value{
				v1.SwitchValueOutput: v1.NewLiteral(float64(1)),
				v1.SwitchCaseOutput:  v1.NewLiteral(int64(1)),
			}),
		},
		{
			// Composition: a switch inside a for_each body, dispatching on the
			// loop's own binding — the scoping stress test from the design. Each
			// iteration takes a different case, the per-iteration records travel
			// in the loop's `results` like any body step's outputs, and the
			// trailing pin proves both iterations ran their own branch.
			Name: "a switch inside a for_each dispatches on the loop binding",
			Workflow: &v1.Workflow{
				Name:    "switch-in-loop",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{{
					Id: "process",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewLiteralList("bucket", "instance"),
						Iterator: "resource",
						Body: []*v1.Node{{
							Id: "dispatch",
							Kind: &v1.Node_Switch{Switch: &v1.Switch{
								Value: v1.NewExpr("resource"),
								Cases: []*v1.Switch_Case{
									{Values: []*v1.Value{v1.NewLiteral("bucket")}, Steps: []*v1.Node{says("check_bucket", "bucket")}},
									{Values: []*v1.Value{v1.NewLiteral("instance")}, Steps: []*v1.Node{says("check_instance", "instance")}},
								},
							}},
						}},
					}},
				}}, pins("show",
					`size(steps.process.results) == 2 && `+
						`steps.process.results[0].dispatch.case == "bucket" && `+
						`steps.process.results[1].dispatch.case == "instance"`)...),
			},
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				// The pin above is the load-bearing assertion; here it is enough
				// that the observing arm ran and its negation did not.
				_, held := out.GetStepValues()["show"]
				_, failed := out.GetStepValues()["show_else"]
				return held && !failed
			},
		},
	}
}
