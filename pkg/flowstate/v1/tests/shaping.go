package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for output shaping written as a mapping, run by both execution drivers.
//
// The language has one spelling for "shape this step's outputs" and two
// encodings of it: a mapping the compiler keeps entry by entry, and the older
// map literal fenced into a string, which arrives as one expression that builds
// a map. Both are legal, and the whole claim of the change that unified them is
// that they *run the same* — so the claim is pinned here, where both drivers
// read it, rather than in whichever package happened to be edited.
//
// It matters more than it looks. The two encodings take different paths inside
// the http task: a mapping lands in the schema's own `outputs` field and is
// evaluated one entry at a time, while the fenced form is evaluated whole and
// its result converted. A difference between those is a difference between two
// files an author would call identical.
//
// And the shaped values are evaluated *on a worker*, inside an activity, which
// is what makes this a two-driver question rather than a compiler one: the
// durable driver's structure crosses the payload converter on its way there.

// OutputShapingCases returns the shared cases for a shaped `outputs:` mapping.
//
// The base URL should come from [NewHTTPServer]: shaping is evaluated against a
// response, so there has to be one.
func OutputShapingCases(httpBaseURL string) []Case {
	return []Case{
		{
			// The mapping form at its plainest, and the assertion is the whole
			// contract: the names the author wrote are the names the step
			// produces, and nothing the task declares survives beside them.
			Name: "a shaped mapping produces exactly its own names",
			Workflow: &v1.Workflow{
				Name:    "shaped-mapping",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{shapedMapping("call", httpBaseURL, map[string]*v1.Value{
					"code": v1.NewExpr("response.status_code"),
					"said": v1.NewLiteral("ok"),
				})},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": {NamedValues: map[string]*v1.Value{
					"code": v1.NewLiteral(int64(200)),
					"said": v1.NewLiteral("ok"),
				}},
			}},
		},
		{
			// Two encodings of one shaping, in one run, asserted to produce the
			// same outputs. Written as two steps rather than two cases because
			// what is being claimed is that they are equal, and two cases can
			// both pass while disagreeing about what "the same" means.
			Name: "both spellings of one shaping produce the same outputs",
			Workflow: &v1.Workflow{
				Name:    "shaped-both-spellings",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					shapedMapping("mapping", httpBaseURL, map[string]*v1.Value{
						"code": v1.NewExpr("response.status_code"),
					}),
					shaped("fenced", httpBaseURL, `{"code": response.status_code}`),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"mapping": {NamedValues: map[string]*v1.Value{"code": v1.NewLiteral(int64(200))}},
				"fenced":  {NamedValues: map[string]*v1.Value{"code": v1.NewLiteral(int64(200))}},
			}},
		},
		{
			// A shaped entry reading the enclosing scope rather than the
			// response. This is the reference compaction has to keep alive: on
			// the durable driver a Continue-As-New prunes step outputs nothing
			// still references, and a shaped mapping's entries are references
			// like any other — held one level inside a structure, which is
			// exactly where a walk that reads only the top of a value stops
			// looking.
			Name: "a shaped entry reads an earlier step",
			Workflow: &v1.Workflow{
				Name:    "shaped-reads-a-step",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{Id: "first", Kind: &v1.Node_Value{Value: v1.NewLiteral("from-first")}},
					shapedMapping("call", httpBaseURL, map[string]*v1.Value{
						"said": v1.NewExpr("steps.first.value"),
					}),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"first": {NamedValues: map[string]*v1.Value{"value": v1.NewLiteral("from-first")}},
				"call":  said("from-first"),
			}},
		},
	}
}

// shapedMapping returns an http step whose `outputs:` is the mapping form: one
// value per name, kept as a structure the way the compiler keeps it.
func shapedMapping(id, httpBaseURL string, entries map[string]*v1.Value) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":           v1.NewLiteral(httpBaseURL + "/status/200"),
				v1.ShapingInput: v1.NewStructureMap(entries),
			},
		}},
	}
}
