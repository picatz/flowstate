package tests

import (
	"net/http"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Workflows is a collection of test workflows used for unit and integration testing.
//
// This allows us to share the same test workflows for execution with and without Temporal
// and ensure that execution behaves consistently across local and Temporal-based execution
// scenarios without duplicating the workflow definitions.
var Workflows = []struct {
	// Name of the workflow, used for test identification.
	Name string
	// Workflow is the actual workflow definition to be tested.
	Workflow *v1.Workflow
	// ExpectedOutputs is the expected outputs of the workflow steps after execution.
	ExpectedOutputs *v1.Workflow_StepOutputs
}{
	{
		Name: "simple echo workflow",
		Workflow: &v1.Workflow{
			Name: "simple",
			Steps: []*v1.Node{
				{
					Id: "a",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewLiteral("hello world"),
							},
						},
					},
				},
			},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"a": {
					NamedValues: map[string]*v1.Value{
						"result": v1.NewLiteral("hello world"),
					},
				},
			},
		},
	},
	{
		Name: "simple multi-step echo workflow",
		Workflow: &v1.Workflow{
			Name: "simple",
			Steps: []*v1.Node{
				{
					Id: "a",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewLiteral("hello world"),
							},
						},
					},
				},
				{
					Id: "b",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewExpr("a.result"),
							},
						},
					},
				},
			},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"a": {
					NamedValues: map[string]*v1.Value{
						"result": v1.NewLiteral("hello world"),
					},
				},
				"b": {
					NamedValues: map[string]*v1.Value{
						"result": v1.NewLiteral("hello world"),
					},
				},
			},
		},
	},
	{
		Name: "simple printf workflow",
		Workflow: &v1.Workflow{
			Name: "simple",
			Steps: []*v1.Node{
				{
					Id: "a",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "printf",
							Inputs: map[string]*v1.Value{
								"format": v1.NewLiteral("%s %s"),
								"args": v1.NewLiteralList(
									"hello",
									"world",
								),
								// "args": {
								// 	Kind: &v1.Value_Literal{
								// 		Literal: &expr.Value{
								// 			Kind: &expr.Value_ListValue{
								// 				ListValue: &expr.ListValue{
								// 					Values: []*expr.Value{
								// 						{Kind: &expr.Value_StringValue{StringValue: "hello"}},
								// 						{Kind: &expr.Value_StringValue{StringValue: "world"}},
								// 					},
								// 				},
								// 			},
								// 		},
								// 	},
								// },
							},
						},
					},
				},
			},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"a": {
					NamedValues: map[string]*v1.Value{
						"result": v1.NewLiteral("hello world"),
					},
				},
			},
		},
	},
	{
		Name: "simple http workflow",
		Workflow: &v1.Workflow{
			Name: "simple",
			Steps: []*v1.Node{
				{
					Id: "web",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":    v1.NewLiteral("https://httpbin.org/status/200"),
								"method": v1.NewLiteral(http.MethodGet),
							},
						},
					},
				},
				{
					Id: "output",
					Kind: &v1.Node_Task{
						Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewExpr("string(web.status_code)"),
							},
						},
					},
				},
			},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"web": {
					NamedValues: map[string]*v1.Value{
						"status_code": v1.NewLiteral(int64(200)),
					},
				},
				"output": {
					NamedValues: map[string]*v1.Value{
						"result": v1.NewLiteral("200"),
					},
				},
			},
		},
	},
	{
		Name: "cel expression workflow",
		Workflow: &v1.Workflow{
			Name: "simple",
			Steps: []*v1.Node{
				{
					Id: "a",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "echo",
						Inputs: map[string]*v1.Value{
							"message": v1.NewLiteral("hello"),
						},
					}},
				},
				{
					Id: "b",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "cel",
						Inputs: map[string]*v1.Value{
							"expr": v1.NewLiteral("a.result + '!'"),
						},
					}},
				},
			},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hello")}},
			"b": {NamedValues: map[string]*v1.Value{"result": {Kind: &v1.Value_Literal{Literal: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "hello!"}}}}}},
		}},
	},
}
