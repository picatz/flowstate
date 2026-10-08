package conformance

import (
	"net/http"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TaskOutputSchemaCases returns the shared cases for #2507: a task's own
// result held to the output schema it declares, at [v1.Task.EvalInScope], the
// one place both drivers funnel every task's result through.
//
// A peer answers status 999 and the step's `expect:` accepts it, which is the
// only way such an answer reaches the outputs. Unshaped, the result carries the
// task's own `status_code` and the declared 100..599 refuses it; shaped, the
// author's `outputs:` replaced the declared names, so the same answer is let
// through.
//
// httpBaseURL should come from [NewHTTPServer].
func TaskOutputSchemaCases(httpBaseURL string) []Case {
	return []Case{
		{
			Name:          "an unshaped result the declared schema refuses fails the step",
			Workflow:      answersStatus999("unshaped-999", httpBaseURL, nil),
			ExpectFailure: true,
		},
		{
			Name: "a shaped result replaces the declared names and is let through",
			Workflow: answersStatus999("shaped-999", httpBaseURL, map[string]*v1.Value{
				"outputs": v1.NewExpr(`{"said": "ok"}`),
			}),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return out.GetStepValues()["fetch"].GetNamedValues()["said"].GetLiteral().GetStringValue() == "ok"
			},
		},
	}
}

func answersStatus999(name, httpBaseURL string, extra map[string]*v1.Value) *v1.Workflow {
	inputs := map[string]*v1.Value{
		"method": v1.NewLiteral(http.MethodGet),
		"url":    v1.NewLiteral(httpBaseURL + "/status/999"),
		"expect": v1.NewExpr("response.status_code == 999"),
	}
	for key, value := range extra {
		inputs[key] = value
	}

	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "fetch",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: inputs}},
		}},
	}
}
