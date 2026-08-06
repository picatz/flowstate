package tests

import (
	"net/http"
	"strconv"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// taskOutputElementBound is the same 10,000-element ceiling
// [v1.CheckInputConstraints] applies to a caller's own submitted input,
// mirrored here rather than imported: the constant lives unexported in
// flowstatev1, and a public test package pinning the number a second time is
// what proves the two sides that are supposed to agree actually do — a
// change to the real constant that this file's cases stop tripping is a
// signal, not a maintenance chore to silence.
const taskOutputElementBound = 10_000

// TaskOutputElementBoundCases returns the shared cases for the *other* half
// of #204: a task's own result, rather than a caller's submitted input,
// carrying more list elements than a later `if:`/`for_each`/`${...}` can
// evaluate an expression over cheaply. See
// [v1.CheckInputConstraints]/[v1.checkInputListElementBound]'s own doc for
// the caller-input half this does not repeat, and eval.go's
// `Task.EvalInScope` for where this is actually bounded — the one place both
// execution drivers funnel every task's result through, built-in or plugin.
//
// The `http` task is what exercises this without a stub task or a registry
// swap: `/json-array/<n>` (see [NewHTTPServer]) returns a body well under the
// byte cap that still carries an arbitrary number of elements, which is
// exactly the asymmetry #204 found — a response bounded in bytes is not
// bounded in element count.
//
// httpBaseURL should come from [NewHTTPServer].
func TaskOutputElementBoundCases(httpBaseURL string) []Case {
	return []Case{
		{
			// At the bound, not past it: [v1.checkTaskOutputElementBound] (via
			// the shared walker both the input and output paths reuse) refuses
			// only once the running total exceeds the bound, so exactly
			// [taskOutputElementBound] elements is the largest a task's result
			// may legitimately carry.
			Name:     "a task result at the element bound succeeds",
			Workflow: fetchesJSONArray("at-bound", httpBaseURL, taskOutputElementBound),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return jsonArrayLen(out, "fetch") == taskOutputElementBound
			},
		},
		{
			// One element past the bound. The point of this case, per #204's own
			// closing line: assert the bound is *reached* on a list sized past
			// it, not merely that a small list is let through.
			Name:          "a task result past the element bound is refused",
			Workflow:      fetchesJSONArray("past-bound", httpBaseURL, taskOutputElementBound+1),
			ExpectFailure: true,
		},
	}
}

// fetchesJSONArray builds a one-step workflow whose `http` step fetches a
// JSON array of n elements and reports it whole as the step's `items`
// output — the shape that puts a large list in a *task's result* rather than
// in anything the caller submitted, which is the resource this bound covers.
func fetchesJSONArray(name, httpBaseURL string, n int) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "fetch",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method":     v1.NewLiteral(http.MethodGet),
						"url":        v1.NewLiteral(httpBaseURL + jsonArrayPath(n)),
						"parse_json": v1.NewLiteral(true),
						"outputs":    v1.NewExpr(`{"items": response.json}`),
					},
				}},
			},
		},
	}
}

// jsonArrayPath formats the path [NewHTTPServer]'s `/json-array/` handler
// reads its element count from.
func jsonArrayPath(n int) string {
	return "/json-array/" + strconv.Itoa(n)
}

// jsonArrayLen reads the length of the `items` list a [fetchesJSONArray] step
// reported, or -1 if the step is absent or `items` is not a list — a
// predicate rather than an exact-value comparison, since asserting equality
// against a 10,000-element literal would only restate the fixture.
func jsonArrayLen(out *v1.Workflow_StepOutputs, stepID string) int {
	step, ok := out.GetStepValues()[stepID]
	if !ok {
		return -1
	}
	items, ok := step.GetNamedValues()["items"]
	if !ok {
		return -1
	}
	list := items.GetLiteral().GetListValue()
	if list == nil {
		return -1
	}
	return len(list.GetValues())
}
