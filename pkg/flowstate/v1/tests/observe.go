package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// How a shared case observes a value, now that nothing local produces one.
//
// `echo`, `printf` and `cel` retired at edition v2026.2, and all three were the same
// thing: an expression wearing a step's clothes. What replaced them is `vars:`, which
// is not a step and has no outputs — so a case that used to read a computed value out
// of `steps.show.result` has nothing left to read. Of the tasks that remain, `log`
// produces no values deliberately and `http` needs a server.
//
// That is not a gap in the tests. It is the model the language now states: a task is an
// *effect*, and a value comes from an expression. The tests have to observe values the
// way an author does, which is through a condition.
//
// A step whose condition is false is *absent* from the run's outputs rather than
// present and empty — pinned by "condition false skips the step" in policy.go, and
// deliberately so, since a present-but-empty entry would let a reference to a step that
// did not run resolve to nothing. Presence is therefore one bit a workflow can set from
// an expression, and that bit is enough.

// pins returns a pair of steps that together observe what an expression evaluates to.
//
// The pair is the point. A single guarded step makes a weak assertion, because its
// absence has two causes that matter differently: the claim was false, or conditions
// stopped being evaluated at all. The negative arm runs in exactly the first case, so a
// failure distinguishes "the value was wrong" from "the driver skipped everything"
// instead of leaving whoever reads the failure to guess.
//
// It is the same habit as writing the negative direction of an isolation test: an
// assertion that only checks the direction you expect to hold passes for reasons that
// have nothing to do with the feature.
func pins(id, claim string) []*v1.Node {
	return []*v1.Node{
		guarded(id, claim, "held: "+claim),
		guarded(id+"_else", "!("+claim+")", "failed: "+claim),
	}
}

// guarded returns a `log:` step that runs only when condition holds. An empty
// condition means the step always runs.
func guarded(id, condition, message string) *v1.Node {
	node := &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
		}},
	}
	if condition != "" {
		node.Condition = v1.NewExpr(condition)
	}

	return node
}

// says returns a `log:` step that always runs, carrying message.
//
// Used where a case needs a step to exist — to be skipped, retried, tolerated, or
// counted — and does not care what it emits.
func says(id, message string) *v1.Node {
	return guarded(id, "", message)
}

// held names the steps a case expects to have run, each contributing no values.
//
// Every step named here is a `log:` step, so the expectation is the empty outputs entry
// that says "this happened and produced nothing" — as distinct from the absence that
// says it did not happen. The two are different in the run record and identical in Go,
// which is why the shared cases assert on the record.
func held(ids ...string) *v1.Workflow_StepOutputs {
	values := make(map[string]*v1.Node_Outputs, len(ids))
	for _, id := range ids {
		values[id] = &v1.Node_Outputs{}
	}

	return &v1.Workflow_StepOutputs{StepValues: values}
}

// withVars attaches a step's own `vars:` block to a node, so a case can build one
// with the helpers above and still declare what the step binds.
func withVars(node *v1.Node, vars map[string]*v1.Value) *v1.Node {
	node.Vars = vars

	return node
}

// counter returns a `for_each` step whose body logs once per item.
//
// It exists because a condition reading a *step output* is a path worth keeping under
// test, and after the retirement a loop is the only step a case can produce one from
// without a server: `results` is a real output, recorded by the engine rather than
// returned by a task, and `size(steps.<id>.results)` is a value a later condition can
// compare.
func counter(id string, items ...string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList(anySlice(items)...),
			Iterator: "item",
			Body:     []*v1.Node{says(id+"_body", "counting")},
		}},
	}
}

func anySlice(items []string) []any {
	out := make([]any, len(items))
	for i, item := range items {
		out[i] = item
	}

	return out
}
