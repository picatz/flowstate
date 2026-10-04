package flowtest

import (
	"fmt"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// callStubTaskPrefix names the synthetic task a stubbed `call:` becomes. The
// grammar of a plugin task name is namespace, dot, name, so the callee's name follows
// it directly.
const callStubTaskPrefix = "call."

// stubCallBoundaries lets a `step:` stub answer a `call:` step at the callee's
// boundary (#1599): the callee's body does not run, and the stub's `returns:`
// stand for the outputs the callee declares.
//
// It does so by rewriting, in a clone of the compiled workflow, each call step
// a step-form stub names into a task step over a synthetic task. A call's
// `with:` arguments and a task's inputs are the same shape, so everything a
// task stub already does carries over unchanged: `where:` filters on
// `inputs.<argument>`, `times:` and `fails:` work, the unused-stub and
// unmatched-invocation reports apply, and a stub cannot reach into the callee's
// scope because the callee never exists in the run. Nothing in the engine
// changes, so neither driver has a second code path: this is a property of
// what the harness hands the local driver.
//
// A call step no stub names is left alone and runs inline, so one caller can
// have a case that stubs the boundary and another that runs the callee. The
// returned workflow is spec itself when nothing is rewritten. The returned map
// names each synthetic task it introduced and the callee it stands for: the
// caller registers the names the way [swapRegistry] registers a stubbed plugin
// task's name, and holds each answer to its callee ([checkCallAnswer]).
func stubCallBoundaries(spec *v1.Workflow, compiled []compiledStub) (*v1.Workflow, map[string]*v1.Workflow, error) {
	wanted := map[string]bool{}
	for i := range compiled {
		if compiled[i].step != "" {
			wanted[compiled[i].step] = true
		}
	}
	calls := map[string]*v1.Call{}
	walkOwnNodes(spec.GetSteps(), func(node *v1.Node) {
		if call := node.GetCall(); call != nil && wanted[node.GetId()] {
			calls[node.GetId()] = call
		}
	})
	if len(calls) == 0 {
		return spec, nil, nil
	}

	// A boundary stub erases the callee, and with it the `sensitive:`
	// declarations the run withholds by (a callee's input or output is masked
	// from the transcript and from diagnostics only while the callee is part
	// of the run). Fail closed: a stub that could print the fixture it stands
	// for is refused, and the callee runs inline instead.
	boundaries := map[string]*v1.Workflow{}
	for _, id := range slices.Sorted(maps.Keys(calls)) {
		callee := calls[id].GetWorkflow()
		if sensitive, err := v1.DeclaresSensitiveValues(callee); err != nil {
			return nil, nil, fmt.Errorf("step %q: reading callee %q: %w", id, callee.GetName(), err)
		} else if sensitive {
			return nil, nil, fmt.Errorf("stub names step %q, whose callee %q declares a `sensitive:` input or output, "+
				"which a boundary stub cannot keep withheld; run the callee inline and stub its tasks with `task:`",
				id, callee.GetName())
		}
		name := callStubTaskPrefix + callStubName(callee)
		if other, ok := boundaries[name]; ok && other != callee && !proto.Equal(other, callee) {
			return nil, nil, fmt.Errorf("stub names step %q, whose callee %q shares the stub task name %q with another stubbed callee; "+
				"rename one of the callees", id, callee.GetName(), name)
		}
		boundaries[name] = callee
	}

	for i := range compiled {
		m := &compiled[i]
		call, ok := calls[m.step]
		if !ok || !m.hasReturns {
			continue
		}
		if err := checkCallReturns(m, call.GetWorkflow()); err != nil {
			return nil, nil, err
		}
	}

	clone := proto.Clone(spec).(*v1.Workflow)
	walkOwnNodes(clone.GetSteps(), func(node *v1.Node) {
		call := node.GetCall()
		if call == nil || calls[node.GetId()] == nil {
			return
		}
		name := callStubTaskPrefix + callStubName(call.GetWorkflow())
		node.Kind = &v1.Node_Task{Task: &v1.Task{Name: name, Inputs: call.GetArguments()}}
	})

	return clone, boundaries, nil
}

// callStubName is the callee's name made to fit the part of a plugin task name
// after the dot (`[a-z][a-z0-9_]*`): lower case, hyphens as underscores, other
// characters dropped, and "callee" for a name with no letter to start from.
func callStubName(callee *v1.Workflow) string {
	var out []byte
	for _, r := range callee.GetName() {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
			out = append(out, byte(r|0x20))
		case r == '-' && len(out) > 0:
			out = append(out, '_')
		case (r >= '0' && r <= '9' || r == '_') && len(out) > 0:
			out = append(out, byte(r))
		}
	}
	if len(out) == 0 {
		return "callee"
	}

	return string(out)
}

// checkCallReturns holds a boundary stub's `returns:` to the callee's
// declared outputs, as the call would produce them: every declared output is
// present, and nothing else is. A name the callee does not declare is the
// typo a stub is most likely to carry, and one it declares but the stub omits
// would surface as a missing output a step later.
func checkCallReturns(m *compiledStub, callee *v1.Workflow) error {
	declared := make([]string, 0, len(callee.GetDeclaredOutputs()))
	for _, out := range callee.GetDeclaredOutputs() {
		declared = append(declared, out.GetName())
	}
	slices.Sort(declared)
	for _, name := range slices.Sorted(maps.Keys(m.returns)) {
		if slices.Contains(declared, name) {
			continue
		}
		if suggestion, ok := nearest.Name(name, declared); ok {
			return fmt.Errorf("stub %d for step %q: returns %q, which callee %q does not declare as an output; did you mean %q?",
				m.ordinal, m.step, name, callee.GetName(), suggestion)
		}

		return fmt.Errorf("stub %d for step %q: returns %q, which callee %q does not declare as an output (it declares %v)",
			m.ordinal, m.step, name, callee.GetName(), declared)
	}
	for _, name := range declared {
		if _, ok := m.returns[name]; !ok {
			return fmt.Errorf("stub %d for step %q: callee %q declares output %q, which this stub's returns: leaves out",
				m.ordinal, m.step, callee.GetName(), name)
		}
	}

	return nil
}

// walkOwnNodes visits every step of one workflow at any depth short of a
// callee, calling visit on each node; a `call:` is visited and not entered.
func walkOwnNodes(nodes []*v1.Node, visit func(*v1.Node)) {
	for _, node := range nodes {
		visit(node)
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				walkOwnNodes(branch.GetSteps(), visit)
			}
		case *v1.Node_Switch:
			for _, body := range v1.SwitchBodies(kind.Switch) {
				walkOwnNodes(body, visit)
			}
		case *v1.Node_ForEach:
			walkOwnNodes(kind.ForEach.GetBody(), visit)
		case *v1.Node_Loop:
			walkOwnNodes(kind.Loop.GetBody(), visit)
		}
	}
}

// checkCallAnswer holds one invocation's resolved `returns:` to the callee's
// declared outputs the way the real call would: each value must have its
// output's declared type and satisfy its `must:`. [checkCallReturns] has
// already settled which names are present, so this is the half that needs the
// values, which an expression-valued `returns:` only has at the invocation.
func checkCallAnswer(profile string, callee *v1.Workflow, returns map[string]any) error {
	table := v1.TypesOf(callee)
	values := v1.NewNamedValues(returns)
	for _, decl := range callee.GetDeclaredOutputs() {
		value, ok := values[decl.GetName()]
		if !ok {
			continue
		}
		if err := v1.CheckOutputValueIn(table, decl, value); err != nil {
			return fmt.Errorf("returns does not satisfy callee %q: %w", callee.GetName(), err)
		}
		if err := v1.CheckOutputConstraint(profile, decl, value); err != nil {
			return fmt.Errorf("returns does not satisfy callee %q: %w", callee.GetName(), err)
		}
	}

	return nil
}
