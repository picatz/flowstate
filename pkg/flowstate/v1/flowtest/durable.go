package flowtest

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The local driver runs a case in one uninterrupted pass, which is the one
// thing a durable run never does: it suspends, serializes its state and resumes
// somewhere else. A case that passes locally therefore says nothing about the
// state a run carries across a Continue-As-New (#1598).
//
// A [DurableRunner] closes that gap. After a case passes locally, its workflow
// runs again on the durable interpreter with a Continue-As-New forced between
// every pair of steps, against freshly bound stubs, and the two runs must
// agree. The interpreter lives behind a func type because it carries the
// Temporal SDK's test environment, which the language server and every other
// importer of this package have no use for; `flow test` supplies it.

// DurableResult is what a durable run produced.
type DurableResult struct {
	// Outputs is the last segment's step outputs: the steps a continued run
	// retains, not every step it ran.
	Outputs *v1.Workflow_StepOutputs

	// Segments is how many workflow executions the run took.
	Segments int
}

// DurableSignal is one signal the local run delivered, replayed to the durable
// run at the same offset from the start.
type DurableSignal struct {
	Name    string
	At      time.Duration
	Payload *v1.Node_Outputs

	// Sender is the very sender the local delivery carried (a rehearsal), so a
	// gate's outputs read the same on both drivers.
	Sender *v1.SignalSender
}

// A DurableRequest is one run to prove on the durable interpreter.
type DurableRequest struct {
	Workflow *v1.Workflow
	Inputs   map[string]*v1.Value

	// Start is when the run begins on the virtual clock, as it does locally.
	Start time.Time

	// Runtime is the secret access the case grants.
	Runtime v1.TaskRuntime

	// Signals are the deliveries the local run accepted, in the order it
	// delivered them. A delivery the local run's signal policy refused is
	// absent: a server refuses it before the workflow ever sees it.
	Signals []DurableSignal
}

// A DurableRunner runs a request on the durable interpreter, one step per
// segment. ctx reaches every task, which is how the case's stubs do, and
// carries the case's trigger.
type DurableRunner func(ctx context.Context, req DurableRequest) (DurableResult, error)

// durableFailureField names a disagreement between the drivers in a report.
const durableFailureField = "driver"

type durableKey struct{}

// contextWithDurable asks the cases run under ctx to be proved on runner too.
func contextWithDurable(ctx context.Context, runner DurableRunner) context.Context {
	if runner == nil {
		return ctx
	}

	return context.WithValue(ctx, durableKey{}, runner)
}

func durableFrom(ctx context.Context) DurableRunner {
	runner, _ := ctx.Value(durableKey{}).(DurableRunner)

	return runner
}

// durableIneligible says why a case cannot be run on the durable driver, or
// the empty string when it can. The reason is reported, never swallowed: a
// green that silently skipped the proof would read as one that passed it.
func durableIneligible(test *Test, workflow *v1.Workflow, compiled []compiledStub) string {
	switch {
	case len(test.Faults) > 0:
		return "it injects faults, which only the local driver's scheduler can"
	case test.Trigger != nil:
		return "it replays a trigger delivery"
	}
	if readsRunFacts(workflow) {
		return "it reads run.local, run.identity, run.workflow_id or run.run_id, which differ by design between a local run and a durable one"
	}
	// A step-scoped stub is matched on the durable side by the step id the
	// activity carries, which names neither the workflow it runs in nor whether
	// it is a compensation: so only where neither can be confused.
	if calls := stepScopeAmbiguity.MatchString(prototext.Format(workflow)); calls {
		for i := range compiled {
			if compiled[i].step != "" {
				return "it stubs a step by id in a workflow with calls or compensations, which the durable driver cannot tell apart from the forward call it serves"
			}
		}
	}
	return ""
}

// divergentRunFacts are the facts about a run that a local run and a durable one answer
// differently by design: that the run is local, who started it, and its address.
var divergentRunFacts = map[string]bool{"local": true, "identity": true, "workflow_id": true, "run_id": true}

// readsRunFacts reports whether a workflow reads one of [divergentRunFacts] as
// `run.<fact>` anywhere an expression can stand, or reaches `run` any other
// way. Scope is not tracked: a name shadowing `run` keeps the case local, which
// costs coverage and never soundness.
func readsRunFacts(wf *v1.Workflow) bool {
	found := false
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		if !found && valueReadsRunFacts(site.Value, 0) {
			found = true
		}
	}})

	return found
}

// maxRunFactDepth bounds the descent into a value or an expression, which a
// specification need not have come from a Flowfile to nest arbitrarily; past it
// the case stays local.
const maxRunFactDepth = 64

func valueReadsRunFacts(value *v1.Value, depth int) bool {
	if depth > maxRunFactDepth {
		return true
	}
	switch kind := value.GetKind().(type) {
	case *v1.Value_Expr:
		return exprReadsRunFacts(kind.Expr.GetExpr(), 0)
	case *v1.Value_Structure_:
		switch structure := kind.Structure.GetKind().(type) {
		case *v1.Value_Structure_List_:
			return slices.ContainsFunc(structure.List.GetValues(), func(v *v1.Value) bool { return valueReadsRunFacts(v, depth+1) })
		case *v1.Value_Structure_Map_:
			return slices.ContainsFunc(slices.Collect(maps.Values(structure.Map.GetEntries())), func(v *v1.Value) bool { return valueReadsRunFacts(v, depth+1) })
		}
	}

	return false
}

func exprReadsRunFacts(e *expr.Expr, depth int) bool {
	if e == nil {
		return false
	}
	if depth > maxRunFactDepth {
		return true
	}
	var children []*expr.Expr
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		// `run` reached any way but a plain `run.<field>` below (indexed, bound
		// to a name, passed on) cannot be told apart from a read of a fact that
		// differs, so it keeps the case local.
		return kind.IdentExpr.GetName() == "run"
	case *expr.Expr_SelectExpr:
		sel := kind.SelectExpr
		if ident := sel.GetOperand().GetIdentExpr(); ident != nil && ident.GetName() == "run" {
			return divergentRunFacts[sel.GetField()]
		}
		children = append(children, sel.GetOperand())
	case *expr.Expr_CallExpr:
		children = append(children, kind.CallExpr.GetTarget())
		children = append(children, kind.CallExpr.GetArgs()...)
	case *expr.Expr_ListExpr:
		children = append(children, kind.ListExpr.GetElements()...)
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			children = append(children, entry.GetMapKey(), entry.GetValue())
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		children = append(children, c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
	}

	return slices.ContainsFunc(children, func(c *expr.Expr) bool { return exprReadsRunFacts(c, depth+1) })
}

// stepScopeAmbiguity matches a workflow that calls another or compensates a step.
var stepScopeAmbiguity = regexp.MustCompile(`\b(call|undo)\s*:`)

// durableDisagreements runs the case's workflow durably under ctx, whose
// registry carries a fresh set of the case's stubs, and reports where it
// differs from the local run that just passed: whether it finished, and, for
// every value the continued run kept, what that step produced.
//
// The comparison is the part both drivers can honestly be asked for. A
// continued run retains only the step outputs later steps read, so the steps
// it dropped are not evidence either way; the ones it kept must equal the
// local run's, because a value that changed across a seam is the bug this
// exists to find.
func durableDisagreements(ctx context.Context, runner DurableRunner, req DurableRequest,
	unanswered *unstubbedTasks, local *v1.Workflow_StepOutputs, localErr error, sensitive sensitiveInputs) (failures []*v1.Diagnostic, localOnly string) {
	workflow := req.Workflow
	got, err := runner(ctx, req)

	// The durable driver gathers no account of what its steps withhold, so a
	// value it printed could be a private one only the durable run saw (a
	// `run.local` branch choosing a different string, say). Where the workflow
	// declares anything sensitive, a disagreement names where it is and quotes
	// nothing.
	declares, declErr := v1.DeclaresSensitiveValues(workflow)
	withhold := (declares || declErr != nil || !sensitive.Empty())
	const withheld = "(withheld: the workflow declares sensitive values)"
	quote := func(text string) string {
		if withhold {
			return withheld
		}

		return redactedErrorText(text, sensitive)
	}

	failure := func(step, format string, args ...any) *v1.Diagnostic {
		return &v1.Diagnostic{
			Field:   durableFailureField,
			Step:    step,
			Message: fmt.Sprintf(format, args...),
		}
	}

	switch {
	case unanswered.matcherFailed():
		// A `where:` that reads a loop binding, say, has none to read in an
		// activity: a limit of the stub, not a disagreement about the workflow,
		// and so said whatever else happened. A `where:` that merely evaluated
		// false is an ordinary miss, compared like any other outcome.
		return nil, "a stub's where: could not be evaluated on the durable driver, so the proof is not made"
	case ctx.Err() != nil:
		return []*v1.Diagnostic{failure("", "the durable run was cancelled: %v", ctx.Err())}, ""
	case err != nil && localErr == nil:
		return []*v1.Diagnostic{failure("", "the run passed locally but failed after %d segment(s) on the durable driver: %s", got.Segments, quote(err.Error()))}, ""
	case err == nil && localErr != nil:
		return []*v1.Diagnostic{failure("", "the run failed locally (%s) but finished on the durable driver", quote(localErr.Error()))}, ""
	case err != nil:
		// Both failed. The text differs by design (the durable driver wraps an
		// activity's failure), so only that they agree it failed is a claim.
		return nil, ""
	}

	var out []*v1.Diagnostic
	kept := got.Outputs.GetStepValues()
	for _, id := range slices.Sorted(maps.Keys(kept)) {
		want, ran := local.GetStepValues()[id]
		if !ran {
			out = append(out, failure(id, "step %q ran on the durable driver (%d segments) and not locally", id, got.Segments))

			continue
		}
		for _, name := range slices.Sorted(maps.Keys(kept[id].GetNamedValues())) {
			durableValue := kept[id].GetNamedValues()[name]
			localValue, present := want.GetNamedValues()[name]
			switch {
			case !present:
				out = append(out, failure(id, "step %q produced %q on the durable driver and not locally", id, name))
			case !sameValue(localValue, durableValue):
				out = append(out, failure(id, "%s.%s changed once the run continued as new between steps:\n  local:   %s\n  durable: %s",
					id, name, quote(oneLine(localValue)), quote(oneLine(durableValue))))
			}
		}
	}

	// A declared output is a complete final result, unlike a pruned
	// intermediate one, so it is compared whole.
	localOut, durableOut := local.GetRunOutputs().GetValues(), got.Outputs.GetRunOutputs().GetValues()
	for _, name := range slices.Sorted(maps.Keys(localOut)) {
		other, present := durableOut[name]
		switch {
		case !present:
			out = append(out, failure("", "output %q was produced locally and not on the durable driver", name))
		case !sameValue(localOut[name], other):
			out = append(out, failure("", "output %q changed once the run continued as new between steps:\n  local:   %s\n  durable: %s",
				name, quote(oneLine(localOut[name])), quote(oneLine(other))))
		}
	}
	for _, name := range slices.Sorted(maps.Keys(durableOut)) {
		if _, present := localOut[name]; !present {
			out = append(out, failure("", "output %q was produced on the durable driver and not locally", name))
		}
	}

	return out, ""
}

// sameValue is equality of two values by what they mean: a map's entries are
// unordered, so two encodings of one map are one value, which byte equality of
// the messages would call two.
func sameValue(a, b *v1.Value) bool {
	if a.GetLiteral() == nil || b.GetLiteral() == nil {
		return proto.Equal(a, b)
	}
	left, errLeft := v1.LiteralToGo(a.GetLiteral())
	right, errRight := v1.LiteralToGo(b.GetLiteral())
	if errLeft != nil || errRight != nil {
		return proto.Equal(a, b)
	}

	return reflect.DeepEqual(left, right)
}

func oneLine(m proto.Message) string {
	return strings.Join(strings.Fields(fmt.Sprint(m)), " ")
}

// matcherFailed reports whether a stub's `where:` failed to evaluate.
func (u *unstubbedTasks) matcherFailed() bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.matcherErrors > 0
}

// durableSignals is what the local run accepted of the case's scripted
// signals, as the durable run is to receive them: at the same offset, from the
// same sender, in the order they were delivered. A script the local run's
// policy refused, a fault dropped, or the run ended before is absent.
func durableSignals(scripts []SignalScript, outcomes *signalOutcomes) []DurableSignal {
	var out []DurableSignal
	for _, n := range outcomes.acceptedScripts() {
		s := scripts[n]
		var at time.Duration
		if s.At != "" {
			// Parsed already by scriptSignals; a script it refused never ran.
			at, _ = time.ParseDuration(s.At)
		}
		out = append(out, DurableSignal{
			Name:    s.Name,
			At:      max(at, 0),
			Payload: &v1.Node_Outputs{NamedValues: v1.NewNamedValues(s.Payload)},
			Sender:  scriptedSender(s.Sender, s.DeliveryID),
		})
	}

	return out
}
