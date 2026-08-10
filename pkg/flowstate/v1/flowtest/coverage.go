package flowtest

import (
	"fmt"
	"sort"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Coverage is which of a workflow's steps at least one case in a `*.test.yaml`
// ran, and which no case ever reached (issue #420).
//
// It is a property of the harness, not of the workflow language: no schema
// field carries it, and it never travels a durable boundary. It is derived
// from the same transcript `expect.ran` is checked against (the
// [v1.Workflow_StepOutputs] a run hands back), so a step counts as reached on
// exactly the evidence `expect.ran` counts on, and nothing weaker.
//
// Two measurement rules, matching #420:
//
//   - A step skipped by `if:` in every case is unreached, not covered. A
//     skipped step produces no recorded outputs, so it is simply absent from
//     every transcript, and absence is what unreached means here. Being named
//     in `expect.skipped` is an assertion about one case, never evidence the
//     branch works.
//   - A step inside a `for_each` or `loop` body counts as reached if any
//     iteration ran it. The body's per-iteration outputs travel in the loop
//     node's `results` output ([v1.LoopResultsField]), so the walk descends
//     into that list and unions the step ids it finds across iterations.
type Coverage struct {
	// Reached is every step id that ran in at least one case, sorted.
	Reached []string

	// Unreached is the complement: every step id in the workflow that no case
	// ran, sorted. This is the line #420 exists to report.
	Unreached []string

	// Accepted maps an unreached step id to the reason the file recorded for it
	// (`coverage.allow_unreached`). Every key here is also in Unreached: it is
	// genuinely not reached, and Accepted is why that is a decision rather than
	// a gap. A step in Accepted does not fail `--coverage-required`.
	Accepted map[string]string

	// Stale is every `coverage.allow_unreached` entry that does not describe a
	// real residual: it names a step some case did reach, or a step id the
	// workflow does not have. Such a record is a false statement about the
	// suite, so it fails `--coverage-required` the same way an unrecorded gap
	// does. Each entry is a sentence naming the step and what is wrong.
	Stale []string
}

// Total is how many steps the workflow has that coverage accounts for.
func (c *Coverage) Total() int { return len(c.Reached) + len(c.Unreached) }

// Gaps is every unreached step the file did not record a reason for: the holes
// in the suite, as opposed to the residuals it accepted. This is what
// `--coverage-required` fails on, together with [Coverage.Stale].
func (c *Coverage) Gaps() []string {
	var gaps []string
	for _, id := range c.Unreached {
		if _, accepted := c.Accepted[id]; !accepted {
			gaps = append(gaps, id)
		}
	}
	return gaps
}

// coverageAccumulator gathers, across every case in one file, the universe of
// a workflow's steps and the union of the steps any case reached.
//
// The universe is a union across cases rather than one workflow's, because a
// `*.test.yaml`'s cases each name their own `workflow:` and could in principle
// target different files. In the ordinary case they all target one workflow
// and the union is exactly that workflow's steps.
type coverageAccumulator struct {
	universe map[string]bool
	reached  map[string]bool

	// allowUnreached is the file's `coverage.allow_unreached`, the residuals it
	// recorded a reason for. Read at result time to partition Unreached into
	// accepted residuals and unrecorded gaps, and to catch a record that no
	// longer describes a real residual.
	allowUnreached map[string]string
}

func newCoverageAccumulator(allowUnreached map[string]string) *coverageAccumulator {
	return &coverageAccumulator{
		universe:       map[string]bool{},
		reached:        map[string]bool{},
		allowUnreached: allowUnreached,
	}
}

// observe folds one case's compiled workflow and its transcript into the
// running coverage. spec may be nil for a case that never compiled a workflow;
// outputs may be nil for a case whose run failed (a failed run returns no step
// outputs), in which case the case widens the universe but reaches nothing.
func (a *coverageAccumulator) observe(spec *v1.Workflow, outputs *v1.Workflow_StepOutputs) {
	if spec != nil {
		collectStepUniverse(spec.GetSteps(), a.universe)
	}
	if spec == nil || outputs == nil {
		return
	}
	markReached(spec.GetSteps(), outputs.GetStepValues(), a.reached)
}

// result renders the accumulated coverage, or nil when no case contributed a
// workflow to account for (a refused file, or one whose every case failed to
// compile). A nil result is the signal to report no coverage line at all,
// rather than a misleading "0/0 steps reached".
func (a *coverageAccumulator) result() *Coverage {
	if len(a.universe) == 0 {
		return nil
	}

	cov := &Coverage{}
	for id := range a.universe {
		if a.reached[id] {
			cov.Reached = append(cov.Reached, id)
		} else {
			cov.Unreached = append(cov.Unreached, id)
		}
	}
	sort.Strings(cov.Reached)
	sort.Strings(cov.Unreached)

	// Partition the recorded residuals: an entry that names a genuinely
	// unreached step of this workflow is accepted; one that names a reached
	// step or a step this workflow does not have is stale, a false record that
	// fails the same way a gap does. The staleness check is the "assert a bound
	// was reached as well as not exceeded" discipline (CLAUDE.md) applied to
	// the record itself: a reason kept past the branch it explained is how the
	// record stops meaning anything.
	for step, reason := range a.allowUnreached {
		switch {
		case a.reached[step]:
			cov.Stale = append(cov.Stale, fmt.Sprintf(
				"coverage.allow_unreached names %q, but a case reached it; remove the entry", step))
		case !a.universe[step]:
			cov.Stale = append(cov.Stale, fmt.Sprintf(
				"coverage.allow_unreached names %q, which is not a step in this workflow; fix the id or remove the entry", step))
		default:
			if cov.Accepted == nil {
				cov.Accepted = map[string]string{}
			}
			cov.Accepted[step] = reason
		}
	}
	sort.Strings(cov.Stale)
	return cov
}

// collectStepUniverse walks a workflow's node tree, adding every step id
// coverage accounts for to universe.
//
// It descends into the bodies of `for_each` and `loop` and into the branches
// of `parallel`, because those hold steps an author wrote and a test can leave
// unreached. It does not descend into a `call:`, whose steps belong to the
// callee's own file and are that file's coverage to account for.
//
// A `parallel` container is not itself counted: it records no outputs of its
// own (the engine merges its branches' outputs and returns nothing under the
// container's id), so counting it would report a step that no transcript can
// ever show as reached. Its branch steps are counted, and they are what
// `parallel:` coverage is about.
func collectStepUniverse(nodes []*v1.Node, universe map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				collectStepUniverse(branch.GetSteps(), universe)
			}
		case *v1.Node_ForEach:
			universe[node.GetId()] = true
			collectStepUniverse(kind.ForEach.GetBody(), universe)
		case *v1.Node_Loop:
			universe[node.GetId()] = true
			collectStepUniverse(kind.Loop.GetBody(), universe)
		default:
			// Task, Wait, Call: a leaf for coverage. A call's own steps are the
			// callee file's to account for, so the call node is counted but not
			// descended into.
			universe[node.GetId()] = true
		}
	}
}

// markReached walks a workflow's node tree against a transcript, marking every
// step the transcript shows as having run.
//
// present is a map from step id to the outputs recorded for it. A top-level
// step and a `parallel` branch step both appear here directly (the engine
// merges a parallel's branch outputs into the enclosing scope). A `for_each` or
// `loop` body step does not: its outputs travel inside the loop node's own
// `results` output, so reaching it means descending into that list.
func markReached(nodes []*v1.Node, present map[string]*v1.Node_Outputs, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			// The container records nothing; its branch steps are merged into
			// present directly, so recurse with the same map.
			for _, branch := range kind.Parallel.GetBranches() {
				markReached(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.ForEach.GetBody(), resultsList(out.GetNamedValues()), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.Loop.GetBody(), resultsList(out.GetNamedValues()), reached)
		default:
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
		}
	}
}

// markReachedInResults descends into a `for_each` or `loop`'s per-iteration
// `results` list, marking every body step any iteration ran.
//
// Each iteration is a map literal from body step id to that step's outputs, so
// a step is reached if it appears in any iteration. A nested `for_each`/`loop`
// carries its own `results` field inside its entry, which is why this recurses
// through [markReachedInLiteral] rather than only unioning the top-level keys:
// a loop two levels down is still a step some case either reached or did not.
func markReachedInResults(body []*v1.Node, iterations []*expr.Value, reached map[string]bool) {
	for _, iteration := range iterations {
		lit := mapEntries(iteration)
		if lit == nil {
			continue
		}
		markReachedInLiteral(body, lit, reached)
	}
}

// markReachedInLiteral is [markReached] over a literal transcript rather than a
// [v1.Node_Outputs] one: inside a loop's `results`, a step's outputs are a CEL
// map literal, not a [v1.Node_Outputs]. The two walks are the same shape over
// two encodings of the same fact.
func markReachedInLiteral(nodes []*v1.Node, present map[string]*expr.Value, reached map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				markReachedInLiteral(branch.GetSteps(), present, reached)
			}
		case *v1.Node_ForEach:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.ForEach.GetBody(), resultsListLiteral(out), reached)
		case *v1.Node_Loop:
			out, ok := present[node.GetId()]
			if !ok {
				continue
			}
			reached[node.GetId()] = true
			markReachedInResults(kind.Loop.GetBody(), resultsListLiteral(out), reached)
		default:
			if _, ok := present[node.GetId()]; ok {
				reached[node.GetId()] = true
			}
		}
	}
}

// resultsList pulls the `results` list out of a loop node's recorded outputs.
func resultsList(named map[string]*v1.Value) []*expr.Value {
	return named[v1.LoopResultsField].GetLiteral().GetListValue().GetValues()
}

// resultsListLiteral pulls the `results` list out of a loop node's outputs when
// those outputs are themselves a CEL map literal, as they are one level down
// inside another loop's `results`.
func resultsListLiteral(v *expr.Value) []*expr.Value {
	for _, entry := range v.GetMapValue().GetEntries() {
		if entry.GetKey().GetStringValue() == v1.LoopResultsField {
			return entry.GetValue().GetListValue().GetValues()
		}
	}
	return nil
}

// mapEntries reads a CEL map literal into a lookup by string key, or nil when
// the value is not a map.
func mapEntries(v *expr.Value) map[string]*expr.Value {
	m := v.GetMapValue()
	if m == nil {
		return nil
	}
	out := make(map[string]*expr.Value, len(m.GetEntries()))
	for _, entry := range m.GetEntries() {
		out[entry.GetKey().GetStringValue()] = entry.GetValue()
	}
	return out
}
