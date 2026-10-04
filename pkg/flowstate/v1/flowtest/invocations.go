package flowtest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// InvocationClaim is one entry of [Expectation.Invocations]: a claim about how
// many times a task ran, or in what order steps ran their tasks, judged
// against the log every stubbed or unstubbed task invocation appends to.
//
// An entry takes one of two shapes:
//
//   - a target (`task:` or `step:`, exactly one) with a quantity (`count:`,
//     `never:`, or `at_least:` and/or `at_most:`), saying how often the target
//     was invoked;
//   - `order:`, a list of at least two step ids whose first invocations must
//     appear in that relative order.
//
// A `task:` target counts every invocation of that task anywhere in the run,
// callees and `undo:` compensations included. A `step:` target counts the
// invocations made on behalf of that step of the workflow under test, one per
// attempt, so a `retry:` that ran the task three times counts three; a
// compensation is not "the step running again" and is not counted.
type InvocationClaim struct {
	// Task is the task name whose invocations are counted.
	Task string `yaml:"task"`

	// Step is the step id, in the workflow under test, whose invocations are
	// counted.
	Step string `yaml:"step"`

	// Count is the exact number of invocations. A pointer because zero is a
	// claim; `never: true` is its readable spelling.
	Count *int `yaml:"count"`

	// AtLeast and AtMost bound the number of invocations from either side;
	// together they state a range.
	AtLeast *int `yaml:"at_least"`
	AtMost  *int `yaml:"at_most"`

	// Never claims the target was not invoked at all.
	Never bool `yaml:"never"`

	// Order names steps whose first invocations must occur in this relative
	// order. Refused when a named step sits inside a `parallel:` block, where
	// the order is not observable (see `--seeds`).
	Order []string `yaml:"order"`
}

// maxInvocationLog bounds one case's invocation log. Past it the log latches
// full and every invocation claim fails closed rather than judging a prefix:
// a count over a truncated log is a number that reads as a fact.
const maxInvocationLog = 100_000

// invocation is one task invocation: the task, and the step it was made for
// ("" for a compensation, which runs off the run-level context).
type invocation struct {
	task string
	ref  auth.StepRef
}

// invocationLog is the append-only record of a case's task invocations. It is
// separate from the transcript's recorder because the transcript is optional
// and truncating, and a claim needs a complete log whenever one is declared.
type invocationLog struct {
	mu   sync.Mutex
	list []invocation
	full bool
}

type invocationLogKey struct{}

func contextWithInvocationLog(ctx context.Context, l *invocationLog) context.Context {
	return context.WithValue(ctx, invocationLogKey{}, l)
}

// noteInvocation appends the invocation of task running under ctx to the
// case's log, when it keeps one.
func noteInvocation(ctx context.Context, task string) {
	l, _ := ctx.Value(invocationLogKey{}).(*invocationLog)
	if l == nil {
		return
	}
	ref, _ := v1.TaskStepRefFromContext(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.list) >= maxInvocationLog {
		l.full = true
		return
	}
	l.list = append(l.list, invocation{task: task, ref: ref})
}

// count reports how many invocations match the claim's target in a workflow
// named root.
func (l *invocationLog) count(c *InvocationClaim, root string) int {
	n := 0
	for _, inv := range l.list {
		if c.Task != "" && inv.task == c.Task || c.Step != "" && inv.ref.Step == c.Step && inv.ref.Workflow == root {
			n++
		}
	}

	return n
}

// first reports the index of the first invocation made for step in root, or -1.
func (l *invocationLog) first(step, root string) int {
	return slices.IndexFunc(l.list, func(inv invocation) bool {
		return inv.ref.Step == step && inv.ref.Workflow == root
	})
}

// checkInvocationShape refuses a malformed entry, named by its position in the
// list. It is the part of the judgment that needs no workflow.
func checkInvocationShape(i int, c *InvocationClaim) error {
	where := fmt.Sprintf("expect.invocations[%d]", i)
	quantities := 0
	for _, set := range []bool{c.Count != nil, c.Never, c.AtLeast != nil || c.AtMost != nil} {
		if set {
			quantities++
		}
	}
	switch {
	case len(c.Order) > 0:
		switch {
		case c.Task != "" || c.Step != "" || quantities > 0:
			return fmt.Errorf("%s: `order:` stands alone; it takes no task, step or quantity", where)
		case len(c.Order) < 2:
			return fmt.Errorf("%s: `order:` needs at least two steps to order", where)
		}
		seen := map[string]bool{}
		for _, step := range c.Order {
			if seen[step] {
				return fmt.Errorf("%s: `order:` names step %q twice", where, step)
			}
			seen[step] = true
		}

		return nil
	case (c.Task == "") == (c.Step == ""):
		return fmt.Errorf("%s: name exactly one of `task:`, `step:` or `order:`", where)
	case quantities == 0:
		return fmt.Errorf("%s: say how many with `count:`, `never: true`, `at_least:` or `at_most:`", where)
	case quantities > 1:
		return fmt.Errorf("%s: `count:`, `never:` and `at_least:`/`at_most:` are alternatives; give one", where)
	}
	for field, n := range map[string]*int{"count": c.Count, "at_least": c.AtLeast, "at_most": c.AtMost} {
		if n != nil && *n < 0 {
			return fmt.Errorf("%s: %s: %d is negative; an invocation count is never below zero", where, field, *n)
		}
	}
	if c.AtLeast != nil && c.AtMost != nil && *c.AtLeast > *c.AtMost {
		return fmt.Errorf("%s: at_least %d exceeds at_most %d, a range nothing satisfies", where, *c.AtLeast, *c.AtMost)
	}

	return nil
}

// checkInvocationNames refuses a claim naming a step the workflow under test
// cannot make it about, before the run, for the reason [checkExpectationNames]
// does: a claim about a ghost passes forever while reading as a check.
func checkInvocationNames(claims []InvocationClaim, spec *v1.Workflow) error {
	if len(claims) == 0 {
		return nil
	}
	all := map[string]bool{}
	collectAllStepIDs(spec.GetSteps(), all)
	containers := map[string]bool{}
	parallelContainers(spec.GetSteps(), containers)
	underParallel := map[string]bool{}
	stepsUnderParallel(spec.GetSteps(), false, underParallel)
	names := slices.Sorted(maps.Keys(all))

	step := func(i int, field, id string, ordered bool) error {
		where := fmt.Sprintf("expect.invocations[%d]", i)
		switch {
		case containers[id]:
			return fmt.Errorf("%s: %s names step %q, a parallel container that invokes no task itself; "+
				"name the branch steps instead", where, field, id)
		case !all[id]:
			if suggestion, ok := nearest.Name(id, names); ok {
				return fmt.Errorf("%s: %s names unknown step %q; did you mean %q?", where, field, id, suggestion)
			}

			return fmt.Errorf("%s: %s names unknown step %q, which this workflow has no step for", where, field, id)
		case ordered && underParallel[id]:
			return fmt.Errorf("%s: order names step %q, which runs inside a `parallel:` block, where the "+
				"order of invocations is not observable; order the steps around the block instead", where, id)
		}

		return nil
	}

	var tasks []string
	for i := range claims {
		c := &claims[i]
		if c.Task != "" {
			if tasks == nil {
				var err error
				if tasks, err = v1.RequiredTaskNames(spec); err != nil {
					return fmt.Errorf("expect.invocations: %w", err)
				}
			}
			if !slices.Contains(tasks, c.Task) {
				where := fmt.Sprintf("expect.invocations[%d]", i)
				if suggestion, ok := nearest.Name(c.Task, tasks); ok {
					return fmt.Errorf("%s: task names unknown task %q; did you mean %q?", where, c.Task, suggestion)
				}

				return fmt.Errorf("%s: task names unknown task %q, which this workflow, its callees and its compensations never invoke", where, c.Task)
			}
		}
		if c.Step != "" {
			if err := step(i, "step", c.Step, false); err != nil {
				return err
			}
		}
		for _, id := range c.Order {
			if err := step(i, "order", id, true); err != nil {
				return err
			}
		}
	}

	return nil
}

// stepsUnderParallel records every step id declared inside a `parallel:`
// branch, at any depth short of a callee.
func stepsUnderParallel(nodes []*v1.Node, inside bool, out map[string]bool) {
	for _, node := range nodes {
		if inside {
			out[node.GetId()] = true
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				stepsUnderParallel(branch.GetSteps(), true, out)
			}
		case *v1.Node_Switch:
			for _, body := range v1.SwitchBodies(kind.Switch) {
				stepsUnderParallel(body, inside, out)
			}
		case *v1.Node_ForEach:
			stepsUnderParallel(kind.ForEach.GetBody(), inside, out)
		case *v1.Node_Loop:
			stepsUnderParallel(kind.Loop.GetBody(), inside, out)
		}
	}
}

// assertInvocations judges the claims against the finished run's log. A log
// the bound truncated fails every claim: nothing honest can be said from it.
func assertInvocations(claims []InvocationClaim, root string, l *invocationLog) []*v1.Diagnostic {
	if len(claims) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var failures []*v1.Diagnostic
	fail := func(i int, step, format string, args ...any) {
		failures = append(failures, &v1.Diagnostic{
			Step:    step,
			Field:   fmt.Sprintf("expect.invocations[%d]", i),
			Message: fmt.Sprintf(format, args...),
		})
	}
	if l.full {
		for i := range claims {
			fail(i, claims[i].Step, "the run made more than %d task invocations, so its log is incomplete and "+
				"this claim cannot be judged; narrow the case", maxInvocationLog)
		}

		return failures
	}

	for i := range claims {
		c := &claims[i]
		if len(c.Order) > 0 {
			failures = append(failures, orderFailures(i, c, root, l)...)

			continue
		}
		target := "task " + fmt.Sprintf("%q", c.Task)
		if c.Step != "" {
			target = "step " + fmt.Sprintf("%q", c.Step)
		}
		got := l.count(c, root)
		switch {
		case c.Never && got > 0:
			fail(i, c.Step, "expected %s never to be invoked, but it was invoked %d time(s)", target, got)
		case c.Count != nil && got != *c.Count:
			fail(i, c.Step, "expected %s to be invoked %d time(s), got %d", target, *c.Count, got)
		case c.AtLeast != nil && got < *c.AtLeast:
			fail(i, c.Step, "expected %s to be invoked at least %d time(s), got %d", target, *c.AtLeast, got)
		case c.AtMost != nil && got > *c.AtMost:
			fail(i, c.Step, "expected %s to be invoked at most %d time(s), got %d", target, *c.AtMost, got)
		}
	}

	return failures
}

// orderFailures judges one `order:` entry: every named step must have been
// invoked, and the first invocations must occur in the order written.
func orderFailures(i int, c *InvocationClaim, root string, l *invocationLog) []*v1.Diagnostic {
	positions := make([]int, len(c.Order))
	for j, step := range c.Order {
		positions[j] = l.first(step, root)
		if positions[j] < 0 {
			return []*v1.Diagnostic{{
				Step:    step,
				Field:   fmt.Sprintf("expect.invocations[%d]", i),
				Message: fmt.Sprintf("expected step %q to be invoked as part of the order [%s], but it never was", step, strings.Join(c.Order, ", ")),
			}}
		}
	}
	if slices.IsSorted(positions) {
		return nil
	}
	actual := slices.Clone(c.Order)
	slices.SortStableFunc(actual, func(a, b string) int {
		return positions[slices.Index(c.Order, a)] - positions[slices.Index(c.Order, b)]
	})

	return []*v1.Diagnostic{{
		Field:   fmt.Sprintf("expect.invocations[%d]", i),
		Message: fmt.Sprintf("expected steps to be invoked in the order [%s], but they were invoked in the order [%s]", strings.Join(c.Order, ", "), strings.Join(actual, ", ")),
	}}
}
