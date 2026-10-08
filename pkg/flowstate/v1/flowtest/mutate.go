package flowtest

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Mutation testing asks the question a green test file cannot answer for
// itself: would it notice the program changing? A mutant is the compiled
// workflow with one deliberate fault; a file whose cases still pass against it
// proves nothing about that part of the program.
//
// The program under test is a [v1.Workflow] message, so a mutant is one field
// edit on a clone of it. There is no parser in the loop and no source rewriting,
// and both drivers read the same message, so a mutant means the same thing to
// each.

// DefaultMutants and MaxMutants bound how many mutants one workflow is tested
// against. Each runs every passing case that targets the workflow, so the
// bound is the work limit.
const (
	DefaultMutants = 100
	MaxMutants     = 1000
)

// MutateOptions asks [RunOptions] to run each workflow's passing cases against
// mutants of it.
type MutateOptions struct {
	// Max bounds the mutants run per workflow; zero asks for no mutation.
	Max int

	// Only, when set, runs the one mutant with that id and no other.
	Only string
}

func (o MutateOptions) enabled() bool { return o.Max > 0 || o.Only != "" }

// A mutant is one change to a workflow: where it is made and how to make it.
type mutant struct {
	// id is `operator@step.field`, stable across runs so a survivor can be
	// replayed by name.
	id       string
	operator string
	// step is the id of the changed step, and ordinal its position in the
	// workflow's document-order walk: two bodies may each declare a step with one id.
	step    string
	ordinal int
	field   string
	// ambiguous marks a step id the workflow declares more than once, whose
	// source position cannot be told from its twin's.
	ambiguous bool
	// describe is the change in words.
	describe string
	// apply makes the change on the node a clone of the base workflow holds at
	// ordinal, and reports whether it did.
	apply func(*v1.Node) bool
}

// mutants enumerates, in document order and then operator order, every change
// the fixed operator set can make to wf.
func mutants(wf *v1.Workflow) []mutant {
	var out []mutant
	// A step id may repeat across loop and switch bodies; every occurrence
	// after the first is named `id#N` so a mutant id names exactly one.
	declared := map[string]int{}
	repeated := map[string]bool{}
	v1.WalkWorkflow(wf, v1.Walk{Node: func(node *v1.Node) {
		if declared[node.GetId()]++; declared[node.GetId()] > 1 {
			repeated[node.GetId()] = true
		}
	}})
	seen := map[string]int{}
	ordinal := -1
	v1.WalkWorkflow(wf, v1.Walk{Node: func(node *v1.Node) {
		ordinal++
		seen[node.GetId()]++
		name := node.GetId()
		if seen[name] > 1 {
			name += "#" + strconv.Itoa(seen[name])
		}
		add := func(operator, field, describe string, apply func(*v1.Node) bool) {
			out = append(out, mutant{
				id:        operator + "@" + name + "." + field,
				ambiguous: repeated[node.GetId()],
				operator:  operator,
				step:      node.GetId(),
				ordinal:   ordinal,
				field:     field,
				describe:  describe,
				apply:     apply,
			})
		}
		step := node.GetId()

		if node.GetCondition() != nil {
			// A literal condition has no expression to wrap; dropping it is
			// the mutant it has.
			if node.GetCondition().GetExpr() != nil {
				add("if-negate", "if", fmt.Sprintf("`if:` negated on step %s", step), func(n *v1.Node) bool {
					negated, ok := negate(n.GetCondition())
					n.Condition = negated

					return ok
				})
			}
			add("if-drop", "if", fmt.Sprintf("`if:` removed from step %s", step), func(n *v1.Node) bool {
				n.Condition = nil

				return true
			})
		}
		if node.GetUndo() != nil {
			add("undo-drop", "undo", fmt.Sprintf("`undo:` removed from step %s", step), func(n *v1.Node) bool {
				n.Undo = nil

				return true
			})
		}
		if node.GetPolicy().GetRetry() != nil {
			add("retry-drop", "retry", fmt.Sprintf("`retry:` removed from step %s", step), func(n *v1.Node) bool {
				n.Policy.Retry = nil

				return true
			})
		}
		if node.GetPolicy() != nil {
			add("continue-flip", "continue_on_error",
				fmt.Sprintf("`continue_on_error:` flipped to %t on step %s", !node.GetPolicy().GetContinueOnError(), step),
				func(n *v1.Node) bool {
					n.Policy.ContinueOnError = !n.Policy.GetContinueOnError()

					return true
				})
		}
		if sw := node.GetSwitch(); sw != nil {
			for i := range sw.GetCases() {
				add("switch-arm-drop", fmt.Sprintf("case[%d]", i),
					fmt.Sprintf("switch case %d removed from step %s", i, step),
					func(n *v1.Node) bool {
						cases := n.GetSwitch().GetCases()
						if i >= len(cases) {
							return false
						}
						n.GetSwitch().Cases = slices.Delete(cases, i, i+1)

						return true
					})
			}
			if sw.GetDefault() != nil {
				add("switch-default-drop", "default", fmt.Sprintf("switch default removed from step %s", step),
					func(n *v1.Node) bool {
						n.GetSwitch().Default = nil

						return true
					})
			}
		}
	}})

	return out
}

// negate wraps a condition in `!`. It reports false for a value that is not an
// expression (a literal condition), which has nothing to wrap.
func negate(cond *v1.Value) (*v1.Value, bool) {
	parsed := cond.GetExpr()
	if parsed == nil {
		return cond, false
	}
	wrapped := proto.Clone(parsed).(*expr.ParsedExpr)
	// A fresh id no node of the original holds: expression ids only have to be
	// unique within the tree.
	wrapped.Expr = &expr.Expr{
		Id: math.MaxInt64,
		ExprKind: &expr.Expr_CallExpr{CallExpr: &expr.Expr_Call{
			Function: "!_",
			Args:     []*expr.Expr{wrapped.GetExpr()},
		}},
	}

	return &v1.Value{Kind: &v1.Value_Expr{Expr: wrapped}}, true
}

// A mutationCase is one passing case the mutants are run against.
type mutationCase struct {
	test         Test
	deliveryPath string
}

// A mutationGroup is the cases that target one workflow, with the compiled
// workflow they ran against.
type mutationGroup struct {
	identity  string
	base      *v1.Workflow
	positions func() *flowfile.Positions
	cases     []mutationCase
}

// mutator accumulates one file's mutation testing: it sees each case as the
// suite runs it, and judges the mutants once the suite is done.
type mutator struct {
	opts   MutateOptions
	groups []*mutationGroup
	// red is set when any case failed: the file's tests cannot tell a killed
	// mutant from a broken test.
	red bool
}

func newMutator(opts MutateOptions) *mutator {
	if !opts.enabled() {
		return nil
	}

	return &mutator{opts: opts}
}

// observe records one case after it ran. A case that did not pass makes the
// whole file unmutatable; a case that replays a trigger delivery is left out,
// since the delivery maps its inputs and a mutant is judged on the authored run.
func (m *mutator) observe(identity string, test *Test, spec *v1.Workflow, positions func() *flowfile.Positions,
	deliveryPath string, result *v1.TestCase) {
	if m == nil {
		return
	}
	if !result.GetPassed() || result.GetError() != "" {
		m.red = true

		return
	}
	if spec == nil || test.Trigger != nil {
		return
	}
	var group *mutationGroup
	for _, g := range m.groups {
		if g.identity == identity {
			group = g
		}
	}
	if group == nil {
		group = &mutationGroup{identity: identity, base: spec, positions: positions}
		m.groups = append(m.groups, group)
	}
	group.cases = append(group.cases, mutationCase{test: *test, deliveryPath: deliveryPath})
}

// run judges every mutant of every observed workflow and reports. posture is
// the suite's: a survivor's id and description are made of step names, which
// are the file's own words and can spell a withheld value (#2229), so they go
// through the same seam a case's error does; a posture that withholds every
// name leaves the operator alone. partial says a selection left cases out.
func (m *mutator) run(ctx context.Context, vars fileVars, timeout time.Duration, posture sensitiveInputs, partial bool) *v1.MutationReport {
	if m == nil {
		return nil
	}
	report := &v1.MutationReport{}
	if partial {
		report.NotRun = "--run left cases out, and a gate only an unselected case asserts would read as a survivor; run the whole file"

		return report
	}
	if m.red {
		report.NotRun = "a case in this file did not pass, and a red suite cannot tell a killed mutant from a broken test; fix it first"

		return report
	}
	for _, group := range m.groups {
		all := mutants(group.base)
		if m.opts.Only != "" {
			all = slices.DeleteFunc(all, func(mu mutant) bool { return mu.id != m.opts.Only })
		}
		limit := cmp.Or(m.opts.Max, MaxMutants)
		if m.opts.Only == "" && len(all) > limit {
			all = all[:limit]
			report.Truncated = true
		}
		for _, mu := range all {
			if ctx.Err() != nil {
				return report
			}
			report.Mutants++
			switch m.judge(ctx, group, mu, vars, timeout) {
			case mutantInvalid:
				report.Invalid++
			case mutantKilled:
				report.Killed++
			case mutantSurvived:
				survivor := &v1.MutationSurvivor{Workflow: group.identity}
				survivor.Operator = mu.operator
				if posture.WithholdAll() {
					survivor.Id = mu.operator
				} else {
					survivor.Id = redactedErrorText(mu.id, posture)
					survivor.Description = redactedErrorText(mu.describe, posture)
					survivor.Where = whereOf(group.identity, group.positions(), mu)
				}
				report.Survivors = append(report.Survivors, survivor)
			}
		}
	}

	return report
}

type mutantVerdict int

const (
	mutantInvalid mutantVerdict = iota
	mutantKilled
	mutantSurvived
)

// judge runs the group's cases against one mutant: killed on the first case
// that no longer passes, survived when every case still does, invalid when the
// mutant is not a workflow the validator accepts.
func (m *mutator) judge(ctx context.Context, group *mutationGroup, mu mutant, vars fileVars, timeout time.Duration) mutantVerdict {
	mutated := proto.Clone(group.base).(*v1.Workflow)
	var target *v1.Node
	ordinal := -1
	v1.WalkWorkflow(mutated, v1.Walk{Node: func(node *v1.Node) {
		ordinal++
		if ordinal == mu.ordinal {
			target = node
		}
	}})
	if target == nil || !mu.apply(target) {
		return mutantInvalid
	}
	if flowfile.Validate(mutated).Err() != nil || v1.Validate(mutated) != nil {
		return mutantInvalid
	}
	load := func() (*v1.Workflow, error) { return proto.Clone(mutated).(*v1.Workflow), nil }
	for _, c := range group.cases {
		test := c.test
		runCtx, cancel := caseContextWithin(ctx, timeout)
		result, _, _, _, _, _ := runCase(runCtx, &test, c.deliveryPath, load, false, vars)
		cancel()
		if ctx.Err() != nil {
			return mutantInvalid
		}
		if !result.GetPassed() {
			return mutantKilled
		}
	}

	return mutantSurvived
}

// whereOf is `line` of the construct a mutant changed, or empty.
func whereOf(identity string, positions *flowfile.Positions, mu mutant) string {
	if mu.ambiguous {
		return ""
	}
	span, ok := positions.Locate(mu.step, mu.field)
	if !ok || !span.IsValid() {
		return ""
	}

	return fmt.Sprintf("%s:%d", identity, span.Start.Line)
}
