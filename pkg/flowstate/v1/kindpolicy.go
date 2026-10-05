package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// A step's policy can name failure kinds: `continue_on_error: [kinds]` narrows
// what is tolerated, and `retry.only` narrows what is retried.
// This file is the one place that decides what those lists mean, so the local
// driver, the durable driver and the validator cannot spell it three ways.

// StepTolerates reports whether a step's policy lets the run continue past a
// failure of the given kind: the step tolerates failures at all, and either
// names no kinds (every kind) or names this one.
func StepTolerates(policy *StepPolicy, kind ErrorKind) bool {
	if !policy.GetContinueOnError() {
		return false
	}
	kinds := policy.GetToleratedKinds()

	return len(kinds) == 0 || slices.Contains(kinds, kind.String())
}

// ToleratesEveryKind reports whether a step's tolerance is unscoped. The
// durable driver hands only that to the activity boundary, which cannot know a
// failure's kind before it happens and uses the answer to label the failure
// benign.
func ToleratesEveryKind(policy *StepPolicy) bool {
	return policy.GetContinueOnError() && len(policy.GetToleratedKinds()) == 0
}

// RetryAllowsKind reports whether a retry policy's `only:` allows another
// attempt after a failure of kind.
//
// It only ever narrows. A caller must still ask whether the failure is
// retryable at all ([RetryPermitted]): naming a permanent kind in `only:` does not
// make it retried, and the validator refuses the list that tries.
func RetryAllowsKind(retry *RetryPolicy, kind ErrorKind) bool {
	on := retry.GetOnly()

	return len(on) == 0 || slices.Contains(on, kind.String())
}

// RetryExcludedKinds returns the retryable built-in kinds a retry policy's `only:`
// rules out, which the durable driver adds to the activity's non-retryable error
// types: when `only:` is written, every retryable kind it does not name. Permanent kinds are not repeated here; they are already
// never retried.
func RetryExcludedKinds(retry *RetryPolicy) []string {
	var excluded []string
	for _, kind := range RetryableErrorKinds() {
		if !RetryAllowsKind(retry, kind) {
			excluded = append(excluded, kind.String())
		}
	}

	return excluded
}

// calleeDeclaredKinds returns the kinds a `call:` step's callee, and whatever it
// calls in turn, declares under `errors:`: failures the caller's own file never
// names and can still see arrive at the call step. Empty for any other step.
func calleeDeclaredKinds(node *Node) map[string]struct{} {
	if node.GetCall() == nil {
		return map[string]struct{}{}
	}

	return declaredKindsFrom(node.GetCall().GetWorkflow(), 1)
}

// declaredKindsFrom returns what root declares under `errors:` and what every
// workflow it calls declares, root being at the given call depth.
//
// Bounded where it is spent, because this runs at admission, before the call
// depth guard has looked at the specification: an explicit stack rather than
// recursion, callees followed only to [MaxCallDepth], and at most
// [maxStructureWalkNodes] nodes visited across all of them. Running out stops the
// search, so a kind it did not reach is refused, which fails closed.
func declaredKindsFrom(root *Workflow, depth int) map[string]struct{} {
	kinds := map[string]struct{}{}

	type frame struct {
		workflow *Workflow
		depth    int
	}
	stack := []frame{{root, depth}}
	nodesLeft := maxStructureWalkNodes
	for len(stack) > 0 && nodesLeft > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, name := range DeclaredErrorNames(top.workflow) {
			kinds[name] = struct{}{}
		}
		if top.depth >= MaxCallDepth {
			continue
		}
		WalkNodes(top.workflow.GetSteps(), Walk{Node: func(inner *Node) {
			nodesLeft--
			if call := inner.GetCall(); call != nil && nodesLeft > 0 {
				stack = append(stack, frame{call.GetWorkflow(), top.depth + 1})
			}
		}})
	}

	return kinds
}

// ReportableFailureKind reports whether a run of wf can end with a failure of
// this kind: a built-in kind, or one wf or any workflow it calls declares. It is
// what lets a server report a declared kind as a run's `kind` without trusting
// an arbitrary string a failure happens to carry.
func ReportableFailureKind(wf *Workflow, kind string) bool {
	if _, builtin := ParseErrorKind(kind); builtin {
		return true
	}
	_, declared := declaredKindsFrom(wf, 0)[kind]

	return declared
}

// failureKindsAt answers, for one step, which kinds beyond the built-in ones its
// kind lists may name: the workflow's own and, on a `call:` step, its callee's.
// Computed once per step so that checking many kinds does not walk the callees
// many times.
type failureKindsAt struct {
	wf     *Workflow
	callee map[string]struct{}
}

func newFailureKindsAt(wf *Workflow, node *Node) failureKindsAt {
	return failureKindsAt{wf: wf, callee: calleeDeclaredKinds(node)}
}

func (k failureKindsAt) known(kind string) bool {
	_, declared := k.callee[kind]

	return declared || KnownFailureKind(k.wf, kind)
}

// FailureKindNamesAt lists the declared kinds a step's kind lists may name beside
// the built-in ones, for a did-you-mean: the workflow's own and, on a `call:`
// step, its callee's.
func FailureKindNamesAt(wf *Workflow, node *Node) []string {
	return append(DeclaredErrorNames(wf), slices.Sorted(maps.Keys(calleeDeclaredKinds(node)))...)
}

// KnownFailureKindAt is [KnownFailureKind] for the step the kind is written on:
// a `call:` step can also fail with what its callee declares, which the caller
// need not repeat to tolerate it.
func KnownFailureKindAt(wf *Workflow, node *Node, kind string) bool {
	return newFailureKindsAt(wf, node).known(kind)
}

// PolicyKindProblem is one refusal of a step policy's kind lists.
type PolicyKindProblem struct {
	// Field is the key the problem is about: `continue_on_error` or `retry.only`.
	Field string

	// Kind is the offending kind, empty for a problem about the list itself.
	Kind string

	// Message is the sentence, worded once for the compiler and for submit.
	Message string
}

// PolicyKindProblems reports what is wrong with the kind lists on one step's
// policy, against the errors wf declares. Nothing is wrong with a policy that
// names no kinds.
//
// A named kind must be one the step can fail with ([KnownFailureKindAt]): built in,
// declared by wf, or, on a `call:` step, declared by its callee. In
// `retry.only:` it must also be retryable: a permanent kind is not retried because
// a list names it, and a declared error is never retried, so naming either would
// promise a retry that never happens.
func PolicyKindProblems(wf *Workflow, node *Node) []PolicyKindProblem {
	policy := node.GetPolicy()
	if policy == nil {
		return nil
	}

	known := newFailureKindsAt(wf, node).known
	var problems []PolicyKindProblem
	add := func(field, kind, format string, args ...any) {
		problems = append(problems, PolicyKindProblem{
			Field:   field,
			Kind:    kind,
			Message: fmt.Sprintf("step %q: %s", node.GetId(), fmt.Sprintf(format, args...)),
		})
	}

	if len(policy.GetToleratedKinds()) > 0 && !policy.GetContinueOnError() {
		add("continue_on_error", "", "names tolerated kinds but does not continue on error")
	}
	for _, kind := range policy.GetToleratedKinds() {
		if !known(kind) {
			add("continue_on_error", kind, "`continue_on_error:` names %q, which is neither a built-in kind (%s) nor declared under `errors:`",
				kind, strings.Join(errorKindNames(), ", "))
		}
	}

	for _, kind := range policy.GetRetry().GetOnly() {
		switch {
		case !known(kind):
			add("retry.only", kind, "`only:` names %q, which is neither a built-in kind (%s) nor declared under `errors:`",
				kind, strings.Join(errorKindNames(), ", "))
		case !ErrorKind(kind).Retryable():
			add("retry.only", kind, "`only:` names %q, which is never retried (%s); a list narrows what is retried and cannot widen it",
				kind, retryableKindList())
		}
	}

	return problems
}

func retryableKindList() string {
	kinds := RetryableErrorKinds()
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.String())
	}
	slices.Sort(names)

	return "only " + strings.Join(names, ", ") + " are"
}

// CheckPolicyKinds is the submit boundary's half of [PolicyKindProblems]: it
// refuses a workflow, built by anything that never was a Flowfile, whose step
// policies name kinds the rules above refuse.
func CheckPolicyKinds(wf *Workflow) error {
	var err error
	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		if err != nil {
			return
		}
		if problems := PolicyKindProblems(wf, node); len(problems) > 0 {
			err = fmt.Errorf("%s", problems[0].Message)
		}
	}})

	return err
}
