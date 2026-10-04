package flowstatev1

import (
	"fmt"
	"slices"
	"strings"
)

// A step's policy can name failure kinds: `continue_on_error: [kinds]` narrows
// what is tolerated, and `retry.only` / `retry.except` narrow what is retried.
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

// RetryAllowsKind reports whether a retry policy's `only:` and `except:` allow
// another attempt after a failure of kind.
//
// It only ever narrows. A caller must still ask whether the failure is
// retryable at all ([RetryPermitted]): naming a permanent kind in `only:` does not
// make it retried, and the validator refuses the list that tries.
func RetryAllowsKind(retry *RetryPolicy, kind ErrorKind) bool {
	if slices.Contains(retry.GetExcept(), kind.String()) {
		return false
	}
	on := retry.GetOnly()

	return len(on) == 0 || slices.Contains(on, kind.String())
}

// RetryExcludedKinds returns the retryable built-in kinds a retry policy's lists
// rule out, which the durable driver adds to the activity's non-retryable error
// types: everything in `except:`, and, when `only:` is written, every retryable
// kind it does not name. Permanent kinds are not repeated here; they are already
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

// PolicyKindProblem is one refusal of a step policy's kind lists.
type PolicyKindProblem struct {
	// Field is the key the problem is about: `continue_on_error`, `retry.only` or
	// `retry.except`.
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
// A named kind must be one a step of wf can fail with ([KnownFailureKind]). In
// `retry.only:` it must also be retryable: a permanent kind is not retried because
// a list names it, and a declared error is never retried, so naming either would
// promise a retry that never happens. In `retry.except:` it must be retryable
// too, since stopping what is never retried changes nothing and would read as if
// it did. A kind in both lists is a contradiction.
func PolicyKindProblems(wf *Workflow, node *Node) []PolicyKindProblem {
	policy := node.GetPolicy()
	if policy == nil {
		return nil
	}

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
		if !KnownFailureKind(wf, kind) {
			add("continue_on_error", kind, "`continue_on_error:` names %q, which is neither a built-in kind (%s) nor declared under `errors:`",
				kind, strings.Join(errorKindNames(), ", "))
		}
	}

	retry := policy.GetRetry()
	for _, field := range []struct {
		name  string
		kinds []string
	}{{"retry.only", retry.GetOnly()}, {"retry.except", retry.GetExcept()}} {
		for _, kind := range field.kinds {
			switch {
			case !KnownFailureKind(wf, kind):
				add(field.name, kind, "`%s:` names %q, which is neither a built-in kind (%s) nor declared under `errors:`",
					strings.TrimPrefix(field.name, "retry."), kind, strings.Join(errorKindNames(), ", "))
			case !ErrorKind(kind).Retryable():
				add(field.name, kind, "`%s:` names %q, which is never retried (%s); a list narrows what is retried and cannot widen it",
					strings.TrimPrefix(field.name, "retry."), kind, retryableKindList())
			}
		}
	}

	if on := retry.GetOnly(); len(on) > 0 {
		for _, kind := range retry.GetExcept() {
			if slices.Contains(on, kind) {
				add("retry.except", kind, "%q is in both `only:` and `except:`", kind)
			} else if KnownFailureKind(wf, kind) && ErrorKind(kind).Retryable() {
				add("retry.except", kind, "`except:` names %q, which `only:` already leaves out; write one list or the other", kind)
			}
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
