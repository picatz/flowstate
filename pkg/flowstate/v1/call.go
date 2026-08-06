package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
)

// Calling one workflow from another, in the half both drivers must agree about.
//
// A call is the only node that runs a *workflow* rather than a piece of one, so
// three things have to be identical across the two drivers and are therefore
// written here rather than twice: what the callee can see, what comes back, and
// how deep this is allowed to go. Everything a driver legitimately differs about —
// how a task is executed, whether the nested steps run in-process or as scheduled
// activities — is the caller's business and stays there.
//
// The retry-attempts lesson in CLAUDE.md is the reason for the shape: a value with
// one meaning written down twice eventually disagrees with itself, and it does so
// silently, because nothing imports both copies.

// MaxCallDepth bounds how deeply calls may nest.
//
// A bound is needed at execution and not only at resolution, because a
// specification does not have to have come from a Flowfile. The parser refuses a
// cycle by walking the files it reads, and that walk protects an author; the Run
// RPC accepts a specification somebody built by hand, and nothing about that
// journey passes through a parser at all. A self-referential spec would otherwise
// recurse until the stack ended the process.
//
// Eight is chosen to be past any composition anybody has argued for and far short
// of anything that costs a worker. Depth is the wrong bound on its own — see
// [CheckSpecSize], which weighs the whole message and is what actually stops a
// wide expansion, since a call tree multiplies breadth at every level exactly as a
// billion-laughs document does.
const MaxCallDepth = 8

// ResolveCallArguments evaluates a call's arguments against the caller's scope,
// returning a copy holding literals.
//
// The schema says an argument crosses the boundary as a value rather than an
// expression the callee evaluates (see [Call.Arguments]'s doc): a callee
// evaluating an expression handed to it would be reading the caller's scope
// through a string, which is exactly the isolation [CallScope] exists to
// refuse. So an expression written under `with:` — `${steps.build.digest}` — is
// resolved once, here, against the scope the caller wrote it in, and what
// [CallScope] and [BindRunInputs] see afterward is never anything but a
// literal — the same shape [ResolveTaskInputs] hands a task, for the same
// reason.
func ResolveCallArguments(ctx context.Context, arguments map[string]*Value, scope *Scope) (map[string]*Value, error) {
	if len(arguments) == 0 {
		return nil, nil
	}

	resolved := make(map[string]*Value, len(arguments))
	ev := DefaultEvaluator()
	for name, v := range arguments {
		if _, isExpr := v.GetKind().(*Value_Expr); !isExpr {
			resolved[name] = v
			continue
		}

		out, err := ev.EvalParsedBase(ctx, scope.GetProfile(), v.GetExpr(), scope.Activation(ctx))
		if err != nil {
			return nil, fmt.Errorf("argument %q: %w", name, err)
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("argument %q: converting result: %w", name, err)
		}
		resolved[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return resolved, nil
}

// CalleeProfile returns the profile a callee's own expressions are evaluated
// under: the callee's, where it declares one, and the caller's otherwise.
//
// Exported and called from three places that all have to agree — [CallScope],
// which stamps it on the callee's scope, and the vars evaluation each driver
// does before that scope exists to evaluate the callee's own `vars:` against.
// A callee's `vars:` and its steps are one file evaluated under one dialect;
// computing the profile twice, by two routes, is exactly the shape CLAUDE.md's
// retry-attempts lesson warns about — a value with one meaning, written down
// twice, disagreeing with itself the moment a callee that names no profile of
// its own is called by two callers compiled against different ones.
func CalleeProfile(caller *Scope, callee *Workflow) string {
	if profile := callee.GetProfile(); profile != "" {
		return profile
	}
	return caller.GetProfile()
}

// CallScope returns the scope a called workflow's steps run in.
//
// A fresh one, holding the callee's bound arguments, the callee's own `vars:`
// (already evaluated — see below), and the profile, and nothing else: no
// caller outputs, no caller `vars:`, no loop binding from the block the call
// sits in.
//
// That isolation is the feature rather than a limitation of it. A workflow that
// can read its caller's scope is not a unit — it cannot be read, tested or reused
// apart from the file that calls it, which is the whole thing a call was meant to
// make possible. It is a security property too, and one that matters more as
// workflows are shared between teams: a library workflow cannot read the values
// its caller resolved, including the ones its caller resolved from a secret.
//
// The profile crosses because it is not data. It names the dialect the expressions
// are written in, and a callee compiled against one profile and evaluated under
// another would be a different language with the same syntax.
//
// vars is the callee's own `vars:` block, already evaluated — this function
// never evaluates it, because *how* to evaluate it safely differs by driver: the
// local driver may simply call [EvalWorkflowVars] in process, while the durable
// driver must go through an activity and then carry the answer across any
// Continue-As-New the call spans, for the same reason [EvalWorkflowVars] is an
// activity at the top level at all — see the doc on `Frame.call_vars`. Passing
// nil is correct for a callee that declares no `vars:`, and CallScope does not
// tell the two cases apart because there is nothing to bind either way.
func CallScope(caller *Scope, callee *Workflow, arguments, vars map[string]*Value) (*Scope, error) {
	bound, err := BindRunInputs(callee, arguments)
	if err != nil {
		return nil, fmt.Errorf("calling %q: %w", callee.GetName(), err)
	}

	// Built from the callee's own profile where it declares one, and the caller's
	// otherwise. A workflow that names its dialect means it, and a file that does
	// not is being run as part of the file that called it.
	profile := CalleeProfile(caller, callee)

	scope := NewScope(profile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	scope.Inputs = bound
	scope.AmbientVars = vars

	// The run's own starter identity crosses despite the isolation above, because
	// it is not something the caller's scope resolved — it is a fact about the run
	// itself, exactly like the profile two lines up. A callee is still a step of
	// the same run, started by the same caller, so `${run.identity.subject}`
	// inside a called workflow must answer the same thing it would answer if the
	// callee's steps were pasted in place rather than called.
	scope.Identity = caller.GetIdentity()
	scope.Local = caller.GetLocal()

	return scope, nil
}

// CallOutputs returns what a call reports, from the scope its steps finished in.
//
// A call's outputs are the callee's declared `outputs:` and nothing else — not the
// callee's step transcript, which belongs to the callee. A caller reading
// `${steps.provision.url}` is reading something the called workflow promised, in
// the same way a caller of a function reads its return value rather than its
// locals.
//
// A callee declaring no outputs produces an empty set rather than none at all:
// the call still ran, so its step is present in the run's outputs the way any
// other step that ran is. A workflow called for its effects has nothing to
// hand back and is the ordinary case this covers.
//
// Evaluated at the same moment and against the same scope as a top-level run's
// outputs, through the same function — which is what makes a workflow's answer the
// same whether it was run directly or called.
func CallOutputs(ctx context.Context, callee *Workflow, scope *Scope) (*Node_Outputs, error) {
	outputs, err := EvalRunOutputs(ctx, callee, scope)
	if err != nil {
		return nil, fmt.Errorf("calling %q: %w", callee.GetName(), err)
	}
	if outputs == nil {
		// Present rather than absent: the call still ran, so its step belongs in
		// the run's outputs exactly as any other step that ran does, whether or
		// not it had anything to say — a `log:` step is stored the identical way.
		// Absence is reserved for a step a condition skipped, which this is not.
		return &Node_Outputs{NamedValues: map[string]*Value{}}, nil
	}

	return &Node_Outputs{NamedValues: outputs.GetValues()}, nil
}

// CheckCallDepth reports whether a call at this depth may run.
//
// Counted from zero at the top-level workflow, so the first call is at depth one.
// The message names the bound and what to do about it rather than only refusing,
// because the author of a specification this deep has a structural problem and
// telling them the number is most of the answer.
func CheckCallDepth(depth int) error {
	if depth <= MaxCallDepth {
		return nil
	}

	return fmt.Errorf(
		"calls are nested %d deep and the limit is %d; a workflow that calls a workflow "+
			"that calls a workflow this far is usually one that calls itself, which the "+
			"parser refuses when the files are read and this refuses when they are not",
		depth, MaxCallDepth)
}
