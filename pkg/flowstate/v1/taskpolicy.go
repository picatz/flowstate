package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// Task-shape policy: deployment-side CEL rules over which identities may
// dispatch which tasks (#187, slice 1: identity and task only — typed
// `inputs.*` access is slice 2, gated on #177's manifest projection).
//
// # Why this exists, and why it cannot live in the Flowfile
//
// `examples/approval-gate`'s own header states the gap this closes (#206 gap
// 3): the file's author can weaken `deploy`'s `if:` to `true` as easily as
// they can delete the `signals:` block above it. A control written in a
// Flowfile is a control the Flowfile's author can delete, so the enforcement
// point has to be one the author does not control at all — the worker,
// governed by configuration the workflow never carries.
//
// # Mirrors netpolicy and auth's assumption rules, deliberately
//
// Same CEL, same fail-closed compilation, same deny-first precedence as
// egress policy ([pkg/flowstate/v1/netpolicy]) and credential-assumption
// policy ([pkg/flowstate/v1/auth]): rules compile and type-check once, at
// load, evaluation is cost-limited so a pathological rule cannot become a
// denial of service, and a rule that fails to evaluate denies rather than
// permits. This is not lifted into a shared package (the design record for
// #187 offers that as a follow-up, not a slice-1 requirement) because the two
// idioms it would unify — netpolicy's — already live at arm's length in their
// own package; this instead lives directly in this package, the way
// [SignalPolicy] does, because doing so is what lets it reuse
// [WorkloadIdentity] itself rather than a parallel identity type that would
// need converting at every boundary.
//
// # The zero case, stated once
//
// A process with no task-shape policy configured — [DefaultTaskPolicy] nil
// and nothing on the context — restricts nothing: every task dispatches
// exactly as it does today. This is not a fail-open hole; it is the same
// opt-in-per-deployment posture egress policy and signal policy both take,
// stated by [SignalPolicy]'s own doc: failing closed on every dispatch the
// day this shipped would deny every existing worker's next run. Once a
// policy *is* configured, dispatch is fail-closed for what it governs: an
// errored rule denies, and — where any allow rule is configured — an
// invocation matching none of them is denied.

// DefaultTaskPolicyRuleCostLimit bounds the CEL evaluation cost of a single
// task-shape rule, so a pathological expression cannot become a denial of
// service by itself — the same limit egress policy defaults to
// ([netpolicy.DefaultRuleCostLimit]) and for the identical reason: this
// evaluates once per task dispatch, so it sits on the hot path of every step.
const DefaultTaskPolicyRuleCostLimit uint64 = 50_000

// ErrTaskPolicyDenied is the sentinel error wrapped by every task-shape
// policy denial. Callers use errors.Is(err, ErrTaskPolicyDenied) to
// distinguish a deliberate policy decision from the task itself failing.
var ErrTaskPolicyDenied = errors.New("denied by task-shape policy")

// ErrInvalidTaskPolicy is the sentinel error wrapped when supplied
// configuration does not describe a usable policy — a CEL rule that does not
// compile, or a file that cannot be parsed. It marks operator configuration
// mistakes, refused at load, never a per-request outcome.
var ErrInvalidTaskPolicy = errors.New("invalid task-shape policy")

// TaskPolicyReason classifies why a task-shape policy denied a dispatch.
type TaskPolicyReason string

const (
	// TaskPolicyReasonDenyRule indicates a CEL deny rule matched the dispatch.
	TaskPolicyReasonDenyRule TaskPolicyReason = "deny rule"

	// TaskPolicyReasonNoAllowRule indicates allow rules are configured and
	// none of them matched — an allowlist, and this dispatch was not on it.
	TaskPolicyReasonNoAllowRule TaskPolicyReason = "allow rules"

	// TaskPolicyReasonRuleError indicates a CEL rule could not be evaluated.
	// Rules fail closed, so an evaluation error denies the dispatch.
	TaskPolicyReasonRuleError TaskPolicyReason = "rule error"
)

// TaskPolicyDeniedError reports that a task-shape policy refused a dispatch.
// It wraps [ErrTaskPolicyDenied], names the task and the rule responsible so
// an operator (or an author reading a denial surfaced to them, per CLAUDE.md's
// "Diagnostics are a feature" — this is a deployment refusal, not a file
// diagnostic, and must read as one) can find the policy source and the
// remedy.
type TaskPolicyDeniedError struct {
	// Task is the qualified task name that was denied, e.g. "codex.exec".
	Task string

	// Reason is the broad category of the denial.
	Reason TaskPolicyReason

	// Detail names the specific rule responsible, or explains why none of the
	// configured allow rules matched.
	Detail string

	// Err is the underlying cause when the denial came from a rule failing to
	// evaluate rather than from a rule matching.
	Err error

	// Local records whether this dispatch was a local rehearsal
	// (any of `flow run local`, `flow test`, `flow task run`, or `flow mcp`
	// serving a local session — anything reaching [Scope.local]) rather than
	// a production one, purely so [Error] can say so. Set by
	// [CheckTaskPolicy] after the decision is already made — nothing in this
	// package, or in [taskPolicyRuleSet.evaluate], ever reads this field,
	// and it can never change which rule matched or whether the dispatch is
	// denied. CLAUDE.md's "a value that exists to be informational and then
	// becomes load-bearing" is the failure this field is deliberately built
	// to be unable to have: it reaches nothing but this struct's own Error()
	// string.
	Local bool

	// Identity describes the identity the policy was evaluated against, in
	// the terms a rule reads it — the provenance half of #652 item 3, and
	// the half [Local] alone cannot supply.
	//
	// Local says a denial came from a rehearsal. It does not say *what the
	// rule was matched against*, and that is the distinction an author most
	// needs: a rehearsal carries no identity at all unless `flow run local
	// --as-*` named one, and a `flow test` case's `starter:` never reaches
	// this surface (documented at flowtest's package doc, landed in #877).
	// So a rule reading `identity.namespace` refuses every dispatch in those
	// venues, and the denial an author reads is textually identical to one
	// where the rule genuinely matched *them*. Naming the evaluated identity
	// is what tells "denied because the rehearsal identity is empty" apart
	// from "denied because the rule matched".
	//
	// Rendered by [describePolicyIdentity] and set by [CheckTaskPolicy]
	// after the decision is already made, under exactly the constraints
	// Local documents above: nothing reads it but [Error]. Empty means no
	// caller recorded one — a denial built by [TaskPolicy.Check] directly,
	// which every driver's dispatch path goes through [CheckTaskPolicy]
	// rather than reaching — and [Error] then says nothing about identity
	// rather than inventing an answer. "No identity at all" is a different
	// string, never the empty one.
	//
	// Claim *values* are deliberately absent from it; see
	// [describePolicyIdentity].
	Identity string
}

// noPolicyIdentity is what [describePolicyIdentity] renders for an identity
// no rule could match on, and the value [TaskPolicyDeniedError.Error] tests
// for when deciding whether to explain where a rehearsal's identity comes
// from. A named constant rather than a string literal in each place, because
// the two are one decision: change the wording and the explanation would
// silently stop being attached to the case it explains.
const noPolicyIdentity = "no identity — every field a rule can read is empty"

// describePolicyIdentity renders the identity a task-shape rule was evaluated
// against, for [TaskPolicyDeniedError.Error] and nothing else.
//
// Two properties it is built for:
//
// A wholly empty identity gets prose rather than a row of empty quotes,
// because that case is the one the message exists to make legible: an author
// staring at `subject="" issuer="" namespace=""` has to know that is what a
// rehearsal without `--as-*` looks like, and an author reading "no identity"
// does not.
//
// Claim values never appear — only the sorted claim *keys*. A claim is
// caller-supplied data of a shape this package does not get to constrain, and
// this string travels wherever the denial does, which on the durable driver
// means into Temporal's failure conversion and therefore into workflow
// history (CLAUDE.md, "secrets never enter workflow history": the rule is
// about a durable, broadly readable log, and a claim value is exactly the
// kind of thing nobody audited before it got there). Keys are enough for the
// provenance question — whether the claim a rule reads was carried at all —
// and are the half a policy author already knows, since they wrote the rule.
func describePolicyIdentity(identity *WorkloadIdentity) string {
	claims := slices.Sorted(maps.Keys(identity.GetClaims()))

	// "every field a rule can read" is exact rather than loose: the
	// activation [TaskPolicy.Check] builds is subject, issuer, namespace and
	// claims — `deployment` is on [WorkloadIdentity] and is not exposed to a
	// task-shape rule — so an identity carrying only a deployment is, to
	// this policy surface, no identity at all, and saying so is honest.
	if identity.GetSubject() == "" && identity.GetIssuer() == "" &&
		identity.GetNamespace() == "" && len(claims) == 0 {
		return noPolicyIdentity
	}

	return fmt.Sprintf("identity subject=%q issuer=%q namespace=%q claims=%v",
		identity.GetSubject(), identity.GetIssuer(), identity.GetNamespace(), claims)
}

// Error implements the error interface. The message names what to do about
// it — the task and the policy source — because a denial an author cannot
// act on is worse than no diagnostic at all.
//
// After #651, a local denial and a production denial for the same identity
// are the same *decision* — correctly so, since a rehearsal exercising a
// deployment's policy is the whole point. Local exists only to keep an
// author reading this message from mistaking the two for the same *cause*:
// a policy file passed to a local invocation and forgotten about reads as a
// deployment refusal with no hint that the run was ever a rehearsal.
//
// Deliberately venue-neutral rather than naming `flow run local`: [Scope.Local]
// is true for every local-driver entry point — `flow run local`, `flow test`,
// `flow task run`, `flow mcp` serving a local session — not only the one
// whose name is easiest to reach for, and this message must read true for
// all of them.
//
// That neutrality is why the rehearsal clause no longer tells the reader to
// check "the --task-policy passed to this local invocation". Half the venues
// it speaks to have no such flag: `--task-policy` is declared on `flow
// worker`, `flow run local`, `flow mcp`, `flow server dev` and `flow task
// run`, and deliberately not on `flow test` (#652 item 2), which instead
// inherits whatever policy the process hosting it installed — `flow mcp
// --task-policy` serving the `flowstate_test` tool, or any caller of
// [SetDefaultTaskPolicy]. Sending a `flow test` author hunting for a flag
// their command does not accept is a false diagnostic, which CLAUDE.md rates
// worse than a missing one, so the clause names the process rather than a
// flag only some of these commands take.
//
// [Identity] is the other half, and the one this message was missing
// entirely: see that field's own doc for why a denial that does not say what
// it evaluated leaves an author unable to tell an empty rehearsal identity
// from a rule that matched them.
func (e *TaskPolicyDeniedError) Error() string {
	rehearsal := ""
	if e.Local {
		rehearsal = " during a local rehearsal"
	}

	provenance := ""
	if e.Identity != "" {
		provenance = fmt.Sprintf("; evaluated against %s", e.Identity)

		if e.Local && e.Identity == noPolicyIdentity {
			provenance += "; a rehearsal carries an identity only where one was named — " +
				"`flow run local --as-subject/--as-issuer/--as-namespace/--as-claim`, and " +
				"nowhere at all under `flow test`, whose `starter:` reaches the workflow's " +
				"own `signals:` policy and not this one — so a rule reading identity refused " +
				"this dispatch for want of an identity to match, not because it matched yours"
		}
	}

	remedy := "contact the operator who configured the task-shape policy if this " +
		"dispatch should be permitted"
	if e.Local {
		remedy = "check the task-shape policy this process installed before blaming the " +
			"deployment's, and " + remedy
	}

	return fmt.Sprintf("%s: task %q refused by deployment task-shape policy%s (%s: %s)%s; "+
		"this is not a mistake in the workflow file — %s",
		ErrTaskPolicyDenied, e.Task, rehearsal, e.Reason, e.Detail, provenance, remedy)
}

// Unwrap returns [ErrTaskPolicyDenied], and the underlying cause when there
// is one, so every denial matches a single sentinel without hiding what went
// wrong.
func (e *TaskPolicyDeniedError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrTaskPolicyDenied}
	}
	return []error{ErrTaskPolicyDenied, e.Err}
}

// TaskPolicy is a compiled, ready-to-evaluate task-shape policy. Build one
// with [TaskPolicyConfig.Policy]; the zero value is not usable directly —
// [CheckTaskPolicy] and [TaskPolicyIn] treat a nil *TaskPolicy as "no
// policy configured", never as an empty, usable one.
type TaskPolicy struct {
	rules taskPolicyRuleSet
}

// Check reports whether policy permits dispatching task under identity. A
// nil identity renders as every field empty — the same reading
// [Scope.identity] itself gives a local run or a scope that predates
// identity — so a rule meaning "no attested caller" writes
// `identity.subject == ""`.
//
// A nil *TaskPolicy permits everything: see the package doc's "zero case".
func (p *TaskPolicy) Check(ctx context.Context, task string, identity *WorkloadIdentity) error {
	if p == nil || p.rules.empty() {
		return nil
	}

	claims := identity.GetClaims()
	if claims == nil {
		// CEL cannot index a null map, and a rule reading claims["x"] against
		// an identity that carries none should simply not match rather than
		// error the evaluation.
		claims = map[string]string{}
	}

	vars := map[string]any{
		"task": task,
		"identity": taskPolicyIdentity{
			Subject:   identity.GetSubject(),
			Issuer:    identity.GetIssuer(),
			Namespace: identity.GetNamespace(),
			Claims:    claims,
		},
	}

	return p.rules.evaluate(ctx, task, vars)
}

// taskPolicyIdentity is the CEL-typed rendering of a [WorkloadIdentity] a
// task-shape rule reads as `identity.<field>`.
//
// A struct with `cel:` tags, exactly as auth's assumption rules render a
// workload (`auth/assume.go`'s `workload` type) and as [runRootValue] renders
// `run.identity` for ordinary expressions — declaring the fields is what
// makes a rule naming `identity.nonexistent` a compile-time error rather than
// a rule that silently never matches. This is a rendering, not a parallel
// identity type: every caller of this package still carries a
// *[WorkloadIdentity], and this shape exists only for the moment a CEL
// environment needs typed fields to check against.
//
// Deliberately narrower than [WorkloadIdentity] itself, the same way
// [Scope.identity] already is: `deployment` answers "which installation ran
// this" rather than "who may run what", which is [WorkloadIdentity.deployment]'s
// own distinction restated for this surface.
type taskPolicyIdentity struct {
	Subject   string            `cel:"subject"`
	Issuer    string            `cel:"issuer"`
	Namespace string            `cel:"namespace"`
	Claims    map[string]string `cel:"claims"`
}

// taskPolicyIdentityTypeName is how [taskPolicyIdentity] is named in CEL,
// which appears in a type error when a rule misuses a field. [ext.NativeTypes]
// derives this from the type's Go *directory* rather than its declared
// package name — "v1", not "flowstatev1" — which is where this package's own
// name and its import path (".../pkg/flowstate/v1") part ways; the same
// pattern `auth.workloadTypeName` pins for its own native type happens to
// read as the package name only because that package's directory and its
// declared name are the same word.
const taskPolicyIdentityTypeName = "v1.taskPolicyIdentity"

// newTaskPolicyEnv builds the CEL environment task-shape rules are compiled
// against. Declaring every attribute here is what makes a misspelled or
// invented one a load-time error rather than a rule that quietly never
// matches.
func newTaskPolicyEnv() (*cel.Env, error) {
	return cel.NewEnv(
		ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeOf(taskPolicyIdentity{})),
		cel.Variable("task", cel.StringType),
		cel.Variable("identity", cel.ObjectType(taskPolicyIdentityTypeName)),
		ext.Strings(ext.StringsVersion(5)),
	)
}

// taskPolicyRule is a compiled CEL task-shape rule. The program is built once,
// when the policy is constructed, and is safe to evaluate concurrently.
type taskPolicyRule struct {
	// src is the original expression text, reported in denial messages so an
	// operator can find the rule that fired.
	src string

	// prg is the compiled program.
	prg cel.Program
}

// eval evaluates the rule against vars. The context is threaded through, so
// an expensive rule is interrupted when the dispatch is cancelled.
func (r taskPolicyRule) eval(ctx context.Context, vars map[string]any) (bool, error) {
	out, _, err := r.prg.ContextEval(ctx, vars)
	if err != nil {
		return false, err
	}

	matched, ok := out.Value().(bool)
	if !ok {
		// The output type is checked at compile time, so reaching here means
		// CEL produced something other than the type it promised.
		return false, fmt.Errorf("rule produced %s, want bool", out.Type().TypeName())
	}

	return matched, nil
}

// taskPolicyRuleSet holds the allow and deny rules that govern task
// dispatch.
type taskPolicyRuleSet struct {
	allow []taskPolicyRule
	deny  []taskPolicyRule
}

// empty reports whether the set has no rules, letting [TaskPolicy.Check]
// skip evaluation entirely.
func (rs taskPolicyRuleSet) empty() bool {
	return len(rs.allow) == 0 && len(rs.deny) == 0
}

// evaluate applies the set to vars and returns a [*TaskPolicyDeniedError] if
// the dispatch is denied. Deny rules run first and take precedence, then
// allow rules gate the dispatch when any are configured. A rule that fails
// to evaluate fails closed — see [taskPolicyRuleFailure].
func (rs taskPolicyRuleSet) evaluate(ctx context.Context, task string, vars map[string]any) error {
	for _, r := range rs.deny {
		matched, err := r.eval(ctx, vars)
		if err != nil {
			return taskPolicyRuleFailure(ctx, "deny", r.src, task, err)
		}
		if matched {
			return &TaskPolicyDeniedError{
				Task:   task,
				Reason: TaskPolicyReasonDenyRule,
				Detail: r.src,
			}
		}
	}

	if len(rs.allow) == 0 {
		return nil
	}

	for _, r := range rs.allow {
		matched, err := r.eval(ctx, vars)
		if err != nil {
			return taskPolicyRuleFailure(ctx, "allow", r.src, task, err)
		}
		if matched {
			return nil
		}
	}

	return &TaskPolicyDeniedError{
		Task:   task,
		Reason: TaskPolicyReasonNoAllowRule,
		Detail: "no allow rule matched",
	}
}

// taskPolicyRuleFailure converts a rule evaluation failure into a denial, so
// a rule that cannot be evaluated fails closed.
//
// A cancelled or expired context is returned as itself rather than as a
// denial: running out of time is not a policy decision, and reporting it as
// one would tell an operator their rules refused a dispatch that in fact
// never finished — the identical rule [netpolicy]'s own `ruleFailure` and
// auth's `assumeRuleFailure` state.
func taskPolicyRuleFailure(ctx context.Context, kind, src, task string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	return &TaskPolicyDeniedError{
		Task:   task,
		Reason: TaskPolicyReasonRuleError,
		Detail: fmt.Sprintf("%s rule %q could not be evaluated: %v", kind, src, err),
		Err:    err,
	}
}

// compileTaskPolicyRules compiles operator-supplied expressions into
// programs, type-checking each so a mistake fails at configuration load
// rather than the first time a task tries to dispatch.
func compileTaskPolicyRules(allow, deny []string, costLimit uint64) (taskPolicyRuleSet, error) {
	if len(allow) == 0 && len(deny) == 0 {
		return taskPolicyRuleSet{}, nil
	}

	env, err := newTaskPolicyEnv()
	if err != nil {
		return taskPolicyRuleSet{}, fmt.Errorf("%w: building task-shape rule environment: %w", ErrInvalidTaskPolicy, err)
	}

	options := []cel.ProgramOption{
		cel.CostLimit(costLimit),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(100),
	}

	compile := func(kind string, sources []string) ([]taskPolicyRule, error) {
		rules := make([]taskPolicyRule, 0, len(sources))

		for _, src := range sources {
			if src == "" {
				return nil, fmt.Errorf("%w: %s rule must not be empty", ErrInvalidTaskPolicy, kind)
			}

			ast, issues := env.Compile(src)
			if issues.Err() != nil {
				return nil, fmt.Errorf("%w: %s rule %q is invalid: %w", ErrInvalidTaskPolicy, kind, src, issues.Err())
			}
			if out := ast.OutputType(); !out.IsExactType(cel.BoolType) {
				return nil, fmt.Errorf("%w: %s rule %q evaluates to %s, want bool",
					ErrInvalidTaskPolicy, kind, src, out.TypeName())
			}

			prg, err := env.Program(ast, options...)
			if err != nil {
				return nil, fmt.Errorf("%w: %s rule %q could not be compiled: %w", ErrInvalidTaskPolicy, kind, src, err)
			}

			rules = append(rules, taskPolicyRule{src: src, prg: prg})
		}

		return rules, nil
	}

	denyRules, err := compile("deny", deny)
	if err != nil {
		return taskPolicyRuleSet{}, err
	}

	allowRules, err := compile("allow", allow)
	if err != nil {
		return taskPolicyRuleSet{}, err
	}

	return taskPolicyRuleSet{allow: allowRules, deny: denyRules}, nil
}
