// Package celrule is the machinery a policy surface needs to hold an
// operator's CEL predicates: compile them at load, bound and interrupt their
// evaluation, insist on a boolean, and apply a deny-first set.
//
// Four surfaces carried this — the identity egress policy in netpolicy, the
// assumption and secret policies in auth, and the task-shape policy in the
// root package — as byte-identical copies of the same thirteen-line evaluator
// around a program each had compiled itself, with its own cost limit and its
// own "compile every rule at load, refuse start-up on one that does not
// type-check" loop (#1708). ARCHITECTURE.md's invariant 2 says the policy
// machinery must not vary between surfaces; keeping it in one place is what
// makes that a mechanism rather than a review comment.
//
// What stays with each surface, on purpose, is the environment: which names a
// rule may read, and what they mean, is the whole of what distinguishes an
// egress rule from a secret rule. This package takes an environment and never
// builds one.
package celrule

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
)

// interruptCheckFrequency is how many evaluation steps pass between checks
// of the context. Every surface used the same number: an expensive rule is
// interrupted when the request is cancelled, and a cheap one finishes before
// the interrupt is next checked.
const interruptCheckFrequency = 100

// Rule is one compiled predicate. The program is built once, at load, and is
// safe to evaluate concurrently.
type Rule struct {
	source  string
	program cel.Program
}

// Source is the expression text the rule was compiled from, for a denial that
// names the rule that fired so an operator can find it.
func (r Rule) Source() string { return r.source }

// Compile type-checks src against env, requires that it evaluates to a bool,
// and builds a program whose evaluation cost is bounded by costLimit and that
// checks for cancellation as it runs.
//
// The errors say what every surface said before, without the surface's own
// prefix: "rule must not be empty", `rule "x" is invalid: …`, `rule "x"
// evaluates to string, want bool`, `rule "x" could not be compiled: …`. A
// surface wraps them with its policy sentinel and the kind of rule — `allow`,
// `secret deny` — so a refusal at start-up reads exactly as it did.
func Compile(env *cel.Env, src string, costLimit uint64) (Rule, error) {
	if src == "" {
		return Rule{}, fmt.Errorf("rule must not be empty")
	}

	ast, issues := env.Compile(src)
	if issues.Err() != nil {
		return Rule{}, fmt.Errorf("rule %q is invalid: %w", src, issues.Err())
	}

	return Build(env, ast, src, costLimit)
}

// Build is [Compile] for a caller that already holds the checked AST: the
// identity egress policy decides which of two environments a rule belongs to
// by compiling against both, and only then builds the program.
func Build(env *cel.Env, ast *cel.Ast, src string, costLimit uint64) (Rule, error) {
	if out := ast.OutputType(); !out.IsExactType(cel.BoolType) {
		return Rule{}, fmt.Errorf("rule %q evaluates to %s, want bool", src, out.TypeName())
	}

	program, err := env.Program(ast,
		// The limit is only enforced while costs are tracked, so the two travel
		// together: a pathological rule cannot become a denial of service.
		cel.CostLimit(costLimit),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(interruptCheckFrequency),
	)
	if err != nil {
		return Rule{}, fmt.Errorf("rule %q could not be compiled: %w", src, err)
	}

	return Rule{source: src, program: program}, nil
}

// Match evaluates the rule against vars. The context is threaded through, so
// an expensive rule is interrupted when the request is cancelled.
func (r Rule) Match(ctx context.Context, vars map[string]any) (bool, error) {
	out, _, err := r.program.ContextEval(ctx, vars)
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

// Kind names which list of a [Set] a rule was in, for a refusal that says so.
type Kind string

const (
	// Allow rules gate what the set permits, when any are configured.
	Allow Kind = "allow"

	// Deny rules refuse, and run first.
	Deny Kind = "deny"
)

// CompileAll compiles every source in order, refusing at the first that does
// not compile, with the kind and the surface's own wrapping applied by wrap.
// The loop every surface wrote at load, once.
func CompileAll(env *cel.Env, kind Kind, sources []string, costLimit uint64, wrap func(Kind, error) error) ([]Rule, error) {
	rules := make([]Rule, 0, len(sources))
	for _, src := range sources {
		rule, err := Compile(env, src, costLimit)
		if err != nil {
			return nil, wrap(kind, err)
		}
		rules = append(rules, rule)
	}

	return rules, nil
}

// Set is a deny-first policy: a matching deny rule refuses; then, when any
// allow rules are configured, one of them must match; when none are, the
// answer is the one WithoutAllow declares.
type Set struct {
	Allow []Rule
	Deny  []Rule

	// WithoutAllow is what the set answers when no deny rule matched and no
	// allow rule is configured. The surfaces disagree here and have to say
	// so (#1634 review, F5): a task-shape or egress policy of deny rules
	// alone permits what it did not name, while a secret policy of deny
	// rules alone permits nothing, because a secret must be permitted by an
	// allow rule. A set that leaves this zero-valued permits.
	WithoutAllow Verdict
}

// Empty reports whether the set has no rules at all, letting a caller skip
// building the activation.
func (s Set) Empty() bool { return len(s.Allow) == 0 && len(s.Deny) == 0 }

// Verdict is what a [Set] decided.
type Verdict int

const (
	// Permitted: no deny rule matched and either an allow rule did or none
	// was configured with WithoutAllow left permitting.
	Permitted Verdict = iota

	// DeniedByRule: a deny rule matched; [Decision.Rule] is that rule.
	DeniedByRule

	// NoAllowRuleMatched: allow rules are configured and none matched.
	NoAllowRuleMatched

	// NoAllowRules: no allow rule is configured and the set's WithoutAllow
	// says that refuses.
	NoAllowRules
)

// Decision is a [Set]'s answer: the verdict, and the rule that decided it
// where one did — the deny that matched, or the allow that permitted, which
// is what an audit record names.
type Decision struct {
	Verdict Verdict
	Rule    Rule
}

// EvalError reports a rule that could not be evaluated, which every surface
// treats as a refusal: a policy that cannot be evaluated is not a policy that
// permits everything. It carries the rule and its kind so the refusal can
// name them, and unwraps to the evaluator's error.
//
// A cancelled or expired context is not this: [Set.Decide] returns the
// context's own error then, because running out of time is not a policy
// decision, and reporting it as one would tell an operator their rules refused
// a request that in fact never finished.
type EvalError struct {
	Kind Kind
	Rule Rule
	Err  error
}

func (e *EvalError) Error() string {
	return fmt.Sprintf("%s rule %q could not be evaluated: %v", e.Kind, e.Rule.Source(), e.Err)
}

func (e *EvalError) Unwrap() error { return e.Err }

// Decide applies the set to vars: deny rules first, in order, then allow
// rules, in order. A rule that cannot be evaluated stops the evaluation with
// an [*EvalError], or with the context's error when that is why.
func (s Set) Decide(ctx context.Context, vars map[string]any) (Decision, error) {
	for _, rule := range s.Deny {
		matched, err := rule.Match(ctx, vars)
		if err != nil {
			return Decision{}, evalError(ctx, Deny, rule, err)
		}
		if matched {
			return Decision{Verdict: DeniedByRule, Rule: rule}, nil
		}
	}

	if len(s.Allow) == 0 {
		if s.WithoutAllow == Permitted {
			return Decision{Verdict: Permitted}, nil
		}
		return Decision{Verdict: NoAllowRules}, nil
	}

	for _, rule := range s.Allow {
		matched, err := rule.Match(ctx, vars)
		if err != nil {
			return Decision{}, evalError(ctx, Allow, rule, err)
		}
		if matched {
			return Decision{Verdict: Permitted, Rule: rule}, nil
		}
	}

	return Decision{Verdict: NoAllowRuleMatched}, nil
}

func evalError(ctx context.Context, kind Kind, rule Rule, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return &EvalError{Kind: kind, Rule: rule, Err: err}
}
