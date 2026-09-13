package netpolicy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"

	"github.com/google/cel-go/ext"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
)

// rule is a compiled CEL policy rule. The program is built once, when the policy
// is constructed, and is safe to evaluate concurrently.
// ruleSet holds the allow and deny rules that apply at one evaluation scope:
// a [celrule.Set], deny first, permitting when only deny rules are configured.
type ruleSet struct {
	celrule.Set
}

// evaluate applies the set to vars and returns a [*DenyError] if the request is
// denied. Deny rules run first and take precedence, then allow rules gate the
// request when any are configured. A rule that fails to evaluate fails closed.
func (rs ruleSet) evaluate(ctx context.Context, target string, vars map[string]any) error {
	decision, err := rs.Decide(ctx, vars)
	if err != nil {
		return ruleFailure(ctx, target, err)
	}

	switch decision.Verdict {
	case celrule.DeniedByRule:
		return &DenyError{
			Reason: ReasonDenyRule,
			Target: target,
			Detail: decision.Rule.Source(),
		}
	case celrule.NoAllowRuleMatched:
		return &DenyError{
			Reason: ReasonNoAllowRule,
			Target: target,
			Detail: "no allow rule matched",
		}
	default:
		return nil
	}
}

// ruleFailure converts a rule evaluation failure into a denial, so that a rule
// that cannot be evaluated fails closed.
//
// A cancelled or expired context is reported as an [*UndecidedError] rather than
// as a denial: running out of time is not a policy decision, and reporting it as
// one would tell an operator their rules rejected a request that in fact never
// finished. The type is what a caller needs to tell "this policy never answered"
// from "this policy answered, and then the request died" — a distinction a bare
// context error cannot carry, because the transport and the peer return context
// errors too. It unwraps to the context's own error, so every errors.Is check
// against [context.Canceled] and [context.DeadlineExceeded] answers exactly as
// it did for the bare value this replaced.
func ruleFailure(ctx context.Context, target string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return &UndecidedError{Target: target, Err: ctxErr}
	}

	// The context did not end, so this is the evaluator's own failure, which
	// [celrule.Set.Decide] reports with the rule and its kind.
	return &DenyError{
		Reason: ReasonRuleError,
		Target: target,
		Detail: err.Error(),
		Err:    errors.Unwrap(err),
	}
}

// ruleCompiler turns operator-supplied expressions into compiled programs. It
// holds one environment per evaluation scope, which is what makes a misscoped
// attribute a compile-time error rather than a surprise at request time.
type ruleCompiler struct {
	requestEnv *cel.Env
	connEnv    *cel.Env
	costLimit  uint64
}

// newRuleCompiler builds the rule environments. Evaluation is cost-limited so a
// pathological rule cannot become a denial of service, and cost tracking is
// enabled alongside it because the limit is only enforced when costs are tracked.
func newRuleCompiler(costLimit uint64) (*ruleCompiler, error) {
	// The workload identity is fixed for the lifetime of a run, so it is known in
	// both scopes: a request-scoped rule can gate a URL by tenant, and a
	// connection-scoped rule can gate a resolved address by tenant. Declaring it as
	// a native type makes a rule naming `identity.nonexistent` a build-time error
	// rather than one that silently never matches.
	identityDecls := []cel.EnvOption{
		ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeFor[Identity]()),
		cel.Variable("identity", cel.ObjectType(identityTypeName)),
	}

	// Request-scoped attributes are known before a connection is made and are
	// fixed for the lifetime of one request.
	requestEnv, err := cel.NewEnv(append([]cel.EnvOption{
		cel.Variable("url", cel.StringType),
		cel.Variable("scheme", cel.StringType),
		cel.Variable("host", cel.StringType),
		cel.Variable("port", cel.IntType),
		cel.Variable("method", cel.StringType),
		cel.Variable("path", cel.StringType),
		// credentials is true when the request carries a worker-resolved
		// credential (a bearer secret, a JIT federation target, or a secret
		// reference nested in the headers or the structured body the task
		// resolves on its way out) — the same fact the http task's cleartext
		// refusal keys on (#963). It is request-scoped, not connection-scoped:
		// whether *this* request carries a credential is not a property of the
		// connection it may share with other requests, so a rule combining it
		// with ip is rejected at compile time the same way method or path
		// would be. Unset
		// reads as false, which is the compatible reading for a rule written
		// before this attribute existed: a rule that names host or method but
		// never credentials keeps meaning exactly what it meant.
		cel.Variable("credentials", cel.BoolType),
		ext.Strings(ext.StringsVersion(5)),
	}, identityDecls...)...)
	if err != nil {
		return nil, fmt.Errorf("building request rule environment: %w", err)
	}

	// Connection-scoped attributes are the ones that identify a connection, and
	// are therefore the ones that remain true for every request that reuses it.
	// Deliberately absent: method, path, and url, which vary per request.
	connEnv, err := cel.NewEnv(append([]cel.EnvOption{
		cel.Variable("scheme", cel.StringType),
		cel.Variable("host", cel.StringType),
		cel.Variable("port", cel.IntType),
		cel.Variable("ip", cel.StringType),
		ext.Strings(ext.StringsVersion(5)),
	}, identityDecls...)...)
	if err != nil {
		return nil, fmt.Errorf("building connection rule environment: %w", err)
	}

	return &ruleCompiler{
		requestEnv: requestEnv,
		connEnv:    connEnv,
		costLimit:  costLimit,
	}, nil
}

// compile compiles src, returning the program and the scope it belongs to. A rule
// is request-scoped unless it references an attribute that is only known once an
// address has been resolved, in which case it is connection-scoped.
func (rc *ruleCompiler) compile(kind, src string) (r celrule.Rule, connScoped bool, err error) {
	if strings.TrimSpace(src) == "" {
		return celrule.Rule{}, false, fmt.Errorf("%s rule must not be empty", kind)
	}

	if requestAST, issues := rc.requestEnv.Compile(src); issues.Err() == nil {
		prg, err := rc.program(rc.requestEnv, requestAST, kind, src)
		if err != nil {
			return celrule.Rule{}, false, err
		}
		return prg, false, nil
	} else {
		connAST, connIssues := rc.connEnv.Compile(src)
		if connIssues.Err() != nil {
			return celrule.Rule{}, false, compileError(kind, src, issues.Err(), connIssues.Err())
		}
		prg, err := rc.program(rc.connEnv, connAST, kind, src)
		if err != nil {
			return celrule.Rule{}, false, err
		}
		return prg, true, nil
	}
}

// program type-checks the result and builds the reusable program, through
// the one builder every policy surface shares, with the kind prefixed to
// whatever it refuses.
func (rc *ruleCompiler) program(env *cel.Env, ast *cel.Ast, kind, src string) (celrule.Rule, error) {
	r, err := celrule.Build(env, ast, src, rc.costLimit)
	if err != nil {
		return celrule.Rule{}, fmt.Errorf("%s %w", kind, err)
	}

	return r, nil
}

// compileError reports a rule that compiles in neither scope. When the two scopes
// disagree the rule mixes attributes from both, which no single evaluation point
// can satisfy, so both errors are reported along with the reason.
func compileError(kind, src string, requestErr, connErr error) error {
	if requestErr.Error() == connErr.Error() {
		return fmt.Errorf("%s rule %q is invalid: %w", kind, src, requestErr)
	}

	return fmt.Errorf(
		"%s rule %q mixes request-scoped and connection-scoped attributes, which cannot be evaluated together; "+
			"as a request rule: %w; as a connection rule: %w",
		kind, src, requestErr, connErr,
	)
}

// compileRules compiles every configured rule into the policy, splitting each one
// into the scope where all of its attributes are known.
func (p *Policy) compileRules() error {
	if len(p.cfg.allowRules) == 0 && len(p.cfg.denyRules) == 0 {
		return nil
	}

	compiler, err := newRuleCompiler(p.cfg.costLimit)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	for _, src := range p.cfg.denyRules {
		r, connScoped, err := compiler.compile("deny", src)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
		}
		if connScoped {
			p.connRules.Deny = append(p.connRules.Deny, r)
		} else {
			p.requestRules.Deny = append(p.requestRules.Deny, r)
		}
	}

	for _, src := range p.cfg.allowRules {
		r, connScoped, err := compiler.compile("allow", src)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
		}
		if connScoped {
			p.connRules.Allow = append(p.connRules.Allow, r)
		} else {
			p.requestRules.Allow = append(p.requestRules.Allow, r)
		}
	}

	return nil
}
