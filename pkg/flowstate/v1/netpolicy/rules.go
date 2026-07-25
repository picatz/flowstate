package netpolicy

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// rule is a compiled CEL policy rule. The program is built once, when the policy
// is constructed, and is safe to evaluate concurrently.
type rule struct {
	// src is the original expression text, reported in denial messages so an
	// operator can find the rule that fired.
	src string

	// prg is the compiled program.
	prg cel.Program
}

// ruleSet holds the allow and deny rules that apply at one evaluation scope.
type ruleSet struct {
	allow []rule
	deny  []rule
}

// empty reports whether the set has no rules, letting callers skip evaluation
// entirely.
func (rs ruleSet) empty() bool {
	return len(rs.allow) == 0 && len(rs.deny) == 0
}

// evaluate applies the set to vars and returns a [*DenyError] if the request is
// denied. Deny rules run first and take precedence, then allow rules gate the
// request when any are configured. A rule that fails to evaluate fails closed.
func (rs ruleSet) evaluate(ctx context.Context, target string, vars map[string]any) error {
	for _, r := range rs.deny {
		matched, err := r.eval(ctx, vars)
		if err != nil {
			return ruleFailure(ctx, "deny", r.src, target, err)
		}
		if matched {
			return &DenyError{
				Reason: ReasonDenyRule,
				Target: target,
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
			return ruleFailure(ctx, "allow", r.src, target, err)
		}
		if matched {
			return nil
		}
	}

	return &DenyError{
		Reason: ReasonNoAllowRule,
		Target: target,
		Detail: "no allow rule matched",
	}
}

// ruleFailure converts a rule evaluation failure into a denial, so that a rule
// that cannot be evaluated fails closed.
//
// A cancelled or expired context is returned as itself rather than as a denial:
// running out of time is not a policy decision, and reporting it as one would tell
// an operator their rules rejected a request that in fact never finished.
func ruleFailure(ctx context.Context, kind, src, target string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	return &DenyError{
		Reason: ReasonRuleError,
		Target: target,
		Detail: fmt.Sprintf("%s rule %q could not be evaluated: %v", kind, src, err),
		Err:    err,
	}
}

// eval evaluates the rule against vars. The context is threaded through, so an
// expensive rule is interrupted when the request is cancelled; a cheap rule
// finishes before the interrupt is next checked.
func (r rule) eval(ctx context.Context, vars map[string]any) (bool, error) {
	out, _, err := r.prg.ContextEval(ctx, vars)
	if err != nil {
		return false, err
	}

	matched, ok := out.Value().(bool)
	if !ok {
		// The output type is checked when the rule is compiled, so reaching here
		// means CEL produced something other than the declared type.
		return false, fmt.Errorf("rule produced %s, want bool", out.Type().TypeName())
	}

	return matched, nil
}

// ruleCompiler turns operator-supplied expressions into compiled programs. It
// holds one environment per evaluation scope, which is what makes a misscoped
// attribute a compile-time error rather than a surprise at request time.
type ruleCompiler struct {
	requestEnv *cel.Env
	connEnv    *cel.Env
	progOpts   []cel.ProgramOption
}

// newRuleCompiler builds the rule environments. Evaluation is cost-limited so a
// pathological rule cannot become a denial of service, and cost tracking is
// enabled alongside it because the limit is only enforced when costs are tracked.
func newRuleCompiler(costLimit uint64) (*ruleCompiler, error) {
	// Request-scoped attributes are known before a connection is made and are
	// fixed for the lifetime of one request.
	requestEnv, err := cel.NewEnv(
		cel.Variable("url", cel.StringType),
		cel.Variable("scheme", cel.StringType),
		cel.Variable("host", cel.StringType),
		cel.Variable("port", cel.IntType),
		cel.Variable("method", cel.StringType),
		cel.Variable("path", cel.StringType),
		ext.Strings(ext.StringsVersion(5)),
	)
	if err != nil {
		return nil, fmt.Errorf("building request rule environment: %w", err)
	}

	// Connection-scoped attributes are the ones that identify a connection, and
	// are therefore the ones that remain true for every request that reuses it.
	// Deliberately absent: method, path, and url, which vary per request.
	connEnv, err := cel.NewEnv(
		cel.Variable("scheme", cel.StringType),
		cel.Variable("host", cel.StringType),
		cel.Variable("port", cel.IntType),
		cel.Variable("ip", cel.StringType),
		ext.Strings(ext.StringsVersion(5)),
	)
	if err != nil {
		return nil, fmt.Errorf("building connection rule environment: %w", err)
	}

	return &ruleCompiler{
		requestEnv: requestEnv,
		connEnv:    connEnv,
		progOpts: []cel.ProgramOption{
			cel.CostLimit(costLimit),
			cel.EvalOptions(cel.OptTrackCost),
			cel.InterruptCheckFrequency(100),
		},
	}, nil
}

// compile compiles src, returning the program and the scope it belongs to. A rule
// is request-scoped unless it references an attribute that is only known once an
// address has been resolved, in which case it is connection-scoped.
func (rc *ruleCompiler) compile(kind, src string) (r rule, connScoped bool, err error) {
	if src == "" {
		return rule{}, false, fmt.Errorf("%s rule must not be empty", kind)
	}

	if requestAST, issues := rc.requestEnv.Compile(src); issues.Err() == nil {
		prg, err := rc.program(rc.requestEnv, requestAST, kind, src)
		if err != nil {
			return rule{}, false, err
		}
		return prg, false, nil
	} else {
		connAST, connIssues := rc.connEnv.Compile(src)
		if connIssues.Err() != nil {
			return rule{}, false, compileError(kind, src, issues.Err(), connIssues.Err())
		}
		prg, err := rc.program(rc.connEnv, connAST, kind, src)
		if err != nil {
			return rule{}, false, err
		}
		return prg, true, nil
	}
}

// program type-checks the result and builds the reusable program.
func (rc *ruleCompiler) program(env *cel.Env, ast *cel.Ast, kind, src string) (rule, error) {
	if out := ast.OutputType(); !out.IsExactType(cel.BoolType) {
		return rule{}, fmt.Errorf("%s rule %q evaluates to %s, want bool", kind, src, out.TypeName())
	}

	prg, err := env.Program(ast, rc.progOpts...)
	if err != nil {
		return rule{}, fmt.Errorf("%s rule %q could not be compiled: %w", kind, src, err)
	}

	return rule{src: src, prg: prg}, nil
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
			p.connRules.deny = append(p.connRules.deny, r)
		} else {
			p.requestRules.deny = append(p.requestRules.deny, r)
		}
	}

	for _, src := range p.cfg.allowRules {
		r, connScoped, err := compiler.compile("allow", src)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
		}
		if connScoped {
			p.connRules.allow = append(p.connRules.allow, r)
		} else {
			p.requestRules.allow = append(p.requestRules.allow, r)
		}
	}

	return nil
}
