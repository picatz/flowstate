package auth

import (
	"context"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// DefaultAssumeRuleCostLimit bounds the CEL evaluation cost of a single
// assumption rule, so a pathological expression cannot become a denial of
// service by itself.
const DefaultAssumeRuleCostLimit uint64 = 50_000

// Attributes available to an assumption rule.
//
// The request is described by two top-level attributes, and the workload by the
// fields of one object. Grouping the workload's attributes is not only tidiness:
// "namespace" is a reserved identifier in CEL and cannot be a variable name, and
// a claim an operator carries could collide with any other reserved word. Under
// an object, every name is a field, and no name is reserved.
const (
	// attrTarget is the operator's name for the system a credential is wanted
	// for, such as "aws-prod".
	attrTarget = "target"

	// attrAudience is the audience the target's exchanger requires.
	attrAudience = "audience"

	// attrIdentity is the object describing who is asking, and it is the spelling
	// to write.
	//
	// Egress rules and task-shape rules have always called this `identity`. This
	// package called the same thing `workload`, so an operator writing all four
	// policy surfaces had to remember which file used which word, and an
	// expression that was correct in one was a compile error in the next. One
	// meaning written down twice is the defect class CLAUDE.md names; this was
	// that class, spelled out across a whole configuration surface (#548).
	attrIdentity = "identity"

	// attrWorkload is the retired spelling of [attrIdentity], bound to the same
	// value so that policies written before the rename keep compiling.
	//
	// Both names are declared and both are populated, so `workload.namespace` and
	// `identity.namespace` are the same object in the same rule. It is deprecated
	// rather than removed because a policy file is a deployment's configuration
	// and this package refuses to start when a rule fails to compile: dropping the
	// old name in one release would take a worker down on upgrade, which is a
	// worse failure than an extra binding.
	attrWorkload = "workload"

	// workloadTypeName is how the workload object is named in CEL, which appears
	// in a type error when a rule misuses a field.
	workloadTypeName = "auth.workload"
)

// workload is the workload half of the attributes an assumption rule sees.
//
// The field tags are the names rules use, and they are deliberately the same
// names as the claims the minted assertion carries: an operator who has read an
// assertion can write a rule about it without translating. Declaring them as a
// struct is what makes a misspelled field a startup error rather than a rule that
// silently never matches.
type workload struct {
	// Subject is the assertion subject that would be minted, which is what a
	// relying party's own policy sees.
	Subject string `cel:"subject"`

	Namespace  string `cel:"namespace"`
	Deployment string `cel:"deployment"`
	Workflow   string `cel:"workflow"`
	Run        string `cel:"run"`
	Step       string `cel:"step"`

	// OnBehalfOf and OnBehalfOfIssuer identify the caller that submitted the run,
	// which is what makes a delegation rule expressible.
	OnBehalfOf       string `cel:"on_behalf_of"`
	OnBehalfOfIssuer string `cel:"on_behalf_of_issuer"`

	// Claims are the claims carried from that caller's token. Reading a claim
	// that is absent is an error, and an errored rule refuses the request, so a
	// rule about an optional claim should test for it first:
	//
	//	"repository" in workload.claims && workload.claims["repository"] == "x"
	Claims map[string]string `cel:"claims"`
}

// assumeRule is a compiled CEL assumption rule. The program is built once, when
// the broker is constructed, and is safe to evaluate concurrently.
type assumeRule struct {
	// src is the original expression text, reported in denials so an operator can
	// find the rule that fired.
	src string

	// prg is the compiled program.
	prg cel.Program
}

// assumeRules holds the allow and deny rules governing credential assumption.
type assumeRules struct {
	allow []assumeRule
	deny  []assumeRule
}

// evaluate applies the rules and returns an [*AssumeDeniedError] when the request
// is refused.
//
// Deny rules run first and win, then allow rules gate the request when any are
// configured. A rule that fails to evaluate refuses the request: a policy that
// cannot be evaluated is not a policy that permits everything.
func (rs assumeRules) evaluate(ctx context.Context, target, subject string, vars map[string]any) error {
	for _, rule := range rs.deny {
		matched, err := rule.eval(ctx, vars)
		if err != nil {
			return assumeRuleFailure(ctx, "deny", rule.src, target, subject, err)
		}
		if matched {
			return &AssumeDeniedError{
				Target:  target,
				Subject: subject,
				Reason:  ReasonAssumeDenyRule,
				Detail:  rule.src,
			}
		}
	}

	if len(rs.allow) == 0 {
		return nil
	}

	for _, rule := range rs.allow {
		matched, err := rule.eval(ctx, vars)
		if err != nil {
			return assumeRuleFailure(ctx, "allow", rule.src, target, subject, err)
		}
		if matched {
			return nil
		}
	}

	return &AssumeDeniedError{
		Target:  target,
		Subject: subject,
		Reason:  ReasonAssumeNoAllowRule,
		Detail:  "no allow rule matched",
	}
}

// assumeRuleFailure converts a rule evaluation failure into a refusal, so a rule
// that cannot be evaluated fails closed.
//
// A cancelled or expired context is returned as itself: running out of time is not
// a policy decision, and reporting it as one would tell an operator their rules
// refused a request that in fact never finished.
func assumeRuleFailure(ctx context.Context, kind, src, target, subject string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	return &AssumeDeniedError{
		Target:  target,
		Subject: subject,
		Reason:  ReasonAssumeRuleError,
		Detail:  fmt.Sprintf("%s rule %q could not be evaluated: %v", kind, src, err),
		Err:     err,
	}
}

// eval evaluates the rule. The context is threaded through, so an expensive rule
// is interrupted when the request is cancelled.
func (r assumeRule) eval(ctx context.Context, vars map[string]any) (bool, error) {
	out, _, err := r.prg.ContextEval(ctx, vars)
	if err != nil {
		return false, err
	}

	matched, ok := out.Value().(bool)
	if !ok {
		// The output type is checked at compile time, so reaching here means CEL
		// produced something other than the type it promised.
		return false, fmt.Errorf("rule produced %s, want bool", out.Type().TypeName())
	}

	return matched, nil
}

// newAssumeEnv builds the CEL environment assumption rules are compiled against.
//
// Declaring every attribute here is what makes a misspelled or invented one a
// startup error rather than a rule that quietly never matches.
func newAssumeEnv() (*cel.Env, error) {
	return cel.NewEnv(
		ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeOf(workload{})),
		cel.Variable(attrTarget, cel.StringType),
		cel.Variable(attrAudience, cel.StringType),
		cel.Variable(attrIdentity, cel.ObjectType(workloadTypeName)),
		cel.Variable(attrWorkload, cel.ObjectType(workloadTypeName)),
		ext.Strings(ext.StringsVersion(5)),
	)
}

// compileAssumeRules compiles the operator's rules, type-checking each one so a
// mistake fails at startup rather than the first time a workload asks for a
// credential.
func compileAssumeRules(allow, deny []string, costLimit uint64) (assumeRules, error) {
	if len(allow) == 0 && len(deny) == 0 {
		return assumeRules{}, nil
	}

	env, err := newAssumeEnv()
	if err != nil {
		return assumeRules{}, fmt.Errorf("%w: building assumption rule environment: %w", ErrInvalidPolicy, err)
	}

	options := []cel.ProgramOption{
		cel.CostLimit(costLimit),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(100),
	}

	compile := func(kind string, sources []string) ([]assumeRule, error) {
		rules := make([]assumeRule, 0, len(sources))

		for _, src := range sources {
			if src == "" {
				return nil, fmt.Errorf("%w: %s rule must not be empty", ErrInvalidPolicy, kind)
			}

			ast, issues := env.Compile(src)
			if issues.Err() != nil {
				return nil, fmt.Errorf("%w: %s rule %q is invalid: %w", ErrInvalidPolicy, kind, src, issues.Err())
			}
			if out := ast.OutputType(); !out.IsExactType(cel.BoolType) {
				return nil, fmt.Errorf("%w: %s rule %q evaluates to %s, want bool",
					ErrInvalidPolicy, kind, src, out.TypeName())
			}

			prg, err := env.Program(ast, options...)
			if err != nil {
				return nil, fmt.Errorf("%w: %s rule %q could not be compiled: %w", ErrInvalidPolicy, kind, src, err)
			}

			rules = append(rules, assumeRule{src: src, prg: prg})
		}

		return rules, nil
	}

	denyRules, err := compile("deny", deny)
	if err != nil {
		return assumeRules{}, err
	}

	allowRules, err := compile("allow", allow)
	if err != nil {
		return assumeRules{}, err
	}

	return assumeRules{allow: allowRules, deny: denyRules}, nil
}

// assumeVars builds the attributes a rule is evaluated against.
func assumeVars(target, subject, audience string, identity WorkloadIdentity, ref StepRef) map[string]any {
	claims := identity.Claims
	if claims == nil {
		// CEL cannot index a null map, and a rule reading claims["x"] for a
		// workload that carries none should simply not match.
		claims = map[string]string{}
	}

	who := workload{
		Subject:          subject,
		Namespace:        orDefault(identity.Namespace),
		Deployment:       orDefault(identity.Deployment),
		Workflow:         ref.Workflow,
		Run:              ref.Run,
		Step:             ref.Step,
		OnBehalfOf:       identity.Subject,
		OnBehalfOfIssuer: identity.Issuer,
		Claims:           claims,
	}

	return map[string]any{
		attrTarget:   target,
		attrAudience: audience,
		attrIdentity: who,

		// The retired spelling, bound to the same value. See [attrWorkload].
		attrWorkload: who,
	}
}
