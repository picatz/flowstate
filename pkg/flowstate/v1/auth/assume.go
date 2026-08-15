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

	// attrIdentity is the authenticated caller: whoever presented the token that
	// started this run.
	//
	// It carries the same four fields, under the same names and with the same
	// meanings, that an egress rule and a task-shape rule already see — subject,
	// issuer, namespace, claims — so a clause about the caller is portable across
	// every policy surface this system has (#548).
	//
	// This is deliberately *not* an alias for [attrWorkload]. The first attempt at
	// unifying the vocabulary made it one, and that was worse than the split it
	// replaced: `identity.subject` would have meant the minted assertion subject
	// here and the authenticated caller everywhere else, so a rule copied from a
	// task-shape policy would compile, run, and quietly decide something other
	// than what it says. A name that means two things is harder to catch than two
	// names that mean two things, because nothing warns you.
	attrIdentity = "identity"

	// attrWorkload is the assertion this request would mint, which is a different
	// principal from the caller and keeps its own name for that reason.
	//
	// Its subject is [WorkloadIdentity.SubjectFor] — what a relying party's own
	// policy will see — and it carries the run context the caller has no notion
	// of: deployment, workflow, run, step. Rules that gate on what Flowstate is
	// about to assert belong here; rules that gate on who asked belong on
	// [attrIdentity].
	attrWorkload = "workload"

	// workloadTypeName is how the workload object is named in CEL, which appears
	// in a type error when a rule misuses a field.
	workloadTypeName = "auth.workload"

	// callerTypeName is the same, for the caller object.
	callerTypeName = "auth.callerIdentity"
)

// callerIdentity is the authenticated caller as a rule sees it.
//
// The fields and tags are exactly netpolicy's and taskpolicy's, and that is the
// point rather than a coincidence: `identity.namespace == "team-a"` has to mean
// one thing whether it is written in an egress policy, a task-shape policy, or
// here. Anything this package knows and they do not belongs on [workload].
type callerIdentity struct {
	// Subject is the caller's own subject, from the token they presented — not
	// the subject of any assertion this request might mint. See [attrIdentity]
	// for why conflating the two was the bug this type exists to prevent.
	Subject string `cel:"subject"`

	// Issuer is the issuer that vouched for the caller.
	//
	// It has no counterpart on [workload], and that absence was the gap left by
	// the first attempt here: `identity.issuer` compiled on two policy surfaces
	// and not the other two. Reading it from the caller closes that, and closes it
	// honestly — this is a token Flowstate received rather than one it minted.
	Issuer string `cel:"issuer"`

	Namespace string `cel:"namespace"`

	// Claims are the caller's claims. Reading an absent one is an error and an
	// errored rule refuses the request, so guard first:
	//
	//	"repository" in identity.claims && identity.claims["repository"] == "x"
	Claims map[string]string `cel:"claims"`
}

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
		ext.NativeTypes(ext.ParseStructTag("cel"),
			reflect.TypeOf(workload{}), reflect.TypeOf(callerIdentity{})),
		cel.Variable(attrTarget, cel.StringType),
		cel.Variable(attrAudience, cel.StringType),
		cel.Variable(attrIdentity, cel.ObjectType(callerTypeName)),
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
func assumeVars(target, mintedSubject, audience string, identity WorkloadIdentity, ref StepRef) map[string]any {
	claims := identity.Claims
	if claims == nil {
		// CEL cannot index a null map, and a rule reading claims["x"] for a
		// workload that carries none should simply not match.
		claims = map[string]string{}
	}

	who := workload{
		Subject:          mintedSubject,
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
		// Two principals, deliberately distinct. See [attrIdentity].
		attrIdentity: callerIdentity{
			Subject:   identity.Subject,
			Issuer:    identity.Issuer,
			Namespace: orDefault(identity.Namespace),
			Claims:    claims,
		},
		attrWorkload: who,
	}
}
