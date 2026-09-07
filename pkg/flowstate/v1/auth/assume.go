package auth

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"

	"github.com/google/cel-go/ext"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
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
	// every policy surface this system has (#548). "Same meaning" includes an
	// unset namespace: it stays the empty string here, exactly as the other
	// three surfaces render it, rather than substituting [defaultComponent] the
	// way [workload.Namespace] does for subject composition (#568).
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

	// Namespace is the caller's namespace exactly as attested, empty when none
	// was set — the raw value, not [defaultComponent].
	//
	// [workload.Namespace] substitutes the placeholder because a minted subject
	// must always have the same number of components (#568's issue explains why
	// that reasoning belongs to [WorkloadIdentity.SubjectFor] and nowhere else).
	// This field has no such constraint: it is compared against operator-written
	// CEL on the same footing as netpolicy's and taskpolicy's `identity.namespace`,
	// which both carry the raw value. Defaulting it here and not there made one
	// name — `identity.namespace` — mean two different things depending on which
	// policy surface evaluated it: a `deny: identity.namespace == "_default"` rule
	// wired to secrets never matched the identical unnamespaced caller on egress or
	// task-shape policy, and the reverse rule wired the other way. Keep this raw so
	// a clause about the caller's namespace is portable, the same promise this
	// type's own doc comment already makes for its other three fields.
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
// assumeRules holds the allow and deny rules governing credential assumption:
// a [celrule.Set], deny first, permitting when only deny rules are configured.
type assumeRules struct {
	celrule.Set
}

// evaluate applies the rules and returns an [*AssumeDeniedError] when the request
// is refused.
//
// Deny rules run first and win, then allow rules gate the request when any are
// configured. A rule that fails to evaluate refuses the request: a policy that
// cannot be evaluated is not a policy that permits everything.
func (rs assumeRules) evaluate(ctx context.Context, target, subject string, vars map[string]any) error {
	decision, err := rs.Decide(ctx, vars)
	if err != nil {
		return assumeRuleFailure(ctx, target, subject, err)
	}

	switch decision.Verdict {
	case celrule.DeniedByRule:
		return &AssumeDeniedError{
			Target:  target,
			Subject: subject,
			Reason:  ReasonAssumeDenyRule,
			Detail:  decision.Rule.Source(),
		}
	case celrule.NoAllowRuleMatched:
		return &AssumeDeniedError{
			Target:  target,
			Subject: subject,
			Reason:  ReasonAssumeNoAllowRule,
			Detail:  "no allow rule matched",
		}
	default:
		return nil
	}
}

// assumeRuleFailure converts a rule evaluation failure into a refusal, so a rule
// that cannot be evaluated fails closed.
//
// A cancelled or expired context is returned as itself: running out of time is not
// a policy decision, and reporting it as one would tell an operator their rules
// refused a request that in fact never finished.
func assumeRuleFailure(ctx context.Context, target, subject string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	return &AssumeDeniedError{
		Target:  target,
		Subject: subject,
		Reason:  ReasonAssumeRuleError,
		Detail:  err.Error(),
		Err:     errors.Unwrap(err),
	}
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

	wrap := func(kind celrule.Kind, err error) error {
		return fmt.Errorf("%w: %s %w", ErrInvalidPolicy, kind, err)
	}

	denyRules, err := celrule.CompileAll(env, celrule.Deny, deny, costLimit, wrap)
	if err != nil {
		return assumeRules{}, err
	}

	allowRules, err := celrule.CompileAll(env, celrule.Allow, allow, costLimit, wrap)
	if err != nil {
		return assumeRules{}, err
	}

	return assumeRules{Set: celrule.Set{Allow: allowRules, Deny: denyRules}}, nil
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
			Subject: identity.Subject,
			Issuer:  identity.Issuer,
			// Raw, deliberately not orDefault: see [callerIdentity.Namespace].
			Namespace: identity.Namespace,
			Claims:    claims,
		},
		attrWorkload: who,
	}
}
