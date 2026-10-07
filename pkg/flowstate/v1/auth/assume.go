package auth

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"

	"github.com/google/cel-go/ext"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
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
	// It is the one [principal.Caller] every policy surface binds — issuer,
	// subject, namespace, kind, principal, claims, actions — so a clause about
	// the caller is portable across all of them (#548). "Same meaning" includes
	// an unset namespace: it stays the empty string here, exactly as the other
	// surfaces render it, rather than substituting [defaultComponent] the way
	// [workload.Namespace] does for subject composition (#568).
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
	// of: deployment, workflow, run, step. It is always the step's subject, even
	// for a target whose `subject_level` makes the assertion carry a coarser one,
	// so a rule gates the step that asked and not the grain the relying party
	// sees. Rules that gate on what Flowstate is
	// about to assert belong here; rules that gate on who asked belong on
	// [attrIdentity].
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

	// The caller that submitted the run, and the claims carried from its token,
	// are not repeated here: they are [callerIdentity]'s subject, issuer and
	// claims, and one value under two names on one surface is the mistake the
	// split between identity and workload exists to prevent (#567 D2).
}

// assumeRules holds the allow and deny rules governing credential assumption:
// a [celrule.Set], deny first, permitting when only deny rules are configured.
// Each rule's program is built once, when the broker is constructed, and is
// safe to evaluate concurrently.
type assumeRules struct {
	celrule.Set
}

// evaluate applies the rules and returns an [*AssumeDeniedError] when the request
// is refused.
//
// Deny rules run first and win, then an allow rule must match: a broker with no
// allow rule permits nothing, the same as secret access. A rule that fails to
// evaluate refuses the request: a policy that cannot be evaluated is not a policy
// that permits everything.
func (rs assumeRules) evaluate(ctx context.Context, target, subject string, vars map[string]any) error {
	if len(rs.Allow) == 0 {
		return &AssumeDeniedError{
			Target:  target,
			Subject: subject,
			Reason:  ReasonAssumeNoAllowRule,
			Detail:  "no allow rule is configured, and a target must be permitted by an allow rule",
		}
	}

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
		ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeFor[workload]()),
		principal.EnvOptions(),
		cel.Variable(attrTarget, cel.StringType),
		cel.Variable(attrAudience, cel.StringType),
		principal.Var(attrIdentity),
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

	return assumeRules{Allow: allowRules, Deny: denyRules}, nil
}

// assumeVars builds the attributes a rule is evaluated against.
func assumeVars(target, mintedSubject, audience string, identity WorkloadIdentity, ref StepRef) map[string]any {
	who := workload{
		Subject:    mintedSubject,
		Namespace:  orDefault(identity.Namespace),
		Deployment: orDefault(identity.Deployment),
		Workflow:   ref.Workflow,
		Run:        ref.Run,
		Step:       ref.Step,
	}

	return map[string]any{
		attrTarget:   target,
		attrAudience: audience,
		// Two principals, deliberately distinct. See [attrIdentity].
		attrIdentity: identity.Caller(),
		attrWorkload: who,
	}
}
