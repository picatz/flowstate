package auth

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// SecretReference is a reference to a secret: the scheme that resolves it and the
// name within that scheme.
//
// It names the accessors generated for the flowstate.v1.SecretRef protobuf
// message, so a compiled reference satisfies it with no conversion, while this
// package keeps its rule of depending on no other Flowstate package. See
// [IdentitySource] for why that rule exists.
type SecretReference interface {
	GetScheme() string
	GetName() string
}

// Attributes a secret rule sees, beyond the workload.
const (
	// attrSecret is the object describing the reference being read.
	attrSecret = "secret"

	// secretTypeName is how that object is named in CEL, which appears in a type
	// error when a rule misuses a field.
	secretTypeName = "auth.secret"
)

// secret is the reference half of the attributes a secret rule sees.
//
// It is an object for the same reason the workload is: every field name is then a
// field rather than a variable, so no name can collide with an identifier CEL
// reserves, and a misspelled one is a startup error instead of a rule that never
// matches.
type secret struct {
	// Scheme is the provider the reference resolves through, such as "env" or
	// "vault".
	Scheme string `cel:"scheme"`

	// Name is the reference within that scheme.
	Name string `cel:"name"`
}

// SecretAccessPolicy is the file form of the rules governing which workloads may
// read which secrets.
//
// # Absent means nothing
//
// A deployment with no rules permits no secret to be read. This is the opposite
// default from credential targets, and the difference is deliberate: a target has
// to be configured before it exists, so an unconfigured one is already a refusal,
// whereas a secret scheme becomes readable the moment a provider is registered.
// The rules are what stands between a registered provider and every workload on
// the deployment, so their absence has to mean nothing rather than everything.
type SecretAccessPolicy struct {
	// Allow are CEL rules permitting access. At least one must match.
	//
	// The attributes are the workload object, as in the assumption rules, plus a
	// secret object with scheme and name:
	//
	//	# secret access policy
	//	allow:
	//	  - 'secret.scheme == "env" && workload.namespace == "acme"'
	//	  - 'secret.name.startsWith(workload.namespace + "/")'
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`

	// Deny are CEL rules refusing access. A reference matching any of them is
	// refused whatever Allow says.
	Deny []string `json:"deny,omitempty" yaml:"deny,omitempty"`

	// RuleCostLimit bounds the CEL evaluation cost of a single rule. Zero uses
	// [DefaultAssumeRuleCostLimit].
	RuleCostLimit uint64 `json:"rule_cost_limit,omitempty" yaml:"rule_cost_limit,omitempty"`
}

// Validate reports whether the rules compile and type-check, so a mistake in them
// fails at startup rather than the first time a workload reads a secret.
func (p SecretAccessPolicy) Validate() error {
	_, err := p.Compile()
	return err
}

// Compile builds the policy into a [SecretPolicy] a secret store can ask.
func (p SecretAccessPolicy) Compile() (*SecretPolicy, error) {
	limit := p.RuleCostLimit
	if limit == 0 {
		limit = DefaultAssumeRuleCostLimit
	}

	rules, err := compileSecretRules(p.Allow, p.Deny, limit)
	if err != nil {
		return nil, err
	}

	return &SecretPolicy{rules: rules, configured: len(p.Allow) > 0 || len(p.Deny) > 0}, nil
}

// SecretPolicy decides whether a workload may read a secret.
//
// It answers the same question as the assumption rules, in the same language,
// against the same workload attributes: a secret and a downstream identity are both
// things a workload should only reach if an operator said so. A secret store calls
// [SecretPolicy.Authorize] before resolving a reference.
//
// The zero value permits nothing, which is also what a policy with no rules does.
// A SecretPolicy is safe for concurrent use.
type SecretPolicy struct {
	rules secretRules

	// configured records whether any rules were given, so that permitting nothing
	// can be reported as an unconfigured deployment rather than as a rule that did
	// not match.
	configured bool
}

// Authorize reports whether the workload may read the referenced secret, returning
// a [*SecretDeniedError] when it may not.
//
// The identity's namespace is the tenant boundary, and it comes from the
// authenticated caller that submitted the run rather than from the workload, so a
// rule written against it cannot be circumvented by the workload it governs.
func (p *SecretPolicy) Authorize(ctx context.Context, identity WorkloadIdentity, ref StepRef, reference SecretReference) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if reference == nil || isNilPointer(reference) {
		return &SecretDeniedError{
			Subject:   identity.Subject,
			Namespace: orDefault(identity.Namespace),
			Reason:    ReasonSecretMalformed,
			Detail:    "no reference given",
		}
	}

	var (
		scheme = reference.GetScheme()
		name   = reference.GetName()
	)

	// An identity Flowstate never established must not be authorized against.
	// Without this check the zero identity resolves to the placeholder namespace
	// and would be permitted by any rule written for that tenant, which would make
	// "no identity" a tenant of its own.
	if err := identity.Validate(); err != nil {
		return &SecretDeniedError{
			Scheme:    scheme,
			Name:      name,
			Subject:   identity.Subject,
			Namespace: orDefault(identity.Namespace),
			Reason:    ReasonSecretNoIdentity,
			Detail:    err.Error(),
			Err:       err,
		}
	}

	subject, err := identity.SubjectFor(ref)
	if err != nil {
		// A workload that cannot be named cannot be authorized: a rule would have
		// nothing to match, and permitting it would mean permitting anything.
		return &SecretDeniedError{
			Scheme:    scheme,
			Name:      name,
			Subject:   identity.Subject,
			Namespace: orDefault(identity.Namespace),
			Reason:    ReasonSecretMalformed,
			Detail:    err.Error(),
			Err:       err,
		}
	}

	denied := func(reason SecretReason, detail string, cause error) error {
		return &SecretDeniedError{
			Scheme:    scheme,
			Name:      name,
			Subject:   subject,
			Namespace: orDefault(identity.Namespace),
			Reason:    reason,
			Detail:    detail,
			Err:       cause,
		}
	}

	switch {
	case scheme == "" || name == "":
		return denied(ReasonSecretMalformed, "a reference needs both a scheme and a name", nil)
	case !p.configured:
		return denied(ReasonSecretNoPolicy,
			"this deployment has configured no secret rules, so no workload may read any secret; add a secrets.allow rule", nil)
	}

	return p.rules.evaluate(ctx, secret{Scheme: scheme, Name: name}, subject, identity, ref, denied)
}

// secretRules holds the compiled allow and deny rules.
type secretRules struct {
	allow []assumeRule
	deny  []assumeRule
}

// evaluate applies the rules, deny first.
func (rs secretRules) evaluate(
	ctx context.Context,
	reference secret,
	subject string,
	identity WorkloadIdentity,
	ref StepRef,
	denied func(SecretReason, string, error) error,
) error {
	vars := assumeVars("", subject, "", identity, ref)
	vars[attrSecret] = reference
	delete(vars, attrTarget)
	delete(vars, attrAudience)

	for _, rule := range rs.deny {
		matched, err := rule.eval(ctx, vars)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return denied(ReasonSecretRuleError, fmt.Sprintf("deny rule %q could not be evaluated: %v", rule.src, err), err)
		}
		if matched {
			return denied(ReasonSecretDenyRule, rule.src, nil)
		}
	}

	for _, rule := range rs.allow {
		matched, err := rule.eval(ctx, vars)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return denied(ReasonSecretRuleError, fmt.Sprintf("allow rule %q could not be evaluated: %v", rule.src, err), err)
		}
		if matched {
			return nil
		}
	}

	if len(rs.allow) == 0 {
		// Deny rules alone would otherwise permit everything they did not name,
		// which is not what a default-deny policy can mean.
		return denied(ReasonSecretNoAllowRule,
			"only deny rules are configured, and a secret must be permitted by an allow rule", nil)
	}

	return denied(ReasonSecretNoAllowRule, "no allow rule matched", nil)
}

// newSecretEnv builds the CEL environment secret rules are compiled against.
func newSecretEnv() (*cel.Env, error) {
	return cel.NewEnv(
		ext.NativeTypes(ext.ParseStructTag("cel"),
			reflect.TypeOf(workload{}), reflect.TypeOf(callerIdentity{}), reflect.TypeOf(secret{})),
		cel.Variable(attrIdentity, cel.ObjectType(callerTypeName)),
		cel.Variable(attrWorkload, cel.ObjectType(workloadTypeName)),
		cel.Variable(attrSecret, cel.ObjectType(secretTypeName)),
		ext.Strings(ext.StringsVersion(5)),
	)
}

// compileSecretRules compiles the operator's rules, type-checking each one.
func compileSecretRules(allow, deny []string, costLimit uint64) (secretRules, error) {
	if len(allow) == 0 && len(deny) == 0 {
		return secretRules{}, nil
	}

	env, err := newSecretEnv()
	if err != nil {
		return secretRules{}, fmt.Errorf("%w: building secret rule environment: %w", ErrInvalidPolicy, err)
	}

	options := []cel.ProgramOption{
		cel.CostLimit(costLimit),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(100),
	}

	compile := func(kind string, sources []string) ([]assumeRule, error) {
		rules := make([]assumeRule, 0, len(sources))

		for _, src := range sources {
			if strings.TrimSpace(src) == "" {
				return nil, fmt.Errorf("%w: %s rule must not be empty", ErrInvalidPolicy, kind)
			}

			ast, issues := env.Compile(src)
			if issues.Err() != nil {
				return nil, fmt.Errorf("%w: secret %s rule %q is invalid: %w", ErrInvalidPolicy, kind, src, issues.Err())
			}
			if out := ast.OutputType(); !out.IsExactType(cel.BoolType) {
				return nil, fmt.Errorf("%w: secret %s rule %q evaluates to %s, want bool",
					ErrInvalidPolicy, kind, src, out.TypeName())
			}

			program, err := env.Program(ast, options...)
			if err != nil {
				return nil, fmt.Errorf("%w: secret %s rule %q could not be compiled: %w", ErrInvalidPolicy, kind, src, err)
			}

			rules = append(rules, assumeRule{src: src, prg: program})
		}

		return rules, nil
	}

	denyRules, err := compile("deny", deny)
	if err != nil {
		return secretRules{}, err
	}

	allowRules, err := compile("allow", allow)
	if err != nil {
		return secretRules{}, err
	}

	return secretRules{allow: allowRules, deny: denyRules}, nil
}
