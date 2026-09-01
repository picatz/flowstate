package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TaskRuntime is the worker-side authority available to one task activity.
// Static-secret and JIT-credential capabilities share the authenticated identity
// and exact execution position, so policy cannot accidentally evaluate two
// different accounts of who is acting.
type TaskRuntime struct {
	Store    *secrets.Store
	Policy   *auth.SecretPolicy
	Broker   *auth.Broker
	Identity auth.WorkloadIdentity
	Step     auth.StepRef
}

// AuthorizeCredential obtains a short-lived credential for target and applies it
// directly to req. Material moves broker-to-request inside the activity and is
// never returned to workflow code.
//
// The decision is recorded (picatz/flowstate#1379): which workload asked to
// assume which target, and what the assumption policy answered. The record
// carries the target's operator-chosen name and never the credential — no
// field of it can hold one, which is the containment argument the audit
// schema makes structurally rather than by scrubbing.
//
// The allow is written after [auth.Broker.Authorize] returns, because the
// policy decision happens inside it, on the way to minting: this seam sees the
// answer and not the moment. It is still ahead of what the decision permits —
// the request has not been sent — so a required recorder that cannot write
// still stops the credential from being used, at the cost of a minted
// assertion that is then discarded. That is the safe direction; a request
// leaving with an unrecorded credential is not.
func AuthorizeCredential(ctx context.Context, req *http.Request, target string) error {
	subject := EnforcementSubject{
		Point:        AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION,
		ResourceKind: AuditResourceKind_AUDIT_RESOURCE_KIND_CREDENTIAL_TARGET,
		ResourceKey:  target,
	}

	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if ok {
		// Before the configured-or-not check below, not after it. A worker
		// with no broker still refuses a *workload*, and "which tenant tried
		// to assume this target" is the fact the record exists to carry —
		// especially for this denial, which is the one an operator meets while
		// a deployment is still being wired (Codex, picatz/flowstate#1394).
		// The only record left without an identity is the one where no runtime
		// reached this seam at all, which has none to name.
		subject.Identity = ProtoWorkloadIdentity(runtime.Identity)
	}

	if !ok || runtime.Broker == nil {
		return auditEnforcementDeny(ctx, subject, AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED,
			fmt.Errorf("workload identity federation is not configured on this worker"))
	}

	err := runtime.Broker.Authorize(ctx, req, runtime.Identity, runtime.Step, target)
	if err == nil {
		return auditEnforcementAllow(ctx, subject)
	}

	code, rule, decided := assumeDenial(err)
	if !decided {
		// The broker refuses for reasons that are not decisions: a token
		// exchange that failed, an identity the run never carried, a context
		// that expired. Recording those as denials would put refusals in the
		// trail that no policy made — the rule [taskPolicyRuleFailure] states
		// for a cancelled evaluation, applied to this seam.
		return err
	}

	subject.Rule = rule

	return auditEnforcementDeny(ctx, subject, code, err)
}

// assumeDenial reads an assumption refusal as the audit schema's closed
// vocabulary: the deny code, the rule that matched if one did, and whether
// this error is a policy decision at all.
//
// Only a rule that matched is copied. [auth.AssumeDeniedError.Detail] holds
// the rule source for a match and the CEL evaluation error for a failure, and
// an evaluation error can quote the data the rule was reading — see
// [EnforcementSubject.Rule].
func assumeDenial(err error) (code AuditDenyCode, rule string, decided bool) {
	if denied, ok := errors.AsType[*auth.AssumeDeniedError](err); ok {
		switch denied.Reason {
		case auth.ReasonAssumeDenyRule:
			return AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, denied.Detail, true
		case auth.ReasonAssumeNoAllowRule:
			return AuditDenyCode_AUDIT_DENY_CODE_NO_ALLOW_RULE, "", true
		case auth.ReasonAssumeRuleError:
			return AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR, "", true
		default:
			return AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, "", true
		}
	}

	if errors.Is(err, auth.ErrUnknownTarget) {
		// A target the deployment never configured cannot be assumed by
		// anyone, which is a refusal an operator reading the trail should see
		// as configuration rather than as policy.
		return AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, "", true
	}

	return AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, "", false
}

type secretRuntimeKey struct{}

// ContextWithTaskRuntime installs secret access for one task execution.
// References remain inert unless all four parts of the runtime are present.
func ContextWithTaskRuntime(ctx context.Context, runtime TaskRuntime) context.Context {
	return context.WithValue(ctx, secretRuntimeKey{}, runtime)
}

// ContextWithSecretStep derives the authority for a nested local task. It keeps
// the authenticated identity, store and policy and changes only the position the
// policy evaluates.
func ContextWithSecretStep(ctx context.Context, workflow, run, step string) context.Context {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok {
		return ctx
	}
	runtime.Step = auth.StepRef{Workflow: workflow, Run: run, Step: step}
	return ContextWithTaskRuntime(ctx, runtime)
}

// TaskStepRefFromContext reports the workflow, run, and step the currently
// executing task was invoked for, when the engine recorded a step on the
// context.
//
// The engine stamps the step id onto each node's context through
// [ContextWithSecretStep] before it runs the node (see runNodes), so any task
// runtime already carrying a [TaskRuntime] can read back which step it is
// serving. It is what lets `flow test` scope a stub to a step id rather than
// only to a task name, without threading a second, parallel channel of the same
// fact through the engine.
//
// It reports ("", false) when no step id is on the context, which is the honest
// answer for a compensation running off the run level context rather than a
// node's: an undo call is not "the step it undoes" running again, so a stub
// scoped to that step must not answer it.
func TaskStepRefFromContext(ctx context.Context) (auth.StepRef, bool) {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok || runtime.Step.Step == "" {
		return auth.StepRef{}, false
	}
	return runtime.Step, true
}

// TaskStepFromContext reports only the id from [TaskStepRefFromContext].
// Callers that need to distinguish steps in different workflows must use the
// full reference instead.
func TaskStepFromContext(ctx context.Context) (string, bool) {
	ref, ok := TaskStepRefFromContext(ctx)
	return ref.Step, ok
}

// ResolveSecret authorizes and resolves a reference from the current task's
// execution context. Authorization runs for every resolution, before the store is
// consulted, so a cache or provider can never turn a denied read into an allowed
// one.
//
// The decision is recorded (picatz/flowstate#1379), and the record is written
// in the same window the authorization is: after the policy answers and before
// the store is asked, so an allow is durable before the value exists and a
// required recorder that cannot write means the value is never fetched. What
// reaches the trail is the *reference* — "scheme:name", which
// [secrets.Ref]'s own doc calls safe to log because it names where a secret
// lives and carries no way to obtain it. The resolved secret is returned to
// the caller and has no field in the record to occupy.
func ResolveSecret(ctx context.Context, ref secrets.Ref) (secrets.Secret, error) {
	subject := EnforcementSubject{
		Point:        AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_SECRET_ACCESS,
		ResourceKind: AuditResourceKind_AUDIT_RESOURCE_KIND_SECRET,
		ResourceKey:  secrets.RefString(ref),
	}

	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if ok {
		// Same ordering, same reason as [AuthorizeCredential]: a missing store
		// or policy is a fact about this worker, and the record still has to
		// say which workload asked to read the reference.
		subject.Identity = ProtoWorkloadIdentity(runtime.Identity)
	}

	if !ok || runtime.Store == nil || runtime.Policy == nil {
		return secrets.Secret{}, auditEnforcementDeny(ctx, subject,
			AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED,
			fmt.Errorf("secret access is not configured on this worker"))
	}

	if err := runtime.Policy.Authorize(ctx, runtime.Identity, runtime.Step, ref); err != nil {
		code, rule, decided := secretDenial(err)
		if !decided {
			// A cancelled or expired context, which [auth.SecretPolicy.Authorize]
			// returns as itself: not something the policy decided.
			return secrets.Secret{}, err
		}
		subject.Rule = rule
		return secrets.Secret{}, auditEnforcementDeny(ctx, subject, code, err)
	}

	if err := auditEnforcementAllow(ctx, subject); err != nil {
		return secrets.Secret{}, err
	}

	resolver, err := runtime.Store.For(secretIdentity{namespace: runtime.Identity.Namespace})
	if err != nil {
		return secrets.Secret{}, err
	}
	return resolver.Resolve(ctx, ref)
}

// secretDenial reads a secret-access refusal as the audit schema's closed
// vocabulary, under [assumeDenial]'s rule about which text may be copied.
//
// A malformed reference and a workload with no established identity are
// recorded as POLICY_DENIED rather than under codes of their own: the finer
// codes each name what a *rule* did, and these two are the policy refusing on
// the shape of what it was asked, which is the case POLICY_DENIED already
// describes.
func secretDenial(err error) (code AuditDenyCode, rule string, decided bool) {
	denied, ok := errors.AsType[*auth.SecretDeniedError](err)
	if !ok {
		return AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, "", false
	}

	switch denied.Reason {
	case auth.ReasonSecretDenyRule:
		return AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, denied.Detail, true
	case auth.ReasonSecretNoAllowRule:
		return AuditDenyCode_AUDIT_DENY_CODE_NO_ALLOW_RULE, "", true
	case auth.ReasonSecretRuleError:
		return AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR, "", true
	case auth.ReasonSecretNoPolicy:
		return AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, "", true
	default:
		return AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, "", true
	}
}

// A deliberately tiny adapter: the secret store needs only the namespace and
// must not gain access to the rest of the caller's identity or claims.
type secretIdentity struct{ namespace string }

func (i secretIdentity) GetNamespace() string { return i.namespace }

// ProtoWorkloadIdentity renders a [TaskRuntime]'s [auth.WorkloadIdentity] as the
// wire type [WorkloadIdentity] carries.
//
// It exists on this side of the boundary rather than as a method on
// auth.WorkloadIdentity because auth deliberately imports no other Flowstate
// package — see [auth.IdentitySource] for why — so the direction that needs the
// generated type has to do the converting. This is the local driver's half of
// the identity the durable driver already has natively: engine/runtime.go's
// activities receive *v1.WorkloadIdentity straight from RunState, while the
// local driver's TaskRuntime.Identity is an auth.WorkloadIdentity built from
// command-line flags, and both need to reach [plugin.NewContextWithIdentity]
// carrying the same shape.
//
// A zero identity converts to a non-nil, all-empty message rather than nil, so
// that a run with no identity still sends an explicitly empty caller across the
// plugin boundary — the negative shape [plugin.IdentityFromContext] and the
// plugin SDK are both written to expect, rather than a caller inventing one.
func ProtoWorkloadIdentity(identity auth.WorkloadIdentity) *WorkloadIdentity {
	mode := WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION
	if identity.IsLocalRehearsal() {
		mode = WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL
	}

	return &WorkloadIdentity{
		Subject:    identity.Subject,
		Issuer:     identity.Issuer,
		Claims:     identity.Claims,
		Namespace:  identity.Namespace,
		Deployment: identity.Deployment,
		Mode:       mode,
	}
}
