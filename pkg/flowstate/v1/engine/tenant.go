package engine

import (
	"context"
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TenantInterceptor restricts a worker to one Flowstate namespace.
//
// Per-tenant task queues ([TaskQueues]) are how a tenant's runs are *addressed*
// to that tenant's worker fleet. This is what makes getting the addressing wrong
// an answer rather than a silence: a run that reaches a worker it does not
// belong to is refused, loudly and terminally, instead of being executed by a
// process holding another tenant's secrets, egress policy, and plugins.
//
// That is the difference the issue this implements names — fail-closed rather
// than fail-quiet. A misrouted run failing is a page somebody acts on; a
// misrouted run *succeeding* is a tenancy breach that leaves no trace at all,
// because every later request about it is still authorized against the run's
// own recorded tenant and still answers correctly.
//
// # What it guards
//
// The run, at the workflow entry point, which is the boundary that matters: a
// refused run schedules no activity, resolves no secret, and reaches no plugin.
// A workflow whose arguments do not carry a [v1.RunState] at all is refused too
// — a worker restricted to one tenant cannot tell whose work an unrecognized
// workflow is, and guessing is the fail-open answer.
//
// Also every activity whose arguments say whose work it is, as a second line
// for the case a wrong-tenant worker shares a queue with a right-tenant one and
// steals an activity task from a run the right-tenant worker already accepted.
// That is [Task] and the authorized arms, which take an identity parameter, and
// [TaskInScope] and [WorkflowVars], which carry one inside their [v1.Scope].
//
// A scope answers this question whether or not it holds an identity, which
// [tenantArg] argues at length because it is the thing that has been got wrong
// in both directions: the default tenant's namespace is the empty string, so a
// scope naming no identity names the default tenant rather than declining to
// answer, and a worker restricted to another tenant must refuse it.
//
// [TaskWithPrev] is the one arm carrying neither shape, which is precisely why
// it has nothing to check — it predates scopes and exists only to replay
// histories that name it.
//
// So this is defense in depth and not the boundary; the boundary is the queue
// plus the run refusal above.
//
// # What the refusal costs
//
// Both refusals are non-retryable, so a run that reaches the wrong worker fails
// rather than being retried until a right one happens to pick it up. Retrying
// would often work — and would leave the misconfiguration in place, unreported,
// until the day no correctly-configured worker is polling. A bound nothing
// reaches is a bound nothing tests, and a misconfiguration nothing reports is a
// misconfiguration nobody fixes.
//
// # What the refusal says
//
// It names the run's own namespace and never the worker's. The run's tenant is
// something its owner already knows; the worker's tenant is another tenant's
// name, and writing it into a failure that lands in this run's history would
// disclose the deployment's tenancy to the wrong party — the same reason
// FlowstateServer.clientFor names only the caller's own namespace when it
// refuses. An operator gets the whole picture from the worker's own logs, which
// are not tenant-readable.
func TenantInterceptor(namespace string) interceptor.WorkerInterceptor {
	return &tenantInterceptor{namespace: namespace}
}

type tenantInterceptor struct {
	interceptor.WorkerInterceptorBase
	namespace string
}

func (t *tenantInterceptor) InterceptWorkflow(
	ctx workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &tenantWorkflowInbound{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		namespace:                      t.namespace,
	}
}

func (t *tenantInterceptor) InterceptActivity(
	ctx context.Context, next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &tenantActivityInbound{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		namespace:                      t.namespace,
	}
}

type tenantWorkflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	namespace string
}

func (t *tenantWorkflowInbound) ExecuteWorkflow(
	ctx workflow.Context, in *interceptor.ExecuteWorkflowInput,
) (any, error) {
	state, ok := runStateArg(in.Args)
	if !ok {
		return nil, tenantRefusal(fmt.Sprintf(
			"this worker executes one tenant's workloads only, and this workflow carries no run state "+
				"for it to read a tenant from (%s)", flowWorkerTenantHint))
	}

	if got := state.GetIdentity().GetNamespace(); got != t.namespace {
		// Logged before returning, because the two audiences need different
		// facts: the run's owner gets the message below, and whoever operates
		// this worker gets both namespaces here, in a log nobody else reads.
		workflow.GetLogger(ctx).Error("refusing a run belonging to another tenant",
			"run_namespace", got, "worker_tenant", t.namespace)

		return nil, tenantRefusal(fmt.Sprintf(
			"this worker executes one tenant's workloads only, and this run belongs to namespace %q; "+
				"it reached a task queue this worker polls, which is a routing misconfiguration (%s)",
			got, flowWorkerTenantHint))
	}

	return t.Next.ExecuteWorkflow(ctx, in)
}

type tenantActivityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	namespace string
}

func (t *tenantActivityInbound) ExecuteActivity(
	ctx context.Context, in *interceptor.ExecuteActivityInput,
) (any, error) {
	// Only when the arguments say whose work this is: see [TenantInterceptor]
	// on why one arm has nothing to check, and why that makes this a second
	// line rather than the boundary.
	if got, ok := tenantArg(in.Args); ok {
		if got != t.namespace {
			return nil, tenantRefusal(fmt.Sprintf(
				"this worker executes one tenant's workloads only, and this task belongs to namespace %q; "+
					"it reached a task queue this worker polls, which is a routing misconfiguration (%s)",
				got, flowWorkerTenantHint))
		}
	}

	return t.Next.ExecuteActivity(ctx, in)
}

// flowWorkerTenantHint names the flag, so the sentence a person finds in a
// failed run points at the thing to look at rather than only at the symptom.
const flowWorkerTenantHint = "see flow worker --tenant"

// tenantRefusal builds the terminal, non-retryable failure a refused run
// reports.
//
// Classified [v1.ErrorKindPolicyDenied] rather than Internal: nothing is broken,
// a deployment's own configuration refused the work. That is the classification
// a client reads back off ApplicationError.Type, the same field
// [classifyRunError] and [activityError] already use.
func tenantRefusal(message string) error {
	return temporal.NewApplicationErrorWithOptions(
		"engine: "+message, v1.ErrorKindPolicyDenied.String(),
		temporal.ApplicationErrorOptions{NonRetryable: true},
	)
}

// runStateArg finds the run state among a workflow's arguments.
func runStateArg(args []any) (*v1.RunState, bool) {
	for _, arg := range args {
		if state, ok := arg.(*v1.RunState); ok && state != nil {
			return state, true
		}
	}

	return nil, false
}

// tenantArg reports the Flowstate namespace an activity's arguments say the
// work belongs to, and whether they say at all.
//
// By type rather than by position, because the arms take these at different
// indexes and a position is a thing that drifts when a signature gains a
// parameter.
//
// A [v1.Scope] answers whether or not it carries an identity, and that is the
// part worth stating, because the obvious reading — "no identity, nothing to
// check" — is what left a hole here. The default tenant's namespace is the
// empty string, so a scope with no identity is not an activity that declines
// to say whose work it is; it is an activity saying the default tenant's. A
// worker restricted to `team-a` must refuse it for the same reason it refuses
// `team-b`'s, and the version of this function that skipped it let exactly
// that activity through.
//
// The converse mistake is the one that made this subtle enough to get wrong
// twice: reading an identity-less scope as the default tenant is only correct
// once every scope that *should* carry an identity does. [WorkflowVars] is
// dispatched with a scope built at the call site rather than one derived from
// a run, and until it was given the run's identity, a `--tenant team-a` worker
// reading its scope as the default tenant would have refused every run
// declaring `vars:` — its own runs included. That is why the two dispatch
// sites set Identity, and why this comment names them.
//
// Only arguments carrying neither shape leave nothing to check: [TaskWithPrev],
// which predates scopes entirely and exists to replay histories that name it.
func tenantArg(args []any) (string, bool) {
	for _, arg := range args {
		if identity, ok := arg.(*v1.WorkloadIdentity); ok && identity != nil {
			return identity.GetNamespace(), true
		}
	}

	for _, arg := range args {
		if scope, ok := arg.(*v1.Scope); ok && scope != nil {
			return scope.GetIdentity().GetNamespace(), true
		}
	}

	return "", false
}
