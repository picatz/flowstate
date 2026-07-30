package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// New creates a new FlowstateServer instance with the provided Temporal client.
func New(temporalClient client.Client, opts ...Option) *FlowstateServer {
	s := &FlowstateServer{
		temporalClient: temporalClient,
		maxStepsPerRun: maxStepsPerRunFromEnv(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a [FlowstateServer].
type Option func(*FlowstateServer)

// WithNamespace sets the Flowstate tenant a caller is treated as belonging to
// when their own identity names none.
//
// It is a fallback and only a fallback: a verified caller's namespace always
// wins, because a tenant decided by how the server was started rather than by who
// is calling would make the boundary decorative — every caller would share one
// tenant. See [FlowstateServer.identityFor], where that precedence lives.
//
// So the useful reading is "the tenant of a single-tenant deployment", whose trust
// policy names no namespaces. It is emphatically **not** the Temporal namespace,
// which it was previously documented as: Temporal routing is the client's own
// configuration, or [WithNamespacePool] when tenants map to separate namespaces.
// The distinction matters because this value ends up in
// [flowstatev1.WorkloadIdentity.Namespace], which every authorization decision
// about a run compares against.
func WithNamespace(name string) Option {
	return func(s *FlowstateServer) { s.namespace = name }
}

// WithDeployment records which Flowstate installation is running a workload.
//
// It appears in the identity a run acts as, so an assertion presented to an
// external system distinguishes a staging deployment from a production one.
func WithDeployment(name string) Option {
	return func(s *FlowstateServer) { s.deployment = name }
}

// WithNamespacePool routes each tenant's runs to the Temporal namespace its
// Flowstate namespace maps to.
//
// Without it every run goes to the one Temporal namespace the process was
// configured with, which is the zero-configuration path and stays correct: tenants
// are then kept apart by the tenant recorded on each run, which every request
// about a run is authorized against.
//
// With it the two protections compose rather than duplicate. The pool means a
// caller's namespace decides which Temporal namespace is even reachable, so a
// caller cannot address another tenant's run at the transport level — there is
// nothing there to describe. The recorded tenant still matters, because a
// deployment that maps several Flowstate namespaces onto one Temporal namespace
// has tenants sharing a namespace again, and that check is what separates them.
func WithNamespacePool(pool *temporalclient.Pool) Option {
	return func(s *FlowstateServer) { s.pool = pool }
}

// WithExecutionTimeout bounds how long a whole workload may take, including
// every Continue-As-New in its chain.
//
// There is deliberately no default. A workload that waits for a human is
// supposed to take as long as the human takes, and picking a number here would
// mean the engine deciding how long someone has to approve a deploy. A
// deployment that wants a ceiling sets one; a step that should not take forever
// gets its own `timeout:`.
func WithExecutionTimeout(d time.Duration) Option {
	return func(s *FlowstateServer) { s.executionTimeout = d }
}

// WithMaxStepsPerRun sets how many steps a run executes before continuing as new.
//
// Otherwise read once from FLOWSTATE_MAX_STEPS_PER_RUN, which an embedder
// building a server has no way to influence, and a test has no way to set without
// mutating the environment every other test shares.
func WithMaxStepsPerRun(steps int) Option {
	return func(s *FlowstateServer) { s.maxStepsPerRun = steps }
}

// WithIdentityClaims names the caller token claims to carry into a run's
// identity.
//
// Only named claims are copied, so the identity records what authorization
// decisions actually need — a repository, an environment, a team — rather than
// becoming a copy of whole tokens in workflow history.
func WithIdentityClaims(claims ...string) Option {
	return func(s *FlowstateServer) { s.identityClaims = claims }
}

// FlowstateServer implements the flowstatev1connect.WorkflowServiceHandler interface
// and provides methods to run and get the status of workflows using Temporal.
type FlowstateServer struct {
	temporalClient client.Client

	// pool routes a tenant's runs to the Temporal namespace its Flowstate
	// namespace maps to. Nil means every run uses temporalClient, which is the
	// zero-configuration path.
	pool *temporalclient.Pool

	// maxStepsPerRun is read once at construction. Reading it per request would
	// let the step budget change between a run and its own Continue-As-New,
	// which must not vary once a run has started.
	maxStepsPerRun int

	// executionTimeout bounds a whole workload chain. Zero means unbounded, which
	// is what a workload waiting on a person needs.
	executionTimeout time.Duration

	namespace      string
	deployment     string
	identityClaims []string
}

// namespaceMemoKey is the memo field recording which tenant a run belongs to.
//
// It is set when the run starts, from the authenticated caller, and it is what
// every later request about that run is authorized against. A memo rather than a
// search attribute because a memo needs no registration in the Temporal cluster,
// and requiring an operator to register attributes before the engine works would
// break the promise that a first run needs nothing but `temporal server
// start-dev`.
const namespaceMemoKey = "flowstate.namespace"

// maxStepsPerRunFromEnv reads the optional step budget.
func maxStepsPerRunFromEnv() int {
	if s := os.Getenv("FLOWSTATE_MAX_STEPS_PER_RUN"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

// clientFor returns the Temporal client a tenant's runs belong on.
//
// It refuses rather than falling back, and that is the whole point of it. When a
// deployment maps namespaces and has no entry for this tenant and no default, the
// pool reports an error; using the configured client anyway would place one
// tenant's runs in another tenant's namespace, where they would execute
// successfully and nothing would ever say so. A refusal is a misconfiguration
// someone fixes; a fallback is a tenancy breach nobody notices.
//
// A deployment that maps nothing gets the configured client, which is not a
// fallback but the answer: there is one namespace, and that is it.
func (s *FlowstateServer) clientFor(namespace string) (client.Client, error) {
	if s.pool == nil {
		return s.temporalClient, nil
	}

	temporal, err := s.pool.For(namespace)
	if err != nil {
		// FailedPrecondition rather than Internal: the deployment cannot route
		// this tenant, which an operator has to fix, and saying so is more useful
		// than reporting a server error. The message names only the caller's own
		// namespace — telling them which others are configured would describe the
		// deployment's tenancy to someone outside it.
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"this deployment has no Temporal namespace configured for %q: %w", namespace, err))
	}

	return temporal, nil
}

// Ensure FlowstateServer implements the WorkflowServiceHandler interface.
var _ flowstatev1connect.WorkflowServiceHandler = (*FlowstateServer)(nil)

// Shutdown gracefully shuts down the FlowstateServer, closing the Temporal client.
func (s *FlowstateServer) Shutdown(ctx context.Context) error {
	// The pool owns the clients it dialed, including the configured one, so
	// closing both would double-close. Its Close is idempotent; this branch is
	// about not closing a client the pool still holds.
	if s.pool != nil {
		s.pool.Close()
		return nil
	}

	s.temporalClient.Close()
	return nil
}

// Run starts a new workflow execution.
func (s *FlowstateServer) Run(ctx context.Context, req *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	// The specification is the largest attacker-controlled value this service
	// accepts, and this was the one RPC that did not check it. Signal, Cancel,
	// Terminate and List all validate here; Run did not, so every rule the schema
	// declares — the name pattern, the step-count ceiling, the URL format, every
	// length bound — held only because the CLI happens to install a protovalidate
	// interceptor in front of the handler.
	//
	// That is a bound enforced by a caller's configuration rather than by the
	// component the bound belongs to, which fails open for any embedder that
	// builds a server without it. Invariant 6 says specification validation denies
	// by default; defaulting is not something a handler can leave to whoever wired
	// it up.
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Size is a separate question from validity, and it has to be asked here
	// because here is where somebody is still listening.
	//
	// A specification under Temporal's blob limit is accepted and then carried,
	// with everything the run accumulates, across every Continue-As-New. Past the
	// limit Temporal refuses the Continue-As-New, which fails the workflow task,
	// which is retried — so the run does not fail, it wedges: RUNNING forever, on
	// an ever-climbing attempt count, occupying a worker each time. Measured at
	// 1.2 MiB: submitted fine, ran a step, attempt 5 forty-five seconds later.
	//
	// Refusing at submit turns that into a sentence an author can act on. The
	// engine keeps its own check for what this one cannot predict.
	if err := v1.CheckSpecSize(req.Msg.GetWorkflow()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID := fmt.Sprintf("flowstate-workflow-%s", uuid.NewString())

	// Capture the identity now, while the authenticated caller is still in scope.
	// The run outlives this request, so anything a later step needs to know about
	// who asked for the work has to be recorded in the state it carries.
	identity := s.identityFor(ctx)

	options := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: engine.RunTaskQueueName,

		// No run timeout.
		//
		// There was a six hour one here, and it made the engine's most
		// distinctive capability impossible: a workload that waits a week for a
		// human approval was terminated on the afternoon of the first day. Run
		// timeout is also the wrong instrument for what it looked like it was
		// doing — history growth is bounded by Continue-As-New, which this engine
		// already does, and a step that should not take forever is bounded by its
		// own `timeout:`, which is where an author looks for it.
		//
		// WorkflowExecutionTimeout bounds the whole chain including every
		// Continue-As-New, and is left to the deployment: there is no honest
		// default, because "how long may a business process take" is a question
		// about the business and not about the engine.
		WorkflowExecutionTimeout: s.executionTimeout,

		// The tenant is recorded as a memo so that authorizing a later request
		// against this run is one Describe rather than a walk through history.
		// A memo needs no cluster-side registration, which keeps this working
		// against `temporal server start-dev` with no operator setup.
		Memo: map[string]any{
			namespaceMemoKey: identity.GetNamespace(),
		},

		// One task queue serves every tenant, so without this the queue is
		// first-come-first-served and a tenant is one large workload away from
		// everyone else's work sitting behind theirs. A five-thousand-iteration
		// loop is an ordinary thing to write and a denial of service to everyone
		// sharing the queue with it — not deliberately, which is what makes it
		// likely.
		//
		// Temporal's fairness mechanism dispatches in proportion to weight per
		// key, so keying on the tenant gives each an equal share regardless of how
		// much work any of them submits. Activities inherit the run's priority, so
		// setting it here covers every task the run goes on to schedule, which is
		// where the contention actually is.
		//
		// Taken from the authenticated identity and never from the request, the
		// same rule as the namespace above: a workload must not be able to name
		// the bucket it is scheduled in, or the first thing anyone writes is the
		// one that puts them in their own.
		Priority: fairnessFor(identity.GetNamespace()),
	}

	// Chosen from the identity established by authenticating the caller, never
	// from anything the request said, so a workload cannot ask to run in another
	// tenant's namespace.
	temporal, err := s.clientFor(identity.GetNamespace())
	if err != nil {
		return nil, err
	}

	run, err := temporal.ExecuteWorkflow(ctx, options, engine.Run, &v1.RunState{
		Workflow:    req.Msg.GetWorkflow(),
		StepsBudget: int32(s.maxStepsPerRun),
		Identity:    identity,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to execute workflow: %w", err))
	}

	return connect.NewResponse(
		&v1.RunResponse{
			WorkflowId: workflowID,
			RunId:      run.GetRunID(),
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	), nil
}

// maxFairnessKeyBytes is what Temporal accepts for a fairness key.
//
// Asserted against [secrets.MaxNamespaceLen] below rather than assumed, because
// the two numbers are owned by different packages and only happen to be
// compatible. A namespace is validated to 63 characters and a fairness key may be
// 64 bytes, so every legal namespace fits — with one byte to spare, which is not
// the kind of margin to leave unwatched.
const maxFairnessKeyBytes = 64

// A compile-time check, so raising the namespace limit fails the build here
// rather than producing keys the server quietly rejects at submit.
var _ = [1]struct{}{}[secrets.MaxNamespaceLen-maxFairnessKeyBytes+1]

// fairnessFor returns the scheduling priority for a tenant.
//
// The empty namespace — an untenanted deployment, or one started with
// --insecure-no-auth — gets the empty key, which is Temporal's own default and
// the right answer: where there is one tenant there is nothing to be fair
// between.
//
// Only the key is set. Weight is left at its default so every tenant gets an
// equal share, and PriorityKey is left alone because ranking one tenant above
// another is a decision for whoever operates the deployment, expressed in task
// queue configuration, rather than something a server should invent.
func fairnessFor(namespace string) temporal.Priority {
	return temporal.Priority{FairnessKey: namespace}
}

// identityFor builds the identity a run will act as from the authenticated
// caller.
//
// The run records who asked for the work so that later steps can act on their
// behalf, and so an assertion presented to an external system states something
// established rather than assumed. Only the configured claims are copied: this
// value is persisted in workflow history, which is durable and broadly readable,
// so it must carry no more than authorization decisions need and nothing secret.
//
// An unauthenticated caller yields an identity with no subject rather than an
// error, because whether to allow that at all is decided by the authenticator,
// not here.
func (s *FlowstateServer) identityFor(ctx context.Context) *v1.WorkloadIdentity {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		// An unauthenticated caller yields an identity with no subject rather
		// than an error: whether to allow that at all is the authenticator's
		// decision, not this one's.
		return &v1.WorkloadIdentity{
			Namespace:  s.namespace,
			Deployment: s.deployment,
		}
	}

	// Derived rather than assembled here, so the namespace precedence has one
	// implementation. The verified caller's namespace wins; WithNamespace is only
	// the fallback for a deployment whose trust policy names none, meaning a
	// single tenant. The other order would make the tenant boundary decorative —
	// a namespace determined by how the server was started rather than by who the
	// caller is means every tenant shares one.
	derived := auth.IdentityFromPrincipal(principal, s.namespace, s.deployment, s.identityClaims...)

	return &v1.WorkloadIdentity{
		Subject:    derived.Subject,
		Issuer:     derived.Issuer,
		Claims:     derived.Claims,
		Namespace:  derived.Namespace,
		Deployment: derived.Deployment,
	}
}

// Get retrieves the status of a workflow execution by its ID (and optionally its run ID).
func (s *FlowstateServer) Get(ctx context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	// Authorized before anything is read. This previously described the run and
	// returned its status to whoever asked, so any caller who knew or guessed an
	// id could read another tenant's run — and a completed run returns its whole
	// outputs, which is the workload's data rather than only its existence.
	temporal, resp, err := s.authorizeRun(ctx, req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	switch respStatus := getWorkflowExecutionStatus(resp); respStatus {
	case v1.RunResponse_STATUS_RUNNING:
		start, closed := runTimes(resp.GetWorkflowExecutionInfo())

		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				StartTime:  start,
				CloseTime:  closed,
			},
		), nil
	case v1.RunResponse_STATUS_COMPLETED:
		var result v1.Workflow_StepOutputs
		// Through the client authorization used, so the run whose outputs are
		// read is the run that was checked. A completed run returns the whole of
		// its outputs, which is the workload's data and not merely its existence.
		if err := temporal.GetWorkflow(ctx, req.Msg.GetWorkflowId(), req.Msg.GetRunId()).Get(ctx, &result); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error getting workflow result: %w", err))
		}
		start, closed := runTimes(resp.GetWorkflowExecutionInfo())

		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				StartTime:  start,
				CloseTime:  closed,
				Kind: &v1.GetResponse_Outputs{
					Outputs: &result,
				},
			},
		), nil
	case v1.RunResponse_STATUS_FAILED, v1.RunResponse_STATUS_CANCELED, v1.RunResponse_STATUS_TERMINATED, v1.RunResponse_STATUS_TIMED_OUT:
		start, closed := runTimes(resp.GetWorkflowExecutionInfo())

		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				StartTime:  start,
				CloseTime:  closed,
				Kind: &v1.GetResponse_Error{
					Error: &v1.RunResponse_Error{
						Message: failureMessage(ctx, temporal, req.Msg.GetWorkflowId(), req.Msg.GetRunId(), respStatus),
					},
				},
			},
		), nil
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown workflow status: %d", respStatus))
	}
}

// failureMessage answers why a run ended the way it did.
//
// It used to answer with the status again — `Error{Message: respStatus.String()}` —
// so a caller was told a run failed and, asked why, told that it failed. The reason
// was never missing: Temporal had it the whole time, and the run had already been
// authorized to read outputs far more sensitive than an error string.
//
// The cost of that was not only silence. `flow watch` grew a `restatesStatus` helper
// whose only job was to notice this answer and drop it, so the terminal did not print
// `run "x" failed: STATUS_FAILED` — a workaround, in another package, for a sentence
// this function was choosing to produce.
//
// # Read through the authorized client
//
// Same client the authorization check used, for the reason the completed branch gives:
// the run whose failure is read has to be the run that was checked.
//
// # The application error, not Temporal's envelope and not the deepest cause
//
// Temporal wraps a workflow's error in a `WorkflowExecutionError` naming the workflow
// type, the id and the run id — all of which the caller already has and none of which
// is a reason. Unwrapping past it reaches the *application* error, which is the
// engine's own sentence: `engine: flowstate run failed: step "boom": ...`.
//
// The outermost application error rather than the deepest, deliberately. The deepest
// is the most specific — `unknown task "nosuchtask" (available: http, log)` — and it
// has lost the one thing an author needs first, which is *which step*. Going deeper
// trades the question "where do I look" for the question "what exactly went wrong",
// and the first is the one somebody reading a failure asks.
//
// Falling back to the status name is deliberate rather than lazy. A terminal run whose
// error cannot be read is a run this cannot describe, and inventing a description
// would be worse than repeating what is known — which is exactly what the old answer
// did in every case, and is now the answer only when there is nothing else.
func failureMessage(
	ctx context.Context,
	temporalClient client.Client,
	workflowID, runID string,
	status v1.RunResponse_Status,
) string {
	err := temporalClient.GetWorkflow(ctx, workflowID, runID).Get(ctx, nil)
	if err == nil {
		return status.String()
	}

	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Message() != "" {
		return app.Message()
	}

	// No application error in the chain, which is what a cancellation or a timeout
	// looks like: the run ended for a reason Temporal knows and the workload never
	// said anything about. Its own text is then the best there is.
	if text := err.Error(); text != "" {
		return text
	}

	return status.String()
}

// getWorkflowExecutionStatus maps the Temporal workflow execution status to Flowstate's run response status.
func getWorkflowExecutionStatus(resp *workflowservice.DescribeWorkflowExecutionResponse) v1.RunResponse_Status {
	return runStatus(resp.GetWorkflowExecutionInfo().GetStatus())
}

// runStatus maps a Temporal execution status to Flowstate's.
//
// Takes the status rather than a Describe response so a listing maps it the same
// way a Get does: two mappings would eventually disagree, and a run reported as
// running in one verb and failed in the other is a bug nobody can reproduce.
func runStatus(status enums.WorkflowExecutionStatus) v1.RunResponse_Status {
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return v1.RunResponse_STATUS_CANCELED
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return v1.RunResponse_STATUS_COMPLETED
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return v1.RunResponse_STATUS_RUNNING
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		return v1.RunResponse_STATUS_FAILED
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return v1.RunResponse_STATUS_TERMINATED
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return v1.RunResponse_STATUS_TIMED_OUT

	case enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		// Running, because the workload is. Continue-As-New closes one execution
		// and opens another, so this status marks a segment of a workload that
		// carried on rather than a workload that ended — and this engine reaches
		// it routinely, since a run that exhausts its step budget continues as new
		// by design.
		//
		// Callers address workloads, not segments: a run id is optional
		// everywhere, and unset means "whichever is current". Reporting a segment
		// as anything else would answer a question about the workload with a fact
		// about its bookkeeping. Left unmapped it fell to the default below, which
		// Get then rejects as an unknown status — asking about an earlier segment
		// by run id returned an internal error.
		return v1.RunResponse_STATUS_RUNNING

	default:
		return v1.RunResponse_STATUS_UNSPECIFIED
	}
}
