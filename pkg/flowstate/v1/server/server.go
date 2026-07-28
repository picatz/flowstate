package server

import (
	"context"
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
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
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

// WithNamespace records the Temporal namespace a workload runs in, which appears
// in the identity the run acts as.
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
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
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
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				Kind: &v1.GetResponse_Outputs{
					Outputs: &result,
				},
			},
		), nil
	case v1.RunResponse_STATUS_FAILED, v1.RunResponse_STATUS_CANCELED, v1.RunResponse_STATUS_TERMINATED, v1.RunResponse_STATUS_TIMED_OUT:
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				Kind: &v1.GetResponse_Error{
					Error: &v1.RunResponse_Error{
						Message: respStatus.String(),
					},
				},
			},
		), nil
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown workflow status: %d", respStatus))
	}
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
