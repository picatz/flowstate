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
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
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

// WithCredentialTargets makes validation deployment-aware: a Flowfile naming a
// JIT target this server's workers do not configure is refused before submission.
func WithCredentialTargets(targets ...string) Option {
	return func(s *FlowstateServer) {
		s.credentialTargetsConfigured = true
		s.credentialTargets = append([]string(nil), targets...)
	}
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

	namespace                   string
	deployment                  string
	identityClaims              []string
	credentialTargets           []string
	credentialTargetsConfigured bool
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

// signalPolicyMemoKey is the memo field recording which signals a run
// declared a delivery policy for, and what it is.
//
// Set once, at submit, from [v1.Workflow.Signals] — the same moment and the
// same mechanism [namespaceMemoKey] uses, and for the identical reason: a
// verb that has to authorize a signal delivery reads this via the
// `DescribeWorkflowExecution` it already calls to check tenancy, rather than
// asking Temporal a second question or reaching into the run's own history.
//
// Absent when the workflow declared no signal policy at all — which is the
// overwhelmingly common case today, and the zero case [v1.SignalPolicyAllows]'s
// own doc comment states: nothing here means every signal name is
// unconstrained, exactly as it was before this key existed. There is
// deliberately no compatibility arm to reach for, because absent already
// means the right thing.
const signalPolicyMemoKey = "flowstate.signalPolicy"

// starterMemoKey is the memo field recording the qualified issuer#subject of
// whoever started a run — the identity [v1.SignalPolicy.distinct_from_starter]
// compares an authorized sender against.
//
// Set once, at submit, from the same identity [namespaceMemoKey] already
// records, through [starterMemoEntry] — the one function both
// [FlowstateServer.Run] and [FlowstateServer.CreateSchedule] use to write
// it, for the identical "one function, two callers" reason
// [signalPolicyMemoEntry] exists. Absent on a run started before this key
// existed; see [memoStarter] in lifecycle.go for what that absence means to
// [authorizeSignal].
const starterMemoKey = "flowstate.starter"

// starterMemoEntry records the qualified issuer#subject of whoever is
// starting a run, under [starterMemoKey], written by both
// [FlowstateServer.Run] and [FlowstateServer.CreateSchedule] so that
// `distinct_from_starter` enforces identically on a direct run and on every
// firing of a schedule — the same discipline [signalPolicyMemoEntry]
// follows for the policy itself, and for the identical reason: a scheduled
// run's starter is whoever created the schedule, captured once and frozen,
// because there is no caller left to derive an identity from when a
// schedule fires at 03:00.
//
// Always written, even for an identity with no subject (an unauthenticated
// caller, only possible in development) — the same rule [namespaceMemoKey]
// follows a few lines above every caller of this function: a memo entry
// that is unconditionally present is what lets a reader tell "recorded as
// empty" apart from "never recorded at all", which is exactly the
// distinction [memoStarter] needs to answer a run that predates this key.
func starterMemoEntry(identity *v1.WorkloadIdentity) map[string]any {
	return map[string]any{
		starterMemoKey: v1.QualifiedSubject(identity.GetIssuer(), identity.GetSubject()),
	}
}

// signalPolicyMemoEntry encodes a workflow's declared signal policy into the
// one memo entry both [FlowstateServer.Run] and [FlowstateServer.CreateSchedule]
// write, so a scheduled run enforces exactly what a direct run does — the two
// paths cannot drift, because there is exactly one place that turns
// [v1.Workflow.Signals] into bytes.
//
// Resolves every rule's subject_from against inputs before encoding anything
// — through [v1.ResolveSignalPolicySubjects], which is also this function's
// one place a rule's subject_from is ever evaluated. inputs must already be
// the value [v1.BindRunInputs] returned; both callers pass it that way, so a
// scheduled run resolves a `subject: ${...}` against exactly the arguments
// the schedule was created with, and a direct run resolves against exactly
// what the caller submitted. The resolved policy — literal subjects only,
// subject_from always cleared — is what gets encoded; the caller's own
// wf.GetSignals() is never mutated.
//
// Returns a nil map (add nothing) when the workflow declares no policy at
// all, which is the zero case [v1.SignalPolicyAllows] documents: absent means
// unconstrained. A workflow that *does* declare a policy always yields a
// non-empty entry — CheckSignalPolicies refuses a `signals:` block that
// compiles to nothing, so "the key is present" and "a policy was recorded"
// are the same fact from every reader's side; see [signalPolicies] in
// lifecycle.go, which relies on that being true to fail closed on a present
// key that decodes to nothing.
//
// Encoded through the specification's own message rather than a bespoke
// wrapper type, which is what lets [signalPolicies] decode it with nothing
// more than `proto.Unmarshal` into a `*v1.Workflow` and read `.GetSignals()`
// back off it.
func signalPolicyMemoEntry(ctx context.Context, wf *v1.Workflow, inputs map[string]*v1.Value) (map[string]any, error) {
	if len(wf.GetSignals()) == 0 {
		return nil, nil
	}

	resolved, err := v1.ResolveSignalPolicySubjects(ctx, wf, inputs)
	if err != nil {
		return nil, fmt.Errorf("resolving the declared signal policy's per-run subjects: %w", err)
	}

	encoded, err := proto.Marshal(&v1.Workflow{Signals: resolved})
	if err != nil {
		return nil, fmt.Errorf("encoding the declared signal policy: %w", err)
	}

	return map[string]any{signalPolicyMemoKey: encoded}, nil
}

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
	if s.credentialTargetsConfigured {
		if err := v1.ValidateCredentialTargets(req.Msg.GetWorkflow(), s.credentialTargets); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	// Rules compile at submit, not at signal-time. Everything [v1.Validate] above
	// cannot see because it is a fact about a *set* of declared policies rather
	// than about one field — a policy for a signal name nothing waits for, a rule
	// that matches every sender — is caught here, once, rather than discovered the
	// first time a signal is actually delivered and denied for a reason the
	// author never saw at submit.
	if err := v1.CheckSignalPolicies(req.Msg.GetWorkflow()); err != nil {
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

	// The caller's half of the input contract, refused here rather than discovered
	// three steps in: every required input present, nothing undeclared, every value
	// of its declared type, and values only — never an expression the server would
	// then be evaluating on a caller's behalf, never a secret reference naming a
	// credential the specification did not choose.
	//
	// Through the same function `flow run local` binds with, so a submission this
	// refuses is one a local rehearsal refuses too and in the same words. Defaults
	// are filled in here, once: what goes into `RunState` is what every segment of
	// the run will see.
	inputs, err := v1.BindRunInputs(req.Msg.GetWorkflow(), req.Msg.GetInputs())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Weighed together, because they travel together. CheckSpecSize above bounds the
	// part an author wrote; this bounds the pair, so a caller cannot push a run past
	// what Temporal will store using arguments alone — which would be the wedged run
	// invariant 9 exists to convert into an answer, reached by the one path the
	// specification's own check cannot see.
	if err := v1.CheckSubmissionSize(req.Msg.GetWorkflow(), inputs); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	workflowID := fmt.Sprintf("flowstate-workflow-%s", uuid.NewString())

	// Capture the identity now, while the authenticated caller is still in scope.
	// The run outlives this request, so anything a later step needs to know about
	// who asked for the work has to be recorded in the state it carries.
	identity := s.identityFor(ctx)

	// The declared signal policy, resolved against inputs and frozen into the
	// memo now, exactly as the tenant is a few lines below — see
	// [signalPolicyMemoEntry], the one function this and
	// [FlowstateServer.CreateSchedule] both call, so a scheduled run and a
	// direct run resolve and enforce identically. And the starter, recorded
	// through [starterMemoEntry] for the same reason — what a policy's
	// `distinct_from_starter` will need to compare an authorized sender
	// against.
	memo := map[string]any{namespaceMemoKey: identity.GetNamespace()}
	for k, v := range starterMemoEntry(identity) {
		memo[k] = v
	}
	signalEntry, err := signalPolicyMemoEntry(ctx, req.Msg.GetWorkflow(), inputs)
	if err != nil {
		// Two different failures share this one call, and they get the same
		// answer for different reasons. CheckSignalPolicies and v1.Validate
		// above already accepted the specification's shape, so an encoding
		// failure is this handler unable to do what it just told the caller
		// it would do — not a caller mistake. But resolving a rule's
		// subject_from evaluates an expression over the caller's own bound
		// inputs, and a value that does not resolve to "<issuer>#<subject>"
		// is exactly the caller's mistake — the same one BindRunInputs above
		// already reports as InvalidArgument for an ordinary input. Either
		// way, refusing before the run starts is what invariant 6 asks for:
		// fail closed rather than start a run whose signal policy the server
		// itself could not finish establishing.
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	for k, v := range signalEntry {
		memo[k] = v
	}

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
		Memo: memo,

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

		// Checked and defaulted, once, above. The engine reads them and never
		// re-derives them, so every segment of the run sees what this submission
		// established.
		Inputs: inputs,
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
				Progress:   runProgress(ctx, temporal, resp),
				// From the same Describe response the status came from, so the
				// answer to "why has this been RUNNING for six hours" costs no
				// further round trip. See pendingActivities for what is and is
				// not claimed.
				PendingActivities: pendingActivities(resp),
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

				// The answer, beside the transcript. Unset when the workflow declared
				// no outputs, which is the same "nothing to report" a run started
				// before any of this existed reports — see the field's own comment,
				// which is why there is no empty message to distinguish them.
				RunOutputs: result.GetRunOutputs(),
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

	// A cancelled run that compensated has something to say, and `Error()` on a
	// cancellation is the bare word "canceled" — Temporal closes such a run with a
	// command whose only payload is the error's details, so the account of what was
	// taken back and what was left behind is in there and nowhere else.
	//
	// Reading it here is what makes `flow get` and `flow watch` show it. Without
	// this the summary is written into history and seen by nobody: the workflow
	// records it, the status is CANCELED, and the operator asking what happened to
	// their half-provisioned tenant is told "canceled" — which is the question, not
	// the answer.
	//
	// Prefixed by the status for the same reason the failure path appends rather
	// than replaces: what stopped, then what was done about it, in that order.
	var canceled *temporal.CanceledError
	if errors.As(err, &canceled) && canceled.HasDetails() {
		var summary string
		if canceled.Details(&summary) == nil && summary != "" {
			return status.String() + summary
		}
	}

	// No application error in the chain, which is what an uncompensated
	// cancellation or a timeout looks like: the run ended for a reason Temporal
	// knows and the workload never said anything about. Its own text is then the
	// best there is.
	if text := err.Error(); text != "" {
		return text
	}

	return status.String()
}

// heartbeatPhase reads the phase a running attempt last heartbeated.
//
// Empty for every shape of "nothing to say": an attempt that has not reported
// yet, an attempt waiting to be retried and therefore not running at all, a
// worker older than the field, or details this cannot decode. Those are different
// facts, and none of them is "the step is doing nothing" — which is why the schema
// says so on the field rather than leaving a renderer to guess.
//
// A decode failure is silence rather than an error, deliberately. This is an aside
// about a running attempt on a response whose subject is the run: a `flow get`
// that failed because a heartbeat payload was written by something encoding
// differently would be refusing to answer a question it can answer.
func heartbeatPhase(details *commonpb.Payloads) string {
	if details == nil || len(details.GetPayloads()) == 0 {
		return ""
	}

	var phase string
	if err := converter.GetDefaultDataConverter().FromPayload(details.GetPayloads()[0], &phase); err != nil {
		return ""
	}

	return phase
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

// progressQueryTimeout bounds the one part of a Get that reaches a worker.
const progressQueryTimeout = 2 * time.Second

// runProgress asks a running workload where it has got to.
//
// Nil on any failure, and that is the whole design of this function. A query reaches
// a *worker*, so it fails for reasons that say nothing about the run: no worker is
// polling the queue right now, the run is pinned to an interpreter built before the
// handler existed, the worker is busy, the query timed out. In every one of those the
// status, the start time and the run id are still correct and still worth returning.
//
// So a failed query costs the caller one optional field rather than the answer. The
// alternative — failing Get because the position could not be fetched — would make
// `flow get` on a healthy run start failing the moment a worker was restarted, which
// is both wrong and the kind of wrong that looks like the run's fault.
//
// Deliberately not logged at error level for the same reason: on a fleet with any
// unversioned or older workers this is an ordinary outcome, and an error line per
// `flow get` would train whoever reads the logs to ignore them.
func runProgress(ctx context.Context, temporal client.Client, resp *workflowservice.DescribeWorkflowExecutionResponse) *v1.RunProgress {
	// Only where Temporal itself says the execution is running.
	//
	// STATUS_RUNNING covers one case where it does not: a segment that continued as
	// new is *closed*, and is reported running because the workload is — the run id
	// somebody holds still names the workload they asked about. Temporal will answer
	// a query against a closed execution by replaying its history, so asking here
	// returns the position that segment finished at, presented as where the workload
	// is now. That is worse than saying nothing: it is a real step id, from the right
	// workload, that the run left behind possibly hours ago.
	//
	// Unset instead. A caller holding a superseded run id is asking about an attempt
	// that has handed off, and "no current position" is the true answer for it.
	if resp.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	workflowID := resp.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId()
	runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// Bounded separately from the request, because the request's own deadline is the
	// wrong bound for an optional field. Measured against a run whose worker is away:
	// the query took 10.5s to give up, and every `flow get` on such a run wore all of
	// it before printing a status it had already had. Waiting that long for something
	// nice to have is worse than not having it.
	//
	// Generous against the normal case rather than tuned — an answering worker
	// replies in milliseconds, so this only ever bites when nothing is going to
	// answer at all.
	ctx, cancel := context.WithTimeout(ctx, progressQueryTimeout)
	defer cancel()

	encoded, err := temporal.QueryWorkflow(ctx, workflowID, runID, engine.ProgressQuery)
	if err != nil {
		return nil
	}

	var progress v1.RunProgress
	if err := encoded.Get(&progress); err != nil {
		return nil
	}

	return &progress
}

// pendingActivities projects what Temporal is retrying into the schema's own
// vocabulary.
//
// The Describe response has carried this all along; the server read only the
// status beside it, so "why is this run stuck" was unobtainable through
// Flowstate and an operator had to leave the tenancy boundary this service
// enforces and ask the temporal CLI directly. Everything here is a projection
// of fields Temporal already answered with — no further round trip, and
// nothing inferred: an activity mid-retry has an attempt count and a last
// failure, and which *step* it is remains the progress query's answer.
func pendingActivities(resp *workflowservice.DescribeWorkflowExecutionResponse) []*v1.PendingActivity {
	infos := resp.GetPendingActivities()
	if len(infos) == 0 {
		return nil
	}

	out := make([]*v1.PendingActivity, 0, len(infos))
	for _, info := range infos {
		pending := &v1.PendingActivity{
			Attempt: info.GetAttempt(),
			// The message alone rather than the whole failure chain: the chain
			// repeats what the attempt count already says, and the outermost
			// message is the sentence the task classified its own failure into.
			LastFailure: info.GetLastFailure().GetMessage(),
		}

		// Only when Temporal set one: an attempt running right now has no next
		// schedule, and inventing a zero time would read as 1970.
		if scheduled := info.GetScheduledTime(); scheduled != nil {
			pending.NextAttemptScheduledTime = scheduled
		}

		// What the running attempt last said it was doing. Without this the phase a
		// worker heartbeats reaches Temporal and stops there — readable with the
		// `temporal` CLI and invisible through Flowstate, which is the same as not
		// having it for anyone inside the tenancy boundary this service exists to
		// enforce.
		//
		// A decode failure is silence rather than an error, and deliberately. This
		// is an aside about a running attempt on a response whose subject is the
		// run: a `flow get` that failed because a heartbeat payload was written by
		// something that encodes differently would be refusing to answer a question
		// it can answer.
		pending.Phase = heartbeatPhase(info.GetHeartbeatDetails())

		out = append(out, pending)
	}

	return out
}
