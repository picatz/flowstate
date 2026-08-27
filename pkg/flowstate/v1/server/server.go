package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
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
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// New creates a new FlowstateServer instance with the provided Temporal client.
//
// It reports an error rather than returning a server an option could not
// configure, which is what makes a misconfiguration a startup failure someone
// reads instead of a value the rest of this package then assumes something
// about — see [WithNamespace] for the assumption that made this necessary.
// `netpolicy.New` and `secrets/vault.NewProvider` are the same shape, for the
// same reason.
func New(temporalClient client.Client, opts ...Option) (*FlowstateServer, error) {
	s := &FlowstateServer{
		temporalClient: temporalClient,
		maxStepsPerRun: maxStepsPerRunFromEnv(),
		// The one place this package names the SDK default. Every read of a
		// memo, a schedule's stored arguments, or a heartbeat detail goes
		// through s.dataConverter, so a deployment that configures a payload
		// codec configures it once, here, through [WithDataConverter]. See
		// that option for why a second GetDefaultDataConverter call anywhere
		// else in this package is a bug rather than a shortcut.
		dataConverter: converter.GetDefaultDataConverter(),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("configuring the Flowstate server: %w", err)
		}
	}
	return s, nil
}

// Option configures a [FlowstateServer]. An option that cannot be satisfied
// reports an error, so a misconfigured deployment fails at startup rather than
// on the first request that depends on the value.
type Option func(*FlowstateServer) error

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
//
// # The namespace is checked here, once, for everything that derives from it
//
// This is the one place a deployment-configured namespace enters the server, so
// it is where [auth.ValidateNamespace] is applied — the same fail-closed check
// [v1.EntityWorkflowID] makes at derivation time, moved to the moment the value
// is chosen rather than repeated at each of the places that consume it. Those
// places all assume the grammar and cannot state it themselves:
//
//   - [scheduleIDFor] composes `flowstate-schedule-<namespace>_<name>` and
//     [scheduleNameFrom] splits at the first underscore. That is unambiguous
//     only because the grammar admits `[a-z0-9-]` and therefore no underscore:
//     a namespace of `team_a` with a schedule named `x` derives the same id as
//     namespace `team` with a schedule named `a_x`.
//   - [runStaticSummary] delimits with backticks on the strength of the same
//     grammar.
//   - [FlowstateServer.taskQueueFor] and the secret providers re-check it and
//     refuse, so an ungrammatical value configured here would surface as a
//     per-request refusal on a routed deployment rather than as a
//     misconfiguration at startup.
//
// The empty namespace stays legal and is the single-tenant default:
// [auth.ValidateNamespace] admits it deliberately, and the derivations above
// handle it — an untenanted deployment's schedule ids carry a leading separator
// no non-empty namespace can produce, and the `_default` task queue and secret
// segments are unforgeable precisely because the grammar forbids the underscore
// they begin with. A deployment that names no tenant must keep working exactly
// as it did, so refusing "" here would break the zero-configuration path this
// option exists to serve.
//
// A namespace carried by a *caller's* identity is not covered by this check and
// does not need to be: it is validated at admission, where it is chosen
// (`auth.TrustedIssuer.namespaceFrom`, `auth/policy.go`). What was uncovered
// until this option validated was the deployment's own fallback, which nothing
// upstream of the process sees.
func WithNamespace(name string) Option {
	return func(s *FlowstateServer) error {
		if err := auth.ValidateNamespace(name); err != nil {
			return fmt.Errorf(
				"WithNamespace: %w (the namespace grammar is auth.ValidateNamespace, "+
					"pkg/flowstate/v1/auth/namespace.go; ids derived from a tenant, such as a "+
					"schedule's, are unambiguous only for namespaces it admits)", err)
		}
		s.namespace = name
		return nil
	}
}

// WithDeployment records which Flowstate installation is running a workload.
//
// It appears in the identity a run acts as, so an assertion presented to an
// external system distinguishes a staging deployment from a production one.
func WithDeployment(name string) Option {
	return func(s *FlowstateServer) error { s.deployment = name; return nil }
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
	return func(s *FlowstateServer) error { s.pool = pool; return nil }
}

// WithTemporalNamespace records which Temporal namespace this server's own client
// is dialed for.
//
// It routes nothing and changes nothing about where a run goes. The client handed
// to [New] was dialed for a namespace before this server existed; this only lets
// the server say which one, because a [client.Client] carries its namespace on
// every request it sends and offers no way to read it back. Temporal's raw APIs
// take the namespace as a request field, so anything reaching past the SDK needs
// it named.
//
// It is emphatically not [WithNamespace], which sets the *Flowstate tenant* a
// caller whose identity names none is treated as belonging to. Those are two
// boundaries with one word between them, and a deployment can legitimately set
// both to values with nothing to do with each other — see [WithNamespace] for the
// documentation bug that made this distinction expensive once already.
//
// It is also not a new spelling of "which namespace". `cmd/flow` already
// computes exactly this value, as `cfg.Options().Namespace`, and already passes it
// into this package as an argument — to [EnsureSearchAttributesRegistered], from
// `cmd/flow/main.go` and `cmd/flow/serverdev.go`. This gives that
// already-computed value a second home on the server rather than inventing a way
// to derive it here.
//
// A deployment routing tenants onto separate Temporal namespaces has no single
// answer to record and does not need one: [WithNamespacePool] supplies the
// per-tenant answer, which [FlowstateServer.temporalNamespaceFor] prefers.
//
// The empty namespace is refused. There is no deployment it is right for —
// [temporalclient.Config.Options] resolves a non-empty namespace for every
// configuration, falling back to [temporalclient.DefaultNamespace] — so a caller
// passing one has an unresolved value in hand, and recording it would put an empty
// namespace on a request that fails somewhere far from here. Better a startup
// error naming the option.
func WithTemporalNamespace(name string) Option {
	return func(s *FlowstateServer) error {
		if name == "" {
			return errors.New("WithTemporalNamespace: the Temporal namespace must be named; " +
				"temporalclient.Config.Options resolves a non-empty one for every configuration, " +
				"so an empty value here is an unresolved one rather than a choice")
		}
		s.temporalNamespace = name
		return nil
	}
}

// WithTaskQueues routes each tenant's runs to a task queue of its own, so a
// per-tenant worker fleet is addressable rather than merely startable.
//
// Without it every run goes to [engine.RunTaskQueueName], which is the
// zero-configuration path and the whole of today's behavior: one queue, every
// tenant, whatever workers poll it. A deployment with one team has nothing to
// route between and should not have to say so.
//
// With it, the queue a run is submitted to is derived from the run's
// authenticated tenant — never from the request, the same rule the namespace
// memo and the fairness key already follow, and for the same reason: a workload
// that could name its own queue could name the fleet that executes it.
//
// It composes with [WithNamespacePool] rather than duplicating it. The pool
// decides which Temporal namespace a tenant's runs live in (history and
// visibility isolation); this decides which worker fleet inside that namespace
// executes them (process isolation — the claim that a compromised worker holds
// one tenant's secrets). A deployment can sensibly do either alone.
//
// The paired half is on the worker: `flow worker --tenant` refuses a run
// belonging to anyone else, which is what turns a routing mistake into a failure
// instead of a cross-tenant execution. See [engine.TenantInterceptor].
func WithTaskQueues(queues engine.TaskQueues) Option {
	return func(s *FlowstateServer) error { s.taskQueues = queues; return nil }
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
	return func(s *FlowstateServer) error { s.executionTimeout = d; return nil }
}

// WithMaxStepsPerRun sets how many steps a run executes before continuing as new.
//
// Otherwise read once from FLOWSTATE_MAX_STEPS_PER_RUN, which an embedder
// building a server has no way to influence, and a test has no way to set without
// mutating the environment every other test shares.
func WithMaxStepsPerRun(steps int) Option {
	return func(s *FlowstateServer) error { s.maxStepsPerRun = steps; return nil }
}

// WithIdentityClaims names the caller token claims to carry into a run's
// identity.
//
// Only named claims are copied, so the identity records what authorization
// decisions actually need — a repository, an environment, a team — rather than
// becoming a copy of whole tokens in workflow history.
func WithIdentityClaims(claims ...string) Option {
	return func(s *FlowstateServer) error { s.identityClaims = claims; return nil }
}

// WithCredentialTargets makes validation deployment-aware: a Flowfile naming a
// JIT target this server's workers do not configure is refused before submission.
func WithCredentialTargets(targets ...string) Option {
	return func(s *FlowstateServer) error {
		s.credentialTargetsConfigured = true
		s.credentialTargets = append([]string(nil), targets...)
		return nil
	}
}

// WithPluginCatalog supplies the server/worker capability snapshot used to pin
// plugin requirements before a durable run is accepted.
func WithPluginCatalog(catalog *v1.PluginCatalog) Option {
	return func(s *FlowstateServer) error {
		if catalog != nil {
			s.pluginCatalog = proto.Clone(catalog).(*v1.PluginCatalog)
		}
		return nil
	}
}

// WithTrustedWorkflows installs deployment-owned workflow specifications for
// remote submission, scoped to namespace exactly as [FlowstateServer.NewWebhookReceiver]'s
// own namespace argument is: empty means this deployment's own, unnamed
// tenant — the same value [FlowstateServer.identityFor] falls back to for an
// unauthenticated caller or one WithNamespace named. A request whose
// namespace and workflow name match one of these entries is authorized and
// executed from the trusted copy, not from the copy carried by the request.
// This is what makes restrictions such as `manual: denied` and
// `manual.allowed_principals` authorization policy rather than caller input.
//
// Workflows not named by this option retain the open submission behavior: the
// Run API accepts ad-hoc specifications. Deployments that use manual restrictions
// must therefore register the restricted workflow here under its name. Cloning at
// configuration time prevents a caller that still holds the source pointer from
// changing policy after the server has started.
//
// namespace is required to be explicit — never derived from a prior
// [WithNamespace] option — because [Option]s apply in the order given and a
// value read from options that may not have run yet would make this entry's
// tenant depend on argument order rather than say what it means. A
// deployment using a non-empty default namespace passes it here the same way
// it does everywhere else that scopes a namespace explicitly.
//
// A registration this option cannot honour never degrades to open submission.
// A name registered twice with different specifications, or one whose
// specification the schema refuses, poisons that one key — see
// [FlowstateServer.refuseTrustedWorkflow] — and a registration carrying no name
// at all, which no key could be refused for, refuses the construction.
func WithTrustedWorkflows(namespace string, workflows ...*v1.Workflow) Option {
	return func(s *FlowstateServer) error {
		if s.trustedWorkflows == nil {
			s.trustedWorkflows = make(map[trustedWorkflowKey]*v1.Workflow)
		}
		for _, workflow := range workflows {
			// A registration naming nothing is the one case that cannot be
			// answered by refusing a key, because it has no key: the map is
			// addressed by (namespace, name) and this entry supplies no name.
			// Skipping it silently — what this used to do — leaves an embedder
			// believing a deployment-owned policy is installed while every
			// submission under whatever name they meant is still open, which is
			// the fail-open direction of the very mechanism this option exists
			// to provide. The two arms below can be scoped to one key and so
			// are; this one cannot, so it refuses construction, which is where
			// [Option] says a misconfigured deployment should fail.
			if workflow == nil || workflow.GetName() == "" {
				return fmt.Errorf("a workflow registered as trusted for namespace %q has no `name:`, "+
					"and the trusted set is addressed by name, so nothing would ever resolve to it "+
					"while submissions under the name intended stay open; give it the name callers "+
					"submit", namespace)
			}
			key := trustedWorkflowKey{namespace: namespace, name: workflow.GetName()}

			// A specification the schema refuses must not become the copy
			// substituted for a caller's. Nothing downstream would catch it:
			// the RPC handlers run [v1.Validate] on the *request*, which is
			// the caller's copy and is checked before the trusted lookup
			// replaces it, and validateSubmission does not ask again after
			// substitution. So an embedder's malformed programmatic workflow
			// would reach Temporal and the engine on the strength of being
			// deployment-owned.
			if err := v1.Validate(workflow); err != nil {
				s.refuseTrustedWorkflow(key, fmt.Sprintf(
					"workflow %q is registered as a trusted specification this deployment refuses: %v",
					workflow.GetName(), err))
				continue
			}

			if existing, ok := s.trustedWorkflows[key]; ok && !proto.Equal(existing, workflow) {
				// Two registrations disagree about one tenant's workflow.
				// Last-writer-wins here would let a later, weaker copy
				// replace `manual: denied` or a narrower
				// `allowed_principals` — silently, and decided by option
				// order rather than by anything an operator wrote down.
				// An [Option] can report an error, and this deliberately
				// does not: a conflict is scoped to one tenant's one
				// workflow, and failing construction would take a whole
				// deployment's other tenants down with it. So the conflict is
				// recorded and every request for this key — and only this key
				// — is refused; see
				// [FlowstateServer.noteTrustedWorkflowConflict]. The malformed
				// specification arm above refuses the same way for the same
				// reason.
				s.refuseTrustedWorkflow(key, fmt.Sprintf(
					"workflow %q is registered twice for this namespace with different specifications, "+
						"so this deployment has no single policy to enforce for it; an operator must "+
						"make the trusted registrations agree", workflow.GetName()))
				continue
			}
			s.trustedWorkflows[key] = proto.Clone(workflow).(*v1.Workflow)
		}
		return nil
	}
}

// refuseTrustedWorkflow marks a key with no usable answer, against the sentence
// [FlowstateServer.trustedWorkflow] will return for it.
//
// Fail closed, per CLAUDE.md: a component that allows when it cannot decide
// will eventually allow everything, and this is precisely a case of not being
// able to decide. Neither candidate can be served — the later one may be the
// weaker policy, and the earlier one may be the stale one — and serving the
// *caller's* copy is the open behavior this whole mechanism exists to remove.
// So the key is refused outright until the deployment's configuration says one
// thing about it.
//
// Identical re-registration is not a conflict. The same specification loaded
// twice (one Flowfile named to two receivers, an option restating what a
// receiver also serves) says one thing, so it authorizes one thing.
//
// Callers hold trustedWorkflowsMu for writing.
func (s *FlowstateServer) refuseTrustedWorkflow(key trustedWorkflowKey, reason string) {
	if s.trustedWorkflowRefusals == nil {
		s.trustedWorkflowRefusals = make(map[trustedWorkflowKey]string)
	}
	// First reason wins: it is the one describing the registration that was
	// actually rejected, and a later one would only restate that the key is
	// still unusable.
	if _, already := s.trustedWorkflowRefusals[key]; !already {
		s.trustedWorkflowRefusals[key] = reason
	}
}

// trustedWorkflowKey identifies a trusted workflow specification by both the
// tenant it is trusted for and its name — a struct, not a concatenated
// string. CLAUDE.md's namespacing rule is why: every character legal in a
// namespace is also legal in a name, so no separator disambiguates
// `namespace + name` from a different pair that happens to concatenate to
// the same string — `"team-a"` + `"deploy"` and `"team"` + `"a-deploy"`
// collide under any single-character or fixed separator. A struct compares
// each field independently and cannot be made to collide this way; see the
// `env` and file secret providers' own namespacing fix for the same mistake
// made twice already.
type trustedWorkflowKey struct {
	namespace string
	name      string
}

// FlowstateServer implements the flowstatev1connect.WorkflowServiceHandler interface
// and provides methods to run and get the status of workflows using Temporal.
type FlowstateServer struct {
	temporalClient client.Client

	// pool routes a tenant's runs to the Temporal namespace its Flowstate
	// namespace maps to. Nil means every run uses temporalClient, which is the
	// zero-configuration path.
	pool *temporalclient.Pool

	// temporalNamespace is the Temporal namespace temporalClient is dialed for,
	// on a deployment with no pool. Empty means nobody said, which
	// [FlowstateServer.temporalNamespaceFor] refuses rather than guesses.
	//
	// Emphatically not [FlowstateServer.namespace], which is the *Flowstate
	// tenant*. See [WithTemporalNamespace].
	temporalNamespace string

	// taskQueues decides which task queue a tenant's runs are submitted to. The
	// zero value routes every run to [engine.RunTaskQueueName], which is the
	// zero-configuration path — see [WithTaskQueues].
	taskQueues engine.TaskQueues

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
	pluginCatalog               *v1.PluginCatalog

	// trustedWorkflowsMu guards trustedWorkflows. [WithTrustedWorkflows] writes it
	// at construction, before the struct exists to race against, but
	// [registerTrustedWorkflow] writes it again after construction — from
	// [NewWebhookReceiver], once per workflow the receiver admits — so a read
	// from a request in flight and a write from a receiver still starting up
	// must not be allowed to race.
	trustedWorkflowsMu sync.RWMutex
	trustedWorkflows   map[trustedWorkflowKey]*v1.Workflow

	// trustedWorkflowRefusals records keys with no usable answer, against
	// the sentence saying why: two registrations that disagreed, or one
	// specification the schema refuses. [FlowstateServer.trustedWorkflow]
	// returns that sentence rather than substituting anything. See
	// [FlowstateServer.refuseTrustedWorkflow].
	trustedWorkflowRefusals map[trustedWorkflowKey]string

	// searchAttributesRegistered records whether [EnsureSearchAttributesRegistered]
	// succeeded against this deployment's Temporal namespace before the server
	// started serving. See [WithSearchAttributesRegistered].
	searchAttributesRegistered bool

	// eagerStart requests that a run's first workflow task be handed straight to
	// a worker sharing this server's Temporal client, rather than going through
	// matching. Off unless the process co-locating both says otherwise — see
	// [WithEagerWorkflowStart].
	eagerStart bool

	// dataConverter reads everything this server wrote through a Temporal
	// client: memos, a schedule's stored arguments, an activity's heartbeat
	// details. It is set in [New] and never nil. See [WithDataConverter].
	dataConverter converter.DataConverter
}

// trustedWorkflow returns the deployment-owned specification registered for
// namespace and requested's name, or requested unchanged if none was.
//
// namespace must be the caller's own — [FlowstateServer.identityFor]'s
// answer for this request, never a value the request itself carries —
// because the trust boundary this implements is exactly that a namespace
// only reaches its own registered entries. Two tenants that each register a
// workflow of the same name under [WithTrustedWorkflows] or
// [FlowstateServer.NewWebhookReceiver] must never be able to substitute the
// other's specification, which a lookup keyed on name alone could not tell
// apart. The clone is per request: validation and preparation currently
// treat specifications as immutable, but keeping the configured authority
// out of request-lifetime code makes that trust boundary robust to later
// normalization.
// It returns an error for a name this deployment registered twice with
// different specifications, rather than an answer drawn from either of them.
// See [FlowstateServer.noteTrustedWorkflowConflict].
func (s *FlowstateServer) trustedWorkflow(namespace string, requested *v1.Workflow) (*v1.Workflow, error) {
	if requested == nil {
		return nil, nil
	}
	key := trustedWorkflowKey{namespace: namespace, name: requested.GetName()}
	s.trustedWorkflowsMu.RLock()
	trusted, ok := s.trustedWorkflows[key]
	refusal := s.trustedWorkflowRefusals[key]
	s.trustedWorkflowsMu.RUnlock()
	if refusal != "" {
		// Named, not silently degraded to open submission: the caller can do
		// nothing about this and the operator must, so the message says whose
		// problem it is. It carries no specification detail — the request came
		// from outside and the conflict is between two deployment-owned
		// copies.
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(refusal))
	}
	if !ok {
		return requested, nil
	}
	return proto.Clone(trusted).(*v1.Workflow), nil
}

// registerTrustedWorkflow adds one deployment-owned specification to the
// trusted set after construction, scoped to namespace, which is what lets
// [NewWebhookReceiver] extend [WithTrustedWorkflows]'s set from the Flowfiles
// a `--webhook` deployment loads: those are exactly the specifications whose
// `manual:`/trigger policy must bind regardless of what a caller submits
// under the same name, and the receiver is the one place that has already
// parsed and validated them by the time a trusted copy is needed. Safe to
// call while the server is serving — the receiver runs before the process
// starts listening, but nothing here assumes that — because every read and
// write of the map takes the same lock.
// It refuses a name already registered with a different specification, rather
// than replacing it. A receiver constructed after [WithTrustedWorkflows] named
// the same workflow, or after an earlier receiver served it, would otherwise
// substitute its own copy for the one the deployment already had — so a weaker
// `manual:` policy could quietly displace a stricter one, decided by nothing
// more than which receiver was constructed last. Unlike [WithTrustedWorkflows]
// this path can report the error, and refusing at construction beats denying at
// request time.
//
// The whole call is one transaction: every workflow is checked before any is
// written, for the same reason [FlowstateServer.NewWebhookReceiver] grants
// trust in a second pass — a rejected registration must leave the trusted set
// exactly as it found it.
func (s *FlowstateServer) registerTrustedWorkflows(namespace string, workflows []*v1.Workflow) error {
	s.trustedWorkflowsMu.Lock()
	defer s.trustedWorkflowsMu.Unlock()

	for _, workflow := range workflows {
		// Refused rather than skipped, for the reason [WithTrustedWorkflows]
		// gives at the same check: a registration with no name occupies no key,
		// so passing over it quietly reports success for trust that was never
		// installed.
		if workflow == nil || workflow.GetName() == "" {
			return fmt.Errorf("a workflow registered as trusted for namespace %q has no `name:`, "+
				"and the trusted set is addressed by name, so nothing would ever resolve to it "+
				"while submissions under the name intended stay open; give it the name callers "+
				"submit", namespace)
		}
		key := trustedWorkflowKey{namespace: namespace, name: workflow.GetName()}
		if reason := s.trustedWorkflowRefusals[key]; reason != "" {
			return fmt.Errorf("%s; resolve that before serving it for webhook deliveries", reason)
		}

		// The same reason [WithTrustedWorkflows] validates, except this path
		// can refuse the whole construction rather than poison one key.
		// NewWebhookReceiver's own register() has already run the schema
		// rules over these, so this is belt and braces for any other caller
		// of registerTrustedWorkflows that arrives later.
		if err := v1.Validate(workflow); err != nil {
			return fmt.Errorf("workflow %q cannot be trusted as a deployment-owned specification "+
				"because this deployment refuses it: %w", workflow.GetName(), err)
		}
		if existing, ok := s.trustedWorkflows[key]; ok && !proto.Equal(existing, workflow) {
			return fmt.Errorf("workflow %q is already registered for this namespace with a different "+
				"specification; two deployment-owned copies under one name are two `manual:` policies "+
				"this server would have to choose between, so serve one of them", workflow.GetName())
		}
	}

	if s.trustedWorkflows == nil {
		s.trustedWorkflows = make(map[trustedWorkflowKey]*v1.Workflow)
	}
	for _, workflow := range workflows {
		// Every element was named, validated and found not to conflict by the
		// loop above, which returned before writing anything if any of that
		// failed — so this pass only writes.
		s.trustedWorkflows[trustedWorkflowKey{namespace: namespace, name: workflow.GetName()}] =
			proto.Clone(workflow).(*v1.Workflow)
	}
	return nil
}

// WithDataConverter sets the data converter this server reads its own recorded
// values back with: a run's tenant and starter memos, the signal policy memo,
// the workflow-name memo, a schedule's stored run state, and an activity's
// heartbeat details.
//
// It exists because the write side is not this server's choice. A memo value is
// encoded with the *client's* data converter, not the SDK default: the Go SDK
// encodes it with the user's converter and falls back to the default only if
// that fails (go.temporal.io/sdk@v1.47.0 internal/internal_workflow_client.go's
// encodeMemoValue, gated on SDKFlagMemoUserDCEncode, which defaults to true). So
// on a deployment that configures a payload codec, every memo this server writes
// is ciphertext, and a server that read it back with the default converter would
// decode nothing. [FlowstateServer.ownedBy] would answer false for every run,
// and every tenant would be told "no such run" about runs it owns. That is not a
// hypothetical: it is what the codec knob would ship with behind it if the read
// side did not move with the write side.
//
// Hence one converter, both directions. Whatever `payloadcodec.Config`'s
// DataConverter handed the Temporal client is what belongs here, and `flow
// server` passes exactly that, from the same resolved config.
//
// The zero value keeps today's behavior byte for byte: a server built without
// this option reads with [converter.GetDefaultDataConverter], which is what an
// unconfigured deployment's client writes with.
//
// Search attributes are deliberately not covered, and cannot be: the SDK always
// encodes those with the default converter because the cluster has to index
// them. That asymmetry is why nothing payload-derived may ever be projected into
// one.
func WithDataConverter(dc converter.DataConverter) Option {
	return func(s *FlowstateServer) error {
		if dc == nil {
			// A nil converter would be a silent outage of exactly the shape
			// this option exists to prevent, so it is refused by being ignored:
			// the server keeps the default [New] gave it, which is a working
			// converter. Reporting an error instead is now possible and is a
			// behavior change to make on its own evidence, not as a side effect
			// of [Option] growing an error.
			return nil
		}
		s.dataConverter = dc
		return nil
	}
}

// WithSearchAttributesRegistered tells the server it may project a run's
// tenant and workflow name into Temporal search attributes at submit time,
// because [EnsureSearchAttributesRegistered] already confirmed both are
// registered on this deployment's Temporal namespace.
//
// Never set from inside [New] itself, and deliberately not attempted lazily
// on the request path: registering is one network round trip to the operator
// API, and Run and CreateSchedule are not the place to pay for a deployment
// question — they run once per submission, no operator API attempts.
//
// Absent this option, the server sets no search attributes at all, which is
// the zero-configuration answer and the only sound one: Temporal refuses
// StartWorkflowExecution outright when a search attribute it carries is not
// registered, so attaching one on the strength of a hopeful registration
// attempt would turn "operator API unreachable" into "no run can start" —
// exactly the failure fail-closed elsewhere in this system exists to avoid
// creating by accident.
//
// Filtering on `name` is unaffected either way, and deliberately so: it
// reads [workflowNameMemoKey], which [workflowNameMemoEntry] writes
// unconditionally, on every run, regardless of this option. A search
// attribute is index-only — a projection into Temporal's visibility store
// for external tooling (`temporal workflow list --query`, the Web UI) — and
// was the wrong place for `flow list --filter 'name == ...'`'s own data to
// live in the first place: gating the filter's only source of the value on
// whether an operator API call happened to succeed at startup would make a
// perfectly good filter answer "nothing matched" on a deployment where
// registration failed, which is indistinguishable from a filter with a typo
// in it — the exact dishonesty CLAUDE.md's List section exists to prevent.
func WithSearchAttributesRegistered() Option {
	return func(s *FlowstateServer) error { s.searchAttributesRegistered = true; return nil }
}

// WithEagerWorkflowStart asks Temporal to hand a new run's first workflow task
// back on the start response, for a worker sharing this server's Temporal
// client to execute immediately, rather than writing it to a task queue for
// some worker to poll for.
//
// It is for one topology and one only: a process that is both the starter and
// the worker, on a single [client.Client]. `flow server dev` is that process —
// one client from the dev server serves both the control plane and the worker
// it starts (`cmd/flow/serverdev.go`) — and is the only caller in this
// repository. The SDK enforces the topology rather than trusting it: a start
// only carries the eager request when the *same client instance* has a running
// workflow worker registered on the run's task queue with a free task slot
// (`internal/internal_eager_workflow.go`, `eagerWorkflowDispatcher.applyToRequest`),
// and when the cluster advertises the capability
// (`internal/internal_workflow_client.go:2172`). Anywhere else — `flow server`
// with `flow worker` in another process, a tenant whose runs go to a client no
// worker shares ([WithNamespacePool], [WithTaskQueues]) — the request is simply
// not made and dispatch proceeds normally.
//
// Off by default, and the default is not timidity about the fallback. Eager
// start does not respect worker versioning: "an eagerly started workflow may
// run on any available local worker even if that worker is not in the default
// build ID set" (`client.StartWorkflowOptions.EnableEagerStart`, SDK v1.47.0),
// and Temporal's own safe-deployments guidance says the same. Pinning a run to
// a Current deployment version is this engine's sole compatibility mechanism
// (see `pkg/flowstate/v1/engine/versioning.go`), so a co-located *versioned*
// deployment that turned this on would let a run begin on whichever binary
// happens to share the client, quietly outside the pin. `flow server dev` is
// safe because its stack is unversioned by construction, which is a property of
// that command rather than of this server — so this stays an option a
// co-located caller adopts knowingly, and must not be promoted into an
// unconditional line in [FlowstateServer.prepareCreate].
//
// Nothing observable about execution changes: the same first workflow task, the
// same history, the same result, taken by a worker in this process instead of
// after a matching round trip. What it removes is that round trip on every
// start in the local authoring loop.
func WithEagerWorkflowStart() Option {
	return func(s *FlowstateServer) error { s.eagerStart = true; return nil }
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
// existed; see [FlowstateServer.memoStarter] in lifecycle.go for what that absence means to
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
// distinction [FlowstateServer.memoStarter] needs to answer a run that predates this key.
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

// workflowNameMemoKey is the memo field recording a workflow's own declared
// name — see [v1.RunSummary.Name] for why this cannot be read off Temporal's
// built-in WorkflowType.
//
// A memo rather than only a search attribute, and this is the fix for the
// bug the search-attribute-only version had: `flow list --filter` composes
// with the tenant memo check unconditionally (`server/list.go`'s [ownedBy]),
// on every deployment, registered or not. A field the filter can read only
// sometimes is worse than a field it cannot read at all, because "sometimes"
// looks identical to "no runs matched" from the caller's side — a filter
// with nothing wrong with it, on a deployment where
// [EnsureSearchAttributesRegistered] happened to fail at startup, would
// silently return an empty page for every `name` comparison. See
// [namespaceMemoKey] for the same reasoning applied to tenancy, which this
// mirrors exactly: no cluster-side registration, so it works against
// `temporal server start-dev` with nothing configured.
const workflowNameMemoKey = "flowstate.workflowName"

// workflowNameMemoEntry encodes a workflow's own declared name into the one
// memo entry both [FlowstateServer.Run] and [FlowstateServer.CreateSchedule]
// write, for [signalPolicyMemoEntry]'s exact reason: two encoders drift, and
// here drift would mean a scheduled run's fired execution silently failing
// to match a `name` filter a direct run with the identical workflow does
// match.
//
// Always non-nil and never empty, unlike [signalPolicyMemoEntry]: a
// workflow's `name` is protovalidate-required, so there is no zero case to
// encode as absence. A run that predates this memo key simply has none —
// [workflowNameOf] treats that as "no name available," the same honest
// absence a filter already handles for a run with no search attribute set,
// documented where [v1.RunSummary.Name] is declared.
func workflowNameMemoEntry(name string) map[string]any {
	return map[string]any{workflowNameMemoKey: name}
}

// labelsMemoKey is the memo field recording a workflow's own declared labels —
// [v1.Workflow.Labels], the author's key-value facts about what this workload is.
//
// A memo rather than a search attribute, for [workflowNameMemoKey]'s reason
// stated there at length and unchanged here: `flow list --filter
// 'labels["team"] == "payments"'` composes with the tenant memo check on every
// deployment, registered or not, and a field readable only where registration
// happened to succeed answers "nothing matched" to a filter with nothing wrong
// with it. See [v1.RunSummary.Labels].
const labelsMemoKey = "flowstate.labels"

// labelsMemoEntry encodes a workflow's declared labels into the one memo entry
// both [FlowstateServer.Run] and [FlowstateServer.CreateSchedule] write —
// [workflowNameMemoEntry]'s "one function, two callers" discipline, and here the
// drift it prevents is a scheduled run failing to match a `labels` filter that a
// direct run of the identical workflow matches.
//
// Returns nil — add nothing — for a workflow that declared no labels, which is
// [signalPolicyMemoEntry]'s zero case rather than [workflowNameMemoEntry]'s:
// labels are optional, so "declared none" is ordinary and has an honest
// encoding, which is absence. [FlowstateServer.labelsOf] reads an absent key and
// a run predating this key identically, and so does the filter: both are a run
// carrying no labels, which is what a `labels` comparison then sees.
func labelsMemoEntry(labels map[string]string) map[string]any {
	if len(labels) == 0 {
		return nil
	}

	return map[string]any{labelsMemoKey: labels}
}

// namespaceSearchAttribute and workflowNameSearchAttribute are the two
// Temporal search attributes a run may carry, alongside the memo that always
// carries the tenant.
//
// Both Keyword rather than Text: a Keyword is matched exactly, which is what
// tenancy and a workflow's declared name are for — Text tokenizes for
// full-text search, which is the wrong match semantics for either and would
// let "team-a" match a query meant for "team-ab".
//
// Named with a package-specific prefix so a deployment that already runs
// other Temporal applications sharing this namespace cannot collide with an
// attribute of its own — `flow` is common enough as a name that Temporal's
// own examples use it.
var (
	namespaceSearchAttribute    = temporal.NewSearchAttributeKeyKeyword("FlowstateNamespace")
	workflowNameSearchAttribute = temporal.NewSearchAttributeKeyKeyword("FlowstateWorkflowName")
)

// runSearchAttributes builds the search attributes both [FlowstateServer.Run]
// and [FlowstateServer.CreateSchedule]'s fired executions carry, when the
// deployment has registration confirmed — see [WithSearchAttributesRegistered].
//
// Purely a projection into Temporal's own visibility store, for tools that
// query it directly — `temporal workflow list --query`, the Web UI. Nothing
// in this server reads a search attribute back to decide anything:
// `flow list --filter` is answered from [v1.RunSummary], populated from the
// memo — see [workflowNameMemoEntry] and [ownedBy] — which is unconditional
// on every deployment. A filter that depended on this instead would silently
// stop working wherever registration failed, which is exactly the bug this
// split exists to avoid; keep it that way when touching either half.
//
// The one function that turns identity and a workflow's declared name into
// search attributes, for [signalPolicyMemoEntry]'s exact reason: two
// encoders drift, and here the failure mode is worse than silent, because an
// unregistered or misspelled attribute name does not merely fail to filter —
// it fails the submission outright (see [WithSearchAttributesRegistered]).
// One function is the only way a scheduled run's fired execution and a
// direct run's execution are guaranteed to carry identical attributes.
//
// Neither value is secret, and neither is derived from a caller-supplied
// input the memo does not already carry: the namespace is the authenticated
// identity's own, exactly as the memo already records, and the workflow name
// is the specification's own required `name` field — constrained by
// protovalidate to `^[A-Za-z0-9-_]+$`, so it carries nothing a Keyword value
// or a query literal built from one could misinterpret. Search attributes
// are Temporal visibility data, exactly as broadly readable as the memo they
// mirror, so nothing crosses the boundary CLAUDE.md's "Secrets never enter
// workflow history" describes — see that document before adding a third
// attribute here, because the rule is about what is *already* public, not
// about search attributes being safer than history.
func runSearchAttributes(namespace, workflowName string) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(
		namespaceSearchAttribute.ValueSet(namespace),
		workflowNameSearchAttribute.ValueSet(workflowName),
	)
}

// runStaticSummary builds the single-line Markdown [client.StartWorkflowOptions.StaticSummary]
// both [FlowstateServer.Run]/[FlowstateServer.SignalWithStart] (through
// [FlowstateServer.prepareCreate]) and [FlowstateServer.CreateSchedule]'s fired
// executions carry, so a run is legible in the Temporal Web workflow list
// without opening it — see #753. One function for [runSearchAttributes]'s
// exact reason: two independent renderings of "which workflow, which tenant"
// drift, and CLAUDE.md's invariant-1 discipline says build this from the same
// values already flowing into the memo and search attributes rather than a
// third rendering of them.
//
// Unconditional, unlike [runSearchAttributes]: this is a Markdown string
// field with no cluster-side registration to depend on, exactly the same
// reason [workflowNameMemoEntry] is unconditional while the search attribute
// beside it is not.
//
// Backtick-delimited, and safe to delimit that way because both inputs are
// already constrained to a grammar with no backtick in it: workflowName by
// the schema's `^[A-Za-z0-9-_]+$` on [v1.Workflow.Name], namespace by
// [auth.ValidateNamespace]'s `[a-z0-9-]` grammar. Neither value is secret and
// neither is derived from anything the memo does not already carry — see
// [runSearchAttributes]'s comment on containment, which applies identically
// here: this is Temporal visibility data, exactly as broadly readable as the
// memo it mirrors.
func runStaticSummary(namespace, workflowName string) string {
	return fmt.Sprintf("`%s` · tenant `%s`", workflowName, namespace)
}

// EnsureSearchAttributesRegistered idempotently registers the search
// attributes Flowstate projects onto a run, against one Temporal namespace.
//
// Called once, synchronously, before the server starts serving — see
// `cmd/flow/main.go`'s `runServer`. Not attempted on the request path and not
// retried in the background: a registration failure here degrades the
// deployment to the same in-process, memo-scanning listing Flowstate always
// had (invariant 8's zero-configuration path, and every deployment before
// this feature existed), which is a correctness-preserving, purely-slower
// fallback rather than a broken one. That is "fail-open on FILTERING only,
// never on tenancy": [FlowstateServer.searchAttributesRegistered] stays
// false, so [runSearchAttributes] is never called and a run never carries an
// attribute Temporal has not agreed to accept — see
// [WithSearchAttributesRegistered] for what happens if it is.
//
// Idempotent because a second `flow server` process, or a restart, asks
// again: Temporal's AddSearchAttributes reports ALREADY_EXISTS for a name
// already registered, which this treats as success rather than an error the
// caller has to special-case, and any *other* attribute — Temporal's own
// built-ins, or one a different application registered — is left alone
// because the request names only the two Flowstate adds.
//
// Not extended to a [temporalclient.Pool]: a deployment that maps tenants
// onto several Temporal namespaces would need this run once per mapped
// namespace, which `cmd/flow/main.go` does not do today — an honest cut
// rather than an oversight. Search attributes are simply never projected in
// that configuration, and every run still lists correctly through the
// memo-scanning path that predates this feature; only the indexing benefit
// is unavailable there.
func EnsureSearchAttributesRegistered(ctx context.Context, temporalClient client.Client, namespace string) error {
	_, err := temporalClient.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: namespace,
		SearchAttributes: map[string]enums.IndexedValueType{
			namespaceSearchAttribute.GetName():    enums.INDEXED_VALUE_TYPE_KEYWORD,
			workflowNameSearchAttribute.GetName(): enums.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("registering Flowstate's search attributes on namespace %q: %w", namespace, err)
	}

	return nil
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

// temporalNamespaceFor returns the Temporal namespace serving a caller in the given
// Flowstate namespace — the namespace of the very client [FlowstateServer.clientFor]
// returns for the same argument.
//
// # Nothing calls this yet, and that is on purpose
//
// It is the plumbing half of a follow-up named in `timeline.go`: `GetTimeline`'s
// round trips are bounded only transitively, through the events it scans, because
// giving them a budget of their own the way `list.go` has one
// (`maxListRequests`) means reaching past the SDK to Temporal's raw
// `GetWorkflowExecutionHistory`, which takes a Temporal namespace as a request
// field — and a Temporal client is dialed for a namespace and can never be asked
// which. This answers that question. It landed on its own rather than inside that
// change because getting it wrong points one tenant's reads at another tenant's
// namespace, where they would succeed and nothing would say so, and a change with
// that failure mode is reviewed by itself. Do not delete it as dead code; see
// maxTimelineScan in `timeline.go` for the ask it unblocks.
//
// # It fails closed, in both configurations
//
// Pooled, it refuses exactly where [FlowstateServer.clientFor] refuses, because
// both are [temporalclient.Pool.resolve]: a deployment that maps namespaces and
// has no entry for this tenant gets an error, never the namespace the process
// happened to be pointed at. An accessor that answered where clientFor refuses
// would be the same tenancy breach clientFor exists to prevent, reached by asking
// for the name instead of for the client.
//
// Pool-less, it refuses when nobody said. Answering "" would hand a caller an
// empty namespace to put in a request field, which fails at the far end of an RPC
// with a message about Temporal rather than about this deployment's configuration.
// A server built for a surface with no Temporal client at all — `flow mcp`'s
// `server.New(nil)`, which answers only Validate, Compile and GetCatalog — is
// refused here for the same reason and correctly: it has no namespace because it
// has nothing to name one for.
func (s *FlowstateServer) temporalNamespaceFor(namespace string) (string, error) {
	if s.pool == nil {
		if s.temporalNamespace == "" {
			return "", connect.NewError(connect.CodeFailedPrecondition, errors.New(
				"this server was not told which Temporal namespace its client is dialed for; "+
					"pass server.WithTemporalNamespace, or server.WithNamespacePool on a "+
					"deployment that routes tenants to namespaces of their own"))
		}
		return s.temporalNamespace, nil
	}

	temporal, err := s.pool.TemporalNamespaceFor(namespace)
	if err != nil {
		// FailedPrecondition and a message naming only the caller's own
		// namespace, for the reasons [FlowstateServer.clientFor] gives: the
		// deployment cannot route this tenant, an operator has to fix it, and
		// naming the others would describe this deployment's tenancy to someone
		// outside it.
		return "", connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
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

	// Captured before the trusted lookup below, not only for its usual reason
	// (the run outlives this request, so anything a later step needs to know
	// about who asked for the work has to be recorded in the state it
	// carries) but because the lookup needs the caller's own namespace to
	// stay a boundary between tenants rather than a single deployment-wide
	// name flat enough for one tenant to reach another's trusted entry.
	identity := s.identityFor(ctx)

	// The caller's copy, kept as it arrived, so the attestation below has
	// something to compare the executed specification against.
	//
	// A clone, and taken here, before the first thing that can change a
	// specification: [FlowstateServer.trustedWorkflow] returns the request's own
	// pointer when this deployment registered nothing under that name, and
	// [FlowstateServer.validateSpecification] then writes the deployment's
	// resolved plugin pin *onto that pointer* (see [FlowstateServer.pinPlugins]).
	// Holding a reference rather than a copy would therefore leave both sides of
	// the comparison pointing at one message that mutates together, which is
	// equality that can only ever answer true.
	submitted := proto.Clone(req.Msg.GetWorkflow()).(*v1.Workflow)

	workflow, err := s.trustedWorkflow(identity.GetNamespace(), req.Msg.GetWorkflow())
	if err != nil {
		return nil, err
	}

	inputs, err := s.validateSubmission(workflow, req.Msg.GetInputs())
	if err != nil {
		return nil, err
	}

	// A random id unless the caller named a business key, in which case the run
	// is addressable by what it *is* rather than by an id nobody wrote down —
	// see [RunRequest.entity_key]'s doc comment for the grammar and the
	// unforgeability argument. The namespace half comes only from the identity
	// just captured above, never from the request: the same rule [fairnessFor]
	// already applies a few lines down, and for the identical reason — a
	// workload must not be able to name the tenant it is addressed under, or
	// the first thing anyone writes is another tenant's key.
	workflowID := fmt.Sprintf("flowstate-workflow-%s", uuid.NewString())
	if key := req.Msg.GetEntityKey(); key != "" {
		entityID, err := v1.EntityWorkflowID(identity.GetNamespace(), key)
		if err != nil {
			// protovalidate already checked entity_key against the same grammar
			// [v1.EntityWorkflowID] enforces, so reaching this is either the
			// composed id exceeding Temporal's own limit or a namespace this
			// caller's identity carries that predates [auth.ValidateNamespace]'s
			// grammar (invariant 6: fail closed rather than guess).
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		workflowID = entityID
	}

	// This is a manual start — the `Run` RPC is what `flow run`, an agent over
	// MCP and any other caller reach — so the workflow's own `manual:` block
	// decides whether it may happen, against the identity this server attested
	// and the reason the caller gave. Refused here, at the boundary, while the
	// caller is still present to be told: authorization belongs on the trigger
	// and not in a step's `if:`, where it would be an expression the workflow
	// evaluates about itself. See [v1.CheckManualStart].
	//
	// A workflow with no `manual:` block passes unchanged, which is every
	// workflow that exists: `triggers:` is not exhaustive, and adding a webhook
	// must never silently stop `flow run` from working.
	if err := v1.CheckManualStart(workflow, identity.GetSubject(), req.Msg.GetReason()); err != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, err)
	}

	memo, temporal, options, err := s.prepareCreate(ctx, identity, workflow, inputs)
	if err != nil {
		return nil, err
	}
	options.ID = workflowID

	// Provenance, in the same memo the tenant and the starter are recorded in and
	// for the same reason: it is read afterwards, by whoever asks how this run
	// came to happen. The reason is recorded only when there is one, because an
	// entry that is always present says nothing, and this one is the answer to a
	// question somebody asks after the fact.
	memo[triggerMemoKey] = v1.TriggerKindManual
	if reason := req.Msg.GetReason(); reason != "" {
		memo[reasonMemoKey] = reason
	}
	options.Memo = memo

	// Answered last, against the specification that is about to be executed
	// rather than against the one the trusted lookup returned, and carried to the
	// response below — see [v1.RunResponse.specification_as_submitted] for what
	// the caller does with it, and #734 for the leak it closes.
	//
	// Last, because everything between the lookup and here may have changed the
	// specification and one thing already does: [FlowstateServer.pinPlugins]
	// writes the deployment's selection of plugin versions onto it, so a
	// plugin-bearing workflow reaches the engine carrying a `resolved_plugins`
	// the caller never sent. Asking the question earlier answered it about a
	// value that had not finished being assembled — true for a specification the
	// engine would then run in a different form, which is precisely the claim the
	// field promises not to make.
	//
	// Compared by value, whole, rather than reported as "was there a trusted
	// entry" or narrowed to the fields a client's redaction happens to read
	// today. A deployment that registers the identical specification a caller
	// submits has substituted nothing observable and keeps the caller's precise
	// view; anything else — a substitution, a pin, a normalization added next
	// year — answers false without needing to be enumerated here. That is the
	// fail-closed direction: a transformation nobody thought to list still costs
	// a caller a precise view rather than costing them a secret.
	asSubmitted := proto.Equal(submitted, workflow)

	run, err := temporal.ExecuteWorkflow(ctx, options, engine.Run, &v1.RunState{
		Workflow:    workflow,
		StepsBudget: int32(s.maxStepsPerRun),
		Identity:    identity,

		// Checked and defaulted, once, above. The engine reads them and never
		// re-derives them, so every segment of the run sees what this submission
		// established.
		Inputs: inputs,

		// And how this run started, established here because here is the only
		// place that knows: a person asked, and this server attested who they
		// are. Carried in state rather than derived, which is what makes
		// `${trigger.kind}` the same value on every replay.
		Trigger: v1.NewManualTriggerContext(identity.GetSubject()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to execute workflow: %w", err))
	}

	return connect.NewResponse(
		&v1.RunResponse{
			WorkflowId: workflowID,
			RunId:      run.GetRunID(),
			Status:     v1.RunResponse_STATUS_RUNNING,

			// Always set, on both answers: the field's whole design rests on
			// silence meaning "this server does not say", so a server that does
			// say must never leave it unset for the false case and let a client
			// read a deliberate answer as an old server's shrug — or, worse,
			// leave it unset for the *true* case, which reads as substitution
			// and redacts a run's outputs that nothing needed to redact.
			SpecificationAsSubmitted: proto.Bool(asSubmitted),
		},
	), nil
}

// validateSubmission is the submission-validation pipeline shared by
// [FlowstateServer.Run] and the create branch of
// [FlowstateServer.SignalWithStart] — credential targets, the declared signal
// policies' shape, the specification's own size, input binding, and the
// specification-plus-inputs size together. One function rather than two
// copies, for the reason CLAUDE.md's "one meaning, written down twice" section
// names generally: two RPCs that can each bring a new run into existence must
// refuse the identical specification for the identical reason, or "may I
// create an entity under this key" would silently become a laxer question
// than "may I Run" purely because SignalWithStart's copy of these checks had
// drifted from Run's.
//
// [FlowstateServer.SignalWithStart] runs this unconditionally — even when the
// entity it addresses turns out to already exist and this validation's result
// is then unused. "May create" is the floor for every call through that RPC,
// not only the ones that end up creating.
func (s *FlowstateServer) validateSubmission(wf *v1.Workflow, rawInputs map[string]*v1.Value) (map[string]*v1.Value, error) {
	if err := s.validateSpecification(wf); err != nil {
		return nil, err
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
	inputs, err := v1.BindRunInputs(wf, rawInputs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Weighed together, because they travel together. CheckSpecSize above bounds the
	// part an author wrote; this bounds the pair, so a caller cannot push a run past
	// what Temporal will store using arguments alone — which would be the wedged run
	// invariant 9 exists to convert into an answer, reached by the one path the
	// specification's own check cannot see.
	if err := v1.CheckSubmissionSize(wf, inputs); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	return inputs, nil
}

// validateSpecification is the half of [FlowstateServer.validateSubmission] that
// asks only about the specification: the deployment's plugin selection, the
// credential targets it permits, the shape of the declared signal policies, and
// the size of the specification itself. It writes the plugin pin onto wf.
//
// Split out because there is one caller that has a specification long before it
// has a submission: [FlowstateServer.NewWebhookReceiver], which is handed the
// workflows a deployment will serve deliveries for at startup. Every answer here
// is already decided at that moment — a plugin below the declared minimum is not
// installed any harder once a delivery arrives — so a receiver asks it then, and a
// deployment that could not serve a webhook refuses to start rather than
// advertising an endpoint whose every delivery is a 422 in a log nobody reads.
//
// What stays in validateSubmission is what a submission brings: the inputs, and
// the size of the pair. Those cannot be asked without a delivery.
func (s *FlowstateServer) validateSpecification(wf *v1.Workflow) error {
	if err := s.pinPlugins(wf); err != nil {
		return err
	}
	if s.credentialTargetsConfigured {
		if err := v1.ValidateCredentialTargets(wf, s.credentialTargets); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	// Rules compile at submit, not at signal-time. Everything [v1.Validate]
	// cannot see because it is a fact about a *set* of declared policies rather
	// than about one field — a policy for a signal name nothing waits for, a rule
	// that matches every sender — is caught here, once, rather than discovered the
	// first time a signal is actually delivered and denied for a reason the
	// author never saw at submit.
	if err := v1.CheckSignalPolicies(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Compiling a Flowfile refuses `timeout:`/`retry:` on a step kind that
	// schedules no activity for either to act on (see
	// [v1.CheckPolicyPlacement]'s doc). A specification built by hand and
	// submitted straight to this RPC arrives without that compiler in front
	// of it, so the same refusal has to run here too, or the unsafe behavior
	// it exists to prevent — a policy that silently does nothing — reaches
	// production for any specification that did not begin life as a
	// Flowfile.
	if err := v1.CheckPolicyPlacement(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The same argument for the nested-loop refusal the compiler reports (see
	// [v1.CheckLoopNesting]'s doc). This one is worth more than parity: what a
	// hand-built specification buys by arriving without the compiler is a run
	// whose two iteration ceilings multiply with no Continue-As-New between
	// them, and that does not fail — it wedges, RUNNING forever once the
	// history it carries is too large for Temporal to accept.
	if err := v1.CheckLoopNesting(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
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
	if err := v1.CheckSpecSize(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// A Flowfile can never compile a structure this deep — the compiler
	// refuses it directly, with a position — but a specification built by
	// hand and submitted straight to this RPC arrives without a compiler in
	// front of it. Every walk this package runs over a structure later
	// (secret authority, reference collection for Continue-As-New, encoding
	// a request body) reads [v1.MaxStructureDepth], so a value nested past it
	// is refused here rather than under-inspected by all of them.
	if err := v1.CheckStructureDepth(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	return nil
}

// pinPlugins records the deployment's plugin selection on a specification about
// to become durable, and refuses a specification the deployment cannot satisfy.
//
// Called by [FlowstateServer.validateSpecification], so by every RPC that shares
// it: [FlowstateServer.Run] and [FlowstateServer.SignalWithStart] through
// [FlowstateServer.validateSubmission], and [FlowstateServer.CreateSchedule]
// directly. All three because all three bring durable work into existence, and a
// schedule that skipped this would persist a specification with requirements and
// no selection: unpinned at three in the morning, when the deployment resolving
// it is not the one the author submitted against and nobody is there to read a
// refusal.
//
// Unconditional, including for a specification that requires nothing. The
// resolved list is the control plane's own answer, so a caller-supplied one is
// overwritten rather than trusted (see [v1.ResolvePlugins]), and "requires
// nothing" is an answer this has to write down rather than a reason to leave
// whatever arrived in place.
func (s *FlowstateServer) pinPlugins(wf *v1.Workflow) error {
	if err := v1.ResolvePlugins(wf, s.pluginCatalog); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"resolving plugins before durable execution: %w", err))
	}

	return nil
}

// pluginCatalogSnapshot is the catalog this server pins against, as a copy.
//
// nil when no catalog was installed, which is a deployment without plugins
// rather than a deployment whose plugins are unknown: a caller asking what it
// may require is told nothing may be required, which is the true answer.
func (s *FlowstateServer) pluginCatalogSnapshot() *v1.PluginCatalog {
	if s.pluginCatalog == nil {
		return nil
	}

	return proto.Clone(s.pluginCatalog).(*v1.PluginCatalog)
}

// prepareCreate builds the memo, the start options, and the namespace-scoped
// client that starting a new run needs — the part of [FlowstateServer.Run]
// after inputs are already bound, factored out so
// [FlowstateServer.SignalWithStart]'s create-if-absent branch goes through the
// identical memo and option construction rather than a second copy that could
// drift from it. inputs must already be the output of [v1.BindRunInputs];
// identity must already be [FlowstateServer.identityFor]'s answer for this
// request. Neither is derived here, because both callers need their own value
// before this is reached — Run to compose an entity id, SignalWithStart to
// decide whether it is even taking the create branch.
//
// The caller still has to set options.ID: this only fills in everything that
// does not depend on which workflow id was chosen.
func (s *FlowstateServer) prepareCreate(
	ctx context.Context, identity *v1.WorkloadIdentity, wf *v1.Workflow, inputs map[string]*v1.Value,
) (map[string]any, client.Client, client.StartWorkflowOptions, error) {
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
	signalEntry, err := signalPolicyMemoEntry(ctx, wf, inputs)
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
		return nil, nil, client.StartWorkflowOptions{}, connect.NewError(connect.CodeInvalidArgument, err)
	}
	for k, v := range signalEntry {
		memo[k] = v
	}

	// Unconditional, unlike the search attribute below: see
	// [workflowNameMemoEntry] for why `flow list --filter 'name == ...'` must
	// not depend on whether registration succeeded.
	for k, v := range workflowNameMemoEntry(wf.GetName()) {
		memo[k] = v
	}

	// The author's labels, on the same terms and through the same one function
	// [FlowstateServer.CreateSchedule] uses — see [labelsMemoEntry]. Nothing is
	// added when the workflow declared none.
	for k, v := range labelsMemoEntry(wf.GetLabels()) {
		memo[k] = v
	}

	// Derived from the authenticated tenant, never from the request — the same
	// rule the memo above and the fairness key below already follow. Refused
	// before anything is started, rather than defaulted onto the shared queue:
	// a tenant whose fleet was asked for but not configured must not quietly
	// land on everyone else's workers.
	taskQueue, err := s.taskQueueFor(identity.GetNamespace())
	if err != nil {
		return nil, nil, client.StartWorkflowOptions{}, err
	}

	options := client.StartWorkflowOptions{
		TaskQueue: taskQueue,

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

		// Unconditional and built from the exact values the memo above already
		// carries — see [runStaticSummary]. Makes a run legible in the Temporal
		// Web workflow list without opening it (#753); StaticDetails is left
		// unset here because prepareCreate has no natural longer-form
		// description at this call site the memo does not already say more
		// tersely — see [FlowstateServer.CreateSchedule], which does.
		StaticSummary: runStaticSummary(identity.GetNamespace(), wf.GetName()),

		// False unless the process that built this server co-locates a worker
		// on the very client below, and said so — see [WithEagerWorkflowStart],
		// which carries the worker-versioning reason this is a deployment's
		// decision rather than an unconditional line here. Set on the options
		// every create path shares, so `flow run`, a create-if-absent
		// SignalWithStart and a webhook delivery all get the co-located
		// dispatch or none of them do.
		EnableEagerStart: s.eagerStart,
	}

	// Projected into visibility only when registration was confirmed at
	// startup — see [WithSearchAttributesRegistered]. Unset otherwise, which
	// is the zero-configuration answer: an unregistered search attribute
	// makes Temporal refuse the whole submission, so a deployment that never
	// registered must never attach one, not even hopefully.
	if s.searchAttributesRegistered {
		options.TypedSearchAttributes = runSearchAttributes(identity.GetNamespace(), wf.GetName())
	}

	// Chosen from the identity established by authenticating the caller, never
	// from anything the request said, so a workload cannot ask to run in another
	// tenant's namespace.
	temporal, err := s.clientFor(identity.GetNamespace())
	if err != nil {
		return nil, nil, client.StartWorkflowOptions{}, err
	}

	return memo, temporal, options, nil
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

// taskQueueFor returns the task queue a tenant's runs are submitted to, or the
// refusal to submit at all.
//
// Unconfigured it is [engine.RunTaskQueueName] and cannot fail, which is what
// keeps a deployment that never sets [WithTaskQueues] byte-identical to what it
// had before this existed — including for a recorded namespace that predates
// [auth.ValidateNamespace]'s grammar, which the routed path has to refuse and
// the unrouted path has no reason to look at.
//
// FailedPrecondition, and worded like [FlowstateServer.clientFor]'s refusal, for
// the identical reason: this is the deployment's configuration being incomplete
// for this caller, not the caller's request being wrong, and the message names
// only the caller's own namespace so that refusing does not describe the
// deployment's tenancy to whoever provoked it.
func (s *FlowstateServer) taskQueueFor(namespace string) (string, error) {
	queue, err := s.taskQueues.For(namespace)
	if err != nil {
		return "", connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("this deployment routes each tenant's runs to their own task queue, "+
				"and namespace %q cannot be placed on one: %w", namespace, err))
	}

	return queue, nil
}

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
		pending, pendingTruncated := s.pendingActivities(resp)

		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				StartTime:  start,
				CloseTime:  closed,
				// Who submitted this run, off the same Describe response
				// everything else here comes from and through the same reader
				// authorization uses. See [FlowstateServer.reportedStarter]; empty is a real
				// answer, and there is no compatibility arm for a run that
				// predates the memo key.
				Starter:  s.reportedStarter(resp),
				Progress: runProgress(ctx, temporal, resp),
				// From the same Describe response the status came from, so the
				// answer to "why has this been RUNNING for six hours" costs no
				// further round trip. See pendingActivities for what is and is
				// not claimed.
				PendingActivities:          pending,
				PendingActivitiesTruncated: pendingTruncated,
				// The one field on this response that answers what a healthy
				// entity actually holds — see [entityState]'s own comment for
				// why a run that is, by design, always RUNNING was otherwise
				// unreadable through this RPC at all.
				EntityState: entityState(ctx, temporal, resp),
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
				// Who submitted this run, off the same Describe response
				// everything else here comes from and through the same reader
				// authorization uses. See [FlowstateServer.reportedStarter]; empty is a real
				// answer, and there is no compatibility arm for a run that
				// predates the memo key.
				Starter: s.reportedStarter(resp),
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
				// Who submitted this run, off the same Describe response
				// everything else here comes from and through the same reader
				// authorization uses. See [FlowstateServer.reportedStarter]; empty is a real
				// answer, and there is no compatibility arm for a run that
				// predates the memo key.
				Starter: s.reportedStarter(resp),
				Kind: &v1.GetResponse_Error{
					Error: failureError(ctx, temporal, req.Msg.GetWorkflowId(), req.Msg.GetRunId(), respStatus),
				},
			},
		), nil
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown workflow status: %d", respStatus))
	}
}

// failureError answers why a run ended the way it did, as the message the
// wire already carried plus the classification P2 (#241) adds beside it.
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
//
// # Kind, from the same application error
//
// [engine.classifyRunError] gives every terminal run failure's application error a
// Type equal to its [v1.ErrorKind.String] — the same field [engine.activityError]
// already uses for a task's own classification, so this reads it the same way the
// durable driver's own step-tolerance logic does ([recordedStepError]'s sibling in
// the engine package). A cancellation or an error this build never classified
// carries no kind, rather than guessing one: an agent branching on Kind must be able
// to tell "classified as X" from "not classified" and not receive a fabricated
// answer for the second.
func failureError(
	ctx context.Context,
	temporalClient client.Client,
	workflowID, runID string,
	status v1.RunResponse_Status,
) *v1.RunResponse_Error {
	err := temporalClient.GetWorkflow(ctx, workflowID, runID).Get(ctx, nil)
	if err == nil {
		return &v1.RunResponse_Error{Message: status.String()}
	}

	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Message() != "" {
		result := &v1.RunResponse_Error{Message: app.Message()}
		if kind, ok := v1.ParseErrorKind(app.Type()); ok {
			result.Kind = kind.String()
		}

		return result
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
			return &v1.RunResponse_Error{Message: status.String() + summary}
		}
	}

	// No application error in the chain, which is what an uncompensated
	// cancellation or a timeout looks like: the run ended for a reason Temporal
	// knows and the workload never said anything about. A raw timeout is the one
	// shape of that worth naming rather than repeating verbatim — an ordinary
	// step timeout is already translated before it gets here (#788,
	// [engine.durableStepTimeoutMessage]), so a *temporal.TimeoutError reaching
	// this branch is the workflow's own execution or run timeout, which this
	// engine never sets a policy-derived value for the way it does a step's. So:
	// name that it was a timeout and which kind, in place of Temporal's own
	// "activity StartToClose timeout (type: StartToClose)" — one line, not a
	// budget value this layer has nothing to compute it from.
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return &v1.RunResponse_Error{
			Message: status.String() + ": timed out (" + timeoutKindText(timeoutErr.TimeoutType()) + ")",
		}
	}

	// Its own text is then the best there is.
	if text := err.Error(); text != "" {
		return &v1.RunResponse_Error{Message: text}
	}

	return &v1.RunResponse_Error{Message: status.String()}
}

// timeoutKindText names a Temporal timeout type in words an author's Flowfile
// never uses, for [failureError]'s fallback: "StartToClose" and its siblings
// are Temporal's own vocabulary, not Flowstate's.
//
// [failureError]'s call site is specifically the *workflow*-level timeout —
// an ordinary step timeout is already translated before it gets there — so
// START_TO_CLOSE and SCHEDULE_TO_CLOSE here name Temporal's
// WorkflowRunTimeout and WorkflowExecutionTimeout respectively, not an
// activity's attempt/retry budget (Codex review on #788: reusing
// activity-attempt wording — "a single attempt" — for a timeout that can
// cover the whole run, including every Continue-As-New segment, gave an
// operator a false diagnosis of the primary timeout this branch exists to
// explain).
func timeoutKindText(kind enums.TimeoutType) string {
	switch kind {
	case enums.TIMEOUT_TYPE_START_TO_CLOSE:
		return "this run exceeded its execution timeout"
	case enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return "the workflow exceeded its total execution timeout across every continue-as-new"
	case enums.TIMEOUT_TYPE_SCHEDULE_TO_START:
		return "it waited too long to start"
	case enums.TIMEOUT_TYPE_HEARTBEAT:
		return "it stopped reporting progress"
	default:
		return "the run's own time budget was exceeded"
	}
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
func (s *FlowstateServer) heartbeatPhase(details *commonpb.Payloads) string {
	if details == nil || len(details.GetPayloads()) == 0 {
		return ""
	}

	var phase string
	if err := s.dataConverter.FromPayload(details.GetPayloads()[0], &phase); err != nil {
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

// entityState asks a running workload what it is carrying — its top-level
// `vars:` and what each active `loop:` holds — through [engine.StateQuery],
// exactly as [runProgress] asks where the workload has got to through
// [engine.ProgressQuery]. See that function's doc comment for why a failed
// query costs the caller one optional field rather than the whole answer, and
// why that is the right trade rather than a compromise: every reason a
// progress query can fail — no worker polling, an interpreter built before
// the handler existed, a busy worker, a timeout — applies here identically,
// and none of them says anything about the run itself.
//
// This is the field that makes an entity's whole point observable. Outputs
// populate only on STATUS_COMPLETED, and an entity — a run shaped as `loop:`
// + `wait_for_signal:` and never meant to reach that status — was otherwise
// unreadable through Get at all: not by waiting for it to finish, which it
// structurally does not, and not without mutating it to provoke a readable
// output, which is signaling used as a read — the wrong tool for the job.
func entityState(ctx context.Context, temporal client.Client, resp *workflowservice.DescribeWorkflowExecutionResponse) *v1.EntityState {
	// Same STATUS_RUNNING-only gate [runProgress] applies, and the same reason:
	// a superseded run id names a *closed* execution that Temporal will answer
	// a query against by replaying history, returning whatever state that
	// segment held when it suspended — a real answer from the wrong moment,
	// which is worse than none.
	if resp.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	workflowID := resp.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId()
	runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// Bounded exactly as [runProgress]'s query is, and for the same reason: the
	// request's own deadline is the wrong bound for an optional field, and a
	// worker that will never answer should not make every `flow get` wait out
	// the whole of it.
	ctx, cancel := context.WithTimeout(ctx, progressQueryTimeout)
	defer cancel()

	encoded, err := temporal.QueryWorkflow(ctx, workflowID, runID, engine.StateQuery)
	if err != nil {
		return nil
	}

	var state v1.EntityState
	if err := encoded.Get(&state); err != nil {
		return nil
	}

	return &state
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
func (s *FlowstateServer) pendingActivities(
	resp *workflowservice.DescribeWorkflowExecutionResponse,
) ([]*v1.PendingActivity, bool) {
	infos := resp.GetPendingActivities()
	if len(infos) == 0 {
		return nil, false
	}

	// Bounded by count, and each message bounded by length, because "a handful
	// of retrying steps" was an assumption rather than a fact: a
	// suspension-opaque block may schedule [v1.MaxAtomicBlockActivities], and
	// nothing stops all of them from retrying at once. Both the number of
	// entries and the length of each one's sentence are the workload's choice,
	// so both are bounded rather than one of them (Codex, #1119).
	//
	// The same shape [v1.RunProgress.PendingWaitsTruncated] uses, down to
	// keeping the entries it did collect: the bound is a count rather than a
	// size, and the first retrying steps of a run holding thousands are still
	// the answer to "what is this stuck on".
	truncated := len(infos) > maxPendingActivities
	if truncated {
		infos = infos[:maxPendingActivities]
	}

	out := make([]*v1.PendingActivity, 0, len(infos))
	for _, info := range infos {
		pending := &v1.PendingActivity{
			Attempt: info.GetAttempt(),
			// The message alone rather than the whole failure chain: the chain
			// repeats what the attempt count already says, and the outermost
			// message is the sentence the task classified its own failure into.
			//
			// Through the deployment's own converter, because a codec-configured
			// deployment has the real message in `encoded_attributes` and the
			// literal string "Encoded failure" in its place — so this reported
			// "Encoded failure" as the reason every retrying step was retrying,
			// which is the answer to "why has this been RUNNING for six hours"
			// made useless on the deployments most likely to be asking. Found
			// while fixing the same read one file over (Codex, #1119).
			LastFailure: boundedFailure(s.decodedFailureMessage(info.GetLastFailure())),
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
		pending.Phase = s.heartbeatPhase(info.GetHeartbeatDetails())

		out = append(out, pending)
	}

	return out, truncated
}

// maxPendingActivities bounds how many retrying steps one answer reports.
//
// Sized like [v1.MaxPendingWaits] rather than derived from anything: past a
// few dozen, "which step is stuck" has stopped being the question and "this
// whole fan-out is stuck" has become it, which the count and the flag together
// already say.
const maxPendingActivities = 64
