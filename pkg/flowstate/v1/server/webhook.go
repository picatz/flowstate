package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The receiver: the path a real delivery takes from a socket to a started run.
//
// Everything a *file* decides about a webhook was already settled — the
// declaration, its checks, and the mapping from a delivery to inputs, which is
// [v1.BindWebhookTriggerInputs] and is called here rather than reimplemented. What
// this adds is the half a deployment decides: which workflow a request addresses,
// whether the delivery is genuine, what happens when the same delivery arrives
// twice, and what is written down about where a run came from.
//
// # The bound, and why it is where it is
//
// A delivery body is attacker-chosen, and the bound on it is the first statement
// of [WebhookReceiver.ServeHTTP]: `r.Body` is replaced with an
// [http.MaxBytesReader] before routing, before verification, before decoding, and
// before anything else in this file can touch it. Every later read — the one
// [io.ReadAll] here, and anything a future middleware or handler adds — goes
// through that reader, because it *is* the request body from that point on.
//
// This is the same reasoning `plugin/transport.go` applies to a response: a cap
// configured through a library's option covers the paths that library remembers to
// carry it through, and connect-go's own non-200 path is the counterexample that
// cost this repository a finding. A cap installed on the stream, below every
// consumer, has no path to miss — there is no second body to read.
//
// # What else a sender controls
//
// Size is not the only resource. How *many* deliveries arrive at once is the
// sender's choice too, so deliveries in flight are bounded ([DefaultWebhookConcurrency])
// and a delivery past that bound is refused with a 503 rather than queued into
// memory. How expensive the `with:` expressions are to evaluate is bounded by
// [v1.DefaultCostLimit], applied inside [v1.BindWebhookTriggerInputs] through the
// same [v1.Scope] every other evaluation in this system uses. How many candidate
// signatures one header offers is bounded in [v1.VerifyWebhookDelivery]. And how
// large the headers may be is [http.Server.MaxHeaderBytes], while how *many* of
// them there are is [http.Server.MaxHeaderValueCount] — separate bounds because a
// sender picks the ratio between them, one enormous value and thirty thousand
// tiny ones costing the same bytes and wildly different numbers of map entries.
// Both belong to whoever runs the server, and `flow server` sets both.
//
// # What a refusal says
//
// One sentence, one status, for every refusal decided before the delivery is known
// to be genuine: an unknown workflow, an unknown trigger, and a signature that did
// not match are indistinguishable from outside. A prober must not be able to
// enumerate which webhooks a deployment serves by reading status codes, and must
// not learn that they found a real one by the shape of the answer. Timing is
// levelled the same way: an unrouted request spends exactly what
// [v1.VerifyWebhookDelivery] spends on any route — [v1.SpendWebhookVerificationWork]
// is that same work with no trigger to decide against — so that "no such webhook"
// costs what a routed refusal costs, whatever that route would have declared.
//
// The operator's channel is the log, which names exactly what happened. That is
// the asymmetry worth having: whoever runs the deployment can debug it, whoever is
// probing it cannot.

// WebhookPathPrefix is where a receiver expects to be mounted, and the prefix it
// parses a delivery's address out of: `/webhooks/<workflow>/<trigger>`.
//
// A constant rather than a parameter so that the mount point and the parser
// cannot disagree — the one bug in this shape that produces a 404 nobody can
// explain, because both halves look right on their own.
const WebhookPathPrefix = "/webhooks/"

// DefaultWebhookConcurrency bounds how many deliveries a receiver processes at
// once.
//
// Each in-flight delivery holds a bounded body, an HMAC over it, a CEL evaluation
// bounded by cost, and one Temporal start. None of those is large; all of them at
// an unbounded multiplicity is the ordinary way an endpoint anybody may POST to
// becomes a memory bound nobody set. 64 is far past a real provider's delivery
// rate and far short of what a worker notices.
const DefaultWebhookConcurrency = 64

// webhookIssuer is the issuer half of the principal a delivery acts as.
//
// A delivery has no Flowstate credential — a payments provider does not hold one —
// so the run's starter is the *trigger*, named in the same `issuer#subject` form
// every other starter is recorded in, which is what lets one memo answer "who
// started this" for a human, a schedule and a delivery alike.
// It is [v1.WebhookPrincipalIssuer] rather than a string written here, because
// the validator composes the same principal to decide whether a `signal:`
// block's gate could ever admit this trigger — two copies of that string is the
// drift invariant 2 refuses, and the copy that drifted would silently make every
// bridged delivery unauthorized.
const webhookIssuer = v1.WebhookPrincipalIssuer

// triggerMemoKey records which trigger started a run, deliveryMemoKey which
// delivery, and reasonMemoKey why a person asked for one.
//
// Memos rather than search attributes, for [namespaceMemoKey]'s reason: no
// cluster-side registration, so provenance is present on a deployment that
// registered nothing.
//
// triggerMemoKey is written by every entry path that creates a run — this
// receiver writes `webhook:<name>`, [FlowstateServer.Run] writes `manual` — so
// "how did this run start" has one answer read from one place rather than being
// inferred from which keys happen to be absent. The other two are written only
// where they mean something: a delivery id by this receiver, a reason by a manual
// start that gave one.
//
// A run started before these keys existed carries none of them, which reads as
// "no trigger recorded" — the same absence [v1.RunState.trigger] carries, and the
// honest answer for a run nothing recorded.
const (
	triggerMemoKey  = "flowstate.trigger"
	deliveryMemoKey = "flowstate.delivery"
	reasonMemoKey   = "flowstate.reason"
)

// errDeliveryNotStarted marks the one failure past verification that is the
// *deployment's* rather than the payload's: everything up to here said the
// delivery was good and then the run could not be created.
//
// It exists so the two are answered differently, which matters to a sender that
// retries: a payload that will never bind must not be retried, and a cluster that
// was briefly unreachable must be. Distinguishing them by error text would be a
// promise about wording that nothing keeps.
var errDeliveryNotStarted = errors.New("the run this delivery names could not be started")

// errDeliveryUnaddressed marks a bridged delivery whose `correlate:` named no
// run this receiver can reach — no such entity key in this tenant, or a run
// recorded under another one.
//
// Its own class because the answer differs from every other post-verification
// refusal: nothing about the delivery is malformed, and nothing about the
// deployment is broken. The gate the sender is answering does not exist here.
var errDeliveryUnaddressed = errors.New("no run this delivery could answer")

// errDeliveryRefused marks a bridged delivery the run's own `signals:` policy
// refused.
//
// Distinct from [errDeliveryUnaddressed] on purpose: "there is no such run" and
// "that run will not take an answer from you" are different facts, and both are
// safe to say to somebody who has already proved they hold the signing key. A
// party who has not cannot reach either — verification is upstream of both, and
// every refusal above it is still the one sentence and the one status.
var errDeliveryRefused = errors.New("the run refused this delivery")

// WebhookReceiver serves deliveries to the webhooks a deployment has been
// configured to accept.
//
// Built by [FlowstateServer.NewWebhookReceiver], which is where every decision
// that can be made before a request arrives is made: which workflows are served,
// which triggers they declare, and whether this deployment can actually resolve
// the signing key each one names. A receiver that exists is one whose whole
// configuration was satisfiable.
type WebhookReceiver struct {
	server *FlowstateServer

	// namespace is the tenant every delivery this receiver accepts is recorded
	// under, and the tenant its signing keys were read in.
	//
	// One field for both, which is the whole of the fix: a sender presents a
	// signature rather than an identity, so there is no caller to derive a tenant
	// from and the operator establishes it. Written down twice — once for the run
	// and once for the resolver — it could disagree with itself, and the
	// disagreement that matters is a webhook reading another tenant's signing key.
	// [FlowstateServer.NewWebhookReceiver] takes the [secrets.Store] rather than
	// an already-scoped [secrets.Resolver] for exactly that reason: the scoping
	// happens here, from this value.
	namespace string

	// routes is workflow name -> trigger name -> what serving it needs. Built
	// once and never written again, which is what makes it safe to read from
	// every request goroutine with no lock.
	routes map[string]map[string]*webhookRoute

	// inFlight is the concurrency bound, as a buffered channel: a token per
	// delivery being processed, and a non-blocking acquire so that exceeding it
	// refuses rather than queues.
	inFlight chan struct{}

	// now is the clock the replay window is measured against. Injectable so a
	// test can pin a signature's timestamp, and so that a receiver and a
	// rehearsal cannot disagree about what "recent" means.
	now func() time.Time

	log *slog.Logger
}

// webhookRoute is one servable trigger: the specification a delivery starts, the
// trigger within it, and the signing material resolved at load.
type webhookRoute struct {
	workflow *v1.Workflow
	trigger  *v1.WebhookTrigger

	// keys is scheme -> resolved key, resolved when configuration loaded.
	//
	// Held rather than resolved per delivery for two reasons, and the second is
	// the one that matters. A secret backend on the path of an endpoint anybody
	// may POST to is an amplifier: one request becomes one Vault call. And
	// resolving at load is what makes "a trigger naming a scheme this deployment
	// cannot satisfy refuses when configuration loads" true rather than aspired
	// to — the alternative discovers it at 3am, on a delivery, with nobody
	// watching.
	//
	// [secrets.Secret] holds its value in a closure, so this map cannot leak one
	// through a `%v` of the receiver, a struct holding it, or a slice of those.
	keys map[string]secrets.Secret
}

// WebhookOption configures a [WebhookReceiver].
type WebhookOption func(*WebhookReceiver)

// WithWebhookConcurrency bounds how many deliveries are processed at once.
//
// A value below one is ignored rather than honoured: a receiver that accepted
// zero would be a configuration mistake that looks exactly like an outage.
func WithWebhookConcurrency(n int) WebhookOption {
	return func(r *WebhookReceiver) {
		if n < 1 {
			return
		}
		r.inFlight = make(chan struct{}, n)
	}
}

// WithWebhookClock sets the clock the replay window is measured against.
func WithWebhookClock(now func() time.Time) WebhookOption {
	return func(r *WebhookReceiver) {
		if now == nil {
			return
		}
		r.now = now
	}
}

// WithWebhookLogger sets where refusals are reported.
//
// Refusals are logged and never returned to the sender, so a receiver with
// nowhere to log is a receiver whose refusals are invisible to its operator too.
// The default discards, because a package that logged to stderr by default would
// be making that choice for every embedder.
func WithWebhookLogger(log *slog.Logger) WebhookOption {
	return func(r *WebhookReceiver) {
		if log == nil {
			return
		}
		r.log = log
	}
}

// NewWebhookReceiver builds the receiver for a set of workflows, refusing now
// anything it could not serve later.
//
// Everything this checks is checked here rather than per request, which is the
// standing fail-closed rule for a policy surface: a scheme nothing implements, a
// trigger with no dedupe key, two triggers a request could not tell apart, and —
// the one that needs a deployment rather than a file to answer — a signing key
// this deployment cannot resolve. A receiver that was built serves every route it
// holds; there is no arm where a delivery arrives and the answer is "we could not
// check that".
//
// # The namespace
//
// namespace is the tenant a delivery's run belongs to, established by the operator
// because a sender has none to offer: a webhook is proved by a signature, not by a
// credential naming a tenant. It decides two things that must be one thing, which
// is why they are one argument:
//
//   - the tenant recorded on every run a delivery starts, and so the Temporal
//     namespace the run is routed to. Left empty on a deployment that maps tenants
//     to namespaces, every genuine delivery was refused: nothing supplied it, so
//     the run was attributed to the empty tenant, which such a mapping has no entry
//     for. That refusal is now taken here, at startup, where an operator can read
//     it.
//   - the tenant its `verify:` keys are read in. A receiver recorded under one
//     tenant and resolving keys in another is a cross-tenant secret read, so the
//     resolver is scoped here, from this value, rather than by the caller: this
//     takes the [secrets.Store] and never an already-scoped [secrets.Resolver],
//     because the two cannot then be given different answers.
//
// The empty namespace is a tenant like any other and the right answer for a
// single-tenant deployment — it is not a wildcard, and it reaches no named
// tenant's secrets. See [secrets.Store.For].
//
// store is the deployment's secret machinery. It is used here and never again:
// what the receiver keeps is the resolved [secrets.Secret], which cannot be
// printed, logged, marshaled or compared with ==. Nothing resolved here reaches a
// specification, a memo or workflow history — the specification carries the
// reference the file wrote, exactly as it does for every other secret in this
// system.
func (s *FlowstateServer) NewWebhookReceiver(
	ctx context.Context, namespace string, workflows []*v1.Workflow, store *secrets.Store, opts ...WebhookOption,
) (*WebhookReceiver, error) {
	if store == nil {
		return nil, fmt.Errorf("a webhook receiver needs a secret store: every trigger's `verify:` names a " +
			"key, and a deployment that cannot resolve one cannot check a delivery")
	}

	// An unnamed receiver belongs to the deployment's own tenant, which is what
	// [WithNamespace] means and is empty on a deployment that names no tenants at
	// all. Resolved here so that the value scoping the keys below is the same value
	// [FlowstateServer.identityFor] will fall back to when the run is created:
	// taking one from here and the other from there is how they come to disagree.
	if namespace == "" {
		namespace = s.namespace
	}

	// Scoped once, from the namespace the runs will be recorded under. A malformed
	// namespace, or an empty one under a store built with
	// [secrets.WithRequiredNamespace], is refused here rather than resolving a key
	// in a tenant nobody chose.
	resolver, err := store.For(secrets.Namespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("scoping the signing keys of the webhooks served for namespace %q: %w",
			namespace, err)
	}

	// And the run half of that same namespace, asked now rather than on the first
	// delivery. A deployment mapping tenants onto Temporal namespaces with no entry
	// for this one can serve no delivery at all, so it must not start advertising
	// an endpoint: the answer would be a refusal per delivery, in a log line, at
	// whatever hour the provider first fired.
	if _, err := s.clientFor(namespace); err != nil {
		return nil, fmt.Errorf("no Temporal namespace is configured for the webhook receiver's namespace %q, "+
			"so no delivery could start a run: give the receiver a namespace this deployment maps, or map "+
			"this one: %w", namespace, err)
	}

	receiver := &WebhookReceiver{
		server:    s,
		namespace: namespace,

		routes:   make(map[string]map[string]*webhookRoute, len(workflows)),
		inFlight: make(chan struct{}, DefaultWebhookConcurrency),
		now:      time.Now,
		log:      slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(receiver)
	}

	for _, workflow := range workflows {
		if err := receiver.register(ctx, workflow, resolver); err != nil {
			return nil, err
		}
	}

	// Trust is granted only once every workflow in this call has been fully
	// admitted, in a second pass rather than interleaved with the loop above.
	// A constructor that mutates the server-wide trusted set as it goes and
	// then returns an error on a later workflow leaves that set holding
	// entries a failed call never actually served — this deployment's own
	// `Run`/`SignalWithStart`/`CreateSchedule` would then substitute a
	// specification for a workflow no webhook route exists for, on the
	// strength of a receiver that was never created.
	//
	// A workflow served for webhook deliveries is deployment-owned in the
	// same sense a `--webhook` Flowfile always is: an operator chose to
	// serve it, at this deployment, under this name and this namespace.
	// Registering it here is what makes its `manual:` policy binding on
	// `Run`/`SignalWithStart`/`CreateSchedule` too — without this, a caller
	// who names the same workflow but submits their own copy authorizes
	// against whatever restriction *they* wrote, not the one this
	// deployment configured. Scoped by the same namespace the receiver
	// itself was just scoped by, so two tenants that both configure a
	// workflow named alike cannot substitute one for the other's.
	//
	// It refuses rather than replaces when this deployment already trusts a
	// different specification under one of these names: see
	// [FlowstateServer.registerTrustedWorkflows].
	if err := s.registerTrustedWorkflows(namespace, workflows); err != nil {
		return nil, err
	}

	return receiver, nil
}

// register admits one workflow's webhooks, or refuses the configuration.
func (r *WebhookReceiver) register(ctx context.Context, workflow *v1.Workflow, resolver secrets.Resolver) error {
	name := workflow.GetName()
	if name == "" {
		return fmt.Errorf("a workflow served for webhook deliveries has no `name:`, and a delivery is " +
			"addressed as /webhooks/<workflow>/<trigger>; give it a name")
	}
	if _, served := r.routes[name]; served {
		return fmt.Errorf("workflow %q is configured twice; a delivery names a workflow, so two "+
			"specifications under one name are two runs a sender could not choose between", name)
	}

	// The schema's own rules, then the whole-set rules a webhook has. Both,
	// because a specification handed to this constructor need not have come from
	// the compiler — the same reason [v1.CheckWebhookTriggers] is called from
	// [v1.BindRunInputs] rather than left to `flow validate`.
	if err := v1.Validate(workflow); err != nil {
		return fmt.Errorf("workflow %q cannot be served: %w", name, err)
	}
	if err := v1.CheckWebhookTriggers(workflow.GetTriggers()); err != nil {
		return fmt.Errorf("workflow %q cannot be served: %w", name, err)
	}

	// And the bridge's rules, at the same moment and for the same reason the
	// signing keys are resolved at the same moment: a `signal:` whose gate no
	// `signals:` rule could admit is a route that answers 403 to every genuine
	// delivery, and an operator can read a refusal here.
	if err := v1.CheckWebhookSignalBridges(workflow); err != nil {
		return fmt.Errorf("workflow %q cannot be served: %w", name, err)
	}

	// And the half a *deployment* answers, asked now rather than on the first
	// delivery. [FlowstateServer.validateSpecification] is the specification-only
	// part of the submission every delivery will make: the plugins this deployment
	// has against the ones the file requires, the credential targets it permits,
	// the declared signal policies, the specification's size. None of those answers
	// can change between here and a delivery, so asking at a delivery only moves
	// the refusal somewhere nobody is looking — a webhook advertised at startup,
	// answering 422 to every genuine delivery, with the reason in a log line.
	//
	// On a clone, because the check pins the plugin selection onto what it is given
	// and the receiver's copy is the pristine specification each delivery clones
	// afresh. What is wanted here is the answer, not the artifact.
	if err := r.server.validateSpecification(proto.Clone(workflow).(*v1.Workflow)); err != nil {
		return fmt.Errorf("workflow %q cannot be served by this deployment, so no delivery to its webhooks "+
			"could ever start a run: %w", name, err)
	}

	triggers := workflow.GetTriggers().GetWebhooks()
	if len(triggers) == 0 {
		return fmt.Errorf("workflow %q declares no webhook triggers, so there is nothing for a delivery "+
			"to address; remove it from the receiver's configuration or declare `triggers:` with a "+
			"`- webhook:` entry", name)
	}

	routes := make(map[string]*webhookRoute, len(triggers))
	for _, trigger := range triggers {
		keys, err := resolveWebhookKeys(ctx, name, trigger, resolver)
		if err != nil {
			return err
		}
		routes[trigger.GetName()] = &webhookRoute{
			workflow: workflow,
			trigger:  trigger,
			keys:     keys,
		}
	}
	r.routes[name] = routes

	return nil
}

// resolveWebhookKeys resolves every key a trigger's `verify:` names, refusing the
// configuration if any of them cannot be reached.
//
// Fail closed at load, which is the whole point: the alternative is a receiver
// that starts, serves, and refuses every genuine delivery for a reason that only
// appears in a log line nobody is reading. The error names the reference — safe,
// it is a location — and never the value.
func resolveWebhookKeys(
	ctx context.Context, workflow string, trigger *v1.WebhookTrigger, resolver secrets.Resolver,
) (map[string]secrets.Secret, error) {
	keys := make(map[string]secrets.Secret, len(trigger.GetVerify()))
	for scheme, value := range trigger.GetVerify() {
		ref := value.GetSecretRef()
		if ref == nil {
			// CheckWebhookTriggers above already refused this, so reaching it
			// means a caller bypassed it. Refused again rather than dereferenced.
			return nil, fmt.Errorf("workflow %q, webhook %q: `verify: {%s: ...}` is not a secret reference",
				workflow, trigger.GetName(), scheme)
		}

		secret, err := resolver.Resolve(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("workflow %q, webhook %q: resolving the %s signing key %s: %w",
				workflow, trigger.GetName(), scheme, secrets.RefString(ref), err)
		}
		keys[scheme] = secret
	}

	return keys, nil
}

// Routes reports what this receiver serves, as `<workflow>/<trigger>` pairs.
//
// For a startup log line and for a test, so that "configured" is observable
// without exercising a delivery. It reports names, which are the file's own
// words, and nothing about the keys behind them.
func (r *WebhookReceiver) Routes() []string {
	var served []string
	for workflow, triggers := range r.routes {
		for trigger := range triggers {
			served = append(served, workflow+"/"+trigger)
		}
	}

	return served
}

// ServeHTTP accepts one delivery.
//
// Mount it at [WebhookPathPrefix], unauthenticated: a sender proves itself with a
// signature over the body, which is the credential a webhook has. That is why the
// bound below is the first thing in the function rather than something the mux in
// front of it was asked to arrange.
func (r *WebhookReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// The bound, before anything else can read a byte. From here on `req.Body`
	// *is* the capped reader, so there is no path — this handler's, a future
	// middleware's, a decoder that reads directly — that can reach the
	// uncapped one. See this file's doc comment for why the cap belongs on the
	// stream rather than in a decoder's options.
	req.Body = http.MaxBytesReader(w, req.Body, v1.MaxWebhookPayloadBytes)

	if req.Method != http.MethodPost {
		// A delivery is a POST. Answered before the concurrency token is taken,
		// because a wrong method costs nothing and should not be able to occupy
		// the budget that real deliveries share.
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "a delivery is POSTed", http.StatusMethodNotAllowed)

		return
	}

	select {
	case r.inFlight <- struct{}{}:
		defer func() { <-r.inFlight }()
	default:
		// Shed rather than queue. A queued delivery is memory the sender chose
		// the size of; a refused one is retried, which is what at-least-once
		// delivery already means everywhere else in this design.
		r.log.WarnContext(req.Context(), "refused a delivery: too many in flight",
			"path", req.URL.Path, "limit", cap(r.inFlight))
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many deliveries in flight", http.StatusServiceUnavailable)

		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			r.log.WarnContext(req.Context(), "refused a delivery: body past the bound",
				"path", req.URL.Path, "limit", v1.MaxWebhookPayloadBytes)
			http.Error(w, "the delivery body is too large", http.StatusRequestEntityTooLarge)

			return
		}

		r.log.WarnContext(req.Context(), "refused a delivery: body could not be read",
			"path", req.URL.Path, "error", err)
		http.Error(w, "the delivery body could not be read", http.StatusBadRequest)

		return
	}

	route, found := r.route(req.URL.Path)
	if !found {
		// Spent, not skipped: an unrouted delivery costs what
		// [v1.VerifyWebhookDelivery] spends against any route — every scheme
		// this build knows, verified once each under a key nobody has — so that
		// the time an unknown webhook takes is the time a routed one takes,
		// regardless of how many schemes that route would have declared (#973).
		// Then the same refusal, so neither is the answer that stands out.
		v1.SpendWebhookVerificationWork(webhookHeaders(req.Header), body, r.now())
		r.refuse(req, "no such webhook", "path", req.URL.Path)
		writeWebhookRefusal(w)

		return
	}

	headers := webhookHeaders(req.Header)
	if err := v1.VerifyWebhookDelivery(route.trigger, route.keys, headers, body, r.now()); err != nil {
		r.refuse(req, "the delivery did not verify",
			"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(), "error", err)
		writeWebhookRefusal(w)

		return
	}

	// Only now is anything the delivery chose parsed, which is the order
	// [v1.BindWebhookTriggerInputs] documents and the reason a malformed body may
	// safely be reported precisely: reaching this line means the sender holds the
	// signing key, so nothing said from here on tells an outsider anything.
	decoded, err := decodeDeliveryBody(body)
	if err != nil {
		r.log.WarnContext(req.Context(), "a verified delivery did not decode",
			"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(), "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	// The span covering the acceptance, opened here and not one line earlier: it
	// carries a link to whatever trace the sender named, and reading a
	// `traceparent` from an unverified request would let anyone at all name our
	// trace ids. See `webhooktrace.go` for why the sender's context is a link
	// rather than a parent, and for what makes that linkage reach the run.
	ctx, span := r.startDeliverySpan(req.Context(), route, req.Header)
	defer span.End()

	deliver := r.start
	if route.trigger.GetSignal() != nil {
		// The one fork in this handler, and it is decided by the *file* rather
		// than by anything the delivery said: a trigger declaring `signal:`
		// answers a gate and a trigger declaring `with:` starts a run, the two
		// are refused together when the file compiles, and a delivery has no
		// spelling that selects between them.
		deliver = r.answer
	}

	accepted, err := deliver(ctx, route, v1.WebhookDelivery{
		// The trace context headers are stripped from what becomes
		// `event.headers`, so a Flowfile mapping a header into an input cannot
		// serialize peer-chosen `traceparent`/`tracestate` into RunState and
		// history. The receiver has already consumed them, from the original
		// request headers, as the delivery span's link — see `withoutTraceHeaders`.
		Headers: withoutTraceHeaders(headers),
		Body:    decoded,

		// True because verification above said so, and set here rather than
		// anywhere earlier: [v1.WebhookDelivery.Verified] has no arm meaning
		// "unchecked, allow anyway", so the only way it becomes true is a check
		// that passed.
		Verified: true,
	})
	recordDeliveryOutcome(span, accepted, err)
	if err != nil {
		// Logged through the delivery span's context rather than the request's,
		// so the line carries this delivery's trace and span ids: `otelslog`
		// reads them off the context, and a failure an operator is reading is
		// exactly when the trace it belongs to is worth one click away.
		r.log.ErrorContext(ctx, "a verified delivery did not start a run",
			"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(), "error", err)

		if errors.Is(err, errDeliveryRefused) {
			// The run exists and its `signals:` policy will not take an answer
			// from this trigger. Said precisely, and only to a party that
			// already proved it holds the signing key: the compiler refuses a
			// `signal:` whose declared policy could never admit its trigger, so
			// reaching this means the *run's own recorded* policy differs — a
			// separation-of-duties clause, a `subject:` resolved from the run's
			// inputs, or a deployment serving a specification the run was not
			// started from.
			http.Error(w, err.Error(), http.StatusForbidden)

			return
		}

		if errors.Is(err, errDeliveryUnaddressed) {
			// 404 rather than 422: the sender's payload is fine and named a run
			// that is not here. A provider retrying is the right behavior when
			// a gate has not been reached yet, and 4xx-that-means-stop would
			// tell it to give up on a delivery that may become deliverable.
			w.Header().Set("Retry-After", "5")
			http.Error(w, err.Error(), http.StatusNotFound)

			return
		}

		if errors.Is(err, errDeliveryNotStarted) {
			// The deployment could not do what it was asked, which a sender must
			// retry — so a status that means "try again" rather than one that
			// means "this payload will never work". The sentence is generic on
			// purpose: what went wrong between this server and Temporal is not
			// the sender's business even when the sender is genuine.
			w.Header().Set("Retry-After", "5")
			http.Error(w, "the delivery could not be started; retry", http.StatusServiceUnavailable)

			return
		}

		// Otherwise the payload did not satisfy the file — a mapping reaching a
		// field it does not carry, an input that will not bind — which no retry
		// fixes. Reported precisely, because whoever holds the signing key is
		// entitled to know why their delivery did nothing.
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)

		return
	}

	r.log.InfoContext(ctx, "accepted a delivery",
		"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(),
		"delivery", accepted.DeliveryID, "run", accepted.RunID, "joined", accepted.Joined)

	status := http.StatusAccepted
	if accepted.Joined {
		// 200 rather than 202: nothing was accepted for processing, because this
		// delivery was already processed. A sender retrying gets a success either
		// way — which is the point of a dedupe key — and an operator watching
		// status codes can still tell the two apart.
		status = http.StatusOK
	}
	writeWebhookJSON(w, status, accepted)
}

// AcceptedDelivery is what a receiver answers a sender with.
//
// Deliberately small. It names the run so a sender can correlate, and says whether
// this delivery started it, and nothing else: a response body is the one part of
// this exchange an attacker who *does* hold the signing key can read, so it
// carries no specification, no inputs and no deployment detail.
type AcceptedDelivery struct {
	// WorkflowID is the run's addressable id, derived from the idempotency key.
	WorkflowID string `json:"workflow_id"`

	// RunID is the Temporal run this delivery started or joined.
	RunID string `json:"run_id"`

	// DeliveryID names the delivery, and is what provenance records — a digest of
	// the idempotency key rather than the key itself. See [webhookDeliveryID].
	DeliveryID string `json:"delivery_id"`

	// Joined is true when this delivery was a redelivery: the run already
	// existed, and no second one was started.
	Joined bool `json:"joined"`
}

// route resolves a request path to what serves it.
//
// Returns not-found for every shape that is not exactly
// `<prefix>/<workflow>/<trigger>`, including a longer path: a receiver that
// ignored trailing segments would serve `/webhooks/a/b/../../c` and every other
// address somebody can construct.
func (r *WebhookReceiver) route(path string) (*webhookRoute, bool) {
	rest, found := strings.CutPrefix(path, WebhookPathPrefix)
	if !found {
		return nil, false
	}
	workflow, trigger, found := strings.Cut(rest, "/")
	if !found || workflow == "" || trigger == "" || strings.Contains(trigger, "/") {
		return nil, false
	}

	triggers, served := r.routes[workflow]
	if !served {
		return nil, false
	}
	route, declared := triggers[trigger]

	return route, declared
}

// start turns a verified delivery into a run, or joins the run a previous
// delivery of the same event already started.
//
// # What the idempotency key addresses
//
// The key names the *event*, so it names the run: the workflow id is derived from
// it, and Temporal's own uniqueness on that id is the dedupe. That choice is what
// makes the guarantee hold for the case a time-window dedupe cannot see — two
// deliveries of one event arriving *simultaneously*, which is exactly what a
// provider's retry storm produces. Both requests reach StartWorkflowExecution;
// the cluster admits one and answers the other with
// [serviceerror.WorkflowExecutionAlreadyStarted] carrying the run id of the one
// that won. There is no window, no local table, and nothing to expire.
//
// A redelivery is *joined* rather than refused: the sender is told about the run
// their event produced, with a 200, so a retry converges instead of alternating
// between a success and an error. Refusing would be the same amount of dedupe and
// a worse answer — a provider that reads a 4xx as "this event was rejected" would
// keep retrying or start alerting about a delivery that in fact succeeded.
func (r *WebhookReceiver) start(ctx context.Context, route *webhookRoute, delivery v1.WebhookDelivery) (AcceptedDelivery, error) {
	// The mapping, which is the one function `flow test` replays offline and the
	// receiver calls here — never a second binding path. It checks the trigger,
	// refuses an unverified delivery, evaluates `with:` and `idempotency_key:`
	// against `event` under [v1.DefaultCostLimit], and binds the result through
	// the same [v1.BindRunInputs] every other entry path uses.
	inputs, key, err := v1.BindWebhookTriggerInputs(ctx, route.workflow, route.trigger, delivery)
	if err != nil {
		return AcceptedDelivery{}, err
	}

	// Cloned because the submission pipeline writes to the specification it is
	// given — [FlowstateServer.pinPlugins] records the deployment's plugin
	// selection on it — and the receiver's copy is long-lived and shared by every
	// delivery to this route.
	spec := proto.Clone(route.workflow).(*v1.Workflow)

	// The same submission the `Run` RPC performs, reached rather than restated:
	// plugins pinned, signal policies checked, specification size bounded, inputs
	// bound and the pair weighed. A delivery that could skip this would be an
	// entry path with laxer rules than the one a person uses.
	bound, err := r.server.validateSubmission(spec, inputs)
	if err != nil {
		return AcceptedDelivery{}, err
	}

	// The principal a delivery acts as. A sender holds no Flowstate credential,
	// so what is recorded is the trigger that admitted them — established by this
	// deployment's configuration, never taken from the request, the same rule
	// every other identity in this server follows.
	//
	// Including the tenant, which is the receiver's own and is the same value its
	// signing keys were resolved under. A principal with no namespace would fall
	// back to whatever [WithNamespace] was given, which is nothing on a deployment
	// that instead maps tenants — and the empty tenant is one such a mapping
	// refuses to route, so every genuine delivery ended in a 422 nobody could act
	// on from outside.
	identity := r.server.identityFor(auth.ContextWithPrincipal(ctx, auth.Principal{
		Issuer:    webhookIssuer,
		Subject:   route.workflow.GetName() + "/" + route.trigger.GetName(),
		Namespace: r.namespace,
	}))

	memo, temporal, options, err := r.server.prepareCreate(ctx, identity, spec, bound)
	if err != nil {
		return AcceptedDelivery{}, err
	}

	deliveryID := v1.WebhookDeliveryID(key)
	memo[triggerMemoKey] = "webhook:" + route.trigger.GetName()
	memo[deliveryMemoKey] = deliveryID
	options.Memo = memo

	options.ID = webhookWorkflowID(identity.GetNamespace(), route.workflow.GetName(), route.trigger.GetName(), key)

	// The two halves of "this event starts one run". Conflict covers a
	// redelivery while the first run is still going; reuse covers one that
	// arrives after it finished, which is the half a receiver that only thought
	// about concurrent retries gets wrong — a provider redelivering an hour later
	// would otherwise start a second run of a completed event.
	options.WorkflowIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	options.WorkflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE

	// Asked for the error rather than the silent join the SDK defaults to, so
	// that "this was a redelivery" is a fact this code establishes rather than
	// infers. The error carries the existing run's id, so recognizing a
	// redelivery costs no extra call.
	options.WorkflowExecutionErrorWhenAlreadyStarted = true

	run, err := temporal.ExecuteWorkflow(ctx, options, engine.Run, &v1.RunState{
		Workflow:           spec,
		StepsBudget:        int32(r.server.maxStepsPerRun),
		Identity:           identity,
		Inputs:             bound,
		MetricWorkflowName: spec.GetName(),

		// How this run started, for the workflow's own steps to read: which
		// webhook, admitted as which principal, by which delivery. The delivery id
		// is the digest [webhookDeliveryID] already computed and never the
		// idempotency key it names — the usual key is a signature header, and this
		// value is written to history, which invariant 8 calls durable and broadly
		// readable.
		Trigger: v1.NewWebhookTriggerContext(
			route.trigger.GetName(), identity.GetSubject(), deliveryID),
	})
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return AcceptedDelivery{
				WorkflowID: options.ID,
				RunID:      already.RunId,
				DeliveryID: deliveryID,
				Joined:     true,
			}, nil
		}

		return AcceptedDelivery{}, fmt.Errorf("%w: %w", errDeliveryNotStarted, err)
	}

	return AcceptedDelivery{
		WorkflowID: options.ID,
		RunID:      run.GetRunID(),
		DeliveryID: deliveryID,
	}, nil
}

// answer delivers a verified delivery to the gate a run is already parked at.
//
// [WebhookReceiver.start]'s sibling, and the whole of the bridge's server-side
// half. It reuses rather than restates: the mapping is
// [v1.BindWebhookTriggerSignal], the address is [v1.EntityWorkflowID], the
// tenancy check is [FlowstateServer.authorizeRunDecision], and the
// authorization is [FlowstateServer.authorizeSignal] — the same four functions
// `flow signal` reaches, called with the principal this receiver already mints
// for a run start.
//
// # What it does not have, deliberately
//
// No dedupe table. A receiver's own ledger of seen delivery ids would be a
// second mechanism guarding a fact the run already knows better: it is
// per-process, per-tenant, empty after a restart, and it cannot see the case
// that matters — a redelivery arriving after the gate was answered, which the
// engine drops at intake by consulting `RunState.consumed_delivery_ids`. What
// the receiver carries is the id, on the sender, and nothing else.
//
// # Every accepted answer is a join
//
// A bridge starts nothing, so `Joined` is always true and the status is always
// 200: the run existed before this delivery and exists after it. A duplicate is
// therefore answered exactly as the first delivery was, which is what a dedupe
// key promises a sender — the difference between them is invisible from
// outside, and is the engine's to make, not the receiver's.
func (r *WebhookReceiver) answer(ctx context.Context, route *webhookRoute, delivery v1.WebhookDelivery) (AcceptedDelivery, error) {
	// The mapping, which is the one function `flow test` replays offline and
	// this calls rather than reimplements. It checks the trigger and the bridge,
	// refuses an unverified delivery, and only then evaluates `correlate:`,
	// `idempotency_key:` and the payload against `event` under
	// [v1.DefaultCostLimit].
	entityKey, payload, key, err := v1.BindWebhookTriggerSignal(ctx, route.workflow, route.trigger, delivery)
	if err != nil {
		return AcceptedDelivery{}, err
	}

	deliveryID := v1.WebhookDeliveryID(key)
	name := route.trigger.GetSignal().GetName()

	// The same principal [WebhookReceiver.start] mints, established by this
	// deployment's configuration and never taken from the request — including
	// the tenant, which is the receiver's own and the one its signing keys were
	// resolved under. Put on the context rather than built beside it, because
	// everything below reads the caller the way every other verb does, from
	// [FlowstateServer.identityFor]: a bridge that derived its own answer would
	// be a second identity path with the same name.
	ctx = auth.ContextWithPrincipal(ctx, auth.Principal{
		Issuer:    webhookIssuer,
		Subject:   v1.WebhookTriggerSubject(route.workflow.GetName(), route.trigger.GetName()),
		Namespace: r.namespace,
	})
	identity := r.server.identityFor(ctx)

	// Stable-key addressing, composed exactly as `Run` and `SignalWithStart`
	// compose it, from the *receiver's* namespace and the delivery's entity key.
	// The namespace is not the sender's to choose, which is what keeps a key
	// holder inside the tenant their trigger was configured in.
	workflowID, err := v1.EntityWorkflowID(identity.GetNamespace(), entityKey)
	if err != nil {
		return AcceptedDelivery{}, fmt.Errorf("%w: %w", errDeliveryUnaddressed, err)
	}

	// Tenancy, through the decision every other verb reaches: the run must exist
	// and must be recorded under this receiver's tenant. A run in another
	// tenant answers exactly as a run that does not exist, which is
	// [FlowstateServer.authorizeRunDecision]'s own rule and not a second one.
	temporal, resp, code, err := r.server.authorizeRunDecision(ctx, workflowID, "")
	if err != nil {
		// The decision, written down. Through the unaudited form and audited
		// here rather than through [FlowstateServer.authorizeRun], for
		// [FlowstateServer.Signal]'s reason: this verb reaches one decision and
		// then adds the gate's own policy to it, and a record per lookup would
		// write a denial for a delivery the next line goes on to accept.
		refusal := fmt.Errorf("%w: no run is waiting under entity key %q in this webhook's tenant",
			errDeliveryUnaddressed, entityKey)
		if code == v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED {
			return AcceptedDelivery{}, refusal
		}

		return AcceptedDelivery{}, r.audited(r.server.auditDeny(ctx, "WebhookSignal",
			v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID, code, refusal), refusal)
	}

	// What the server attests about this delivery, built here for the reason
	// [FlowstateServer.Signal] builds one: the sender is what the server
	// established, never what the payload claims. The delivery id rides along,
	// so the run can recognize a redelivery of it later.
	sender := &v1.SignalSender{
		Identity:   identity,
		AcceptedAt: timestamppb.Now(),
		DeliveryId: deliveryID,
	}

	// The gate's own policy, checked by the same function `flow signal` is
	// checked by, before Temporal sees anything. A refusal here never reaches
	// the workflow at all.
	if err := r.server.authorizeSignal(resp, name, sender); err != nil {
		refusal := fmt.Errorf("%w: %w", errDeliveryRefused, err)

		return AcceptedDelivery{}, r.audited(r.server.auditDeny(ctx, "WebhookSignal",
			v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, refusal), refusal)
	}

	// And the acceptance, before anything is delivered — the same order
	// [FlowstateServer.Signal] writes it in, so a run's audit trail reads the
	// same whether a person or a webhook answered its gate. A required recorder
	// that cannot record refuses the delivery rather than proceeding
	// unrecorded, which the sender may retry.
	if err := r.server.auditAllow(ctx, "WebhookSignal",
		v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return AcceptedDelivery{}, fmt.Errorf("%w: %w", errDeliveryNotStarted, err)
	}

	runID := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	// Routed to the current execution rather than to the one just described:
	// a run that continued as new between the describe above and this line is
	// still the same run to everybody outside, and naming the execution would
	// deliver into a segment that has already closed. That is
	// [FlowstateServer.Signal]'s own reading of a chain, applied here.
	if err := temporal.SignalWorkflow(ctx, workflowID, "", name, &v1.SignalDelivery{
		Payload: payload,
		Sender:  sender,
	}); err != nil {
		return AcceptedDelivery{}, fmt.Errorf("%w: %w", errDeliveryNotStarted, err)
	}

	return AcceptedDelivery{
		WorkflowID: workflowID,
		RunID:      runID,
		DeliveryID: deliveryID,

		// Always: see this function's doc. A delivery that answers a gate never
		// started anything, so 200 is the honest status for the first one and
		// for its redeliveries alike.
		Joined: true,
	}, nil
}

// audited reconciles what [FlowstateServer.auditDeny] returns with what a
// sender is told.
//
// auditDeny hands back the refusal it was given in the ordinary case, so that a
// call site reads `return s.auditDeny(...)` and cannot answer a denied request
// with success — and hands back something *else* when a required recorder could
// not record, which is a deployment failure rather than a verdict about this
// delivery. This receiver has to tell those apart, because they get different
// statuses: the refusal is the sender's business and the recorder's failure is
// retryable.
func (r *WebhookReceiver) audited(returned, refusal error) error {
	if errors.Is(returned, refusal) {
		return refusal
	}

	return fmt.Errorf("%w: %w", errDeliveryNotStarted, returned)
}

// webhookWorkflowID derives the run's id from the delivery's idempotency key.
//
// Hashed rather than interpolated, for two reasons that both have to hold. A key
// is an expression over an attacker-chosen payload, so it can contain anything at
// all — characters Temporal refuses, a length past its limit, or a crafted value
// intended to collide with an id somebody else's run is addressed by. And a key
// is frequently a *signature header*, which is credential-shaped material that
// must not become a durable, broadly readable identifier. A digest is neither: it
// is fixed-length, alphabet-safe, and reveals nothing about the key it names.
//
// The tenant, the workflow and the trigger are inside the digest, so the same
// event delivered to two triggers, or to two tenants, is two runs — a dedupe key
// dedupes within the source that issued it, never across sources.
//
// The `flowstate-webhook-` prefix keeps this namespace distinct from
// [v1.EntityWorkflowID]'s, so a delivery can never address, join or block a run
// created by an entity key.
func webhookWorkflowID(namespace, workflow, trigger, key string) string {
	digest := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s", namespace, workflow, trigger, key))

	return "flowstate-webhook-" + hex.EncodeToString(digest[:])
}

// decodeDeliveryBody reads the payload the way `flow test` reads a stored one.
//
// The same decoder settings, deliberately: [json.Decoder.UseNumber] followed by
// [v1.NormalizeDeliveryNumbers], so `"amount": 4200` is an integer here exactly as
// it is in a replayed delivery. A receiver that decoded numbers as float64s would
// refuse an input declared `int` for a mapping that a rehearsal accepted, which is
// the rehearsal lying about production.
func decodeDeliveryBody(body []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("the delivery body is not a JSON document: %w", err)
	}

	// And nothing after it. [json.Decoder.Decode] reads one value and stops, so
	// `{"id":"a"} {"id":"b"}` — or a document followed by arbitrary bytes — decoded
	// as the first value and silently discarded the rest, starting a run from a
	// prefix that can mean something other than what the payload as a whole says.
	// A delivery is one document, so end of input is part of the contract and is
	// checked rather than assumed.
	//
	// This reads no more than the first decode could: the reader is over `body`,
	// which [http.MaxBytesReader] bounded to [v1.MaxWebhookPayloadBytes] before a
	// byte of it was read, and a [bytes.Reader] cannot yield more than it holds.
	// Into a [json.RawMessage] so that nothing is built from what follows — the
	// question is only whether anything does, and either answer that is not
	// [io.EOF] is a refusal.
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("the delivery body carries more than one JSON document: a delivery is a " +
			"single JSON value with nothing after it, so send one document per delivery")
	}

	return v1.NormalizeDeliveryNumbers(decoded), nil
}

// webhookHeaders flattens a request's headers into what `event.headers` holds.
//
// The first value of each, which is what an expression reading
// `event.headers["stripe-signature"]` means. Lower-cased by
// [v1.NewWebhookEvent] on the way into `event`, so a stored delivery and a live
// one produce the same value for the same header spelled differently.
func webhookHeaders(header http.Header) map[string]string {
	headers := make(map[string]string, len(header))
	for name, values := range header {
		if len(values) > 0 {
			headers[strings.ToLower(name)] = values[0]
		}
	}

	return headers
}

// refuse logs why a delivery was refused, which is the only place the reason is
// said out loud.
func (r *WebhookReceiver) refuse(req *http.Request, reason string, args ...any) {
	r.log.WarnContext(req.Context(), "refused a delivery: "+reason, args...)
}

// writeWebhookRefusal is the one answer every pre-verification refusal gets.
//
// One status and one sentence for "no such workflow", "no such trigger" and "that
// signature is wrong", because the difference between them is exactly what a
// prober is trying to learn. 404 rather than 403 for the same reason: a
// deployment should not confirm that an endpoint exists to somebody who cannot
// sign for it.
func writeWebhookRefusal(w http.ResponseWriter) {
	http.Error(w, "the delivery was not accepted", http.StatusNotFound)
}

// writeWebhookJSON answers an accepted delivery.
func writeWebhookJSON(w http.ResponseWriter, status int, accepted AcceptedDelivery) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(accepted)
}
