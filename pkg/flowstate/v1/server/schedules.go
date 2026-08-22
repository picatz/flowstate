package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	common "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	// Aliased because the local name `temporal` is taken throughout this file by
	// the Temporal *client* a tenant's schedules live on, and shadowing the package
	// with the client is how a sentinel comparison silently stops compiling.
	sdk "go.temporal.io/sdk/temporal"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// A schedule belongs to a tenant twice over, and both are load-bearing.
//
// **The id encodes it.** A schedule carries a name somebody chose, and a name
// somebody chose collides: two teams sharing a Temporal namespace will both want
// `nightly-report`, and one of them would be told the name was taken — which
// denies them a name *and* discloses that the other team exists. So the Temporal
// schedule id is derived from the tenant and the name together, and a caller
// cannot express an id for a tenant that is not theirs, because the tenant half
// comes from their authenticated identity and never from the request.
//
// **The memo records it.** Exactly as [FlowstateServer.Run] records the tenant on
// a run, Create records it on the schedule, and every later request reads it back
// and refuses a mismatch. That is what [ownedBy] already answers for runs, asked
// here unchanged.
//
// Both, and not one, because they fail differently. The id derivation is what
// makes another tenant's schedule *unaddressable* — the id a caller's request
// resolves to simply is not the one that exists. The memo is what still holds if
// the derivation ever changes, if a schedule is created by something other than
// this code path, or if a deployment maps several Flowstate namespaces onto one
// Temporal namespace. A boundary with one implementation is a boundary one
// refactor from being decorative.
//
// # The separator is unambiguous, which is not automatic
//
// This repository has already been bitten by a namespaced key that could be read
// two ways: the env secret provider derived `prefix + NAMESPACE + "_" + name`, and
// because every character legal in a namespace was legal in a name, tenant
// `team-a`'s secret was readable by two other tenants. So the encoding here is
// checked rather than assumed.
//
// A Flowstate namespace is lowercase letters, digits and dashes
// (`secrets.ValidateNamespace`), and may not contain an underscore. A schedule
// name may. So splitting at the *first* underscore after the fixed prefix
// recovers exactly the pair that was written, whatever either half contains — the
// namespace cannot reach across the separator and a name cannot forge one, and
// the empty namespace of an untenanted deployment produces a leading separator no
// non-empty namespace can produce. TestScheduleIDsAreUnambiguous holds that.

// schedulePrefix is what marks a Temporal schedule as one of Flowstate's.
//
// Present so a listing can tell this engine's schedules from anything else
// sharing the namespace, which is the same reason `List` scopes its query to this
// engine's workflow type: a Temporal namespace is not necessarily Flowstate's
// alone, and every id a listing returns is a live argument to `flow schedule
// delete`.
const schedulePrefix = "flowstate-schedule-"

// scheduleIDFor is the Temporal schedule id a tenant's schedule name maps to.
//
// It does not check the namespace, and deliberately does not: the grammar the
// separator argument above rests on is guaranteed where the namespace is
// chosen, not re-asked here. Both of the two ways a namespace reaches this
// function check it — [WithNamespace] for the deployment's own fallback tenant
// (#823) and `auth.TrustedIssuer.namespaceFrom` for one a caller's token
// carries — so a value that reaches here has passed [auth.ValidateNamespace]
// exactly once, which is also why there is no error for a caller to have to
// turn into a status code.
func scheduleIDFor(namespace, name string) string {
	return schedulePrefix + namespace + "_" + name
}

// scheduleNameFrom recovers the schedule name from a Temporal id, for the caller's
// tenant, and reports false for an id that is not this tenant's Flowstate schedule.
//
// Fail closed on anything unexpected: an id without the prefix belongs to another
// application, and an id whose namespace half is not the caller's belongs to
// another tenant. Neither is named back to the caller.
func scheduleNameFrom(id, namespace string) (string, bool) {
	rest, ok := strings.CutPrefix(id, schedulePrefix)
	if !ok {
		return "", false
	}

	owner, name, separated := strings.Cut(rest, "_")
	if !separated || owner != namespace || name == "" {
		return "", false
	}

	return name, true
}

// CreateSchedule arranges for a workflow to run on a cadence.
//
// The refusals are `Run`'s, deliberately and in the same order, because a schedule
// is a run somebody arranged in advance and every reason to refuse one now is a
// reason to refuse it at three in the morning — with nobody there to read it. That
// is why this shares [FlowstateServer.validateSpecification] with `Run` rather than
// hand-rolling a second copy of it: a schedule that refused a specification `Run`
// accepts, or accepted one `Run` refuses, would be exactly the "may create" versus
// "may Run" drift `validateSubmission`'s own doc warns about, one RPC further out.
// The only addition is the cadence itself, which `Run` has no opinion about.
func (s *FlowstateServer) CreateSchedule(ctx context.Context, req *connect.Request[v1.CreateScheduleRequest]) (*connect.Response[v1.CreateScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Captured before the trusted lookup below, for the same reason [FlowstateServer.Run]
	// captures it before its own: the lookup needs the caller's own namespace,
	// or it is a single deployment-wide name rather than a boundary between
	// tenants. Otherwise captured where it always was — see the later comment
	// by its second use, just before it is frozen into the schedule.
	identity := s.identityFor(ctx)

	// Through the trusted lookup, for the identical reason [FlowstateServer.Run]
	// and [FlowstateServer.SignalWithStart] do: a schedule is a run somebody
	// arranged in advance, so the trust boundary that keeps `manual:` and
	// `manual.allowed_principals` policy rather than caller input has to bind
	// here too. Without it, a caller could take a trusted webhook-only workflow
	// with `manual: denied`, add a `schedule:` trigger to their own copy, and
	// have this handler create and later fire *that* copy under the trusted
	// name.
	//
	// The caller's copy is kept as it arrived first, so the attestation below has
	// something to compare the scheduled specification against — a clone, taken
	// before the first thing that can change a specification, for the reason
	// [FlowstateServer.Run] gives at its own capture: the lookup returns the
	// request's own pointer when nothing is registered under that name, and
	// [FlowstateServer.validateSpecification] then pins plugin versions onto that
	// pointer, so holding a reference would be an equality that can only answer
	// true.
	submitted := proto.Clone(req.Msg.GetWorkflow()).(*v1.Workflow)

	workflow, err := s.trustedWorkflow(identity.GetNamespace(), req.Msg.GetWorkflow())
	if err != nil {
		return nil, err
	}

	// pinPlugins, credential targets, declared signal policies, spec size and
	// structure depth — everything `Run` asks about a specification on its own,
	// before it ever sees a submission's inputs. See that function's doc.
	if err := s.validateSpecification(workflow); err != nil {
		return nil, err
	}

	// The cadence. A schedule with none is one Temporal creates happily and never
	// fires — the silent success that is worse than a refusal, since the only
	// evidence of it is a report nobody receives.
	trigger := workflow.GetTriggers().GetSchedule()
	if err := v1.CheckScheduleTrigger(trigger); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"workflow %q cannot be scheduled: %w", workflow.GetName(), err))
	}

	if err := v1.CheckScheduleBackfillForTrigger(trigger, req.Msg.GetBackfill()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Bound here, once, through the very function `Run` and `flow run local` bind
	// with. Every firing then starts from the checked and defaulted map rather than
	// re-deriving it, so a declaration edited after this does not change what a
	// schedule already created passes — the same rule `RunState.inputs` follows
	// across a Continue-As-New, for the same reason.
	inputs, err := v1.BindRunInputs(workflow, req.Msg.GetInputs())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := v1.CheckSubmissionSize(workflow, inputs); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// identity was captured above, before the trusted lookup, and is used here
	// unchanged: every firing for the life of this schedule acts as whoever
	// created it. That is the honest reading of what a schedule is — a
	// standing instruction left by a person — and the alternative has no
	// answer, since at 03:00 there is no caller to derive an identity from.
	// It is also why deleting a schedule matters when somebody leaves.
	namespace := identity.GetNamespace()

	name := v1.ScheduleNameFor(req.Msg.GetName(), workflow)
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(
			"the schedule has no name and the workflow has none to borrow; pass a name"))
	}

	temporal, err := s.clientFor(namespace)
	if err != nil {
		return nil, err
	}

	// The same derivation [FlowstateServer.prepareCreate] applies to a directly
	// submitted run, through the same function, so a scheduled run lands on its
	// tenant's fleet exactly as a submitted one does. Resolved at creation
	// rather than at each firing for the reason everything else here is: there
	// is nobody at 03:00 to be told the queue could not be composed.
	taskQueue, err := s.taskQueueFor(namespace)
	if err != nil {
		return nil, err
	}

	spec, err := scheduleSpecOf(trigger)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The declared signal policy, resolved against this schedule's own bound
	// inputs and encoded through the exact function [Run] uses —
	// [signalPolicyMemoEntry] — so a scheduled run resolves and enforces
	// exactly what a direct run does, on every firing. This was the hole a
	// scheduled approval gate had: a schedule's fired execution used to
	// carry only the tenant memo, never this one, so `Signal` read the fired
	// run's memo, found no policy entry, and allowed any in-tenant sender —
	// the zero case, reached by a workflow that had in fact declared a
	// policy. Sharing the one encoding function with [Run] is what makes
	// that impossible to reintroduce by editing one path and not the other.
	signalEntry, err := signalPolicyMemoEntry(ctx, workflow, inputs)
	if err != nil {
		// Symmetric with Run's own refusal: an InvalidArgument covers a
		// caller-supplied input that a rule's subject_from cannot resolve to
		// a qualified subject; anything else is this handler unable to
		// encode a specification CheckSignalPolicies and v1.Validate already
		// accepted, and must not create a schedule whose every firing would
		// silently enforce nothing.
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The schedule's own memo (below) governs the Schedule object itself —
	// what `ownedBy`/`ListSchedules`/`DescribeSchedule` read — and is never
	// consulted by `Signal`, which authorizes against the *fired execution's*
	// memo instead (the Action's, further down). The policy entry is written
	// to both anyway, for the same reason the tenant already is on both: a
	// schedule and its firings are two different Temporal objects, and
	// keeping their memos in the same shape means a future reader of either
	// finds what it expects rather than discovering the split by tracing
	// through `Signal`.
	// The starter, recorded through [starterMemoEntry] on both memos for the
	// same reason and in the same shape the policy entry above is: whoever
	// creates a schedule is the starter of every run it fires, frozen once
	// here because there is no caller left at 03:00 to derive one from.
	scheduleMemo := map[string]any{namespaceMemoKey: namespace}
	for k, v := range starterMemoEntry(identity) {
		scheduleMemo[k] = v
	}
	for k, v := range signalEntry {
		scheduleMemo[k] = v
	}

	actionMemo := map[string]any{namespaceMemoKey: namespace, triggerMemoKey: v1.TriggerKindSchedule + ":" + name}
	for k, v := range starterMemoEntry(identity) {
		actionMemo[k] = v
	}
	for k, v := range signalEntry {
		actionMemo[k] = v
	}

	// Unconditional, through the exact function [FlowstateServer.Run] uses —
	// see [workflowNameMemoEntry]. Written to both memos for the reason the
	// comment above already gives for the tenant and the signal policy: a
	// `name` filter has to see identically whichever memo it happens to be
	// reading, and this is the one function that guarantees that.
	for k, v := range workflowNameMemoEntry(workflow.GetName()) {
		scheduleMemo[k] = v
		actionMemo[k] = v
	}

	// Answered last, against the specification about to be frozen into the
	// schedule's action rather than against the one the trusted lookup returned —
	// the same place in the same order [FlowstateServer.Run] answers it, and for
	// the reason its comment gives at length: [FlowstateServer.validateSpecification]
	// pins the deployment's plugin selection onto this specification above, so a
	// question asked before that would be answered about a message that had not
	// finished being assembled. Whole-message equality, so a transformation nobody
	// has thought of yet costs a caller the precise view rather than costing them
	// a secret.
	//
	// Before the create rather than after it, because the value below is what
	// `Args` carries into every firing and nothing between here and the response
	// touches it — computing it after would read the same message and say so less
	// clearly.
	asSubmitted := proto.Equal(submitted, workflow)

	_, err = temporal.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:               scheduleIDFor(namespace, name),
		Spec:             spec,
		Overlap:          overlapOf(trigger.GetOverlap()),
		Paused:           req.Msg.GetPaused(),
		CatchupWindow:    scheduleCatchupWindowOf(trigger),
		PauseOnFailure:   trigger.GetPauseOnFailure(),
		ScheduleBackfill: scheduleBackfillsOf(req.Msg.GetBackfill()),

		// The tenant, recorded the same way and under the same key a run records
		// it, so [ownedBy] answers for a schedule without a second implementation
		// of what ownership means.
		Memo: scheduleMemo,

		Action: &client.ScheduleWorkflowAction{
			// Use the schedule object's tenant-scoped id as the readable base for
			// every firing. Temporal appends the scheduled time to keep firings
			// distinct; including the tenant keeps same-named schedules in a shared
			// Temporal namespace distinct too.
			ID:        scheduleIDFor(namespace, name),
			Workflow:  engine.Run,
			TaskQueue: taskQueue,

			WorkflowExecutionTimeout: s.executionTimeout,

			// Everything a submitted run carries, so a scheduled run is
			// indistinguishable from one somebody started — which is the point. It
			// is authorized by the same memo, listed by the same scan, and scheduled
			// under the same tenant's fairness key, so a schedule firing every
			// minute cannot crowd out another tenant's work. This is the memo
			// [authorizeRun]/[authorizeSignal] actually read for every fired
			// execution — see the comment above signalEntry.
			Memo:     actionMemo,
			Priority: fairnessFor(namespace),

			// Through [runStaticSummary], the exact function
			// [FlowstateServer.prepareCreate] uses — see that function's doc
			// for why one function is what keeps a scheduled run's fired
			// execution and a direct run's execution legible the same way in
			// the Temporal Web workflow list (#753).
			StaticSummary: runStaticSummary(namespace, workflow.GetName()),

			// Unlike prepareCreate's call site, a fired schedule execution has
			// a natural longer-form description on hand: which schedule fired
			// it. Schedule names share the workflow name's grammar
			// (`^[A-Za-z0-9-_]+$`, schedule.proto), so this is exactly as safe
			// to render as [runStaticSummary]'s backticks.
			StaticDetails: fmt.Sprintf("Fired by schedule `%s`.", name),

			// Through the exact function [FlowstateServer.Run] uses — see
			// [runSearchAttributes] — for the identical reason signalEntry
			// above is: a scheduled run's fired execution and a direct run's
			// execution must carry the same attributes, or a filter that
			// matches one silently misses the other. Guarded by the same
			// registration flag Run checks, and for the same reason: an
			// unregistered search attribute makes Temporal refuse the
			// schedule's create outright, so a deployment that never
			// confirmed registration must never attach one.
			TypedSearchAttributes: func() sdk.SearchAttributes {
				if s.searchAttributesRegistered {
					return runSearchAttributes(namespace, workflow.GetName())
				}
				return sdk.SearchAttributes{}
			}(),

			Args: []any{&v1.RunState{
				Workflow:    workflow,
				StepsBudget: int32(s.maxStepsPerRun),
				Identity:    identity,
				Inputs:      inputs,

				// How every firing of this schedule started, frozen here for the
				// reason Identity above is frozen here: there is no caller left to
				// derive anything from when a schedule fires at 03:00, so the answer
				// is captured once, while somebody is present, and carried into every
				// execution. That is also what makes `if: ${trigger.kind !=
				// "schedule"}` mean the same thing on the first firing and the
				// thousandth.
				Trigger: v1.NewScheduleTriggerContext(name, identity.GetSubject()),
			}},
		},
	})
	if err != nil {
		// Matched on the SDK's own sentinel rather than on a service error type: the
		// schedule client already classifies this one, translating Temporal's
		// `WorkflowExecutionAlreadyStarted` into `ErrScheduleAlreadyRunning`, so the
		// transport-level type never reaches here. Matching what does not arrive is
		// how a clear refusal turns back into a 500.
		if errors.Is(err, sdk.ErrScheduleAlreadyRunning) {
			// Named plainly, because within a tenant this is the caller's own
			// schedule and telling them about it is not disclosure — the id
			// derivation is what stops the same answer describing somebody else's.
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
				"a schedule called %q already exists; delete it, or create this one under another name with --name", name))
		}

		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("creating schedule %q: %w", name, err))
	}

	// Described rather than merely confirmed, so the answer carries the next firing
	// times. A cadence meaning something other than what was intended is almost
	// always visible in the first two of those and almost never visible in the
	// expression, which is exactly why a caller should not have to ask twice.
	description, err := s.describeSchedule(ctx, temporal, namespace, name)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&v1.CreateScheduleResponse{
		Schedule: description,

		// Always set, on both answers, for the reason [FlowstateServer.Run]
		// always sets it: the field's design rests on silence meaning "this
		// server does not say", so a server that does say must never let a
		// deliberate answer be read as an old server's shrug.
		SpecificationAsSubmitted: proto.Bool(asSubmitted),
	}), nil
}

// maxScheduleScan bounds how many schedules one listing may read.
//
// A bound is needed for the reason `List`'s is: the tenant is a memo, Temporal
// cannot filter on one, and so the number examined is not the number returned — in
// a namespace shared by several tenants, finding none of yours can mean reading all
// of theirs. What is different is the scale. Schedules are created one at a time by
// people, so this number is chosen to be past any real deployment rather than to be
// a page: reaching it means something has gone wrong, and the listing says so
// instead of presenting part of an answer as the whole of it.
const maxScheduleScan = 10_000

// ListSchedules returns the schedules belonging to the caller's tenant.
func (s *FlowstateServer) ListSchedules(ctx context.Context, req *connect.Request[v1.ListSchedulesRequest]) (*connect.Response[v1.ListSchedulesResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	namespace := s.identityFor(ctx).GetNamespace()

	// The caller's namespace decides which Temporal namespace is listed at all,
	// exactly as it decides which schedules are addressable. Where a deployment maps
	// namespaces, another tenant's schedules are not filtered out here — they were
	// never in the listing.
	temporal, err := s.clientFor(namespace)
	if err != nil {
		return nil, err
	}

	iterator, err := temporal.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("listing schedules: %w", err))
	}

	schedules := make([]*v1.ScheduleSummary, 0, 8)
	scanned := 0
	truncated := false

	// HasNext fetches at most one page per call and answers false on a page that
	// came back empty, so this loop's round trips are bounded by the entries it
	// reads — which is what the scan bound counts. That is the property CLAUDE.md's
	// rule about peer-controlled loops asks for, and it is the SDK's behavior rather
	// than an assumption: an empty page with a next-page token ends the iteration
	// here rather than continuing it.
	for iterator.HasNext() {
		if scanned >= maxScheduleScan {
			truncated = true
			break
		}
		scanned++

		entry, err := iterator.Next()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("listing schedules: %w", err))
		}

		name, mine := scheduleNameFrom(entry.ID, namespace)
		if !mine {
			continue
		}

		// Checked again against the recorded tenant, even though the id already
		// answered. See this file's header: the two protections fail differently,
		// and a listing is precisely where a wrong answer is cheapest to get and
		// most expensive to notice.
		if !s.ownedBy(namespace, entry.Memo) {
			continue
		}

		schedules = append(schedules, &v1.ScheduleSummary{
			Name:        name,
			Paused:      entry.Paused,
			Note:        entry.Note,
			NextRunTime: firstTime(entry.NextActionTimes),
		})
	}

	return connect.NewResponse(&v1.ListSchedulesResponse{
		Schedules: schedules,
		Truncated: truncated,
	}), nil
}

// DescribeSchedule reports one schedule in full.
func (s *FlowstateServer) DescribeSchedule(ctx context.Context, req *connect.Request[v1.DescribeScheduleRequest]) (*connect.Response[v1.DescribeScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	temporal, namespace, err := s.scheduleClientFor(ctx)
	if err != nil {
		return nil, err
	}

	description, err := s.describeSchedule(ctx, temporal, namespace, req.Msg.GetName())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&v1.DescribeScheduleResponse{Schedule: description}), nil
}

// DeleteSchedule removes a schedule.
func (s *FlowstateServer) DeleteSchedule(ctx context.Context, req *connect.Request[v1.DeleteScheduleRequest]) (*connect.Response[v1.DeleteScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	handle, err := s.authorizeSchedule(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}

	if err := handle.Delete(ctx); err != nil {
		return nil, actOnScheduleError("deleting", req.Msg.GetName(), err)
	}

	return connect.NewResponse(&v1.DeleteScheduleResponse{}), nil
}

// PauseSchedule stops a schedule firing without removing it.
func (s *FlowstateServer) PauseSchedule(ctx context.Context, req *connect.Request[v1.PauseScheduleRequest]) (*connect.Response[v1.PauseScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	handle, err := s.authorizeSchedule(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}

	if err := handle.Pause(ctx, client.SchedulePauseOptions{Note: req.Msg.GetNote()}); err != nil {
		return nil, actOnScheduleError("pausing", req.Msg.GetName(), err)
	}

	return connect.NewResponse(&v1.PauseScheduleResponse{}), nil
}

// ResumeSchedule lets a paused schedule fire again.
func (s *FlowstateServer) ResumeSchedule(ctx context.Context, req *connect.Request[v1.ResumeScheduleRequest]) (*connect.Response[v1.ResumeScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	handle, err := s.authorizeSchedule(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}

	if err := handle.Unpause(ctx, client.ScheduleUnpauseOptions{Note: req.Msg.GetNote()}); err != nil {
		return nil, actOnScheduleError("resuming", req.Msg.GetName(), err)
	}

	return connect.NewResponse(&v1.ResumeScheduleResponse{}), nil
}

// TriggerSchedule fires a schedule now.
func (s *FlowstateServer) TriggerSchedule(ctx context.Context, req *connect.Request[v1.TriggerScheduleRequest]) (*connect.Response[v1.TriggerScheduleResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	handle, err := s.authorizeSchedule(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}

	// The schedule's own overlap policy decides what happens if the last firing is
	// still going. Unspecified here means exactly that — it is not a default this
	// invents, and overriding a policy the file declared would make a manual trigger
	// behave unlike the schedule it is meant to be testing.
	if err := handle.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
		return nil, actOnScheduleError("triggering", req.Msg.GetName(), err)
	}

	return connect.NewResponse(&v1.TriggerScheduleResponse{}), nil
}

// scheduleClientFor returns the Temporal client the caller's schedules live on,
// with the tenant they belong to.
func (s *FlowstateServer) scheduleClientFor(ctx context.Context) (client.Client, string, error) {
	namespace := s.identityFor(ctx).GetNamespace()

	temporal, err := s.clientFor(namespace)
	if err != nil {
		return nil, "", err
	}

	return temporal, namespace, nil
}

// authorizeSchedule reports whether the caller may act on a schedule, and returns
// the handle to act through.
//
// The handle rather than only a yes, for the reason [authorizeRun] returns its
// client: a verb that checked one thing and acted on another would be a check that
// proved nothing, and handing back the checked handle makes doing it right the path
// of least effort.
//
// The refusal is "no such schedule" rather than "denied", the same answer a run in
// another tenant gets. Denied would confirm that a schedule of that name exists
// somewhere, which is the one fact a caller in the wrong tenant must not learn.
func (s *FlowstateServer) authorizeSchedule(ctx context.Context, name string) (client.ScheduleHandle, error) {
	temporal, namespace, err := s.scheduleClientFor(ctx)
	if err != nil {
		return nil, err
	}

	handle := temporal.ScheduleClient().GetHandle(ctx, scheduleIDFor(namespace, name))

	// GetHandle validates nothing, so the check is a Describe — which is also what
	// reads the memo. One round trip answers both "does it exist" and "is it mine".
	description, err := handle.Describe(ctx)
	if err != nil {
		return nil, noSuchSchedule(name)
	}

	if !s.ownedBy(namespace, description.Memo) {
		return nil, noSuchSchedule(name)
	}

	return handle, nil
}

// noSuchSchedule is the one answer every absent or unauthorized schedule gets.
func noSuchSchedule(name string) *connect.Error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("no such schedule %q", name))
}

// actOnScheduleError classifies a failure to act on a schedule already authorized.
//
// A schedule deleted between the authorization and the act is the ordinary race,
// and it is not a server fault: reported as NotFound in the same words an absent
// schedule gets, so a caller sees one answer for one situation rather than a 500
// for the half of it that happened to lose a race.
func actOnScheduleError(verb, name string, err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return noSuchSchedule(name)
	}

	return connect.NewError(connect.CodeInternal, fmt.Errorf("%s schedule %q: %w", verb, name, err))
}

// describeSchedule projects Temporal's description into the schema's own.
//
// Two sources, deliberately. What the schedule is *doing* — paused, when it next
// fires, what it has fired lately — is the cluster's answer and is read from the
// description. What it *is* — which workflow, which cadence, which arguments — is
// read back out of the specification the schedule stores, because that is where the
// author's own words survive: Temporal rewrites a cron expression into a calendar
// spec when it stores one, and answering `0 9 * * MON-FRI` with a list of ranges is
// accurate and unrecognisable.
func (s *FlowstateServer) describeSchedule(ctx context.Context, temporal client.Client, namespace, name string) (*v1.ScheduleDescription, error) {
	handle := temporal.ScheduleClient().GetHandle(ctx, scheduleIDFor(namespace, name))

	description, err := handle.Describe(ctx)
	if err != nil {
		return nil, noSuchSchedule(name)
	}

	if !s.ownedBy(namespace, description.Memo) {
		return nil, noSuchSchedule(name)
	}

	reported := &v1.ScheduleDescription{
		Name:                          name,
		NumActions:                    int64(description.Info.NumActions),
		NumActionsMissedCatchupWindow: int64(description.Info.NumActionsMissedCatchupWindow),
		NumActionsSkippedOverlap:      int64(description.Info.NumActionsSkippedOverlap),
	}

	// Temporal's own types here are plain structs rather than generated messages,
	// so there are no accessors that tolerate a nil — and a schedule described by a
	// cluster that answered without a state block is a real shape rather than a
	// hypothetical one.
	if state := description.Schedule.State; state != nil {
		reported.Paused = state.Paused
		reported.Note = state.Note
	}

	for _, at := range description.Info.NextActionTimes {
		reported.NextRunTimes = append(reported.NextRunTimes, timestamppb.New(at))
	}

	for _, action := range description.Info.RecentActions {
		result := &v1.ScheduleActionResult{
			ScheduleTime: timestamppb.New(action.ScheduleTime),
			ActualTime:   timestamppb.New(action.ActualTime),
		}
		if started := action.StartWorkflowResult; started != nil {
			result.WorkflowId = started.WorkflowID
			result.RunId = started.FirstExecutionRunID
		}
		reported.RecentRuns = append(reported.RecentRuns, result)
	}

	// Best effort, and the failure is silent on purpose: a schedule whose stored
	// arguments cannot be decoded — created by a build whose message shape has since
	// moved — is still describable in every other respect, and failing the whole
	// description over one field would take the answer away along with the doubt.
	// What is absent reads as absent, which is invariant 10's own rule.
	//
	// A schedule holds its bound inputs *persistently*, for as long as the schedule
	// exists — unlike a run, which answers a `Get` with its declared outputs but
	// never its inputs (see [v1.GetResponse]). That makes this the one call site in
	// the server that renders a bound input value at all, and #211 found it doing so
	// unconditionally: an input a Flowfile declared `sensitive: true` came back in
	// the clear. state.GetWorkflow() carries the same [v1.InputDeclaration] the
	// author wrote, sensitive flag included, so redactInputs applies it before the
	// value ever reaches [reported] — see that function's own comment for the
	// fail-closed case this branch's `state != nil` guard already produces for "no
	// spec at all."
	if state := s.storedRunState(description.Schedule.Action); state != nil {
		reported.WorkflowName = state.GetWorkflow().GetName()
		reported.Inputs = redactInputs(state.GetInputs(), sensitiveInputNames(state.GetWorkflow()))
		reported.Trigger = state.GetWorkflow().GetTriggers().GetSchedule()
	}

	return reported, nil
}

// redactedInputMarkerFormat and redactedInputValue mirror
// cmd/flow/sensitive.go's redactedMarkerFormat / redactedValue exactly —
// `[redacted: <name>]`, the shape [v1.InputDeclaration.Sensitive]'s own doc
// comment promises — rather than importing them.
//
// They cannot be imported: this package is a dependency of `cmd/flow`, not the
// other way around, and #211's correction is that the redaction belongs here,
// server-side, before the value crosses the wire at all — a client-side fix
// would leave the value in the response body, in proxy logs, and in every other
// consumer of the RPC (see the DescribeSchedule/ListSchedules doc comments this
// change updates). Lifting the marker into a shared package both sides import
// would remove this duplication, but doing that is a cross-package move outside
// this change's scope (see this branch's own working agreement: touch
// schedules.go, its tests, and cmd/flow/schedule.go only) and would need
// coordinating with whoever owns cmd/flow/sensitive.go next, since moving a
// well-commented constant out from under an in-flight package invites exactly
// the stale-snapshot confusion CLAUDE.md's "working alongside other agents"
// section warns about. One duplicated format string, kept byte-for-byte
// identical on purpose, is the smaller risk.
const redactedInputMarkerFormat = "[redacted: %s]"

func redactedInputValue(name string) *v1.Value {
	return &v1.Value{
		Kind: &v1.Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_StringValue{StringValue: fmt.Sprintf(redactedInputMarkerFormat, name)},
			},
		},
	}
}

// sensitiveInputNames is the set of declared input names a workflow
// specification marked `sensitive: true`, or nil when no specification is
// available to consult — the same nil-vs-empty-set distinction
// cmd/flow/sensitive.go's sensitiveOutputNames documents and for the same
// reason: nil is the fail-closed case that withholds everything, and an empty,
// non-nil set is a real specification that declared nothing sensitive, which
// withholds nothing.
func sensitiveInputNames(workflow *v1.Workflow) map[string]bool {
	if workflow == nil {
		return nil
	}

	names := make(map[string]bool)
	for _, declared := range workflow.GetDeclaredInputs() {
		if declared.GetSensitive() {
			names[declared.GetName()] = true
		}
	}

	return names
}

// redactInputs returns a schedule's bound inputs with every value this call
// site cannot vouch for replaced by [redactedInputValue].
//
// sensitive nil is the fail-closed case CLAUDE.md's "fail closed" section
// requires: every value is withheld rather than guessed at, because nothing
// here can determine which ones the workflow actually marked. In practice
// [describeSchedule] never reaches this function with sensitive == nil — its
// caller only calls it inside the `state != nil` branch, and state is exactly
// what sensitiveInputNames needs a non-nil answer from — but redactInputs stays
// fail-closed on its own rather than depend on that being true forever, the
// same discipline sensitiveOutputNames documents for the same shape.
//
// A non-nil sensitive redacts precisely the names it names and nothing else,
// which is also how an older run's or an untagged declaration's inputs pass
// through unchanged: a name the specification never declared sensitive is not
// this function's business to guess about.
//
// There is deliberately no `reveal` parameter here the way
// cmd/flow/sensitive.go's redactRunOutputsValues has one:
// [v1.DescribeScheduleRequest] and [v1.ListSchedulesRequest] have no field to
// carry an operator's request to see the value, and adding one is a proto
// change — out of scope for this branch. An operator who needs the value today
// has to read it from Temporal directly (e.g. `temporal schedule describe`,
// which is not this server and not scrubbed by it); wiring a `--reveal-sensitive`
// escape hatch through this RPC is a real, wanted follow-up, just not one this
// change can do without touching proto/.
func redactInputs(inputs map[string]*v1.Value, sensitive map[string]bool) map[string]*v1.Value {
	if len(inputs) == 0 {
		return inputs
	}

	failClosed := sensitive == nil

	redacted := make(map[string]*v1.Value, len(inputs))
	for name, value := range inputs {
		if failClosed || sensitive[name] {
			redacted[name] = redactedInputValue(name)
			continue
		}

		redacted[name] = value
	}

	return redacted
}

// storedRunState reads back the run state a schedule starts each firing with.
//
// Describe returns the action's arguments as payloads rather than as the values
// that went in — the SDK says so — so this decodes the one argument
// `engine.Run` takes, through the same data converter that encoded it, which is
// this server's own rather than the SDK default. See [WithDataConverter].
// Nil for anything unexpected, which every caller treats as "not known" rather than
// as an error.
func (s *FlowstateServer) storedRunState(action client.ScheduleAction) *v1.RunState {
	workflowAction, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || len(workflowAction.Args) != 1 {
		return nil
	}

	payload, ok := workflowAction.Args[0].(*common.Payload)
	if !ok {
		return nil
	}

	var state v1.RunState
	if err := s.dataConverter.FromPayload(payload, &state); err != nil {
		return nil
	}

	return &state
}

// firstTime returns the first of a list of moments as a timestamp, or nil.
//
// Nil rather than the zero instant, the rule [runTimes] follows: a schedule with no
// next firing has not been scheduled for 1970.
func firstTime(times []time.Time) *timestamppb.Timestamp {
	if len(times) == 0 {
		return nil
	}

	return timestamppb.New(times[0])
}

// scheduleSpecOf projects a declared cadence onto Temporal's own spec.
//
// A projection and nothing more: no next firing time is computed here, no cron
// expression is expanded here, and nothing here decides what an overlap means.
// Temporal owns all of that, which is the whole reason this surface is thin.
func scheduleSpecOf(trigger *v1.ScheduleTrigger) (client.ScheduleSpec, error) {
	spec := client.ScheduleSpec{
		CronExpressions: trigger.GetCron(),
		TimeZoneName:    trigger.GetTimeZone(),
		Jitter:          trigger.GetJitter().AsDuration(),
	}

	// Assigned only when the bound was written, and the reason is a trap the
	// generated accessors set: `(*timestamppb.Timestamp)(nil).AsTime()` is not the
	// zero `time.Time` that means "unset" to the SDK. It is 1970-01-01, a real
	// instant. Copied in unconditionally, a schedule with no `end_at`, which is
	// every schedule anybody has created so far, reaches Temporal declaring that
	// it ended before it was written, and is created with no future firing at all.
	// The one thing worse than a schedule that does not fire is a schedule that
	// does not fire and reports success, which is exactly what that was. The two
	// server tests that describe a live schedule's next firing times caught it.
	if start := trigger.GetStartAt(); start != nil {
		spec.StartAt = start.AsTime()
	}
	if end := trigger.GetEndAt(); end != nil {
		spec.EndAt = end.AsTime()
	}

	for _, calendar := range trigger.GetCalendars() {
		spec.Calendars = append(spec.Calendars, client.ScheduleCalendarSpec{
			Second: scheduleRangesOf(calendar.GetSecond()), Minute: scheduleRangesOf(calendar.GetMinute()),
			Hour: scheduleRangesOf(calendar.GetHour()), DayOfMonth: scheduleRangesOf(calendar.GetDayOfMonth()),
			Month: scheduleRangesOf(calendar.GetMonth()), Year: scheduleRangesOf(calendar.GetYear()),
			DayOfWeek: scheduleRangesOf(calendar.GetDayOfWeek()), Comment: calendar.GetComment(),
		})
	}

	if every := trigger.GetEvery(); every != nil {
		spec.Intervals = []client.ScheduleIntervalSpec{{Every: every.AsDuration()}}
	}

	// Checked again here rather than trusted from the caller's own validation, for
	// the reason `Run` re-validates a specification the CLI already checked: a bound
	// enforced by whoever happened to call is not a bound.
	if len(spec.CronExpressions) == 0 && len(spec.Intervals) == 0 && len(spec.Calendars) == 0 {
		return client.ScheduleSpec{}, errors.New(
			"the schedule says nothing about when to fire; write `cron:`, `every:` or `calendars:` " +
				"under `triggers.schedule`")
	}

	return spec, nil
}

// scheduleCatchupWindowOf is how late a missed firing may still be taken.
//
// The default is applied here rather than left to the cluster, because Temporal's
// own default for an unset window is one year: a schedule that says nothing about
// catch-up would take a year of missed firings the moment a long outage ended,
// which is the unbounded burst these controls exist to prevent. An absent field is
// not an operator asking for a year, it is an operator not having thought about it,
// and the fail-closed answer to that is the bounded default rather than the
// largest number in the system. Saying `catchup_window:` explicitly still gets
// anything up to [v1.MaxScheduleCatchupWindow].
func scheduleCatchupWindowOf(trigger *v1.ScheduleTrigger) time.Duration {
	if window := trigger.GetCatchupWindow(); window != nil {
		return window.AsDuration()
	}

	return v1.DefaultScheduleCatchupWindow
}

// scheduleRangesOf projects a calendar field's ranges onto Temporal's.
//
// Nil for a field nobody wrote rather than an empty slice, because the two mean
// different things to Temporal: an absent `Hour` takes the field's default, and
// the distinction is what keeps a calendar that names only a day of the month
// from being read as one that names an hour too.
func scheduleRangesOf(in []*v1.ScheduleTrigger_Calendar_Range) []client.ScheduleRange {
	if len(in) == 0 {
		return nil
	}

	out := make([]client.ScheduleRange, 0, len(in))
	for _, r := range in {
		out = append(out, client.ScheduleRange{Start: int(r.GetStart()), End: int(r.GetEnd()), Step: int(r.GetStep())})
	}
	return out
}

// scheduleBackfillsOf projects the create-time replay request onto Temporal's.
//
// Every range here has been through [v1.CheckScheduleBackfill] already, both at
// the CLI and again in [FlowstateServer.CreateSchedule], so this is a projection
// and holds no bound of its own.
func scheduleBackfillsOf(in []*v1.ScheduleBackfill) []client.ScheduleBackfill {
	if len(in) == 0 {
		return nil
	}

	out := make([]client.ScheduleBackfill, 0, len(in))
	for _, b := range in {
		out = append(out, client.ScheduleBackfill{Start: b.GetStartAt().AsTime(), End: b.GetEndAt().AsTime(), Overlap: overlapOf(b.GetOverlap())})
	}
	return out
}

// overlapOf maps the declared overlap policy onto Temporal's.
//
// A switch rather than a cast, the rule [inputTypeOf] follows: the two enums share
// names and numbers today and are owned by different projects, so a conversion
// between them is something somebody wrote and somebody reviewed.
func overlapOf(overlap v1.ScheduleTrigger_Overlap) enums.ScheduleOverlapPolicy {
	switch overlap {
	case v1.ScheduleTrigger_OVERLAP_SKIP:
		return enums.SCHEDULE_OVERLAP_POLICY_SKIP
	case v1.ScheduleTrigger_OVERLAP_BUFFER_ONE:
		return enums.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
	case v1.ScheduleTrigger_OVERLAP_BUFFER_ALL:
		return enums.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	case v1.ScheduleTrigger_OVERLAP_CANCEL_OTHER:
		return enums.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
	case v1.ScheduleTrigger_OVERLAP_TERMINATE_OTHER:
		return enums.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	case v1.ScheduleTrigger_OVERLAP_ALLOW_ALL:
		return enums.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	default:
		// Unspecified, which Temporal reads as its own default. Left to it rather
		// than resolved here, so "the author said nothing" keeps meaning that.
		return enums.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
	}
}
