package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// taskDef builds the engine's definition of a task from a plugin's manifest for
// it.
//
// The result is a [flowstatev1.TaskDef] like any other. That is the registry
// invariant applied across a process boundary: the engine consults declared
// fields rather than testing a task's name, so a task that arrived from a plugin
// takes the same paths through resolution, validation, and dispatch as one
// compiled in.
func (p *Plugin) taskDef(manifest *pluginv1.TaskManifest, cfg Config) (flowstatev1.TaskDef, error) {
	name := manifest.GetName()

	// The def is registered under the name an author writes, which is the
	// manifest's name qualified by the plugin's: `slack.post` for the `post`
	// task of `flowstate-plugin-slack`. The wire keeps the bare name — the
	// plugin knows its task as `post`, and telling it otherwise would make every
	// plugin parse its own name back out of a prefix the host added.
	qualified := p.name + "." + name

	inputs, err := messageDescriptor(manifest.GetInputDescriptor(), manifest.GetInputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, pluginError(p.name, p.path, fmt.Errorf("task %q inputs: %w", truncate(name, 64), err))
	}

	outputs, err := messageDescriptor(manifest.GetOutputDescriptor(), manifest.GetOutputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, pluginError(p.name, p.path, fmt.Errorf("task %q outputs: %w", truncate(name, 64), err))
	}

	return flowstatev1.TaskDef{
		Name:           qualified,
		Summary:        manifest.GetSummary(),
		Inputs:         inputs,
		Outputs:        outputs,
		DeferredInputs: manifest.GetDeferredInputs(),

		// What an input has to *be*, as distinct from who evaluates it. Without
		// this a plugin could mark an input deferred and had no way to require it
		// be written as an expression — so a literal compiled, validated, and
		// failed inside the plugin, which is the failure the engine's own
		// ExpressionInputs was added to move back to the author's terminal.
		ExpressionInputs: manifest.GetExpressionInputs(),

		// The scope is what a task's own expressions evaluate against, so a task
		// that declared it needs one needs prior step outputs to build it.
		NeedsPrevOutputs: manifest.GetNeedsScope(),

		// Whether this task replaces its declared outputs with an author's
		// shaping — declared, never inferred from the fact that an input happens
		// to be called `outputs`. A plugin that declares an ordinary input by
		// that name and leaves this false keeps ordinary diagnostics against the
		// outputs its descriptor promises, which is what its executor actually
		// returns.
		ShapesOutputs: manifest.GetShapesOutputs(),

		// The whole-value list the manifest declared, carried onto the def so a
		// description of this task (DescribeTask, the catalog, `flow plugins`)
		// can say so. Enforcement itself still reads the manifest directly,
		// closed over below in taskFunc — this copy is for visibility, not for
		// the resolve-or-refuse decision.
		SecretInputs: manifest.GetSecretInputs(),

		// Nothing here declares [flowstatev1.TaskDef.AuthorityInputs] or
		// .CredentialInputs for a plugin task's secret inputs — see the
		// cross-reference on AuthorityInputs itself — and that is deliberate
		// rather than an omission: [flowstatev1.TaskNeedsAuthority] already
		// scans every input of a concrete task invocation for a held
		// [flowstatev1.SecretRef], wherever in it the reference sits, which is
		// what routes a step using one to the identity-aware activity
		// regardless of which task it names. A plugin task with a secret input
		// is already covered by the same scan a built-in task's `bearer:` is.
		Fn: p.taskFunc(manifest),
	}, nil
}

// taskFunc returns the function that executes a task by asking the plugin to.
func (p *Plugin) taskFunc(manifest *pluginv1.TaskManifest) flowstatev1.TaskFunc {
	// Two names for one task, each used where it is true. The wire carries the
	// bare manifest name, because that is what the plugin calls it; every error
	// carries the qualified one, because that is what the author wrote and what
	// they will search their file for.
	name := manifest.GetName()
	qualified := p.name + "." + name
	needsScope := manifest.GetNeedsScope()
	secretInputs := manifest.GetSecretInputs()

	return func(ctx context.Context, inputs map[string]*flowstatev1.Value, scope *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		ctx = telemetryBaggage(ctx, p.name, qualified)
		ctx, _, finish := p.telemetry.start(ctx, "execute", p.name, qualified)
		var callErr error
		defer func() { finish(callErr) }()
		inst, err := p.ready()
		if err != nil {
			callErr = err
			// Unavailability is the one retryable classification, so a step's
			// retry policy gets to decide whether to wait out a restart.
			return nil, flowstatev1.NewTaskError(qualified, flowstatev1.ErrorKindUpstream, err)
		}

		// The one place a secret reference in a plugin task's inputs becomes a
		// value, and the scrubber that everything this call can leak through —
		// the response, and the error — is registered against from here on.
		// See [resolvePluginSecretInputs] for why this is where it has to
		// happen, and for the boundary this local transport depends on.
		resolvedInputs, scrubber, err := resolvePluginSecretInputs(ctx, qualified, secretInputs, inputs)
		if err != nil {
			callErr = err
			return nil, err
		}

		identity, _ := IdentityFromContext(ctx)

		request := &pluginv1.ExecuteRequest{
			Task: &flowstatev1.Task{
				Name:   name,
				Inputs: resolvedInputs,
			},
			Identity:  identity,
			Namespace: identity.GetNamespace(),
		}

		// The scope travels only when the manifest said the task evaluates its
		// own expressions. Sending it otherwise would put every prior step's
		// outputs on the wire for a task that has nothing to do with them.
		if needsScope {
			request.Scope = scope
		}

		callCtx, cancel := p.callContext(ctx)
		defer cancel()

		// The other place a step spends real time, and the one whose duration is
		// decided by code this repository did not write.
		flowstatev1.ReportProgress(ctx, flowstatev1.PhaseCallingPlugin)

		resp, err := p.executeTask(ctx, callCtx, inst, request)
		if err != nil {
			// Classified before it is scrubbed — see [taskError] for why the
			// order is load-bearing — and scrubbed before it is wrapped, so
			// that a resolved secret a peer reflected back through an RPC
			// failure — the same hazard the http task's own scrubber exists
			// for — cannot reach the task error this becomes, which is
			// surfaced to users and written to workflow history. The scrubbed
			// error is also what the telemetry span records: an exported trace
			// is exactly as durable and as readable as history, so the raw RPC
			// error may not take the side door a span would give it.
			scrubbed := taskError(qualified, p.name, err, scrubber)
			callErr = scrubbed
			return nil, scrubbed
		}

		outputs := resp.GetOutputs()
		if err := scrubPluginOutputs(scrubber, outputs); err != nil {
			callErr = err
			return nil, flowstatev1.NewTaskError(qualified, flowstatev1.ErrorKindInvalidInput, err)
		}

		return outputs, nil
	}
}

// executeTask asks the plugin to run one task, relaying any progress it
// reports along the way to ctx's own reporter (see [flowstatev1.ReportProgress]),
// and returns the terminal ExecuteResponse.
//
// Whether to attempt ExecuteStream at all is decided once, from the manifest
// this plugin already described itself with — CAPABILITY_TASK_PROGRESS — never
// by dialing the RPC and reading whatever error comes back.
//
// That is deliberate rather than an optimization avoiding a wasted round
// trip. An unregistered route and a task's own application-level failure
// that happens to classify as CodeUnimplemented are indistinguishable on the
// wire: [sdk.asConnectError] passes an author's own *connect.Error straight
// through, so a task can legitimately fail with that exact code after doing
// real work — the same way [sdk.NotFound] fails with CodeNotFound. A host
// that treated any CodeUnimplemented from ExecuteStream as "this plugin
// predates the method" and retried on Execute, unary, would rerun such a
// task's Fn a second time on the strength of an error the task deliberately
// chose to return — corruption for a plugin whose task has side effects, and
// exactly the shape of bug CLAUDE.md's rewriter section warns rewriting the
// wrong scope always turns into. Reading the manifest is unambiguous: it is
// what the plugin said before this or any other call happened, not something
// inferred from how one call's failure happens to be coded.
func (p *Plugin) executeTask(
	ctx, callCtx context.Context,
	inst *instance,
	request *pluginv1.ExecuteRequest,
) (*pluginv1.ExecuteResponse, error) {
	if !p.HasCapability(pluginv1.Capability_CAPABILITY_TASK_PROGRESS) {
		resp, err := inst.clients.task.Execute(callCtx, connect.NewRequest(request))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	streamReq := &pluginv1.ExecuteStreamRequest{
		Task:      request.GetTask(),
		Scope:     request.GetScope(),
		Identity:  request.GetIdentity(),
		Namespace: request.GetNamespace(),
	}

	stream, err := inst.clients.taskStream.ExecuteStream(callCtx, connect.NewRequest(streamReq))
	if err != nil {
		return nil, err
	}

	// progressFrames counts every progress message this call has received,
	// relayed or not. Once it passes p.cfg.MaxProgressFrames, further
	// progress frames in this same call are received (so the stream keeps
	// moving toward its terminal response) but not forwarded — see
	// DefaultMaxProgressFrames for why dropping, not failing the call, is the
	// answer to a task that is behaving exactly as documented, and
	// [newClients]'s streaming transport for the separate byte headroom that
	// makes this bound something other than cosmetic: without it, a task
	// under the cap could still exhaust the shared response budget one
	// legitimate tiny frame at a time before its own terminal response
	// arrived (#804).
	maxProgressFrames := p.cfg.MaxProgressFrames
	var progressFrames int

	for stream.Receive() {
		msg := stream.Msg()

		if progress := msg.GetProgress(); progress != nil {
			// The reserve's arithmetic (progressReserve, transport.go) is
			// per-frame: it holds only while no single frame exceeds
			// maxProgressFrameWireBytes. That has to be enforced here, not
			// assumed — a frame carrying protobuf unknown fields (a schema
			// this build has never seen, or a hostile peer padding one) can
			// be arbitrarily large up to the transport ceiling, and enough of
			// them would spend the terminal response's own share, recreating
			// the starvation the reserve exists to prevent. An oversized
			// frame is a protocol violation, refused rather than dropped:
			// dropping would leave its bytes already spent against the
			// ceiling while this loop reported nothing wrong.
			if err := checkProgressFrameSize(msg); err != nil {
				return nil, fmt.Errorf("plugin %q: task %q: %w",
					p.name, request.GetTask().GetName(), err)
			}
			progressFrames++
			if progressFrames > maxProgressFrames {
				continue
			}
			reportWirePhase(ctx, progress.GetPhase())
			continue
		}

		if resp := msg.GetResponse(); resp != nil {
			// The terminal message has arrived; nothing about the call's
			// outcome depends on how the stream itself ends from here, so a
			// close error is a diagnostic and never this call's error.
			if closeErr := stream.Close(); closeErr != nil {
				p.log.Debug("closing plugin progress stream after its terminal response",
					"task", request.GetTask().GetName(), "error", closeErr)
			}
			return resp, nil
		}
	}

	if err := stream.Err(); err != nil {
		return nil, err
	}

	// The stream ended without a terminal response and without an error,
	// which ExecuteStream's contract does not allow a well-behaved plugin to
	// produce — see the message's own doc comment in plugin.proto.
	return nil, fmt.Errorf("plugin %q: task %q: ExecuteStream ended without a response",
		p.name, request.GetTask().GetName())
}

// reportWirePhase forwards a plugin's TaskPhase to ctx's own reporter, if it
// names one of the closed vocabulary [flowstatev1.ReportProgress] accepts.
// TASK_PHASE_UNSPECIFIED and any value newer than this build knows about are
// dropped rather than forwarded — the fail-closed direction for a value on
// its way into an activity heartbeat and, from there, workflow history. See
// progress.go's own doc comment for why that vocabulary is closed, and
// plugin.proto's TaskPhase for why the two must keep naming the same set.
//
// Each branch below calls ReportProgress with a declared phase named
// directly, rather than through a variable computed from the switch: that is
// not a style choice, it is what
// [flowstatev1.TestEveryPhaseReportedIsOneOfTheDeclaredOnes] in progress_test.go
// requires of every call site in the tree, on purpose — a variable holding a
// phase reads identically at that AST walk whether it can only ever hold one
// of the three constants below, as this one can, or whether it holds
// something built from a task's own inputs, which is exactly the mistake the
// walk exists to catch. Spelling each branch as a literal call is what keeps
// this call site indistinguishable, to that check, from every other one that
// was always written by hand.
func reportWirePhase(ctx context.Context, phase pluginv1.TaskPhase) {
	switch phase {
	case pluginv1.TaskPhase_TASK_PHASE_REQUESTING:
		flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)
	case pluginv1.TaskPhase_TASK_PHASE_READING_RESPONSE:
		flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)
	case pluginv1.TaskPhase_TASK_PHASE_CALLING_PLUGIN:
		flowstatev1.ReportProgress(ctx, flowstatev1.PhaseCallingPlugin)
	}
}

// resolvePluginSecretInputs resolves a host secret reference in a plugin
// task's inputs into the value it names, and refuses one anywhere the
// manifest did not declare.
//
// This is the host-side half of the design described on
// TaskManifest.secret_inputs: the plugin process must never receive a
// [flowstatev1.SecretRef], only the value it resolves to, exactly as a
// built-in task's own secret input is resolved inside the activity that needs
// it rather than in workflow code. Both directions are refused rather than
// guessed at:
//
//   - a reference in an input this task did declare is resolved, under the
//     caller's authenticated identity and namespace, through whatever the host
//     is holding TaskRuntime for;
//   - a reference in an input this task did not declare is refused, since an
//     undeclared field is not one a Flowfile author routed a secret to on
//     purpose, and resolving it anyway would let a plugin fish for whatever a
//     workflow happened to reference nearby;
//   - a reference nested inside a list or a mapping is refused unconditionally
//     — no plugin task input accepts one there today (see
//     [flowstatev1.TaskDef.NestedSecretInputs], which [Plugin.taskDef] never
//     sets) — as defense in depth for a specification built by hand rather
//     than compiled from a Flowfile, where the compiler already refuses this
//     shape at `flow validate` time.
//
// # This resolve-then-send path is scoped to a local transport
//
// The value crosses into the plugin process over a Unix socket only this
// worker's user can open, which is the one policy enforcement point between
// the secret store and the value's only other reader. That is what makes
// resolving here and handing the plugin a value, rather than a reference, safe
// today. A remote plugin endpoint (tracked as issue #151) is a different
// machine: sending a resolved value there is credential disclosure to a third
// party's process, not merely crossing a process boundary on the same host.
// Extending the transport to a remote endpoint must NOT route through this
// function without a per-endpoint secret-release policy — deny by default —
// deciding first whether that endpoint may receive the value at all; no such
// policy exists yet, so a remote transport must refuse every secret_inputs
// reference outright rather than resolve one here.
func resolvePluginSecretInputs(
	ctx context.Context,
	taskName string,
	declared []string,
	inputs map[string]*flowstatev1.Value,
) (map[string]*flowstatev1.Value, *secrets.Scrubber, error) {
	scrubber := secrets.NewScrubber()

	if len(inputs) == 0 {
		return inputs, scrubber, nil
	}

	resolved := make(map[string]*flowstatev1.Value, len(inputs))
	for name, v := range inputs {
		ref, isWholeRef := v.GetKind().(*flowstatev1.Value_SecretRef)

		switch {
		case isWholeRef && slices.Contains(declared, name):
			secret, err := flowstatev1.ResolveSecret(ctx, ref.SecretRef)
			if err != nil {
				kind := flowstatev1.ErrorKindPolicyDenied
				if secrets.Retryable(err) {
					kind = flowstatev1.ErrorKindUpstream
				}
				return nil, nil, flowstatev1.NewTaskError(taskName, kind, fmt.Errorf(
					"resolving input %q (%s): %w", name, secretRefText(ref.SecretRef), err))
			}
			scrubber.Add(secret)
			resolved[name] = &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_StringValue{StringValue: secret.Reveal()},
			}}}

		case isWholeRef:
			return nil, nil, flowstatev1.NewTaskError(taskName, flowstatev1.ErrorKindInvalidInput, fmt.Errorf(
				"input %q is a secret reference, which this task did not declare as accepting one%s",
				name, acceptedPluginSecretInputsHelp(declared)))

		case flowstatev1.ValueHoldsSecretRef(v):
			return nil, nil, flowstatev1.NewTaskError(taskName, flowstatev1.ErrorKindInvalidInput, fmt.Errorf(
				"input %q holds a secret reference nested inside a list or a mapping, "+
					"which no plugin task input accepts", name))

		default:
			resolved[name] = v
		}
	}

	return resolved, scrubber, nil
}

// acceptedPluginSecretInputsHelp names the inputs a task did declare, for a
// refusal that points somewhere rather than only saying no.
func acceptedPluginSecretInputsHelp(declared []string) string {
	if len(declared) == 0 {
		return "; this task declares no inputs that accept one"
	}

	return fmt.Sprintf("; this task accepts one in %s", strings.Join(slices.Sorted(slices.Values(declared)), ", "))
}

// secretRefText renders a reference the way an author wrote it, for an error
// naming what failed to resolve. It names the reference, never the value.
func secretRefText(ref *flowstatev1.SecretRef) string {
	return ref.GetScheme() + ":" + ref.GetName()
}

// scrubPluginOutputs redacts every registered secret from a plugin's response
// outputs in place, and refuses one that holds a bare secret reference.
//
// The response crossed back over the same transport a resolved value went out
// on, so anything the plugin echoed — deliberately or by reflecting an input
// back, the same hazard the http task's own response scrubbing exists for —
// is caught here, before it can become a step output or workflow history. A
// reference is refused outright rather than scrubbed: [secrets.Scrubber] only
// redacts known plaintext, and a bare [flowstatev1.SecretRef] a plugin
// constructed itself (rather than being given by the host) names a secret this
// activity never resolved, so there is no value to redact — only a token this
// activity must not forward into a step output.
func scrubPluginOutputs(scrubber *secrets.Scrubber, outputs *flowstatev1.Node_Outputs) error {
	for name, v := range outputs.GetNamedValues() {
		if flowstatev1.ValueHoldsSecretRef(v) {
			return fmt.Errorf(
				"output %q holds a secret reference, which a task output must never be: "+
					"step outputs are written to workflow history", name)
		}
		if v == nil {
			continue
		}
		if err := scrubMessage(scrubber, v.ProtoReflect()); err != nil {
			return err
		}
	}

	return nil
}

// scrubMessage redacts every string and byte field in a protobuf message,
// including map keys and values and fields nested in lists or messages. Plugin
// outputs use flowstate.v1.Value, whose non-literal variants can also carry
// strings: notably Error.message, Structure entries, and ParsedExpr nodes. A
// walk limited to Value.literal therefore leaves valid response shapes able to
// carry a resolved secret into workflow history.
func scrubMessage(scrubber *secrets.Scrubber, msg protoreflect.Message) error {
	// Collected before anything is written back, rather than mutated inside
	// Range: protoreflect leaves the effect of mutating a message mid-range
	// unspecified, and a scrub that depends on that is a scrub that can be
	// changed by a dependency bump.
	type populated struct {
		field protoreflect.FieldDescriptor
		value protoreflect.Value
	}
	fields := make([]populated, 0, 8)
	msg.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		fields = append(fields, populated{field, value})

		return true
	})

	for _, f := range fields {
		switch {
		case f.field.IsMap():
			if err := scrubMap(scrubber, f.value.Map(), f.field); err != nil {
				return err
			}
		case f.field.IsList():
			list := f.value.List()
			for i := 0; i < list.Len(); i++ {
				scrubbed, err := scrubFieldValue(scrubber, f.field, list.Get(i))
				if err != nil {
					return err
				}
				list.Set(i, scrubbed)
			}
		default:
			scrubbed, err := scrubFieldValue(scrubber, f.field, f.value)
			if err != nil {
				return err
			}
			// A nested message was scrubbed in place and is the same value;
			// setting it back would be a mutation with nothing to change.
			if kind := f.field.Kind(); kind != protoreflect.MessageKind && kind != protoreflect.GroupKind {
				msg.Set(f.field, scrubbed)
			}
		}
	}

	return nil
}

// scrubMap scrubs a map's keys and values, and refuses a map whose keys collide
// once scrubbed.
//
// Two distinct keys can redact to the same text — the resolved secret itself
// and a literal "[REDACTED]" is the easy case — and writing both back would
// silently drop one. Which one survives depends on protobuf map iteration
// order, which is deliberately unspecified, so the same plugin response could
// produce one output locally and a different one durably: the drivers
// disagreeing about a value neither of them is wrong to compute.
//
// Refusing is the fail-closed answer and costs a plugin nothing it should have
// been doing: a map key is a name, and a resolved secret is not one.
func scrubMap(scrubber *secrets.Scrubber, m protoreflect.Map, field protoreflect.FieldDescriptor) error {
	keyField, valueField := field.MapKey(), field.MapValue()

	type entry struct {
		key   protoreflect.MapKey
		value protoreflect.Value
	}
	entries := make([]entry, 0, m.Len())
	originalKeys := make([]protoreflect.MapKey, 0, m.Len())

	var rangeErr error
	m.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		originalKeys = append(originalKeys, key)

		scrubbedKey, err := scrubFieldValue(scrubber, keyField, key.Value())
		if err != nil {
			rangeErr = err

			return false
		}
		scrubbedValue, err := scrubFieldValue(scrubber, valueField, value)
		if err != nil {
			rangeErr = err

			return false
		}
		entries = append(entries, entry{scrubbedKey.MapKey(), scrubbedValue})

		return true
	})
	if rangeErr != nil {
		return rangeErr
	}

	seen := make(map[any]struct{}, len(entries))
	for _, item := range entries {
		if _, collides := seen[item.key.Interface()]; collides {
			return fmt.Errorf(
				"two keys of output field %q redact to the same key %v, so one would silently "+
					"replace the other: a map key is a name, and a resolved secret must not be used as one",
				field.Name(), item.key.Interface())
		}
		seen[item.key.Interface()] = struct{}{}
	}

	// Clear the original keys first: a scrubbed key is not necessarily equal to
	// the key that arrived from the plugin.
	for _, key := range originalKeys {
		m.Clear(key)
	}
	for _, item := range entries {
		m.Set(item.key, item.value)
	}

	return nil
}

func scrubFieldValue(
	scrubber *secrets.Scrubber,
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(scrubber.Scrub(value.String())), nil
	case protoreflect.BytesKind:
		original := string(value.Bytes())
		if scrubbed := scrubber.Scrub(original); scrubbed != original {
			return protoreflect.ValueOfBytes([]byte(scrubbed)), nil
		}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if err := scrubMessage(scrubber, value.Message()); err != nil {
			return value, err
		}
	}

	return value, nil
}

// taskError classifies a plugin's task failure into the engine's own error
// kinds, which is what decides whether the step is attempted again, and
// scrubs it before it becomes something surfaced to users and written to
// workflow history.
//
// A plugin can say so explicitly, by failing the RPC with an ExecuteResponse
// attached as an error detail: only the plugin knows whether its backend's
// failure was transient, and the schema puts that answer on the response's
// retryable and unknown_outcome fields. Without one, the Connect code is
// mapped to the closest kind.
//
// # Classify before scrubbing, never after
//
// [secrets.Scrubber.ScrubError] deliberately returns a value with no Unwrap
// and no errors.As — that is what stops Temporal's failure converter from
// walking back to the unredacted original (see the note on ScrubError itself).
// The cost of that guarantee is that nothing downstream can reach the
// *connect.Error underneath a scrubbed one either: [kindForCode] and
// [verdictFromDetails] both use errors.As, and once err has been through
// ScrubError there is nothing left for them to find. Classifying err — the
// unscrubbed original — before scrubbing it is therefore not an optimization,
// it is the only order that keeps classification working at all, and the one
// caller that matters most is the one this bug would corrupt silently:
// [flowstatev1.ErrorKindUpstreamUnknown] degrading to a retryable
// [flowstatev1.ErrorKindInternal] the moment a plugin's failure message
// happens to contain the very secret that made this a failure, which is
// exactly backwards — an unknown outcome is the one case where retrying is
// worse than doing nothing, and it is the case a leak would silently disarm.
//
// The verdict extracted from the detail is booleans and a bounded duration,
// never a string, so there is nothing in it for the scrubber to have missed;
// if a future field on ExecuteResponse ever carries text, it must be scrubbed
// before it reaches anything this function returns, the same as
// [scrubPluginOutputs] already does for a task's ordinary outputs.
//
// What crosses from err into the result is data, not the error itself: kind,
// retryable, and retry_after are read and copied, and the only thing wrapped
// into the returned [flowstatev1.TaskError] is the scrubbed message — so a
// caller that unwraps this can never reach the original no matter what
// classified it.
//
// Note one deliberate gap. The schema says a plugin that says nothing should get
// the non-retrying answer, but the engine's error kinds have no member meaning
// "the task failed permanently for a reason of its own": every permanent kind
// names a specific cause — bad inputs, an unknown task, a denied policy, an
// exceeded limit — and claiming one of those for an unclassified plugin failure
// would make the diagnostic lie about the cause. So an unclassified failure is
// [flowstatev1.ErrorKindInternal], which is retryable, on the same reasoning the
// engine already applies to its own unclassified errors. Plugins built with the
// SDK always answer explicitly, so this applies only to one that returns a bare
// error.
func taskError(task, plugin string, err error, scrubber *secrets.Scrubber) error {
	kind := kindForCode(err)

	verdict, said := verdictFromDetails(err)
	if said {
		switch {
		case verdict.unknownOutcome:
			// Named explicitly rather than inferred from "permanent and the code
			// disagreed": [sdk.OutcomeUnknown] is a distinct claim from
			// [sdk.Failed], and the one kind that names it precisely is this one
			// — see [flowstatev1.ErrorKindUpstreamUnknown]. Mapping it onto
			// [flowstatev1.ErrorKindInvalidInput] the way an unqualified
			// permanent verdict is below would misname why the step is not
			// retried: nothing about the inputs was wrong, an outcome was lost.
			kind = flowstatev1.ErrorKindUpstreamUnknown

		case verdict.retryable:
			// The plugin's own verdict decides only whether to retry, not what
			// the failure was. Letting it decide both would mean a plugin's
			// NotFound and its PermissionDenied arriving as "inputs that do not
			// satisfy the task's schema", which tells a workflow author to fix
			// inputs that are fine.
			kind = flowstatev1.ErrorKindUpstream

		case kind.Retryable():
			// It says permanent and the code says otherwise. The plugin knows
			// its own backend, so it wins — and the only permanent kind that
			// describes "the task failed" without naming a cause it cannot know
			// is this one.
			kind = flowstatev1.ErrorKindInvalidInput
		}
	}

	// Everything above read err as it arrived from the wire. Nothing below may
	// read it again: the only thing that reaches the returned error from here
	// on is the scrubbed message.
	scrubbed := scrubber.ScrubError(err)

	taskErr := flowstatev1.NewTaskError(task, kind, fmt.Errorf("plugin %q: %w", plugin, scrubbed))
	if said && verdict.retryable {
		taskErr.RetryAfter = verdict.retryAfter
	}

	return taskErr
}

// kindForCode maps a Connect status code onto the engine's error kinds.
//
// The codes not listed are the ones that say a plugin broke rather than that
// something it was asked to do could not be done, and they land on
// [flowstatev1.ErrorKindInternal] — which is retryable, for the reason the engine
// gives for retrying its own internal errors: a genuine defect is better
// surfaced by exhausting attempts than by being silently swallowed. A plugin that
// means "do not retry this" says so with the detail read above, and every plugin
// built with the SDK does.
func kindForCode(err error) flowstatev1.ErrorKind {
	// A cancelled or expired caller context is not the plugin's failure, and
	// classifying it as one would blame the plugin in the step's error.
	//
	// The two are told apart because since #1147 the deadline on a plugin call
	// *is* the step's `timeout:` — so an expired one is the engine's own bound
	// being reached, which is exactly [flowstatev1.ErrorKindTimeout] and is what
	// the drivers answer when they end the attempt themselves rather than
	// letting the call return (#915). Reporting Upstream for it made one fact
	// have two names depending on which side of a race won: locally the plugin
	// call returns first and this classifies it, durably Temporal's
	// StartToClose fires and engine.recordedStepKind classifies it, and the two
	// drivers must not disagree about a value that travels on
	// RunResponse.Error.kind. A cancellation is not a timeout and keeps its own
	// answer; both remain retryable, so this changes what an operator is told
	// and nothing about what is attempted.
	if errors.Is(err, context.DeadlineExceeded) {
		return flowstatev1.ErrorKindTimeout
	}
	if errors.Is(err, context.Canceled) {
		return flowstatev1.ErrorKindUpstream
	}

	code, ok := connectError(err)
	if !ok {
		return flowstatev1.ErrorKindInternal
	}

	switch code {
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
		return flowstatev1.ErrorKindInvalidInput
	case connect.CodePermissionDenied, connect.CodeUnauthenticated:
		return flowstatev1.ErrorKindPolicyDenied
	case connect.CodeUnimplemented, connect.CodeNotFound:
		return flowstatev1.ErrorKindUnknownTask
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeAborted, connect.CodeResourceExhausted:
		// Resource exhaustion belongs here rather than with the limits: a plugin
		// reporting it is rate limited or out of capacity is reporting something
		// that passes, unlike a response that was too large to read.
		return flowstatev1.ErrorKindUpstream
	case connect.CodeCanceled:
		// Cancellation the plugin reported rather than the caller's own, which
		// the check above catches. Nothing about the task is wrong, so it is not
		// reported as though something is.
		return flowstatev1.ErrorKindUpstream
	default:
		return flowstatev1.ErrorKindInternal
	}
}

// pluginVerdict is a plugin's own account of one failure, read from the
// ExecuteResponse attached to a failed RPC as an error detail.
type pluginVerdict struct {
	retryable      bool
	unknownOutcome bool
	retryAfter     time.Duration
}

// maxPluginRetryAfter bounds how long a plugin may ask the engine to wait,
// the same bound and the same reason [maxRetryAfter] applies to the http
// task's own Retry-After: an unbounded delay from something this repository
// did not write would let a misbehaving backend park a step's retry budget
// indefinitely.
const maxPluginRetryAfter = 5 * time.Minute

// verdictFromDetails reads a plugin's own verdict on one failure from an
// ExecuteResponse attached to the error.
func verdictFromDetails(err error) (pluginVerdict, bool) {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return pluginVerdict{}, false
	}

	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err != nil {
			// A detail that will not unmarshal is a malformed answer, not an
			// answer, so it is skipped rather than treated as either verdict.
			continue
		}
		response, ok := value.(*pluginv1.ExecuteResponse)
		if !ok {
			continue
		}

		verdict := pluginVerdict{
			retryable:      response.GetRetryable(),
			unknownOutcome: response.GetUnknownOutcome(),
		}
		if d := response.GetRetryAfter().AsDuration(); d > 0 && d <= maxPluginRetryAfter {
			verdict.retryAfter = d
		}

		return verdict, true
	}

	return pluginVerdict{}, false
}
