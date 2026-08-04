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
		inst, err := p.ready()
		if err != nil {
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

		resp, err := inst.clients.task.Execute(callCtx, connect.NewRequest(request))
		if err != nil {
			// Classified before it is scrubbed — see [taskError] for why the
			// order is load-bearing — and scrubbed before it is wrapped, so
			// that a resolved secret a peer reflected back through an RPC
			// failure — the same hazard the http task's own scrubber exists
			// for — cannot reach the task error this becomes, which is
			// surfaced to users and written to workflow history.
			return nil, taskError(qualified, p.name, err, scrubber)
		}

		outputs := resp.Msg.GetOutputs()
		if err := scrubPluginOutputs(scrubber, outputs); err != nil {
			return nil, flowstatev1.NewTaskError(qualified, flowstatev1.ErrorKindInvalidInput, err)
		}

		return outputs, nil
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
		scrubLiteral(scrubber, v.GetLiteral())
	}

	return nil
}

// scrubLiteral redacts every string a CEL literal holds, recursively through a
// list or a mapping.
func scrubLiteral(scrubber *secrets.Scrubber, lit *expr.Value) {
	if lit == nil {
		return
	}

	switch kind := lit.GetKind().(type) {
	case *expr.Value_StringValue:
		kind.StringValue = scrubber.Scrub(kind.StringValue)
	case *expr.Value_BytesValue:
		// A plugin that decodes a secret input into bytes and returns it in a
		// bytes output ships the credential to history unredacted unless this
		// case exists — bytes are not a string, so the case above never
		// matches, and there is otherwise nothing here that looks at them at
		// all. [secrets.Scrubber]'s registered forms already include base64
		// and hex, so a value transported as bytes and one transported as text
		// are matched the same way; only the field kind holding it differs.
		if scrubbed := scrubber.Scrub(string(kind.BytesValue)); scrubbed != string(kind.BytesValue) {
			kind.BytesValue = []byte(scrubbed)
		}
	case *expr.Value_ListValue:
		for _, element := range kind.ListValue.GetValues() {
			scrubLiteral(scrubber, element)
		}
	case *expr.Value_MapValue:
		for _, entry := range kind.MapValue.GetEntries() {
			scrubLiteral(scrubber, entry.GetKey())
			scrubLiteral(scrubber, entry.GetValue())
		}
	}
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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
