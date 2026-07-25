package plugin

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// taskDef builds the engine's definition of a task from a plugin's manifest for
// it.
//
// The result is a [flowstatev1.TaskDef] like any other. That is the registry
// invariant applied across a process boundary: the engine consults declared
// fields rather than testing a task's name, so a task that arrived from a plugin
// takes the same paths through resolution, validation, and dispatch as one
// compiled in.
func (p *Plugin) taskDef(manifest *flowstatev1.TaskManifest, cfg Config) (flowstatev1.TaskDef, error) {
	name := manifest.GetName()

	inputs, err := messageDescriptor(manifest.GetInputDescriptor(), manifest.GetInputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, pluginError(p.name, p.path, fmt.Errorf("task %q inputs: %w", truncate(name, 64), err))
	}

	outputs, err := messageDescriptor(manifest.GetOutputDescriptor(), manifest.GetOutputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, pluginError(p.name, p.path, fmt.Errorf("task %q outputs: %w", truncate(name, 64), err))
	}

	return flowstatev1.TaskDef{
		Name:           name,
		Summary:        manifest.GetSummary(),
		Inputs:         inputs,
		Outputs:        outputs,
		DeferredInputs: manifest.GetDeferredInputs(),

		// The scope is what a task's own expressions evaluate against, so a task
		// that declared it needs one needs prior step outputs to build it.
		NeedsPrevOutputs: manifest.GetNeedsScope(),

		Fn: p.taskFunc(manifest),
	}, nil
}

// taskFunc returns the function that executes a task by asking the plugin to.
func (p *Plugin) taskFunc(manifest *flowstatev1.TaskManifest) flowstatev1.TaskFunc {
	name := manifest.GetName()
	needsScope := manifest.GetNeedsScope()

	return func(ctx context.Context, inputs map[string]*flowstatev1.Value, scope *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		inst, err := p.ready()
		if err != nil {
			// Unavailability is the one retryable classification, so a step's
			// retry policy gets to decide whether to wait out a restart.
			return nil, flowstatev1.NewTaskError(name, flowstatev1.ErrorKindUpstream, err)
		}

		identity, _ := IdentityFromContext(ctx)

		request := &flowstatev1.ExecuteTaskRequest{
			Task: &flowstatev1.Task{
				Name:   name,
				Inputs: inputs,
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

		resp, err := inst.clients.task.Execute(callCtx, connect.NewRequest(request))
		if err != nil {
			return nil, taskError(name, p.name, err)
		}

		return resp.Msg.GetOutputs(), nil
	}
}

// taskError classifies a plugin's task failure into the engine's own error
// kinds, which is what decides whether the step is attempted again.
//
// A plugin can say so explicitly, by failing the RPC with an ExecuteTaskResponse
// attached as an error detail: only the plugin knows whether its backend's
// failure was transient, and the schema puts that answer on the response's
// retryable field. Without one, the Connect code is mapped to the closest kind.
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
func taskError(task, plugin string, err error) error {
	if retryable, ok := retryableFromDetails(err); ok {
		kind := flowstatev1.ErrorKindInvalidInput
		if retryable {
			kind = flowstatev1.ErrorKindUpstream
		}
		return flowstatev1.NewTaskError(task, kind, fmt.Errorf("plugin %q: %w", plugin, err))
	}

	kind := flowstatev1.ErrorKindInternal

	if code, ok := connectError(err); ok {
		switch code {
		case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
			kind = flowstatev1.ErrorKindInvalidInput
		case connect.CodePermissionDenied, connect.CodeUnauthenticated:
			kind = flowstatev1.ErrorKindPolicyDenied
		case connect.CodeUnimplemented, connect.CodeNotFound:
			kind = flowstatev1.ErrorKindUnknownTask
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeAborted, connect.CodeResourceExhausted:
			// Resource exhaustion belongs here rather than with the limits: a
			// plugin reporting it is rate limited or out of capacity is
			// reporting something that passes, unlike a response that was too
			// large to read.
			kind = flowstatev1.ErrorKindUpstream
		}
	}

	// A cancelled or expired caller context is not the plugin's failure, and
	// classifying it as one would blame the plugin in the step's error.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		kind = flowstatev1.ErrorKindUpstream
	}

	return flowstatev1.NewTaskError(task, kind, fmt.Errorf("plugin %q: %w", plugin, err))
}

// retryableFromDetails reads a plugin's own verdict on whether a failure could
// succeed on another attempt, from an ExecuteTaskResponse attached to the error.
func retryableFromDetails(err error) (retryable, found bool) {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return false, false
	}

	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err != nil {
			// A detail that will not unmarshal is a malformed answer, not an
			// answer, so it is skipped rather than treated as either verdict.
			continue
		}
		if response, ok := value.(*flowstatev1.ExecuteTaskResponse); ok {
			return response.GetRetryable(), true
		}
	}

	return false, false
}
