package payloadcodec

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// codecConverter is the converter [Config.DataConverter] returns when a codec
// is configured. Outside a workflow it is the codec converter unchanged; bound
// to one ([workflow.ContextAware]), it becomes [inWorkflowConverter].
//
// The binding is the one place every workflow-side decode passes through,
// whoever made the value's channel or future: the SDK creates a signal
// channel for a signal that arrives before workflow code asks for it, with
// the workflow's root converter, and binds that converter the same way.
// A check installed anywhere nearer the interpreter misses those.
type codecConverter struct{ converter.DataConverter }

// WithWorkflowContext implements [workflow.ContextAware].
func (c codecConverter) WithWorkflowContext(ctx workflow.Context) converter.DataConverter {
	return inWorkflowConverter{DataConverter: bindWorkflow(c.DataConverter, ctx)}
}

// WithContext implements [workflow.ContextAware].
func (c codecConverter) WithContext(ctx context.Context) converter.DataConverter {
	return codecConverter{DataConverter: bindContext(c.DataConverter, ctx)}
}

// inWorkflowConverter decodes for workflow code, and fails the run on a
// payload this process cannot read rather than returning that as an error.
//
// Returned, such an error is taken for the payload's own. A signal channel
// drops a signal it cannot decode as corrupt, losing an approval; an activity
// or child result that fails to decode fails its step, which runs
// `continue_on_error:` or `undo:` for a step that succeeded, and a replay on a
// worker that can read it then takes the other branch. The run's decisions
// would depend on which worker read it and whether a key provider answered,
// not on its history. Workflow code cannot make the task retry instead
// (every panic meets the worker's panic policy, engine/panicpolicy.go), so
// the run fails, loudly and recoverably, with the payload still in history.
type inWorkflowConverter struct{ converter.DataConverter }

// FromPayload implements [converter.DataConverter].
func (c inWorkflowConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	err := c.DataConverter.FromPayload(payload, valuePtr)
	failIfUnreadableHere(err)
	return err
}

// FromPayloads implements [converter.DataConverter].
func (c inWorkflowConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error {
	err := c.DataConverter.FromPayloads(payloads, valuePtrs...)
	failIfUnreadableHere(err)
	return err
}

// WithWorkflowContext implements [workflow.ContextAware].
func (c inWorkflowConverter) WithWorkflowContext(ctx workflow.Context) converter.DataConverter {
	return inWorkflowConverter{DataConverter: bindWorkflow(c.DataConverter, ctx)}
}

// WithContext implements [workflow.ContextAware].
func (c inWorkflowConverter) WithContext(ctx context.Context) converter.DataConverter {
	return inWorkflowConverter{DataConverter: bindContext(c.DataConverter, ctx)}
}

// failIfUnreadableHere panics when err says this process could not decode a
// payload, rather than that the payload is wrong: [ErrUnavailable] or
// [ErrNotReadableHere].
func failIfUnreadableHere(err error) {
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrNotReadableHere) {
		panic(fmt.Errorf("decoding a payload in workflow code: %w", err))
	}
}

func bindWorkflow(dc converter.DataConverter, ctx workflow.Context) converter.DataConverter {
	if aware, ok := dc.(workflow.ContextAware); ok {
		return aware.WithWorkflowContext(ctx)
	}
	return dc
}

func bindContext(dc converter.DataConverter, ctx context.Context) converter.DataConverter {
	if aware, ok := dc.(workflow.ContextAware); ok {
		return aware.WithContext(ctx)
	}
	return dc
}
