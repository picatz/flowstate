package server

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// awaitRun holds a delivery open for the run it started or joined, and builds
// the response document a trigger that declared `respond_within:` answers with.
//
// # The wait is the whole of what this adds
//
// The same call [FlowstateServer.get] reads a finished run's outputs with,
// `GetWorkflow(...).Get`, under a context that ends at the author's bound. The
// document is [v1.WebhookResponse], which `flow test` calls too, so the
// receiver holds no second description of what a response says.
//
// Every way the wait can end short of an answer is the same document, `running`:
// the bound passing, the sender hanging up, an answer too large to send, a
// deployment that cannot be asked. The run is unaffected by any of them. This
// never turns a delivered delivery into an error status: a provider reads a
// non-2xx as "retry", and the run already exists.
//
// # What it spends
//
// The delivery's own [DefaultWebhookConcurrency] slot, for the whole wait: a
// waiting delivery is in flight, and a second pool for them would be a second
// bound to reason about beside the first. It is a throughput cost an author
// chose by writing the field, 64 deliveries held for `respond_within:` at most,
// and past that the receiver sheds with 503 as it always has.
func (r *WebhookReceiver) awaitRun(ctx context.Context, route *webhookRoute, accepted v1.AcceptedDelivery, within time.Duration) v1.WebhookResponseDocument {
	// The same fail-closed answer for everything that is not a finished run:
	// no outputs, no failure, and the run's address to read it with.
	running := v1.WebhookResponse(accepted, v1.WebhookRun{Status: v1.WebhookRunRunning}, route.workflow)

	namespace := r.principalIdentity(ctx, route).GetNamespace()
	client, err := r.server.clientFor(namespace)
	if err != nil {
		r.log.WarnContext(ctx, "a waiting delivery could not reach the run's namespace; answering it as running",
			"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(), "error", err)

		return running
	}

	waitCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()

	var result v1.Workflow_StepOutputs
	waitErr := client.GetWorkflow(waitCtx, accepted.WorkflowID, accepted.RunID).Get(waitCtx, &result)
	if waitErr != nil && waitCtx.Err() != nil {
		// The bound passed, or the sender went away. Not a failure of the run.
		return running
	}

	// What decides what is withheld is the specification this run *ran*, read
	// from its own start, and not this receiver's current one: a redelivery can
	// join a run an earlier configuration started, and a sensitive output must
	// stay withheld under the declaration that run was written against. Unread,
	// it is nil, and nil withholds everything.
	var (
		spec   *v1.Workflow
		inputs map[string]*v1.Value
	)
	if state, err := r.server.startedRunState(ctx, namespace, accepted.WorkflowID, accepted.RunID); err == nil {
		spec, inputs = state.GetWorkflow(), state.GetInputs()
	} else {
		r.log.WarnContext(ctx, "a waiting delivery could not read the run's specification; withholding its answer",
			"workflow", route.workflow.GetName(), "webhook", route.trigger.GetName(), "error", err)
	}

	if waitErr == nil {
		return v1.WebhookResponse(accepted, v1.WebhookRun{
			Status:  v1.WebhookRunCompleted,
			Outputs: result.GetRunOutputs(),
		}, spec)
	}

	// The failure sentence is the one `Get` reports, from the same function, so
	// the two surfaces cannot word one failure two ways. Its kind is dropped:
	// this surface carries the sentence and nothing else.
	failure := failureError(ctx, client, accepted.WorkflowID, accepted.RunID, terminalStatus(waitErr),
		func(string) bool { return false })

	return v1.WebhookResponse(accepted, v1.WebhookRun{
		Status:  v1.WebhookRunFailed,
		Failure: failure.GetMessage(),
		Inputs:  inputs,
	}, spec)
}

// terminalStatus names how a run that did not complete ended, from the error
// its result carried, for the fallbacks [failureError] words by status.
func terminalStatus(err error) v1.RunResponse_Status {
	var (
		canceled   *temporal.CanceledError
		terminated *temporal.TerminatedError
		timeout    *temporal.TimeoutError
	)

	switch {
	case errors.As(err, &canceled):
		return v1.RunResponse_STATUS_CANCELED
	case errors.As(err, &terminated):
		return v1.RunResponse_STATUS_TERMINATED
	case errors.As(err, &timeout):
		return v1.RunResponse_STATUS_TIMED_OUT
	default:
		return v1.RunResponse_STATUS_FAILED
	}
}
