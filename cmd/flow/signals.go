package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The flags of `flow signal`, which addresses a durable run rather than a local
// one.
var ()

// withLocalSignals attaches a signal source to a local run, seeded with whatever
// the caller supplied.
//
// A local run is a process, so there is nothing for a person to signal after it
// starts — which is why the answers are given up front. That is enough to make an
// approval gate something an author can exercise while writing it, and exercising
// it locally is the point: a gate whose first real run is in production is a gate
// nobody has tested.
//
// A waiter is attached even when nothing was supplied, so that a workload reaching
// a gate with no answer times out or blocks exactly as it would in production,
// rather than failing with a message about local tooling.
func withLocalSignals(ctx context.Context, flags []string) (context.Context, error) {
	signals := v1.NewLocalSignals()
	answered := make(map[string]bool, len(flags))

	for _, flag := range flags {
		name, payload, err := parseSignalFlag(flag)
		if err != nil {
			return nil, err
		}
		if err := signals.Deliver(name, payload); err != nil {
			return nil, err
		}
		answered[name] = true
	}

	return v1.NewContextWithSignalWaiter(ctx, signals), nil
}

// reportUnansweredGates warns about gates this run will block on.
//
// A local run with no answer for a gate is not wrong — it waits, exactly as
// production would, until the gate's own timeout. But it looks like a hang, and an
// author watching a terminal do nothing for a day will conclude the feature is
// broken rather than that they forgot a flag. So the run says what it is waiting
// for and what would release it, before it starts waiting.
func reportUnansweredGates(out io.Writer, workflow *v1.Workflow, flags []string) {
	answered := make(map[string]bool, len(flags))
	for _, flag := range flags {
		if name, _, found := strings.Cut(flag, "="); found {
			answered[strings.TrimSpace(name)] = true
		}
	}

	for _, name := range v1.SignalNames(workflow) {
		if answered[name] {
			continue
		}
		fmt.Fprintf(out,
			"this workload waits for signal %q, which nothing here will send; it will block until that wait times out\n"+
				"  to answer it now:  --signal %s='{\"approved\": true}'\n",
			name, name)
	}
}

// parseSignalFlag reads one --signal name=json flag.
//
// The payload becomes the waiting step's outputs, so the JSON keys are what a later
// step reads as ${approval.approved}. Reporting a malformed one names the flag and
// what was wrong with it, because a quoting mistake in a shell is the most likely
// way to get here.
func parseSignalFlag(flag string) (string, *v1.Node_Outputs, error) {
	name, raw, found := strings.Cut(flag, "=")
	if !found {
		return "", nil, fmt.Errorf(
			"--signal %q needs a name and a payload, as name=json, e.g. --signal deploy-approved='{\"approved\": true}'", flag)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil, fmt.Errorf("--signal %q names no signal", flag)
	}

	payload, err := parseSignalPayload("--signal "+name, raw)
	if err != nil {
		return "", nil, err
	}

	return name, payload, nil
}

// parseSignalPayload turns a JSON object into a waiting step's outputs.
//
// Shared by the local flag and by `flow signal`, so that a payload means exactly
// the same thing whichever driver receives it — a rehearsal that reads its
// answer differently from production is a rehearsal of the wrong thing.
//
// The source names where the payload came from, because the keys here become what
// a later step reads as ${approval.approved}: a quoting mistake is otherwise
// indistinguishable from a workflow bug.
func parseSignalPayload(source, raw string) (*v1.Node_Outputs, error) {
	// An empty payload is a signal that carries nothing, which is a reasonable
	// thing to send: the wait still completes and still reports timed_out false.
	if strings.TrimSpace(raw) == "" {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}, nil
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, fmt.Errorf("%s: payload is not a JSON object: %w", source, err)
	}

	outputs := &v1.Node_Outputs{NamedValues: make(map[string]*v1.Value, len(fields))}
	for key, value := range fields {
		outputs.NamedValues[key] = v1.NewValue(value)
	}

	return outputs, nil
}

// runSignal delivers a signal to a run waiting for one.
//
// This is the other half of --signal on a local run: the same payload with the
// same meaning, addressed to a workload already waiting somewhere rather than to
// a process about to start.
//
// A sender names the workload, not a run. The run may have been waiting for a
// week and may have been continued as new several times since it started, and
// neither is something an approver knows or should have to: they are approving
// the deploy, not one attempt at it. --run-id is there for the case where
// somebody genuinely means one attempt.
func runSignal(cmd *cobra.Command, args []string) error {
	workflowID, name := args[0], args[1]

	server := serverFlagsOf(cmd)
	data, _ := cmd.Flags().GetString("data")
	runID, _ := cmd.Flags().GetString("run-id")

	payload, err := parseSignalPayload("--data", data)
	if err != nil {
		return err
	}

	// A signal that carries nothing travels as an *absent* payload rather than an
	// empty one, because Node.Outputs.named_values is required: an empty map is not
	// something the schema lets a message say, and sending one is refused before it
	// leaves. The server turns absent back into empty outputs, which is what keeps
	// ${approval.timed_out} resolving on a gate somebody answered with nothing to
	// add.
	if len(payload.GetNamedValues()) == 0 {
		payload = nil
	}

	request := &v1.SignalRequest{
		WorkflowId: workflowID,
		RunId:      runID,
		Name:       name,
		Payload:    payload,
	}

	// The schema's own rules, read off the descriptor rather than restated here,
	// so this cannot drift from what the server enforces. It runs before the round
	// trip because a mistyped signal name is worth reporting on the spot rather
	// than as a remote invalid-argument. The server validates again regardless: a
	// client-side check is a convenience, never a control.
	if err := v1.Validate(request); err != nil {
		// The rule is the schema's, but a pattern is not a thing to hand somebody
		// as advice, so the hint says what the rule is for rather than restating it.
		return fmt.Errorf("%w\n  a signal name is the one its wait_for_signal step declares: "+
			"a letter or digit, then letters, digits, - or _", err)
	}

	if _, err := newWorkflowServiceClient(server).Signal(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("signalling", workflowID, server, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "delivered %s to %s\n", name, workflowID)
	return nil
}
