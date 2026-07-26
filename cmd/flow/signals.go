package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// localSignals holds the --signal flags of `flow run local`.
var localSignals []string

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

	// An empty payload is a signal that carries nothing, which is a reasonable
	// thing to send: the wait still completes and still reports timed_out false.
	if strings.TrimSpace(raw) == "" {
		return name, &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}, nil
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return "", nil, fmt.Errorf("--signal %s: payload is not a JSON object: %w", name, err)
	}

	outputs := &v1.Node_Outputs{NamedValues: make(map[string]*v1.Value, len(fields))}
	for key, value := range fields {
		outputs.NamedValues[key] = v1.NewValue(value)
	}

	return name, outputs, nil
}
