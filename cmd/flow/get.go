package main

import (
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// getRunID holds the --run-id flag of `flow get`.
var getRunID string

// runGet reports what a run is doing, and what it produced if it is finished.
//
// `flow run` already polls this while it waits, which covered the case where the
// person who started a workload is still watching it. That is the case durable
// execution is least needed for. A workload that outlives the terminal that
// started it has to be askable about afterwards, and an approval gate is the
// clearest example: it is waiting precisely because nobody is watching.
func runGet(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat()
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	workflowID := args[0]

	request := &v1.GetRequest{WorkflowId: workflowID}

	// Left absent rather than empty when unset. The schema requires a run id to be
	// a UUID when present, so sending "" would be refused for not looking like one
	// instead of meaning "whichever run is current".
	if getRunID != "" {
		request.RunId = &getRunID
	}

	if err := v1.Validate(request); err != nil {
		return fmt.Errorf("%w\n  a run id is the UUID Temporal gave one attempt at the workload; "+
			"omit it to ask about whichever attempt is current", err)
	}

	response, err := newWorkflowServiceClient().Get(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedRun("reading", workflowID, err)
	}

	msg := response.Msg

	// A machine reader gets the whole answer as one document — status, both ids,
	// outputs, and the failure if there was one — rather than the outputs alone
	// with the status split off onto another stream. Splitting is right for a
	// person, who is reading two things; it is wrong for a program, which is
	// reading one.
	//
	// The exit status is still the run's outcome, so `flow get x -o json && ...`
	// behaves the way the shell reader expects either way.
	if format.Machine() {
		if err := writeJSON(surface, format, msg); err != nil {
			return err
		}

		if failure := msg.GetError(); failure != nil {
			return fmt.Errorf("run %s ended %s: %s",
				workflowID, strings.ToLower(statusLabel(msg.GetStatus())), failure.GetMessage())
		}

		return nil
	}

	// The status goes to stderr and the outputs to stdout, so `flow get x | jq`
	// receives a workload's data and nothing else, while somebody watching a
	// terminal still sees what happened. A run that is still going produces no
	// stdout at all, which is the honest answer to "what did it produce".
	// The status is the one value on this line worth finding before the line is
	// read, which is what a filled label is for. It is the word either way: strip
	// the styling and the sentence still says what happened.
	//
	// ErrTheme, because this line goes to stderr: `flow get x | jq` has a piped
	// stdout and a terminal stderr, and the palette for one is not the palette for
	// the other.
	fmt.Fprintf(surface.Err, "%s workflow %s run %s\n",
		surface.ErrTheme.Pill(statusTone(msg.GetStatus()), statusLabel(msg.GetStatus())),
		msg.GetWorkflowId(), msg.GetRunId())

	if outputs := msg.GetOutputs(); outputs != nil {
		encoded, err := protojson.Marshal(outputs)
		if err != nil {
			return fmt.Errorf("formatting the outputs of %s: %w", workflowID, err)
		}
		fmt.Fprintf(surface.Out, "%s\n", encoded)
	}

	// A run that failed is reported as a failure, so `flow get x && ...` behaves
	// the way a shell reader expects. The query itself succeeded; what is being
	// reported is the workload's outcome, which is what was asked about.
	if failure := msg.GetError(); failure != nil {
		return fmt.Errorf("run %s ended %s: %s",
			workflowID, strings.ToLower(statusLabel(msg.GetStatus())), failure.GetMessage())
	}

	return nil
}

// statusLabel renders a run status the way a person would say it.
//
// The generated enum names carry a STATUS_ prefix that exists to keep the
// constants distinct in the schema, and repeating it on a terminal only makes a
// line harder to scan.
func statusLabel(status v1.RunResponse_Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}
