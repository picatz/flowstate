package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// runCancel asks a run to stop, letting it clean up on the way out.
func runCancel(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	server := serverFlagsOf(cmd)
	runID, _ := cmd.Flags().GetString("run-id")

	request := &v1.CancelRequest{WorkflowId: workflowID, RunId: runID}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).Cancel(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("cancelling", workflowID, server, err)
	}

	// Reported as a request rather than as a result, because that is what it is.
	// Cancellation is cooperative: the run has been asked and is now finishing its
	// response, so saying "cancelled" here would claim something not yet true, and
	// somebody would build on the claim.
	fmt.Fprintf(cmd.ErrOrStderr(),
		"asked %s to stop; it runs its cleanup before finishing, so ask `flow get %s` whether it has\n",
		workflowID, workflowID)

	return nil
}

// runTerminate stops a run immediately, running none of its cleanup.
func runTerminate(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	server := serverFlagsOf(cmd)
	runID, _ := cmd.Flags().GetString("run-id")
	reason, _ := cmd.Flags().GetString("reason")

	request := &v1.TerminateRequest{
		WorkflowId: workflowID,
		RunId:      runID,
		Reason:     reason,
	}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).Terminate(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("terminating", workflowID, server, err)
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "terminated %s; no cleanup ran\n", workflowID)

	return nil
}

// runList reports the runs belonging to the caller.
//
// The listing is a bounded scan rather than a query, because the tenant a run
// belongs to is recorded as a memo and Temporal cannot filter on one. A page can
// therefore come back short, or empty, with runs still to find — so the paging
// here is not a convenience. Stopping at the first short page is how a caller
// silently misses their own runs, which is why --all exists and why a partial
// listing says so on stderr.
func runList(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	server := serverFlagsOf(cmd)
	client := newWorkflowServiceClient(server)
	rendering := newListRendering(surface, format)

	pageSize, _ := cmd.Flags().GetInt32("page-size")
	all, _ := cmd.Flags().GetBool("all")
	token, _ := cmd.Flags().GetString("page-token")

	// The walk is its own function so that every way out of it — success, a
	// refused page, a token that stopped moving — passes through the one flush
	// below.
	//
	// A tabwriter buffers until flushed, so returning an error directly from
	// inside the loop discards rows that were retrieved and formatted correctly.
	// `--all` makes that worth caring about: page four failing is no reason to
	// throw away pages one through three, and a caller who sees an error and no
	// output cannot tell how far it got. The same reasoning applies to the JSON
	// forms, so they are flushed through the same path.
	walk := func() error {
		for {
			request := &v1.ListRequest{PageSize: pageSize, PageToken: token}
			if err := v1.Validate(request); err != nil {
				return err
			}

			response, err := client.List(cmd.Context(), connect.NewRequest(request))
			if err != nil {
				return refusedList(server, err)
			}

			if err := rendering.add(response.Msg.GetRuns()); err != nil {
				return err
			}

			previous := token
			token = response.Msg.GetNextPageToken()

			// An empty token is the only end of the listing. A short page is not:
			// the scan may have spent its budget among runs belonging to somebody
			// else.
			if token == "" || !all {
				return nil
			}

			// A token that has not moved means the next request is the one just
			// made. `--all` is the only loop here whose end is decided by the far
			// side, so it is the only one a server that is wrong or hostile can
			// make run forever — and a CLI that hangs reads as a slow listing
			// rather than a fault, which is how somebody loses an afternoon.
			if token == previous {
				return fmt.Errorf("the server returned the same page token twice, so continuing "+
					"would ask it the same question forever; %d run(s) listed before stopping",
					rendering.rows)
			}
		}
	}

	walkErr := walk()

	if err := rendering.flush(token); err != nil {
		return err
	}
	if walkErr != nil {
		return walkErr
	}

	// Said on stderr, and only to a person: a program asked for a format that
	// carries the token in the answer itself, so telling it again in prose it
	// would have to parse is worse than saying nothing.
	if format == FormatText {
		if rendering.rows == 0 && token == "" {
			fmt.Fprintln(surface.Err, "no runs")
		}

		// Said plainly, because the alternative is a caller concluding from a
		// short page that they have seen everything.
		if token != "" {
			fmt.Fprintf(surface.Err,
				"more runs remain; continue with --page-token %s, or pass --all to walk the rest\n", token)
		}
	}

	return nil
}

// listRendering accumulates a listing and writes it in the requested shape.
//
// The three shapes want three different moments to write at, and pretending
// otherwise is how one of them ends up badly served:
//
//   - text writes rows as they arrive into a tabwriter, which aligns them at the
//     flush. The header is deliberately withheld until the first row: a listing
//     that failed before returning anything used to print a bare header to stdout,
//     which a pipe reads as a successful listing that found nothing.
//   - jsonl writes each run immediately, so a reader gets the first one without
//     waiting for the last — which is the point of the line-per-record form.
//   - json has to hold everything, because one document cannot be written until
//     its last element is known.
type listRendering struct {
	surface *ui.UI
	format  OutputFormat

	table  *tabwriter.Writer
	header bool

	// runs is held only for the single-document form.
	runs []*v1.RunSummary

	// rows is what was rendered, for the messages that count it.
	rows int
}

func newListRendering(surface *ui.UI, format OutputFormat) *listRendering {
	rendering := &listRendering{surface: surface, format: format}
	if format == FormatText {
		rendering.table = tabwriter.NewWriter(surface.Out, 0, 8, 2, ' ', 0)
	}

	return rendering
}

// add renders one page.
func (r *listRendering) add(runs []*v1.RunSummary) error {
	for _, run := range runs {
		r.rows++

		switch r.format {
		case FormatJSON:
			r.runs = append(r.runs, run)

		case FormatJSONL:
			if err := writeJSON(r.surface, r.format, run); err != nil {
				return err
			}

		default:
			if !r.header {
				fmt.Fprintln(r.table, "WORKFLOW_ID\tSTATUS\tSTARTED\tFINISHED")
				r.header = true
			}

			fmt.Fprintf(r.table, "%s\t%s\t%s\t%s\n",
				run.GetWorkflowId(),
				r.surface.Theme.Tone(statusTone(run.GetStatus())).Render(statusLabel(run.GetStatus())),
				formatRunTime(run.GetStartTime().AsTime(), run.GetStartTime() != nil),
				formatRunTime(run.GetCloseTime().AsTime(), run.GetCloseTime() != nil),
			)
		}
	}

	return nil
}

// flush writes whatever the shape could not write as it went.
//
// The page token is carried into the single-document form rather than only into
// the prose on stderr, because a program reading JSON has no way to act on a
// sentence — and a listing that stopped early without saying so is how a caller
// silently misses their own runs.
func (r *listRendering) flush(token string) error {
	switch r.format {
	case FormatJSON:
		return writeJSON(r.surface, r.format, &v1.ListResponse{
			Runs:          r.runs,
			NextPageToken: token,
		})

	case FormatJSONL:
		return nil

	default:
		return r.table.Flush()
	}
}

// formatRunTime renders a run's timestamp, or a placeholder when it has none.
//
// A run that has not finished has no close time, and printing the zero instant
// would report it as having finished in 1970.
func formatRunTime(t time.Time, present bool) string {
	if !present {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

// refusedList explains a refused listing.
//
// Separate from [refusedRun] because a listing names no run, so the "check the
// id" advice that helps there would be answering a question nobody asked.
func refusedList(server serverFlags, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while listing runs: %w", err)
	case connect.CodeUnavailable:
		return fmt.Errorf("no Flowstate server answered at %s (set --address or FLOWSTATE_ADDRESS "+
			"to point somewhere else): %w", server.address, err)
	default:
		return fmt.Errorf("listing runs: %w", err)
	}
}

// lifecycleCommands builds the verbs that list and stop runs.
//
// Returned rather than registered here so that main.go keeps one place where the
// command tree is assembled, and so the ordering of groups and flags stays
// visible in a single read.
func lifecycleCommands() []*cobra.Command {
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List your runs",
		Long: "List the runs belonging to your tenant, newest first. A page can come back " +
			"short or empty with runs still to find, because the server scans a bounded " +
			"number of executions per request; pass --all to walk the rest.",
		Args: cobra.NoArgs,
		RunE: runList,
		Example: `# List your runs:
flow list

# Walk every page rather than stopping at the first:
flow list --all

# Keep only the workflow ids, which is what get, signal, cancel and terminate take:
flow list -o jsonl | jq -r .workflowId

# Every run that is still going:
flow list --all -o json | jq '.runs[] | select(.status == "STATUS_RUNNING")'`,
	}

	addOutputFlag(listCmd)

	listCmd.Flags().Int32("page-size", 0,
		"how many runs to return per page; unset takes the server's default")
	listCmd.Flags().String("page-token", "",
		"continue a previous listing from where it stopped")
	listCmd.Flags().Bool("all", false,
		"keep asking until the listing is exhausted, rather than returning one page")

	// Cancel and terminate are separate verbs rather than one with a flag,
	// because the difference is not an option on a single act: one lets a
	// workload finish releasing what it holds and the other does not. A flag
	// would make the destructive reading the easy typo.
	cancelCmd := &cobra.Command{
		Use:   "cancel [workflow-id]",
		Short: "Ask a run to stop, letting it clean up",
		Long: "Ask a run to stop. Cancellation is cooperative: the run is told to stop and " +
			"gets to finish responding, so a workload that has to release a lock or undo a " +
			"partial change still does. A run wedged on something that never returns may not " +
			"stop at all — `flow terminate` is the answer then, and not before.",
		Args: cobra.ExactArgs(1),
		RunE: runCancel,
		Example: `# Ask a run to stop:
flow cancel flowstate-workflow-3f7c

# Check whether it has:
flow get flowstate-workflow-3f7c`,
	}

	cancelCmd.Flags().String("run-id", "",
		"pin the request to one run of the workload; unset addresses whichever run is current")

	terminateCmd := &cobra.Command{
		Use:   "terminate [workflow-id]",
		Short: "Stop a run immediately, without letting it clean up",
		Long: "Stop a run immediately. No further step runs and nothing the workload would " +
			"have done on the way out is done, so anything it was responsible for releasing " +
			"stays held. Prefer `flow cancel`; reach for this when a run must stop now, or " +
			"when cancelling did not stop it.",
		Args: cobra.ExactArgs(1),
		RunE: runTerminate,
		Example: `# Stop a wedged run, saying why:
flow terminate flowstate-workflow-3f7c --reason "stuck on a dependency that is never coming back"`,
	}

	terminateCmd.Flags().String("run-id", "",
		"pin the request to one run of the workload; unset addresses whichever run is current")
	terminateCmd.Flags().String("reason", "",
		"recorded on the terminated run; a terminated run leaves no account of itself, "+
			"so this is the only explanation anyone will find")

	return []*cobra.Command{listCmd, cancelCmd, terminateCmd}
}
