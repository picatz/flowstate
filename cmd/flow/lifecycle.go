package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The flags of the verbs that stop a run, and of `flow list`.
var (
	cancelRunID    string
	terminateRunID string
	terminateWhy   string

	listPageSize  int32
	listPageToken string
	listAll       bool
)

// runCancel asks a run to stop, letting it clean up on the way out.
func runCancel(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	request := &v1.CancelRequest{WorkflowId: workflowID, RunId: cancelRunID}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient().Cancel(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("cancelling", workflowID, err)
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

	request := &v1.TerminateRequest{
		WorkflowId: workflowID,
		RunId:      terminateRunID,
		Reason:     terminateWhy,
	}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient().Terminate(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("terminating", workflowID, err)
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
	client := newWorkflowServiceClient()

	// The listing goes to stdout as a table and everything else to stderr, so
	// `flow list | ...` sees rows and nothing else.
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN\tSTATUS\tSTARTED\tFINISHED")

	token := listPageToken
	rows := 0

	// The walk is its own function so that every way out of it — success, a
	// refused page, a token that stopped moving — passes through the one flush
	// below.
	//
	// A tabwriter buffers until flushed, so returning an error directly from
	// inside the loop discards rows that were retrieved and formatted correctly.
	// `--all` makes that worth caring about: page four failing is no reason to
	// throw away pages one through three, and a caller who sees an error and no
	// output cannot tell how far it got.
	walk := func() error {
		for {
			request := &v1.ListRequest{PageSize: listPageSize, PageToken: token}
			if err := v1.Validate(request); err != nil {
				return err
			}

			response, err := client.List(cmd.Context(), connect.NewRequest(request))
			if err != nil {
				return refusedList(err)
			}

			for _, run := range response.Msg.GetRuns() {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
					run.GetWorkflowId(),
					statusLabel(run.GetStatus()),
					formatRunTime(run.GetStartTime().AsTime(), run.GetStartTime() != nil),
					formatRunTime(run.GetCloseTime().AsTime(), run.GetCloseTime() != nil),
				)
				rows++
			}

			previous := token
			token = response.Msg.GetNextPageToken()

			// An empty token is the only end of the listing. A short page is not:
			// the scan may have spent its budget among runs belonging to somebody
			// else.
			if token == "" || !listAll {
				return nil
			}

			// A token that has not moved means the next request is the one just
			// made. `--all` is the only loop here whose end is decided by the far
			// side, so it is the only one a server that is wrong or hostile can
			// make run forever — and a CLI that hangs reads as a slow listing
			// rather than a fault, which is how somebody loses an afternoon.
			if token == previous {
				return fmt.Errorf("the server returned the same page token twice, so continuing "+
					"would ask it the same question forever; %d run(s) listed before stopping", rows)
			}
		}
	}

	walkErr := walk()

	if err := tw.Flush(); err != nil {
		return err
	}
	if walkErr != nil {
		return walkErr
	}

	if rows == 0 && token == "" {
		fmt.Fprintln(cmd.ErrOrStderr(), "no runs")
	}

	// Said plainly, because the alternative is a caller concluding from a short
	// page that they have seen everything.
	if token != "" {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"more runs remain; continue with --page-token %s, or pass --all to walk the rest\n", token)
	}

	return nil
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
func refusedList(err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while listing runs: %w", err)
	case connect.CodeUnavailable:
		return fmt.Errorf("no Flowstate server answered at %s (set --address or FLOWSTATE_ADDRESS "+
			"to point somewhere else): %w", flowstateAddress, err)
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

# Keep only the ids:
flow list | tail -n +2 | awk '{print $1}'`,
	}

	listCmd.Flags().Int32Var(&listPageSize, "page-size", 0,
		"how many runs to return per page; unset takes the server's default")
	listCmd.Flags().StringVar(&listPageToken, "page-token", "",
		"continue a previous listing from where it stopped")
	listCmd.Flags().BoolVar(&listAll, "all", false,
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

	cancelCmd.Flags().StringVar(&cancelRunID, "run-id", "",
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

	terminateCmd.Flags().StringVar(&terminateRunID, "run-id", "",
		"pin the request to one run of the workload; unset addresses whichever run is current")
	terminateCmd.Flags().StringVar(&terminateWhy, "reason", "",
		"recorded on the terminated run; a terminated run leaves no account of itself, "+
			"so this is the only explanation anyone will find")

	return []*cobra.Command{listCmd, cancelCmd, terminateCmd}
}
