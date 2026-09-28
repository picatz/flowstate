package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// `flow debug attach`, `flow debug get` and `flow debug do`: the terminal's
// door to a durable run's debug session, over the server's debug RPCs.
//
// The vocabulary is the prompt's, spoken by [flowdebug.Driver], so a person who
// debugged a local run knows these commands already. The session lives in the
// run: this process holds a session id and a heartbeat, and a second process
// can rejoin with `--session`.

const debugAttachLong = `Attach a debugger to a durable run, hold it at its next step boundary, and
drive it: step in, over and out of calls, run until a step, set conditional
breakpoints, and evaluate read-only CEL against the held scope.

The run's workflow must declare ` + "`debug:`" + ` naming you, and your credentials must
carry workload.debug (and workload.debug_inspect to evaluate). A hold parks
workflow code at a step boundary only: work already dispatched keeps running,
and so does time. The session is leased: this command renews it while it runs,
and a session nobody renews lapses and the run resumes on its own.

Commands are read from the terminal, or from --script. Leaving with ` + "`detach`" + `, or
at the end of input, releases the run; ` + "`disconnect`" + ` leaves the session attached
for a later ` + "`flow debug attach --session <id>`" + `.`

const debugAttachExample = `# Attach, stop at the next step, and drive it interactively:
flow debug attach order-1234

# The same, as a reproducible script, with each answer as a JSON line:
flow debug attach order-1234 --script debug.txt -o jsonl

# Rejoin a session another process left attached:
flow debug attach order-1234 --session 5d3f…`

func addDebugRemoteCommands(debugCmd *cobra.Command) {
	attachCmd := &cobra.Command{
		Use:     "attach <workflow-id>",
		Short:   "Attach a debugger to a durable run and drive it",
		Long:    debugAttachLong,
		Args:    cobra.ExactArgs(1),
		RunE:    runDebugAttach,
		Example: debugAttachExample,
	}
	addServerFlags(attachCmd)
	addOutputFlag(attachCmd)
	attachCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain; unset follows the current one")
	attachCmd.Flags().String("session", "", "rejoin this session instead of attaching a new one")
	attachCmd.Flags().Duration("lease", 2*time.Minute, "how long each renewal holds the session; the engine bounds it")
	attachCmd.Flags().String("script", "", "read commands from this file instead of the terminal")
	attachCmd.Flags().String("program", "", "the Flowfile the run was started from, for source lines; used only if it compiles to the program the run executes, "+
		"the deployment's plugin and task pins aside")
	attachCmd.Flags().Duration("wait", time.Minute, "how long a movement waits for the next stop before reporting the run still running")

	getCmd := &cobra.Command{
		Use:   "get <workflow-id>",
		Short: "Show a durable run's debug session",
		Long: "Read a durable run's debug state: whether a session is attached, where the run " +
			"is held and why, its frames, breakpoints and recent observations. It changes nothing.",
		Args: cobra.ExactArgs(1),
		RunE: runDebugGet,
		Example: `# Where a run's debug session stands, and what it last did:
flow debug get order-1234

# The same snapshot, as the schema's JSON:
flow debug get order-1234 -o json`,
	}
	addServerFlags(getCmd)
	addOutputFlag(getCmd)
	getCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain")

	doCmd := &cobra.Command{
		Use:   "do <workflow-id> <command...>",
		Short: "Run one debugger command against a session",
		Long: "Run one debugger command — next, inspect steps.fetch, break charge if amount > 500 — " +
			"against a session a previous attach left attached, and print the answer. For " +
			"scripts and agents that drive a session one step at a time.",
		Args: cobra.MinimumNArgs(2),
		RunE: runDebugDo,
		Example: `flow debug do order-1234 --session 5d3f… next
flow debug do order-1234 --session 5d3f… inspect steps.quote.total -o json`,
	}
	addServerFlags(doCmd)
	addOutputFlag(doCmd)
	doCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain")
	doCmd.Flags().String("session", "", "the session to act in (required)")
	_ = doCmd.MarkFlagRequired("session")
	doCmd.Flags().Duration("wait", 30*time.Second, "how long a movement waits for the next stop")

	debugCmd.AddCommand(attachCmd, getCmd, doCmd)
}

func runDebugAttach(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")
	session, _ := cmd.Flags().GetString("session")
	lease, _ := cmd.Flags().GetDuration("lease")
	script, _ := cmd.Flags().GetString("script")
	program, _ := cmd.Flags().GetString("program")
	wait, _ := cmd.Flags().GetDuration("wait")

	var sourceMap *v1.DebugSourceMap
	if program != "" {
		workflow, source, err := loadDebuggedWorkflow(program)
		if err != nil {
			return err
		}
		sourceMap = source.sourceMap(workflow)
	}

	ctx := cmd.Context()
	remote, receipt, err := flowdebug.AttachRemote(ctx, newWorkflowServiceClient(serverFlagsOf(cmd)), args[0], runID,
		flowdebug.RemoteOptions{SessionID: session, Lease: lease, SourceMap: sourceMap, Wait: 5 * time.Second})
	if err != nil {
		return err
	}
	surface := newSurface(cmd)
	if !format.Machine() {
		fmt.Fprintf(surface.Out, "attached to %s — session %s (%s)\n", args[0], remote.SessionID(),
			strings.TrimSpace(flowdebug.FormatReceipt(receipt)))
		if program != "" && !remote.SourceMapVerified() {
			fmt.Fprintf(surface.Out, "%s does not match the program this run executes; lines are not shown\n", program)
		}
	}

	driver := flowdebug.NewDriver(remote)
	driver.Wait = wait

	// The first stop, or the news that the run has not reached a boundary.
	first, err := driver.Do(ctx, "status")
	if err != nil {
		_ = remote.Disconnect()

		return err
	}
	if first.Snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		if held, err := remote.WaitSnapshot(withWait(ctx, wait), first.Snapshot.GetRevision()); err == nil {
			first = &flowdebug.DriveResult{Snapshot: held, Text: flowdebug.FormatSnapshot(held)}
		}
	}
	if err := writeDriveResult(surface.Out, format, "status", first); err != nil {
		return err
	}

	in, interactive := io.Reader(cmd.InOrStdin()), stdinIsInteractive(cmd)
	if script != "" {
		file, err := os.Open(script)
		if err != nil {
			_ = remote.Close()

			return err
		}
		defer file.Close()
		in, interactive = file, false
	}

	keep := false
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 4096), flowdebug.MaxCommandBytes+1)
	for {
		if interactive {
			fmt.Fprint(surface.Out, flowdebug.Prompt)
		}
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		switch line {
		case "":
			continue
		case "disconnect":
			keep = true
		case "quit", "q", "exit":
			line = "detach"
		}
		if keep {
			break
		}

		result, err := driver.Do(ctx, line)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Fprintf(surface.Err, "%v\n", err)

			continue
		}
		if err := writeDriveResult(surface.Out, format, line, result); err != nil {
			return err
		}
		if line == "detach" || terminalDebugState(result.Snapshot.GetState()) {
			return remote.Disconnect()
		}
	}

	if keep {
		if !format.Machine() {
			fmt.Fprintf(surface.Out, "left session %s attached; rejoin with `flow debug attach %s --session %s` before its lease lapses\n",
				remote.SessionID(), args[0], remote.SessionID())
		}

		return remote.Disconnect()
	}

	// The end of input releases the run: a debugger that is gone must not
	// keep a production run held.
	return remote.Close()
}

func terminalDebugState(state v1.DebugRunState) bool {
	switch state {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return true
	default:
		return false
	}
}

func withWait(ctx context.Context, wait time.Duration) context.Context {
	if wait <= 0 {
		return ctx
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	context.AfterFunc(ctx, cancel)

	return ctx
}

func runDebugGet(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")

	response, err := newWorkflowServiceClient(serverFlagsOf(cmd)).DebugGet(cmd.Context(),
		newDebugGetRequest(args[0], runID))
	if err != nil {
		return err
	}

	snapshot := response.Msg.GetSnapshot()
	surface := newSurface(cmd)
	if format.Machine() {
		return writeJSON(surface, format, snapshot)
	}
	fmt.Fprint(surface.Out, flowdebug.FormatSnapshot(snapshot))
	for _, observation := range snapshot.GetObservations() {
		fmt.Fprintf(surface.Out, "  · %s\n", observation.GetText())
	}

	return nil
}

func runDebugDo(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")
	session, _ := cmd.Flags().GetString("session")
	wait, _ := cmd.Flags().GetDuration("wait")

	remote, _, err := flowdebug.AttachRemote(cmd.Context(), newWorkflowServiceClient(serverFlagsOf(cmd)), args[0], runID,
		flowdebug.RemoteOptions{SessionID: session, Wait: 5 * time.Second})
	if err != nil {
		return err
	}
	// One command, then leave the session as it was found: attached.
	defer func() { _ = remote.Disconnect() }()

	driver := flowdebug.NewDriver(remote)
	driver.Wait = wait
	line := strings.Join(args[1:], " ")
	result, err := driver.Do(cmd.Context(), line)
	if err != nil {
		return err
	}
	if receipt := result.Receipt; receipt != nil && receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED &&
		receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE && receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		_ = writeDriveResult(newSurface(cmd).Out, format, line, result)

		return errors.New("the command was not applied: " + strings.TrimSpace(flowdebug.FormatReceipt(receipt)))
	}

	return writeDriveResult(newSurface(cmd).Out, format, line, result)
}

// writeDriveResult writes one command's answer: its rendering, or the typed
// messages it carried as one JSON object.
func writeDriveResult(out io.Writer, format OutputFormat, command string, result *flowdebug.DriveResult) error {
	if !format.Machine() {
		_, err := fmt.Fprint(out, result.Text)

		return err
	}

	parts := []string{fmt.Sprintf("%q:%q", "command", command)}
	for _, part := range []struct {
		name    string
		message proto.Message
		present bool
	}{
		{"receipt", result.Receipt, result.Receipt != nil},
		{"snapshot", result.Snapshot, result.Snapshot != nil},
		{"inspect", result.Inspect, result.Inspect != nil},
	} {
		if !part.present {
			continue
		}
		encoded, err := v1.MarshalSchemaJSON(part.message, false)
		if err != nil {
			return err
		}
		parts = append(parts, fmt.Sprintf("%q:%s", part.name, encoded))
	}

	_, err := fmt.Fprintf(out, "{%s}\n", strings.Join(parts, ","))

	return err
}

func newDebugGetRequest(workflowID, runID string) *connect.Request[v1.DebugGetRequest] {
	return connect.NewRequest(&v1.DebugGetRequest{WorkflowId: workflowID, RunId: runID})
}
