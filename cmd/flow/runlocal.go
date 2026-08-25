package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Local execution and durable execution are two drivers over one execution model,
// and this CLI is where an author compares them. So they have to answer with the
// same document, and they did not.
//
// `flow run` renders through [marshalJSON], which emits unpopulated fields;
// `flow run local` rendered through a bare protojson.Marshal, which does not. One
// finished step therefore read
//
//	{"stepValues":{"hello":{"namedValues":{}}}}   from the durable driver
//	{"stepValues":{"hello":{}}}                   from the local one
//
// so `jq .stepValues.hello.namedValues` answered `{}` against production and `null`
// against the rehearsal. That is exactly the difference [marshalJSON]'s comment
// exists to remove: a missing key and a null are the same question, and only one of
// them is answerable without already knowing the schema.
//
// And `flow run local` declared no `--output` at all, so `-o json` was an
// unknown-flag error on the driver an author reaches for first — the two formats
// that exist for programs were available only from the one that needs a server.

// runLocalWorkflow executes a workload in this process and reports it.
//
// The two streams are the rule the rest of the CLI states: stdout is the answer,
// stderr is the account of it. This command is the hardest place to keep them apart,
// because it is the only verb that both narrates a run and produces its result, in
// one process, at the same time.
func runLocalWorkflow(cmd *cobra.Command, args []string) error {
	rendering, err := resolveRunRendering(cmd)
	if err != nil {
		return err
	}

	format := rendering.format

	// Before the policies load and before any plugin process starts, because
	// those are work, and the venue is announced before a run does any. The
	// counterpart line in `flow run` names an address; this one names no server
	// because there is none to name, which is the whole difference between the
	// two venues said in one word. See venue.go.
	announceVenue(cmd, localVenue())

	// Telemetry, which this command reached for and never started.
	//
	// [startTelemetry] had exactly two callers — [temporalConfig] and the RPC
	// client's constructor — and a local run touches neither: it dials no
	// Temporal cluster and makes no RPC. So every span this driver opens, and
	// every `log:` record [telemetryLogHandler] bridges below, went to the
	// global no-op provider in the one invocation that had asked for telemetry
	// by pointing OTEL_EXPORTER_OTLP_* somewhere. That is CLAUDE.md's "a
	// capability is not done until it is reachable" in its exact shape: the
	// spans existed, the tests that recorded them installed their own provider,
	// and the command a person actually types installed none.
	//
	// Before [telemetryLogHandler] is built further down, which is the ordering
	// the other two call sites already state for their own reason: an instrument
	// built ahead of this captures the no-op globals and keeps them for the life
	// of the process.
	//
	// Off unless the operator configured an endpoint — no exporter, no provider,
	// no spans — and a warning rather than a refusal when it is configured and
	// cannot start, for the reason the client states best: the command a person
	// asked for is `flow run local`, not `flow run local with tracing`, and a
	// mistyped endpoint should cost them the trace rather than the rehearsal.
	//
	// Nothing is flushed here. [main] calls [flushTelemetry] after every command
	// returns, precisely because a command that lives for a second is shorter
	// than a batch exporter's window — which is this command exactly.
	if _, err := startTelemetry(cmd.Context()); err != nil {
		log.Printf("WARNING: telemetry is configured but could not be started, "+
			"so this run emits no trace: %v", err)
	}

	// The same flag the worker takes, because a rehearsal under a different egress
	// policy rehearses a different production. A file that does not load refuses
	// the run, exactly as it refuses the worker.
	//
	// Before the plugins launch, rather than after: these three read files this
	// process was pointed at, and a policy that cannot load must refuse the
	// command without first starting somebody else's programs.
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}

	// And the same for #187's task-shape policy: a local run exists to tell
	// an author what production will do, which means rehearsing under the
	// task-shape policy a worker would enforce.
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}

	// Resolved and validated here, and applied nowhere: a local run has no
	// boundary for a codec to sit on. What this buys is that a codec which
	// cannot come up refuses the rehearsal exactly as it refuses the worker.
	// See [localPayloadCodec] for the argument, which is deliberately an
	// argument rather than an omission.
	if _, err := localPayloadCodec(); err != nil {
		return err
	}

	// The secret providers, built here rather than inside [withLocalTaskRuntime]
	// further down, because the plugin host launching below registers a plugin's
	// own secrets backends into this registry. That is the worker's order
	// (secretRegistry, startPlugins, then the runtime), and holding it here is
	// what keeps a scheme a plugin serves resolvable in the rehearsal as well as
	// in production.
	providers, err := localSecretProviders(cmd)
	if err != nil {
		return err
	}
	defer providers.close()

	// Before the file is read, because a plugin's tasks are not in the registry
	// until its process is running, and [loadWorkflow] validates: a step naming
	// `example.greet` is a diagnostic about an unknown task right up until the
	// plugin providing it is loaded. This is the same [startPlugins] the worker,
	// the server, `flow task run`, `flow plugins`, `flow mcp` and `flow lsp` all
	// call, with the same discovery hardening, the same handshake and the same
	// refusal of a directory other users can write to. A local run that discovered plugins
	// its own way would be rehearsing a different deployment.
	catalog, closePlugins, err := startPlugins(cmd, providers.registry)
	if err != nil {
		return err
	}
	defer closePlugins()

	workflow, err := loadWorkflow(args[0])
	if err != nil {
		return err
	}

	// What this process can run, against what the file says it needs. The server
	// does exactly this to a submission before it becomes durable
	// (server.go's pinPlugins), and the refusals are that function's, word for
	// word, because they are the same function: a plugin that is not installed, a
	// major version that is a different contract, a deployment below the floor
	// the file sets.
	//
	// It also pins, onto the specification this process is about to execute. The
	// worker-side admission check that pin exists for cannot say anything here,
	// because the process that resolved the pin is the process that will run the
	// steps, so there is no rolling deployment for it to catch. What the author
	// gets is the half that can be wrong on a laptop: whether the plugins their
	// file requires are the ones they have.
	if err := v1.ResolvePlugins(workflow, catalog); err != nil {
		return fmt.Errorf("resolving plugins before this run: %w", err)
	}

	// What the run is started with, read before anything happens: a command line that
	// does not satisfy the workflow's `inputs:` is a refusal about the command line,
	// and reporting it after a `log:` step has already narrated two lines would make
	// it look like the run got somewhere first.
	inputs, err := runInputs(cmd, workflow)
	if err != nil {
		return err
	}
	if err := checkRunInputs(workflow, inputs); err != nil {
		return err
	}

	// A workload that waits for a signal needs something able to deliver one, or it
	// blocks with nothing that could ever release it.
	localSignals, _ := cmd.Flags().GetStringArray("signal")

	ctx, err := withLocalSignals(cmd.Context(), cmd, workflow, inputs, localSignals)
	if err != nil {
		return err
	}
	reportUnansweredGates(cmd.ErrOrStderr(), workflow, localSignals)
	ctx, err = withLocalTaskRuntimeUsing(cmd, ctx, workflow, providers)
	if err != nil {
		return err
	}

	// `log:` steps go to stderr, where the run's own commentary already goes, so the
	// result on stdout stays a single JSON document a pipe can read. A workflow that
	// narrates itself must not break `flow run local ... | jq`.
	//
	// And to a collector when one is configured, so that the two drivers agree about
	// where a `log:` line ends up: the durable driver exports the same records from
	// the worker. The trace id agrees too, since #523's gap 3 — the local driver
	// opens the same `flowstate.task/<name>` span around the step, so the record
	// carries the trace of the step that emitted it here as well.
	surface := newSurface(cmd)
	ctx = v1.ContextWithLogger(ctx,
		slog.New(telemetryLogHandler(newRunLogHandler(surface.Err, surface.ErrTheme))))

	// The same session `flow test --debug` runs, attached to a real run: the
	// engine holds at each step boundary, and `inspect` answers through the
	// run's own evaluator and activation, so it is cost-bounded and refuses a
	// `${secret(...)}` exactly as the file's own expressions would be.
	//
	// The console joins the run's account on stderr — where `log:` steps and
	// the status pill already print — rather than taking stdout the way the
	// test verb's does. Each verb keeps the console beside its own narration,
	// and this one's narration was never on stdout: the answer is, and it
	// stays a document a pipe can read under every --output, which is why
	// none of the test verb's refusals apply here. The session observes as
	// well as gates, so each step's own account arrives at the prompt that
	// paused it.
	reveal := revealSensitiveRequested(cmd)
	if debugging, _ := cmd.Flags().GetBool("debug"); debugging {
		// A debugger is a reveal: the session narrates each step's values as
		// they complete and `inspect` reaches anything in scope, so on a
		// workflow whose declarations make the final render withhold its
		// transcript, attaching one quietly opens exactly the side channel
		// redaction closes (Codex, #1109) — and #928's own rule is that debug
		// output sits behind the same redaction, never a parallel copy. There
		// is no parallel redactor here on purpose: the honest shapes are this
		// refusal, or the explicit flag every other surface already shares.
		if decideCarriedValues(workflow, reveal) != carriedValuesShown {
			return fmt.Errorf("--debug narrates step values and evaluates expressions over them, and "+
				"%q declares sensitive inputs or outputs whose transcript this command would otherwise "+
				"withhold; add --reveal-sensitive to debug it with values shown, or drop --debug",
				workflow.GetName())
		}
		session, err := flowdebug.New(flowdebug.Options{
			In:   cmd.InOrStdin(),
			Out:  surface.Err,
			Emit: debugEmitter(surface.Err, surface.ErrTheme),
		})
		if err != nil {
			return err
		}
		// This process is about to exit either way, so the reader parking
		// costs nothing here — closed anyway because a session's owner closes
		// it, and a habit that holds only where it is load-bearing is one that
		// will be missing where it is.
		defer func() { _ = session.Close() }()

		fmt.Fprintf(surface.Err, "%s\n", surface.ErrTheme.Accent.Render(
			fmt.Sprintf("debugging %s — `help` lists the commands", workflow.GetName())))
		ctx = v1.NewContextWithDebugger(ctx, session)
		ctx = v1.NewContextWithRunObserver(ctx, session)
	}

	started := time.Now()

	// The local driver's own submit boundary, which binds the arguments and applies
	// the declared defaults exactly as the server does before a durable run starts.
	// The check above is for the message; this is the one that decides.
	outputs, runErr := v1.RunWithInputs(ctx, workflow, inputs)
	response := localRun(outputs, runErr, cmd.Context().Err(), started, time.Now())

	// This process just parsed workflow itself, so redaction here is precise
	// against its own `sensitive:` declarations rather than the fail-closed case
	// a renderer with no specification falls back to — see sensitive.go.
	if reveal {
		noteRevealedSensitiveValues(surface)
	}
	response = redactGetResponse(response, workflow, reveal)

	if runErr != nil {
		// A machine caller is owed a document about the failure, which is the half
		// of the durable driver's behaviour that was missing here: `flow run -o json`
		// on a run that fails writes a GetResponse carrying STATUS_FAILED and the
		// reason, then exits non-zero. `flow run local -o json` wrote nothing at all,
		// leaving a program that had asked for JSON to recover the reason by parsing
		// prose off stderr.
		//
		// The text shape still writes nothing, and that is not an inconsistency: an
		// empty stdout is a meaningful value there, because the answer is the outputs
		// and a failed run has none. `{}` would claim it produced none *successfully*.
		//
		// rendering.WantsDocument() rather than format.Machine() alone, so --raw with
		// the default text format is honoured on a failed local run the same way the
		// success and task-failure paths already honour it.
		if rendering.WantsDocument() {
			if err := writeRunJSON(surface, rendering, response); err != nil {
				return err
			}
		}

		return wrapLoopbackDenial(cmd, fmt.Errorf("error running workflow locally: %w", runErr))
	}

	// The same word `flow get` uses for the same outcome, through the same pill, on
	// the same stream. This was `log.Println("run completed")` — the one user-facing
	// line in the CLI that went through the standard logger, so it arrived
	// timestamped and unstyled directly beneath the themed lines a `log:` step had
	// just written. Two renderings of one program's output, a line apart.
	//
	// Only for a person, which is the rule `flow run` already follows for its own
	// prose: a machine format carries the status inside the document it writes, so
	// saying it again on another stream is a second spelling of one fact.
	if format == FormatText {
		fmt.Fprintf(surface.Err, "%s workflow %s\n",
			surface.ErrTheme.Pill(statusTone(response.GetStatus()),
				statusLabel(response.GetStatus())),
			workflow.GetName())
	}

	if err := writeRun(surface, rendering, response); err != nil {
		return fmt.Errorf("writing the outputs of %s: %w", workflow.GetName(), err)
	}

	return nil
}

// localRun is a finished local run in the shape the schema already has for one.
//
// A GetResponse rather than the bare outputs, because that is what the durable
// driver's machine formats emit and the entire point is that one jq expression
// works against both. The two identity fields stay empty, which is the honest
// answer and the real difference between the drivers: a local run is a process and
// has no durable identity to address it by. Inventing an id would hide that behind
// something a caller could try to `flow watch`.
//
// The two timestamps are the wall clock either side of the run rather than anything
// the engine reports, so they mean here what they mean in a listing: when the
// workload began and when it finished.
//
// interrupted is the command's own context after the run, and it decides between
// three different things that all arrive as a non-nil error. A run somebody stopped
// is not a fault and must not be reported as one — [statusTone] says the same thing
// about colour, and a machine consumer has only this field.
func localRun(outputs *v1.Workflow_StepOutputs, runErr, interrupted error, started, closed time.Time) *v1.GetResponse {
	response := &v1.GetResponse{
		Status:    v1.RunResponse_STATUS_COMPLETED,
		StartTime: timestamppb.New(started),
		CloseTime: timestamppb.New(closed),
	}

	if runErr != nil {
		response.Status = interruptedStatus(interrupted)

		// v1.ClassifyError, unwrapped: the local driver's error is the bare Go
		// chain the executor produced, with no activity envelope to strip and
		// no application-error Type to parse back — the durable driver's
		// failureError does that work only because Temporal's wire made it
		// necessary. Left unset for an interrupted (not failed) run — a
		// classification would claim this driver knows why a workload it
		// itself stopped went wrong.
		errorResponse := &v1.RunResponse_Error{Message: runErr.Error()}
		if response.GetStatus() == v1.RunResponse_STATUS_FAILED {
			errorResponse.Kind = v1.ClassifyError(runErr).String()
		}
		response.Kind = &v1.GetResponse_Error{Error: errorResponse}

		return response
	}

	if outputs != nil {
		// Left unset when there are none, matching [writeStepOutputs]: a `kind` of
		// `outputs` holding nothing would say the run produced no outputs
		// *successfully*, and the oneof being absent says there is no answer here.
		response.Kind = &v1.GetResponse_Outputs{Outputs: outputs}

		// The answer, beside the transcript, in the field a durable run reports it
		// in — the server copies it out of the completion payload the same way
		// (`server.go`, STATUS_COMPLETED). Without this line the declared outputs
		// would be readable at `.outputs.runOutputs` from a local run and at
		// `.runOutputs` from a durable one: one document with two spellings, which
		// is exactly the divergence [writeRun] exists to prevent.
		response.RunOutputs = outputs.GetRunOutputs()
	}

	return response
}

// interruptedStatus says why a local run stopped early, from the state of the
// command's own context rather than from the error it produced.
//
// Which context is the whole of it, and the durable driver already learned this
// lesson: [followPlainly] checks `ctx.Err()` before folding an answer in, because a
// poll cut short by ctrl+c fails like any other refusal and would otherwise be read
// as the server having stopped answering.
//
// Here the trap is sharper. A step's own `timeout:` expires an *inner* context, so
// its failure arrives wrapping [context.DeadlineExceeded] exactly as a run that ran
// out of time does — and those are different facts about different things. A step
// that timed out is a step that failed, and the run failed with it. Only the
// command's context can tell them apart, because nothing inside the engine can
// cancel it.
func interruptedStatus(interrupted error) v1.RunResponse_Status {
	switch {
	case errors.Is(interrupted, context.Canceled):
		// Somebody stopped it — ctrl+c, or a parent process going away. Not a
		// fault, and the schema has a word for it that is not FAILED.
		return v1.RunResponse_STATUS_CANCELED

	case errors.Is(interrupted, context.DeadlineExceeded):
		// The whole command was given a deadline and reached it.
		return v1.RunResponse_STATUS_TIMED_OUT

	default:
		return v1.RunResponse_STATUS_FAILED
	}
}
