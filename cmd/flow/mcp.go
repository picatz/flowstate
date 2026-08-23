package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
)

// `flow mcp` is this binary's own wiring around the protocol surface.
//
// The surface itself — tool derivation from the schema, RPC dispatch, resource
// and UI registration — lives in cmd/flow/internal/mcp (#410), because none of
// it needs a flag, an egress policy, or a secret: it is the part of this file
// with a plausible non-CLI caller. What stays here is the two tools that are
// not RPCs, because both execute against this binary's own flags — egress,
// secrets, plugins — which only the command line can supply.
//
// Three tools answer locally and the rest speak to a server, split by what the
// method needs. Validate, Compile and GetCatalog touch no run and no tenant — the
// server's own handlers take a nil Temporal client, which is the proof — so
// they run in-process and an agent gets a working authoring loop with nothing
// else stood up. The lifecycle verbs address durable runs, which only a server
// has; without --address they explain that rather than failing opaquely.
//
// GetCatalog is the one exception to "in-process, always": with --address or
// FLOWSTATE_ADDRESS explicitly naming a deployment, it answers from that
// deployment instead (#439). The deployment is what will actually run a
// submitted workflow and may have plugins or a version this binary does not,
// so an agent authoring against this binary's own build would write a file
// that validates here and is refused on submission. It refuses rather than
// falling back to the local answer when that deployment cannot be reached —
// this repository fails closed, and a silent fallback here would be the
// identical defect one level up: an answer that looks like the deployment's
// and is not. Validate and Compile stay unconditionally local: neither
// consults a task registry (flowfile's own validator deliberately does not,
// per CLAUDE.md's "report what is a property of the file, and stay silent
// about what a deployment decides"), so there is no deployment-shaped answer
// for either to diverge toward.

// remoteCatalogAddressFor decides whether flowstate_get_catalog should
// dispatch to the addressed deployment rather than answering from this
// binary's own build — see [flowmcp.Deps.RemoteCatalogAddress] and #439.
//
// An operator names a deployment two ways — --address on the command line or
// FLOWSTATE_ADDRESS in the environment (addServerFlags reads the latter into
// the former's default at declaration, so a flag left untouched still carries
// it) — and either one counts. cmd.Flags().Changed alone would miss the
// common case of an environment-configured deployment with no flag typed;
// reading the environment variable directly, rather than comparing the flag's
// resolved value against defaultServerAddress, is what keeps this from
// mistaking a deployment that genuinely runs at the default host:port for
// "nothing was configured".
//
// A helper of its own rather than inlined in runMCP so the decision is
// testable without standing up the stdio server — see
// TestRemoteCatalogAddressForRespectsExplicitAddress.
func remoteCatalogAddressFor(cmd *cobra.Command, flags serverFlags) string {
	if cmd.Flags().Changed("address") || os.Getenv("FLOWSTATE_ADDRESS") != "" {
		return flags.address
	}

	return ""
}

// runMCP implements the mcp sub-command.
func runMCP(cmd *cobra.Command, args []string) error {
	flags := serverFlagsOf(cmd)

	// The execution posture is decided here, once, before a client can call
	// anything — which is the whole of "per-call escalation is impossible". The
	// tool's arguments carry a workflow and its signals and nothing else; every
	// lever that widens what a run may reach is a flag on this process.
	//
	// Both of these refuse the process rather than the call. A server that came up
	// with an unloadable egress policy would serve run_local under the default one
	// while its operator believes the file applies, which is the fail-open the
	// flag exists to prevent.
	if err := applyMCPEgressPolicy(cmd); err != nil {
		return err
	}
	// #187's task-shape policy takes the same zero case here as everywhere
	// else: nothing configured restricts nothing. Unlike egress there is no
	// MCP-specific stricter default to fall back to — a task-shape policy
	// governs which *tasks* a workflow may dispatch, not which network
	// addresses a running task may reach, so a model composing a workflow is
	// not a materially different caller than a person running the same
	// workflow through `flow run local`.
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}
	if _, err := localWorkloadIdentity(cmd); err != nil {
		return err
	}

	// Launched here, once, before the first tool call can arrive — never per
	// call, and never from anything but this command's own --plugin-dir, for
	// the reasons given where the flag is declared in main.go. nil rather than
	// a secret registry: the plugin registration server.go's own runServer
	// passes secretProviders for is what lets a *worker* resolve a secret
	// scheme a plugin claims, and this process has the same secret backend
	// flowstate_run_local already takes through --secret-env/--secret-dir,
	// wired separately in withLocalTaskRuntime per call.
	_, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		return err
	}
	defer closePlugins()

	// See runLSP's identical line: a person who types `flow mcp` at a terminal,
	// following the root help's own example, is owed an account of why nothing
	// is happening rather than silence. Gated on stdin being a terminal and
	// written to stderr, so an agent host's pipe never sees it and the MCP
	// stream on stdout is never touched (picatz/flowstate#398).
	//
	// Last of the fallible setup, not first: every refusal above this line
	// exits the process, and every one of them is a posture decision made
	// once so that per-call escalation is impossible. Announcing readiness
	// before an egress policy has loaded would name a state this command has
	// not reached and may never reach.
	writeStdioBanner(cmd.ErrOrStderr(), stdinIsInteractive(cmd), mcpBanner)

	// Constructed lazily and at most once, so the local tools never dial and the
	// remote ones share a client.
	var remote flowstatev1connect.WorkflowServiceClient
	remoteClient := func() flowstatev1connect.WorkflowServiceClient {
		if remote == nil {
			remote = newWorkflowServiceClient(flags)
		}

		return remote
	}

	// The local half is the server's own handlers over no Temporal client: one
	// implementation of Validate, whoever calls it. See server/validate.go for
	// why a nil client is safe for exactly these two methods.
	local, err := server.New(nil)
	if err != nil {
		return err
	}

	deps := flowmcp.Deps{
		Redact: func(response *v1.GetResponse) *v1.GetResponse {
			return redactGetResponse(response, nil, revealSensitiveRequested(cmd))
		},
	}

	deps.RemoteCatalogAddress = remoteCatalogAddressFor(cmd, flags)
	extra := []flowmcp.ToolRegistration{
		{Tool: flowmcp.RunLocalTool(), Handler: runLocalToolHandler(cmd)},
		// No timeout: stdio's single caller is the process that launched this
		// one, and this surface is unchanged by the bound `flow mcp serve`
		// applies for its own reasons. See [testToolHandler].
		{Tool: flowmcp.TestTool(), Handler: testToolHandler(0)},
	}

	return flowmcp.ServeTools(cmd.Context(), flowmcp.NewServer(version), local, remoteClient, deps, extra...)
}

// The one tool that is not an RPC.
//
// Everything else on this surface projects a service method, and run_local
// deliberately does not: it is the local driver, in this process, with no server
// and no Temporal — the same thing `flow run local` is. Making it an RPC would
// have made it a *service* capability, which is the opposite of what it is for.
//
// It exists because the authoring loop dead-ended at compile. An agent could
// write a Flowfile, be told it was valid, be handed a specification — and had
// nowhere to run it, so the first execution of anything it wrote was durable, on
// somebody's cluster. Now the loop closes where the author is.
//
// What it must never become is a way to widen this process. Egress and secrets
// are decided by the flags `flow mcp` was started with; the arguments carry a
// workflow and its signals, and there is deliberately no argument that unlocks
// anything. See runLocalPosture for the flags, and note there is no path from
// [runLocalArguments] to any of them.

// maxRunLocalLogRecords bounds how many `log:` lines are carried back.
//
// Same reason [flowmcp.MaxResultBytes] gives one level up, and bounded by count
// rather than bytes because a loop is how this gets large: the run controls how
// many records there are, and each one is small.
const maxRunLocalLogRecords = 200

// runLocalArguments is the tool's whole input surface.
//
// Two fields, and the absence of a third is the design: there is no vars, no
// egress override, no secret argument. Workflow variables come from the file,
// which is where an author would put them, and everything that governs what a
// run may reach is process configuration.
type runLocalArguments struct {
	// Source is the Flowfile YAML, exactly as it would be written to disk.
	Source string `json:"source"`

	// Signals answers wait_for_signal steps up front, mirroring
	// `flow run local --signal name=json`. A local run is a process, so there is
	// nobody to signal it once it starts; the waiter buffers these so a gate
	// reached later still finds its answer, which is what Temporal does for a
	// durable run.
	Signals map[string]json.RawMessage `json:"signals,omitempty"`

	// Inputs are the arguments the run is started with, keyed by the name the
	// submitted source declares under `inputs:`.
	//
	// A JSON object, which is what `--input-file` takes, rather than the
	// `name=value` words a shell hands over: the caller here is composing a
	// document and already has types. It goes through the same decoder and the
	// same binder, so an argument means one thing on both surfaces.
	//
	// Unlike a signal, this is not an escape hatch around the file — it is the
	// file's own contract. A name the source does not declare is refused, with
	// the declared names listed, before any step runs.
	Inputs map[string]json.RawMessage `json:"inputs,omitempty"`
}

// addLocalRunFlags declares the flags that decide what a run_local call may do.
//
// The same opt-ins `flow run local` takes, at the one moment a long-lived
// process can take them: start-up. A client speaks to this over stdio and never
// gets to choose any of it.
//
// The subset is deliberate. --output has no meaning when the answer is a tool
// result, and --signal has no meaning when signals arrive per call; everything
// that decides *reach* — egress, secrets, the identity policy is rehearsed as —
// is here, because leaving one out would silently make the rehearsal weaker than
// the worker it rehearses.
func addLocalRunFlags(cmd *cobra.Command) {
	addEgressPolicyFlag(cmd)
	addTaskPolicyFlag(cmd)
	addSecretFlags(cmd)

	cmd.Flags().String("as-subject", "local-user",
		"authenticated subject to rehearse policy as (local runs only)")
	cmd.Flags().String("as-issuer", "flowstate:local",
		"authenticated issuer to rehearse policy as (local runs only)")
	cmd.Flags().String("as-namespace", "",
		"tenant namespace to rehearse policy as (local runs only)")
	cmd.Flags().String("as-deployment", "local",
		"Flowstate deployment name to rehearse policy as (local runs only)")
	cmd.Flags().StringArray("as-claim", nil,
		"authenticated string claim NAME=VALUE to rehearse policy as (repeatable)")
	cmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to an access policy whose secrets rules authorize local runs served to an agent")
	cmd.Flags().String("identity-key", os.Getenv("FLOWSTATE_IDENTITY_KEY"),
		"PKCS#8 PEM key used to mint short-lived workload assertions for federation targets")

	// A bound `flow run local` does not need and this does.
	//
	// There, a workload that waits on a gate nobody will answer is a terminal a
	// person can see and interrupt. Here it is a tool call that never returns,
	// holding a model's turn open for as long as the workflow asks — and the
	// workflow is the untrusted input. `sleep: 24h` is a legal Flowfile.
	cmd.Flags().Duration("run-local-timeout", 2*time.Minute,
		"how long a flowstate_run_local call may execute for before the run is stopped and reported as timed out")

	// Decided at process start-up rather than per call, for the same reason
	// every other flag in this function is: a client speaks to this over stdio
	// and never gets to choose per-call. An operator who starts `flow mcp
	// --reveal-sensitive` is making one deliberate, written-down decision that
	// every call this process serves shows declared-sensitive values in the
	// clear — never a default, and never something a tool argument can turn on.
	addRevealSensitiveFlag(cmd)
}

// defaultLocalRunPosture is the deny-by-default posture as a flag set, for
// callers that have no command to read one from.
//
// Every flag at its declared default, which is the configuration an operator
// gets by running `flow mcp` with no arguments. Tests use it so that what they
// exercise is that posture rather than one assembled for the occasion.
func defaultLocalRunPosture() *cobra.Command {
	cmd := &cobra.Command{Use: "mcp"}
	addLocalRunFlags(cmd)

	return cmd
}

// applyMCPEgressPolicy installs the egress policy run_local executes under.
//
// With --egress-policy it is that file, loaded exactly as the worker and
// `flow run local` load it. Without one it is *deny everything*, which is
// stricter than the CLI's default and deliberately so.
//
// The difference is who is holding the keyboard. `flow run local` is run by the
// person who wrote the file, on their own machine, against a default policy that
// blocks internal ranges and metadata endpoints but allows the public internet —
// a reasonable position for someone exercising their own workflow. `flow mcp`
// serves a *model* the ability to compose a workflow and have this process fetch
// a URL of its choosing, which is a different capability wearing the same clothes.
// So egress is a thing an operator turns on, in writing, naming what may be
// reached: an empty allowlist is the honest starting point for a surface whose
// caller is not a person.
//
// The rule is a deny rule rather than an empty allowlist because it is evaluated
// before the address is resolved, so a denied request performs no DNS lookup —
// the same reason validation refuses to resolve hosts on an author's keystroke
// path.
func applyMCPEgressPolicy(cmd *cobra.Command) error {
	if path, _ := cmd.Flags().GetString("egress-policy"); path != "" {
		return applyEgressPolicy(cmd)
	}

	policy, err := netpolicy.New(netpolicy.WithDenyRules("true"))
	if err != nil {
		return fmt.Errorf("building the deny-by-default egress policy for flowstate_run_local: %w", err)
	}

	if err := v1.DefaultRegistry().Register(v1.HTTPTaskDef(policy)); err != nil {
		return fmt.Errorf("registering the http task for flowstate_run_local: %w", err)
	}

	return nil
}

// runLocalToolHandler executes one submitted workflow.
//
// posture carries the process's flags — the only place a run's reach comes from.
func runLocalToolHandler(posture *cobra.Command) mcp.ToolHandler {
	if posture == nil {
		posture = defaultLocalRunPosture()
	}

	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args runLocalArguments

		if raw := req.Params.Arguments; len(raw) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))

			// The mirror of the schema's additionalProperties:false, and refused
			// for the same reason the RPC tools refuse an unknown field: an
			// argument silently dropped is a tool that "worked" while doing
			// something other than what was asked.
			decoder.DisallowUnknownFields()

			if err := decoder.Decode(&args); err != nil {
				return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", flowmcp.RunLocalToolName, err)), nil
			}
		}

		if strings.TrimSpace(args.Source) == "" {
			return flowmcp.ToolError(errors.New(
				"source is required: pass the Flowfile YAML to execute, e.g. \"edition: v2026.3\\nname: demo\\nsteps:\\n- id: hi\\n  log:\\n    message: hello\"")), nil
		}

		workflow, err := parseFlowfileSource([]byte(args.Source))
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		signals, err := runLocalSignalFlags(args.Signals)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		// Bound before the timeout is started and before any provider is opened,
		// because an argument that does not satisfy the source's `inputs:` is a
		// fact about the call rather than about the run — and the refusal is the
		// binder's own text, which is what an agent needs in order to correct the
		// call rather than the workflow.
		inputs, err := runLocalToolInputs(workflow, args.Inputs)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		timeout, _ := posture.Flags().GetDuration("run-local-timeout")
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ctx, err = withLocalSignals(ctx, posture, workflow, inputs, signals)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		ctx, closeSecretProviders, err := withLocalTaskRuntime(posture, ctx, workflow)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		defer closeSecretProviders()

		// `log:` steps go into the answer rather than onto a stream, and that is
		// not a nicety: stdout is the MCP transport. A workflow that narrates
		// itself must not write into the protocol, and a workflow's narration is
		// exactly what an agent needs to see to debug it — so it is collected and
		// returned as data.
		logs := newRunLocalLogs()
		ctx = v1.ContextWithLogger(ctx, slog.New(logs))

		started := time.Now()
		outputs, runErr := v1.RunWithInputs(ctx, workflow, inputs)

		// ctx here is the run's own deadline, which is what distinguishes a step
		// that timed out from a run that did: a step's `timeout:` expires an inner
		// context, leaving this one clean, and the run is reported FAILED. Only
		// this context expiring means the call ran out of time.
		response := localRun(outputs, runErr, ctx.Err(), started, time.Now())

		// An agent's context is an untrusted-consumer surface exactly like a
		// terminal — a leaked credential in a transcript is a leaked credential —
		// so this tool result honours `sensitive:` the same way `flow run local`
		// does. workflow was just parsed from the submitted source, so redaction
		// here is precise against its own declarations rather than the
		// fail-closed case a spec-less renderer falls back to; see sensitive.go.
		response = redactGetResponse(response, workflow, revealSensitiveRequested(posture))

		encoded, err := renderRunLocalResult(response, logs.records())
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		return &mcp.CallToolResult{
			// A failed run is still an answer — the document carries the status
			// and the reason — but it is flagged, because a model that cannot tell
			// a run that failed from one that succeeded will report success.
			IsError: runErr != nil,
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// The other tool that is not an RPC.
//
// flowstate_run_local closed the loop for a workflow that reaches the real
// network — once an operator opted this process into it. It left a colder
// dead end behind: an `http:` step, or any other task, denied by the
// deny-by-default egress this process starts under (see
// [applyMCPEgressPolicy]) proves only that the file parses. There was no way
// over MCP to verify a condition, a retry, or a data-flow expression at all,
// which is the repo's own "complete, tested, and impossible to use" pattern
// applied to an agent (#241): `flow test` already answers this on a
// developer's machine, and nothing served it here.
//
// flowstate_test is that tool, and it needs neither egress nor an operator
// opt-in — not because it is trusted with less, but because a stubbed run
// cannot reach anything. See [flowtest.caseRegistry] in
// pkg/flowstate/v1/flowtest/run.go: every task this build registers, real or
// plugin, has its Fn replaced before a step of the submitted workflow ever
// runs — by a stub's canned answer, or by a fail-closed refusal naming the
// unstubbed task — so no task's real implementation executes, whatever this
// process was started with. A `${secret(...)}` reference fails the same way,
// for an independent reason: [v1.ResolveSecret] refuses unless
// [v1.ContextWithTaskRuntime] installed a store and a policy on the context
// first, and nothing on this path ever does. Both claims are exercised by
// TestTheTestToolStubsMakeNoRequest and TestTheTestToolNeedsNoEgressPolicy.

// testToolArguments is the tool's whole input surface: a workflow and the
// test cases to run against it, both inline — the seam
// [flowtest.RunSource] adds over [flowtest.RunFile] so this tool can offer
// bytes where the CLI takes two paths.
type testToolArguments struct {
	// Workflow is the Flowfile YAML under test, exactly as it would be
	// written to disk.
	Workflow string `json:"workflow"`

	// Tests is a `*.test.yaml` document naming the cases to run against
	// Workflow. See [flowtest.LoadSource] for why a case's own `workflow:`
	// is accepted but never consulted here.
	Tests string `json:"tests"`
}

// testToolHandler runs the submitted cases and reports one v1.TestReport.
//
// Unlike [runLocalToolHandler] this takes no posture: there is no flag on
// this surface that changes what a stubbed run may do, because a stubbed run
// never reaches anything a flag could govern in the first place. See the
// package comment above [flowmcp.TestToolName].
func testToolHandler(timeout time.Duration) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args testToolArguments

		if raw := req.Params.Arguments; len(raw) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))

			// The mirror of the schema's additionalProperties:false, for the
			// same reason every other tool on this surface refuses an unknown
			// field: an argument silently dropped is a tool that "worked"
			// while doing something other than what was asked.
			decoder.DisallowUnknownFields()

			if err := decoder.Decode(&args); err != nil {
				return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", flowmcp.TestToolName, err)), nil
			}
		}

		if strings.TrimSpace(args.Workflow) == "" {
			return flowmcp.ToolError(errors.New(
				"workflow is required: pass the Flowfile YAML under test, e.g. " +
					"\"edition: v2026.3\\nname: demo\\nsteps:\\n- id: hi\\n  log:\\n    message: hello\"")), nil
		}
		if strings.TrimSpace(args.Tests) == "" {
			return flowmcp.ToolError(errors.New(
				"tests is required: pass a *.test.yaml document naming at least one case, e.g. " +
					"\"tests:\\n  - name: it runs\\n    expect:\\n      failed: false\"")), nil
		}

		// timeout <= 0 is stdio's posture, unchanged: one trusted caller, at a
		// terminal or behind an agent host it launched, who can interrupt a
		// case the virtual clock cannot advance past. A positive timeout is
		// what `flow mcp serve` passes, because there the submitted workflow
		// is untrusted input and a `wait_for_signal:` with no timeout and no
		// scripted signal is a legal Flowfile that never completes — see
		// [flowtest.RunSourceContext].
		runCtx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			// Derived from the request's context, so a caller that
			// disconnects also ends the run rather than leaving it to burn
			// the whole budget with nobody left to answer.
			runCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		report := flowtest.RunSourceContext(runCtx, "<submitted>", []byte(args.Workflow), []byte(args.Tests))

		// A serving deadline must never be readable as a workflow's own
		// failure. [flowtest] compares a case's `expect.failed` against
		// whether the run returned an error, and a cancelled context produces
		// one — so a case that expected failure would *pass* on a workflow
		// that never completed, every later case would run against an already
		// expired context and pass the same way, and the tool would answer
		// success. The report is therefore discarded outright when the bound
		// this handler imposed is what ended the run: an answer about cases
		// that were never really run is worse than no answer. Reported by
		// Codex on picatz/flowstate#807.
		if runCtx.Err() != nil {
			return flowmcp.ToolError(fmt.Errorf(
				"the submitted tests did not finish within %s and were stopped, so no verdict is "+
					"reported: a case that never completes is usually a `wait_for_signal:` with no "+
					"`timeout:` and no stub scripting its signal, which parks the virtual clock with "+
					"no deadline to advance to. Script the signal, give the wait a timeout, or split "+
					"the file into smaller cases", timeout)), nil
		}

		encoded, err := renderTestResult(report)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		return &mcp.CallToolResult{
			// The same reason [runLocalToolHandler] flags a failed run: a model
			// that cannot tell a suite that failed from one that passed will
			// report success. testReportFailed reads the whole report — a case
			// that failed, or a `tests` document [flowtest.LoadSource] refused
			// outright before any case ran — either way.
			IsError: testReportFailed(report),
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// testReportFailed reports whether report should flag the tool result as an
// error: the submitted `tests` document was refused outright, or at least one
// case did not pass.
func testReportFailed(report *v1.TestReport) bool {
	if report.GetRefused() != "" {
		return true
	}
	for _, c := range report.GetCases() {
		if !c.GetPassed() {
			return true
		}
	}

	return false
}

// maxTestFailureMessageBytes bounds one diagnostic's Message when the whole
// answer needs shrinking — see [renderTestResult]. A diagnostic's message
// compares a case's `expect.outputs` against what the run actually produced
// ([flowtest]'s compareOutputs formats both sides with %v), so the message is
// the one part of a TestReport a case's own stubs or a workflow's own
// computed values can make large; everything else (case names, verdicts,
// durations, how many cases and stubs a file may declare) is already bounded
// by [flowtest.MaxTestsPerFile] and [flowtest.MaxStubsPerTest] before a case
// ever runs.
const maxTestFailureMessageBytes = 4 << 10

// renderTestResult brings a v1.TestReport under [flowmcp.MaxResultBytes] —
// [renderRunLocalResult]'s own bound and its own discipline, reused rather
// than reinvented: stop at a document that still parses, and say what left
// rather than truncating bytes into something a caller cannot decode. The
// steps differ because what a workflow can make large differs between the two
// answers — run_local's is step outputs and log lines, this one is
// diagnostic messages built by comparing them — but the bound, the shape of
// the ladder, and the floor that is returned whether or not it fits are the
// same ones [renderRunLocalResult] already established.
func renderTestResult(report *v1.TestReport) ([]byte, error) {
	trimmed, ok := proto.Clone(report).(*v1.TestReport)
	if !ok {
		return nil, errors.New("rendering the report: the report is not a TestReport")
	}

	encoded, _, err := flowmcp.FitResult(
		func() ([]byte, error) {
			encoded, err := marshalJSON(report, false)
			if err != nil {
				return nil, fmt.Errorf("rendering the report: %w", err)
			}

			return encoded, nil
		},

		// First, cap every failure's own message: a mismatch's %v of a large
		// stubbed or computed value is the one part of this document a case
		// controls the size of, and capping it keeps every case, every verdict,
		// and every field/step/value a diagnostic named.
		func() ([]byte, error) {
			for _, c := range trimmed.GetCases() {
				for _, f := range c.GetFailures() {
					if len(f.GetMessage()) > maxTestFailureMessageBytes {
						// ToValidUTF8 because the cut lands wherever the byte
						// count does, and protojson refuses to marshal a string
						// field holding invalid UTF-8 — slicing mid-rune would
						// turn a large answer into an encoding error, which is
						// the failure this ladder exists to replace arriving by
						// a different door.
						f.Message = strings.ToValidUTF8(f.GetMessage()[:maxTestFailureMessageBytes], "") +
							fmt.Sprintf("... (truncated, exceeded %d bytes)", maxTestFailureMessageBytes)
					}
				}
			}

			encoded, err := marshalJSON(trimmed, false)
			if err != nil {
				return nil, fmt.Errorf("rendering the report: %w", err)
			}

			return encoded, nil
		},

		// Still too big — enough cases with enough failures each that even capped
		// messages do not fit. Report per-case verdicts only, dropping the
		// diagnostics themselves down to a count: a report with no verdicts at
		// all is worse than no answer, so this floor is returned whether or not
		// it fits, the same reasoning [renderRunLocalResult]'s own last rung
		// gives for the fields nothing further can drop.
		func() ([]byte, error) {
			summary := &v1.TestReport{File: report.GetFile(), Refused: report.GetRefused()}
			for _, c := range trimmed.GetCases() {
				caseError := c.GetError()
				if caseError == "" && len(c.GetFailures()) > 0 {
					caseError = fmt.Sprintf(
						"%d failure(s); their diagnostics were dropped because the answer exceeded %d bytes",
						len(c.GetFailures()), flowmcp.MaxResultBytes)
				}
				summary.Cases = append(summary.Cases, &v1.TestCase{
					Name:     c.GetName(),
					Passed:   c.GetPassed(),
					Duration: c.GetDuration(),
					Error:    caseError,
				})
			}

			encoded, err := marshalJSON(summary, false)
			if err != nil {
				return nil, fmt.Errorf("rendering the report: %w", err)
			}

			return encoded, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

// parseFlowfileSource compiles submitted YAML into a workflow, reporting
// diagnostics as text.
//
// The same two passes [loadWorkflow] performs, on bytes that never touch a disk:
// parse, then validate, and refuse to execute a file with any diagnostic. The
// diagnostics are joined verbatim because each one already begins with its
// position — line:column — and position is the whole reason to return them to
// something that is about to rewrite the file.
//
// Source submitted this way has no location of its own, so a `call:` step in it
// is refused with a diagnostic saying so rather than resolved — there is no
// directory to resolve it relative to, and inventing one would make the answer
// depend on a path nobody submitted.
func parseFlowfileSource(source []byte) (*v1.Workflow, error) {
	workflow, err := flowfile.Unmarshal(source)
	if err != nil {
		return nil, fmt.Errorf("the submitted source is not a valid Flowfile: %w", err)
	}

	diagnostics, err := flowfile.ValidateSource(source)
	if err != nil {
		return nil, fmt.Errorf("validating the submitted source: %w", err)
	}
	if len(diagnostics) > 0 {
		lines := make([]string, 0, len(diagnostics)+1)
		lines = append(lines, "the submitted Flowfile has problems and was not executed:")
		for _, d := range diagnostics {
			lines = append(lines, "  "+d.Error())
		}

		return nil, errors.New(strings.Join(lines, "\n"))
	}

	return workflow, nil
}

// runLocalSignalFlags renders the tool's signals as the flags the CLI takes.
//
// Through `--signal name=json` rather than beside it, so a payload means exactly
// the same thing whichever way it was supplied — the divergence this repository
// keeps refinding is a value written down twice, and a second signal parser is
// how that starts.
//
// A name carrying `=` is refused rather than cut at it. Signal names are letters,
// digits, `-` and `_` by schema, so such a name can never match a wait step: the
// choice is between a clear refusal and a signal delivered under a name the
// author did not write.
func runLocalSignalFlags(signals map[string]json.RawMessage) ([]string, error) {
	flags := make([]string, 0, len(signals))
	for name, payload := range signals {
		if strings.Contains(name, "=") {
			return nil, fmt.Errorf("signal name %q contains '=': a signal name is the one its "+
				"wait_for_signal step declares: a letter or digit, then letters, digits, - or _", name)
		}

		flags = append(flags, name+"="+string(payload))
	}

	// Sorted so a failure reports the same signal each time, whatever order the
	// arguments were decoded in.
	slices.Sort(flags)

	return flags, nil
}

// runLocalToolInputs binds the tool's `inputs` object against the submitted
// source's declarations.
//
// Reassembled into one document and handed to [inputsFromJSON] rather than
// converted here, so this surface and `--input-file` read a value through one
// decoder: the same reason [runLocalSignalFlags] renders signals as the flags the
// CLI already parses. An agent and a person composing the same arguments get the
// same run, or the same refusal.
//
// The refusal is checked here rather than left to the driver for the reason the
// CLI checks early: [v1.RunWithInputs] binds authoritatively a moment later, and
// its error would arrive wrapped in an account of a run that never started.
func runLocalToolInputs(workflow *v1.Workflow, submitted map[string]json.RawMessage) (map[string]*v1.Value, error) {
	if len(submitted) == 0 {
		// Absent rather than empty, so a source declaring no `inputs:` is run
		// exactly as it is without this argument.
		return nil, checkToolRunInputs(workflow, nil)
	}

	document, err := json.Marshal(submitted)
	if err != nil {
		return nil, fmt.Errorf("reading the inputs argument: %w", err)
	}

	inputs, err := inputsFromJSON("the inputs argument", document, declaredInputs(workflow))
	if err != nil {
		return nil, err
	}

	return inputs, checkToolRunInputs(workflow, inputs)
}

// checkToolRunInputs is [checkRunInputs] with the CLI's closing advice replaced by
// this surface's, since an agent has no flags to correct.
func checkToolRunInputs(workflow *v1.Workflow, inputs map[string]*v1.Value) error {
	if _, err := v1.BindRunInputs(workflow, inputs); err != nil {
		return fmt.Errorf("%w\n  arguments go in the `inputs` object of this call, keyed by the name the "+
			"source declares under `inputs:`", err)
	}

	return nil
}

// runLocalResult is the document the tool answers with.
//
// The run is carried verbatim as the protojson of a GetResponse — the same bytes
// `flow run local -o json` writes and the same message flowstate_get answers
// with — so one expression reads a rehearsal and a production run alike. It is
// wrapped rather than extended because logs are not part of that schema, and
// inventing a field for them would make this surface a second dialect.
type runLocalResult struct {
	Run  json.RawMessage     `json:"run"`
	Logs []runLocalLogRecord `json:"logs,omitempty"`
	Note string              `json:"note,omitempty"`
}

// renderRunLocalResult assembles the answer and brings it under the cap.
//
// Shrinking is in order of what a reader can most afford to lose, and it stops
// at a document that still parses. Cutting the JSON at the limit would produce
// bytes no caller can read, which converts a large answer into no answer at all;
// dropping a part and *saying so* leaves the status and the reason — the two
// things a model needs to decide what to do next — intact.
// The rungs, and the floor's own reason for being returned whether or not it
// fits, are unchanged; what left is [flowmcp.FitResult] doing the measuring, so
// the "re-encode, re-measure, stop at the first that fits" discipline this
// function used to spell out by hand is the same code the other two shrinking
// answers on this surface run.
func renderRunLocalResult(response *v1.GetResponse, logs []runLocalLogRecord) ([]byte, error) {
	run, err := marshalJSON(response, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the run: %w", err)
	}

	trimmed, ok := proto.Clone(response).(*v1.GetResponse)
	if !ok {
		return nil, errors.New("rendering the answer: the run is not a GetResponse")
	}

	encoded, _, err := flowmcp.FitResult(
		func() ([]byte, error) {
			encoded, err := json.Marshal(runLocalResult{Run: run, Logs: logs})
			if err != nil {
				return nil, fmt.Errorf("rendering the answer: %w", err)
			}

			return encoded, nil
		},

		// First the logs, which are commentary on the outputs.
		func() ([]byte, error) {
			encoded, err := json.Marshal(runLocalResult{
				Run:  run,
				Note: fmt.Sprintf("logs were dropped: the answer exceeded %d bytes", flowmcp.MaxResultBytes),
			})
			if err != nil {
				return nil, fmt.Errorf("rendering the answer: %w", err)
			}

			return encoded, nil
		},

		// Then the step transcript, keeping the status, the timing, any error, and
		// the run's declared outputs — a run reported without its transcript is
		// still an answer; an unparsable document is not.
		//
		// Reduced rather than cleared. `GetResponse.kind` is a required oneof, so
		// clearing it answers with a document protojson accepts and v1.Validate
		// rejects; the transcript arm keeps a bounded subset of its real steps
		// instead. See [flowmcp.ReduceTranscript], which is the same reduction
		// flowstate_get performs, for why nothing is synthesized to stand in for
		// what was omitted. Found by review on #853, in this ladder as well as in
		// the one that borrowed its shape.
		func() ([]byte, error) {
			note := "the step outputs and logs were dropped"

			if outputs := trimmed.GetOutputs(); outputs != nil {
				kept, total := flowmcp.ReduceTranscript(outputs)
				note = fmt.Sprintf(
					"the logs were dropped and the step outputs reduced to %d of their %d steps",
					kept, total)
			}

			return renderTrimmedRun(trimmed, fmt.Sprintf(
				"%s: the answer exceeded %d bytes. "+
					"Have the workflow carry less, or read the values it needs in a step of its own",
				note, flowmcp.MaxResultBytes))
		},

		// Last, what the workflow declared it answers with. This is the most
		// valuable part of the document and so the last to go — but it is chosen by
		// the same submitted workflow as everything above it, so a single `outputs:`
		// expression building a megabyte of string is enough to carry a run past the
		// cap on its own. Dropping the transcript while leaving this untouched was
		// the hole: the cap bounded the part a workflow was least able to abuse.
		//
		func() ([]byte, error) {
			// Both places a run's declared outputs can live — see
			// [flowmcp.DropDeclaredOutputs]. A local run carries them nested in
			// the transcript arm, so the bare `trimmed.RunOutputs = nil` this
			// replaced dropped nothing at all here once the rung above stopped
			// clearing the arm outright.
			flowmcp.DropDeclaredOutputs(trimmed)

			return renderTrimmedRun(trimmed, fmt.Sprintf(
				"the declared outputs, step outputs and logs were dropped: the answer exceeded %d bytes. "+
					"Read what the run produced with `flow get`, or have the workflow answer with less",
				flowmcp.MaxResultBytes))
		},

		// The floor, and the rung that says "possibly a failure message" was not
		// good enough. A run's failure message is workload-chosen and unbounded
		// in the schema, so a local run that failed with a megabyte of error text
		// walked every rung above as a no-op and came back over the ceiling —
		// the same defect review found in flowstate_get's ladder (#853), in the
		// ladder that one was modelled on.
		//
		// Truncated, never dropped: the reason a run failed is why anyone reads a
		// failed run at all. What remains after this is a status, two ids, two
		// timestamps and a bounded message — all bounded by the schema or by
		// [flowmcp.CapErrorMessage] rather than by the workflow — so there is
		// genuinely nothing left to drop that would not take the answer with it,
		// and it is returned whether or not it fits, which is
		// [flowmcp.FitResult]'s contract for a last rung.
		func() ([]byte, error) {
			flowmcp.CapErrorMessage(trimmed.GetError())

			return renderTrimmedRun(trimmed, fmt.Sprintf(
				"the declared outputs, step outputs and logs were dropped and this run's failure "+
					"message truncated: the answer exceeded %d bytes. Read the run in full with "+
					"`flow get`, or have the workflow answer with less",
				flowmcp.MaxResultBytes))
		},
	)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

// renderTrimmedRun encodes one shrinking step's document, with the note that
// says what left and why.
func renderTrimmedRun(trimmed *v1.GetResponse, note string) ([]byte, error) {
	run, err := marshalJSON(trimmed, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the run: %w", err)
	}

	encoded, err := json.Marshal(runLocalResult{Run: run, Note: note})
	if err != nil {
		return nil, fmt.Errorf("rendering the answer: %w", err)
	}

	return encoded, nil
}

// runLocalLogRecord is one `log:` line, as data.
type runLocalLogRecord struct {
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

// runLocalLogs collects what a run logged.
//
// A handler rather than a buffer of rendered lines, because the consumer is a
// model: the level and the fields are addressable here and would have to be
// parsed back out of prose there. It is the same decision [runLogHandler] makes
// in the other direction for a person at a terminal.
//
// Bounded, and the count is kept past the bound so a truncated collection says
// how much it is missing rather than quietly being short.
type runLocalLogs struct {
	// sink is shared by every handler derived from this one, so a line logged
	// through WithAttrs lands in the answer rather than in a copy nothing reads.
	sink  *runLocalLogSink
	attrs []slog.Attr
}

// runLocalLogSink is the collection itself, held apart from the handler because
// slog.Handler is copied by WithAttrs and the records must not be.
type runLocalLogSink struct {
	mu   sync.Mutex
	seen int
	held []runLocalLogRecord
}

// newRunLocalLogs returns an empty collector.
func newRunLocalLogs() *runLocalLogs { return &runLocalLogs{sink: &runLocalLogSink{}} }

// Enabled reports whether a level is emitted, which every level is: an author
// wrote the step, and filtering by level is a deployment's concern.
func (l *runLocalLogs) Enabled(context.Context, slog.Level) bool { return true }

// Handle records one line.
func (l *runLocalLogs) Handle(_ context.Context, record slog.Record) error {
	l.sink.mu.Lock()
	defer l.sink.mu.Unlock()

	l.sink.seen++
	if len(l.sink.held) >= maxRunLocalLogRecords {
		return nil
	}

	fields := make(map[string]string, record.NumAttrs()+len(l.attrs))
	for _, attr := range l.attrs {
		fields[attr.Key] = attr.Value.String()
	}
	record.Attrs(func(attr slog.Attr) bool {
		fields[attr.Key] = attr.Value.String()

		return true
	})
	if len(fields) == 0 {
		fields = nil
	}

	label, _ := logLabel(record.Level)
	l.sink.held = append(l.sink.held, runLocalLogRecord{
		Level:   label,
		Message: record.Message,
		Fields:  fields,
	})

	return nil
}

// WithAttrs returns a handler that also emits attrs, collecting into the same
// sink.
func (l *runLocalLogs) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &runLocalLogs{
		sink:  l.sink,
		attrs: append(append([]slog.Attr{}, l.attrs...), attrs...),
	}
}

// WithGroup returns the handler unchanged; a `log:` step's fields are flat.
func (l *runLocalLogs) WithGroup(string) slog.Handler { return l }

// records returns what was collected, with a note appended when lines were
// dropped.
func (l *runLocalLogs) records() []runLocalLogRecord {
	l.sink.mu.Lock()
	defer l.sink.mu.Unlock()

	if l.sink.seen <= len(l.sink.held) {
		return l.sink.held
	}

	return append(append([]runLocalLogRecord{}, l.sink.held...), runLocalLogRecord{
		Level: "INFO",
		Message: fmt.Sprintf("%d further log lines were dropped: a run may carry back %d",
			l.sink.seen-len(l.sink.held), maxRunLocalLogRecords),
	})
}
