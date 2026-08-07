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

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The agent surface is the machine surface, taken seriously.
//
// `flow mcp` serves the control plane to a model the way `--output json` serves
// it to a pipe: the same schema messages, projected. Every WorkflowService RPC
// becomes one tool, discovered by walking the service descriptor rather than
// kept in a list — so an RPC added to the schema is a tool the day the code is
// regenerated, and there is no tool list to fall behind the engine. docs/DSL.md
// wrote this down as a rule before the surface existed: MCP is generated, not
// written.
//
// Three tools answer locally and the rest speak to a server, split by what the
// method needs. Validate, Compile and GetCatalog touch no run and no tenant — the
// server's own handlers take a nil Temporal client, which is the proof — so
// they run in-process and an agent gets a working authoring loop with nothing
// else stood up. The lifecycle verbs address durable runs, which only a server
// has; without --address they explain that rather than failing opaquely.

// mcpToolPrefix namespaces the tools, since a client may aggregate servers.
const mcpToolPrefix = "flowstate_"

// mcpDescriptions is the one hand-written thing on this surface: a sentence per
// RPC for the model to choose tools by.
//
// Descriptions are prose for a reader, which is the one thing a descriptor does
// not carry at runtime — protoc-gen-go strips source comments, so deriving
// these would mean shipping a descriptor set alongside the binary for a
// sentence each. Hand-written is acceptable exactly because the *set* is not:
// TestEveryToolHasADescription holds this map to the service descriptor in both
// directions, so a method added without a sentence fails the build rather than
// shipping mute.
var mcpDescriptions = map[string]string{
	"Run": "Submit a compiled workflow specification to run durably. Returns ids to watch it by; it does not wait. " +
		"Author a Flowfile, check it with flowstate_validate, compile it with flowstate_compile, and submit the result here.",
	"Get":       "Report a run's status, timing, current position, and its outputs once finished.",
	"Signal":    "Deliver a named signal to a run waiting for one — how an approval reaches a workload.",
	"List":      "List the caller's runs, paged. A short or empty page with a nextPageToken is not the end of the listing; keep paging.",
	"Cancel":    "Ask a run to stop, letting it clean up on the way out.",
	"Terminate": "Stop a run immediately, running none of its cleanup. Prefer cancel.",
	"Validate":  "Check Flowfile YAML sources and report positioned diagnostics without executing anything. Pure and safe to loop on; answers locally, no server needed.",
	"Compile": "Compile Flowfile YAML into the workflow specification flowstate_run submits. A file with problems answers with its " +
		"diagnostics and no specification. Answers locally, no server needed.",
	"GetCatalog": "What this build can execute: every task with its typed inputs and outputs, and every CEL function an expression may call. " +
		"Read this before writing a Flowfile. Answers locally, no server needed.",
	"CreateSchedule": "Create a schedule that runs a workflow specification on the cadence its triggers.schedule declares. Arguments are bound and " +
		"type-checked here, once, rather than at each firing. Create it paused to read its next firing times before it takes one.",
	"ListSchedules":    "List the caller's schedules, with whether each is live and when it next fires.",
	"DescribeSchedule": "Report one schedule: its cadence, the arguments every firing runs with, when it next fires, and what it has run lately.",
	"DeleteSchedule":   "Delete a schedule. Future firings stop; runs it already started are unaffected and are cancelled with flowstate_cancel.",
	"PauseSchedule":    "Stop a schedule firing without deleting it, recording a note saying why.",
	"ResumeSchedule":   "Let a paused schedule fire again. Firings missed while it was paused are not made up.",
	"TriggerSchedule":  "Fire a schedule now rather than waiting for its cadence, which is how a schedule is tested. It returns no run id; describe the schedule to see what it started.",
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
	local := server.New(nil)

	srv := mcp.NewServer(&mcp.Implementation{Name: "flowstate", Version: version}, nil)

	return serveMCPTools(cmd.Context(), srv, local, remoteClient, cmd)
}

// serveMCPTools registers one tool per RPC and runs the server on stdio.
func serveMCPTools(
	ctx context.Context,
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	posture *cobra.Command,
) error {
	addMCPCapabilities(srv, local, remote, posture)

	return srv.Run(ctx, &mcp.StdioTransport{})
}

// addMCPCapabilities is the one registration, shared with the tests so what they
// exercise is what an agent connects to — two registration sites would be the
// two-copies defect this repository keeps refinding, on a new surface.
//
// Two halves, and the split is what each is for: tools are the verbs, resources
// are what an agent reads before choosing one. See mcpresources.go.
func addMCPCapabilities(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	posture *cobra.Command,
) {
	addMCPTools(srv, local, remote, posture)
	addMCPResources(srv, local)
}

// addMCPTools registers one tool per RPC, plus the one that is not an RPC.
func addMCPTools(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	posture *cobra.Command,
) {
	for _, method := range workflowServiceMethods() {
		srv.AddTool(&mcp.Tool{
			Name:        mcpToolName(method.name),
			Description: mcpDescriptions[method.name],
			InputSchema: schemaForMessage(method.input),
		}, mcpHandler(method, local, remote, posture))
	}

	srv.AddTool(runLocalTool(), runLocalToolHandler(posture))
}

// mcpToolName renders an RPC name as a tool name: GetCatalog becomes
// flowstate_get_catalog, which is the casing MCP tools conventionally use.
func mcpToolName(rpc string) string {
	var b strings.Builder
	b.WriteString(mcpToolPrefix)
	for i, r := range rpc {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}

	return b.String()
}

// serviceMethod is one RPC, as the tool derivation needs it.
type serviceMethod struct {
	name  string
	input protoreflect.MessageDescriptor
	call  func(ctx context.Context, local *server.FlowstateServer,
		remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error)
}

// workflowServiceMethods enumerates the service.
//
// The names and shapes come from the descriptor — asserted against it by test,
// in both directions — while the dispatch is written out, because Go generics
// cannot rank over connect's typed methods without reflection that would cost
// more clarity than these lines do. A method added to the service without a row
// here fails TestEveryRPCIsATool.
func workflowServiceMethods() []serviceMethod {
	return []serviceMethod{
		{
			name:  "Validate",
			input: (&v1.ValidateRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.Validate(ctx, connect.NewRequest(in.(*v1.ValidateRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Compile",
			input: (&v1.CompileRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.Compile(ctx, connect.NewRequest(in.(*v1.CompileRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "GetCatalog",
			input: (&v1.GetCatalogRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.GetCatalog(ctx, connect.NewRequest(in.(*v1.GetCatalogRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Run",
			input: (&v1.RunRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Run(ctx, connect.NewRequest(in.(*v1.RunRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Get",
			input: (&v1.GetRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Get(ctx, connect.NewRequest(in.(*v1.GetRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Signal",
			input: (&v1.SignalRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Signal(ctx, connect.NewRequest(in.(*v1.SignalRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "List",
			input: (&v1.ListRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().List(ctx, connect.NewRequest(in.(*v1.ListRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Cancel",
			input: (&v1.CancelRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Cancel(ctx, connect.NewRequest(in.(*v1.CancelRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Terminate",
			input: (&v1.TerminateRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Terminate(ctx, connect.NewRequest(in.(*v1.TerminateRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},

		// The schedule verbs, all remote: a schedule is an object in a cluster, so
		// unlike validate and compile there is nothing an agent could be told about
		// one without a server to ask. Creating one is deliberately as available to
		// an agent as running a workflow is — it is the same permission, and an agent
		// that can start a workload every night should have to say so in a tool call
		// somebody can read rather than by writing a loop that sleeps.
		{
			name:  "CreateSchedule",
			input: (&v1.CreateScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().CreateSchedule(ctx, connect.NewRequest(in.(*v1.CreateScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "ListSchedules",
			input: (&v1.ListSchedulesRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().ListSchedules(ctx, connect.NewRequest(in.(*v1.ListSchedulesRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "DescribeSchedule",
			input: (&v1.DescribeScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().DescribeSchedule(ctx, connect.NewRequest(in.(*v1.DescribeScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "DeleteSchedule",
			input: (&v1.DeleteScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().DeleteSchedule(ctx, connect.NewRequest(in.(*v1.DeleteScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "PauseSchedule",
			input: (&v1.PauseScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().PauseSchedule(ctx, connect.NewRequest(in.(*v1.PauseScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "ResumeSchedule",
			input: (&v1.ResumeScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().ResumeSchedule(ctx, connect.NewRequest(in.(*v1.ResumeScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "TriggerSchedule",
			input: (&v1.TriggerScheduleRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().TriggerSchedule(ctx, connect.NewRequest(in.(*v1.TriggerScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
	}
}

// mcpHandler adapts one RPC into a tool handler.
//
// Arguments arrive as JSON and leave as protojson of the response message —
// the same bytes `--output json` prints, from the same schema, which is what
// keeps this surface from being a second dialect.
func mcpHandler(
	method serviceMethod,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	posture *cobra.Command,
) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in := newMessage(method.input)

		// DiscardUnknown stays false on purpose: the schema advertised
		// additionalProperties false, and honouring a field the schema does not
		// have would make the tool "work" while doing something other than what
		// was asked.
		if raw := req.Params.Arguments; len(raw) > 0 {
			if err := protojson.Unmarshal(raw, in); err != nil {
				return toolError(fmt.Errorf("arguments do not match %s: %w", method.input.FullName(), err)), nil
			}
		}

		out, err := method.call(ctx, local, remote, in)
		if err != nil {
			return toolError(err), nil
		}

		// An agent's context is an untrusted-consumer surface exactly like a
		// terminal, so `flowstate_get` honours `sensitive:` too. This tool
		// addresses a run by id alone, over a generic RPC dispatch shared by
		// every method in the service — there is no workflow specification
		// anywhere in reach here, which is the fail-closed case [sensitive.go]'s
		// package comment names for `flow get`: workflow is nil, so every
		// declared output is withheld unless the server was started with
		// --reveal-sensitive.
		if response, ok := out.(*v1.GetResponse); ok {
			out = redactGetResponse(response, nil, revealSensitiveRequested(posture))
		}

		encoded, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(out)
		if err != nil {
			return toolError(err), nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// toolError reports a failure as the tool's result rather than a protocol
// error, which is what lets a model read the reason and correct itself.
func toolError(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
	}
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

// runLocalToolName is the tool an agent calls to execute what it just wrote.
const runLocalToolName = mcpToolPrefix + "run_local"

// maxRunLocalResultBytes bounds the tool's answer.
//
// An agent-facing surface is an untrusted-consumer surface: the run's outputs
// are whatever the submitted workflow chose to produce, and a workflow that
// produces a megabyte of step outputs would otherwise spend it all in a model's
// context window. [v1.MaxRunStateBytes] is the wrong number here — it bounds
// what Temporal can carry, which is nearly two megabytes and has nothing to do
// with what is useful to read.
//
// Exceeding it drops the outputs and says so, rather than cutting the document
// short: a truncated JSON document is one a caller cannot parse at all, which
// turns a large answer into no answer.
const maxRunLocalResultBytes = 256 << 10

// maxRunLocalLogRecords bounds how many `log:` lines are carried back.
//
// Same reason, one level down, and bounded by count rather than bytes because a
// loop is how this gets large: the run controls how many records there are, and
// each one is small.
const maxRunLocalLogRecords = 200

// runLocalToolDescription is written for the model that has to decide whether
// this is the tool it wants, and what it will and will not have proved by using
// it.
const runLocalToolDescription = "Execute a Flowfile immediately, in this process, with no server and no Temporal " +
	"— the same rehearsal `flow run local` performs. Use it to verify a workflow you just authored: " +
	"conditions, retries, timeouts, loops, waits and step outputs behave here the way they behave in " +
	"production, and the answer is the same document flowstate_get returns for a durable run.\n\n" +
	"Fail-closed by default: network egress from `http:` steps is denied and no secret scheme is " +
	"registered unless the operator started this server with the flags that permit them " +
	"(--egress-policy, --secret-env, --secret-dir, --auth-policy). Nothing in this tool's arguments " +
	"can widen that, so a denied request means the server was not configured for it, not that the " +
	"workflow is wrong.\n\n" +
	"What it does not prove: durability. A local run has no run id, nothing can watch it, it does not " +
	"survive this process, Continue-As-New compaction never happens, and parallel steps are rehearsed " +
	"rather than genuinely distributed. Submit the compiled specification with flowstate_run when the " +
	"rehearsal is right.\n\n" +
	"A source declaring `inputs:` is given them in the `inputs` object of this call, keyed by declared " +
	"name and typed as declared; a required one left out, an undeclared name, or a mistyped value is " +
	"refused before any step runs. What the source declares under `outputs:` comes back as `runOutputs`.\n\n" +
	"Answers with {\"run\": <GetResponse>, \"logs\": [...]}: the run's status, timing and step outputs, " +
	"plus whatever `log:` steps emitted. Invalid sources come back as an error carrying positioned " +
	"diagnostics (line:column) to correct against."

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

// runLocalTool declares the tool.
func runLocalTool() *mcp.Tool {
	return &mcp.Tool{
		Name:        runLocalToolName,
		Description: runLocalToolDescription,
		InputSchema: runLocalInputSchema(),
	}
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
				return toolError(fmt.Errorf("arguments do not match %s: %w", runLocalToolName, err)), nil
			}
		}

		if strings.TrimSpace(args.Source) == "" {
			return toolError(errors.New(
				"source is required: pass the Flowfile YAML to execute, e.g. \"edition: v2026.2\\nname: demo\\nsteps:\\n- id: hi\\n  log:\\n    message: hello\"")), nil
		}

		workflow, err := parseFlowfileSource([]byte(args.Source))
		if err != nil {
			return toolError(err), nil
		}

		signals, err := runLocalSignalFlags(args.Signals)
		if err != nil {
			return toolError(err), nil
		}

		// Bound before the timeout is started and before any provider is opened,
		// because an argument that does not satisfy the source's `inputs:` is a
		// fact about the call rather than about the run — and the refusal is the
		// binder's own text, which is what an agent needs in order to correct the
		// call rather than the workflow.
		inputs, err := runLocalToolInputs(workflow, args.Inputs)
		if err != nil {
			return toolError(err), nil
		}

		timeout, _ := posture.Flags().GetDuration("run-local-timeout")
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ctx, err = withLocalSignals(ctx, signals)
		if err != nil {
			return toolError(err), nil
		}

		ctx, closeSecretProviders, err := withLocalTaskRuntime(posture, ctx, workflow)
		if err != nil {
			return toolError(err), nil
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
			return toolError(err), nil
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
				"wait_for_signal step declares — a letter or digit, then letters, digits, - or _", name)
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
func renderRunLocalResult(response *v1.GetResponse, logs []runLocalLogRecord) ([]byte, error) {
	run, err := marshalJSON(response, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the run: %w", err)
	}

	encoded, err := json.Marshal(runLocalResult{Run: run, Logs: logs})
	if err != nil {
		return nil, fmt.Errorf("rendering the answer: %w", err)
	}
	if len(encoded) <= maxRunLocalResultBytes {
		return encoded, nil
	}

	// First the logs, which are commentary on the outputs.
	result := runLocalResult{
		Run:  run,
		Note: fmt.Sprintf("logs were dropped: the answer exceeded %d bytes", maxRunLocalResultBytes),
	}
	encoded, err = json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("rendering the answer: %w", err)
	}
	if len(encoded) <= maxRunLocalResultBytes {
		return encoded, nil
	}

	// Then the step transcript, keeping the status, the timing, any error, and
	// the run's declared outputs — a run reported without its transcript is
	// still an answer; an unparsable document is not.
	trimmed, ok := proto.Clone(response).(*v1.GetResponse)
	if !ok {
		return nil, errors.New("rendering the answer: the run is not a GetResponse")
	}
	if trimmed.GetOutputs() != nil {
		trimmed.Kind = nil
	}

	encoded, err = renderTrimmedRun(trimmed, fmt.Sprintf(
		"the step outputs and logs were dropped: the answer exceeded %d bytes. "+
			"Have the workflow carry less, or read the values it needs in a step of its own",
		maxRunLocalResultBytes))
	if err != nil {
		return nil, err
	}
	if len(encoded) <= maxRunLocalResultBytes {
		return encoded, nil
	}

	// Last, what the workflow declared it answers with. This is the most
	// valuable part of the document and so the last to go — but it is chosen by
	// the same submitted workflow as everything above it, so a single `outputs:`
	// expression building a megabyte of string is enough to carry a run past the
	// cap on its own. Dropping the transcript while leaving this untouched was
	// the hole: the cap bounded the part a workflow was least able to abuse.
	trimmed.RunOutputs = nil

	encoded, err = renderTrimmedRun(trimmed, fmt.Sprintf(
		"the declared outputs, step outputs and logs were dropped: the answer exceeded %d bytes. "+
			"Read what the run produced with `flow get`, or have the workflow answer with less",
		maxRunLocalResultBytes))
	if err != nil {
		return nil, err
	}

	// Deliberately returned whether or not it fits. What remains is a status, two
	// ids, two timestamps and possibly a failure message — bounded by the schema
	// rather than by the workflow — so there is nothing left to drop that would
	// not take the answer with it.
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

// newMessage constructs an empty message for a descriptor.
func newMessage(md protoreflect.MessageDescriptor) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		// Unreachable for the compiled-in schema; loud if it ever is not.
		panic("flow mcp: no type for " + string(md.FullName()))
	}

	return mt.New().Interface()
}
