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
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
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

// workflowServiceName addresses the service whose prose this surface reads.
const workflowServiceName protoreflect.FullName = "flowstate.v1.WorkflowService"

// mcpToolDescription is the sentence a model chooses a tool by, read from the
// schema that declares the RPC.
//
// It used to be a hand-written map here, one entry per RPC, which is the
// written-twice defect this repository keeps refinding: the schema's service
// section describes the same RPCs, so every description existed in two places
// and only one of them moved when the behavior did. The prose now lives in
// proto/flowstate/v1/flowstate.proto and arrives through
// [protodoc.Method], so a sentence corrected in the schema is the sentence an
// agent is handed, and there is no second copy to correct. Slice 2 of #424.
//
// The whole comment rather than [protodoc.FirstSentence], deliberately. A
// one-line context needs one line, and this is not one: the sentences that make
// these tools usable are the ones after the first: keep paging past a short
// page, prefer Cancel to Terminate, a file that does not compile answers with
// diagnostics rather than an error. Taking only the first sentence would drop
// exactly the operational half the old map existed to carry.
//
// Fails closed on an RPC the schema does not document, returning "" so that
// [TestEveryToolHasADescription] fails rather than an agent being handed a mute
// tool.
func mcpToolDescription(rpc string) string {
	description, ok := protodoc.Method(workflowServiceName, protoreflect.Name(rpc))
	if !ok {
		return ""
	}

	for _, note := range []string{mcpToolNotes[rpc], mcpLocalToolNote(rpc)} {
		if note != "" {
			description += "\n\n" + note
		}
	}

	return description
}

// mcpToolNotes are the per-tool paragraphs that are about this surface rather
// than about the RPC, appended after the schema's own prose.
//
// The rule for what belongs here is the one #424 set: prose flows schema to
// surface and never back, and a surface needing different words says so
// explicitly, visibly, at the call site. So a note earns a place only by being
// false of the RPC and true of `flow mcp`. The one entry qualifies twice over.
// The argument recipe is spelled in JSON field names (`workflowId`,
// `signalName`) and a tool name (`flowstate_signal`), which are this
// transport's spellings of things the schema calls `workflow_id`, a signal name
// and [WorkflowService.Signal]; and the identity caveat is a fact about stdio,
// where every call arrives as this process and the schema's `Signal` knows
// nothing of that.
var mcpToolNotes = map[string]string{
	"Get": "On this surface that call is flowstate_signal, with this run's workflowId, name set to the gate's " +
		"signalName, and payload.namedValues.approved set to {\"literal\": {\"boolValue\": true}} or false.\n\n" +
		"Over stdio the signal is delivered as this process's own identity, not as the identity of whoever " +
		"asked for it. Nothing on this transport can attest that a particular human approved anything, and an " +
		"interactive card rendering this result changes none of that; an attested approver waits on the remote " +
		"MCP surface.",

	// A fact about where this surface dispatches the call, which the schema's
	// GetCatalog cannot know: runMCP answers it from the in-process server, so
	// against a deployment addressed with --address the catalog described is
	// this binary's own build, and a deployment running other plugins or
	// another version answers the wire RPC differently.
	"GetCatalog": "On this surface the answer is this binary's own build (its task registry and any plugins " +
		"this process started), not the deployment --address points at; a deployment with other plugins or " +
		"another version may differ.",
}

// mcpLocalToolNote says that a tool needs nothing stood up, for the tools where
// that is true.
//
// Derived from [mcpLocalTools] rather than written into each description,
// because which side a tool answers on is one decision and the reference table
// already renders it from there. A model reading a description is the other
// reader of that same decision, and the two must not be able to disagree.
func mcpLocalToolNote(rpc string) string {
	if !mcpLocalTools[rpc] {
		return ""
	}

	return "Answers locally, in this process. No server and no Temporal needed."
}

// mcpToolViews names the tool each UI resource renders, by RPC.
//
// One entry, and choosing it was the design decision worth writing down.
// [v1.GetResponse] is the only answer on this surface that carries both a run's
// coordinates and its open gates: `progress.pending_waits` reports each parked
// `wait_for_signal:` with the prompt the author wrote, the signal name that
// releases it, its deadline and whether it is policed, and `starter` says who
// asked for the run - which is exactly the set an approval card has to render,
// and exactly the set a `distinct_from_starter` policy is compared against.
//
// The alternatives do not hold the data. `flowstate_signal` answers with an empty
// SignalResponse, so a card on it would have nothing to draw and would be a form
// rather than a view of anything. `flowstate_list` reports many runs and no gates
// at all: it can say a run is RUNNING and cannot say it is waiting on a person.
// So the tool that reports a run and its pending gates is Get, and it is the one
// the card is declared on.
//
// A map keyed by RPC name rather than a field on [serviceMethod], so that the set
// of tools carrying a view is one reviewable list, in the file that explains why
// a view is not a permission.
var mcpToolViews = map[string]string{
	"Get": mcpApprovalCardURI,
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

	return serveMCPTools(cmd.Context(), newMCPServer(version), local, remoteClient, cmd)
}

// newMCPServer constructs the server an agent connects to, capabilities and all.
//
// One constructor, shared with the tests, for the reason [addMCPCapabilities] is
// one registration: a second construction is a second set of capabilities, and
// the one an agent negotiates against would eventually stop being the one the
// tests negotiate against. The extension declared here is what a host reads to
// learn that this server serves views at all.
func newMCPServer(version string) *mcp.Server {
	return mcp.NewServer(
		&mcp.Implementation{Name: "flowstate", Version: version},
		&mcp.ServerOptions{Capabilities: mcpUIServerCapabilities()},
	)
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
	addMCPUIResources(srv)
}

// addMCPTools registers one tool per RPC, plus the one that is not an RPC.
func addMCPTools(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	posture *cobra.Command,
) {
	for _, method := range workflowServiceMethods() {
		tool := &mcp.Tool{
			Name:        mcpToolName(method.name),
			Description: mcpToolDescription(method.name),
			InputSchema: schemaForMessage(method.input),
		}
		if view, ok := mcpToolViews[method.name]; ok {
			tool.Meta = mcpUIToolMeta(view)
		}

		srv.AddTool(tool, mcpHandler(method, local, remote, posture))
	}

	srv.AddTool(runLocalTool(), runLocalToolHandler(posture))
	srv.AddTool(testTool(), testToolHandler())
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
			// The entity idiom's entry point: address a run by business key and
			// deliver to it, creating it if it is not there yet. An agent driving
			// an order or a subscription needs this rather than Run, because it
			// does not know — and must not have to know — whether this is the
			// first event for that key.
			name:  "SignalWithStart",
			input: (&v1.SignalWithStartRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().SignalWithStart(ctx, connect.NewRequest(in.(*v1.SignalWithStartRequest)))
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

		// The bound this surface already holds `run_local` to, applied where
		// every other tool leaves — see [maxMCPResultBytes]. Refusing rather
		// than shortening, because nothing here knows which field of an
		// arbitrary response could be dropped without changing what it says,
		// and half a JSON document is not a smaller answer but an unreadable
		// one.
		if len(encoded) > maxMCPResultBytes {
			return toolError(fmt.Errorf(
				"%s answered with %d bytes, over this surface's %d byte limit, so nothing was returned "+
					"rather than a document cut short; ask for less — a single run by id rather than a "+
					"listing, a smaller page, or a narrower filter",
				mcpToolName(method.name), len(encoded), maxMCPResultBytes)), nil
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

// maxMCPResultBytes bounds what any tool on this surface may answer with.
//
// An agent-facing surface is an untrusted-consumer surface: a run's outputs are
// whatever the submitted workflow chose to produce, and a workflow producing a
// megabyte of step outputs would otherwise spend it all in a model's context
// window. [v1.MaxRunStateBytes] is the wrong number here — it bounds what
// Temporal can carry, which is nearly two megabytes and has nothing to do with
// what is useful to read.
//
// It is one constant for the whole surface rather than one per tool, which is
// the correction #300 records. It began life bounding `run_local` alone,
// because that was the tool being designed when the rule was written — and
// every other tool answers over [mcpHandler], which had no bound at all. So
// `flowstate_get` could return the whole two megabytes the paragraph above
// calls the wrong number, on the same surface, from the same session. A bound
// belongs on the path every answer leaves through, not on the tool whose
// review happened to raise it; the same argument plugin/transport.go makes for
// putting the HTTP cap on the RoundTripper rather than on a library option.
//
// How it is enforced differs by what the tool knows, and only there.
// `run_local` degrades by *shape* — drop the transcript, then the outputs —
// because it understands its own document. [mcpHandler] cannot: a protojson
// response has no field it knows is safe to drop, so it refuses with a bounded,
// parseable error naming what to ask for instead. Both refuse to cut a document
// short, because a truncated JSON body is one a caller cannot parse at all,
// which turns a large answer into no answer.
const maxMCPResultBytes = 256 << 10

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

		ctx, err = withLocalSignals(ctx, posture, workflow, inputs, signals)
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

// testToolName is the tool an agent calls to rehearse what it just wrote.
const testToolName = mcpToolPrefix + "test"

// testToolDescription is written for the model choosing between this tool and
// flowstate_run_local.
const testToolDescription = "Run a Flowfile against inline test cases the way `flow test` runs a *.test.yaml " +
	"beside a workflow on disk — the identical machinery (flowtest.RunSource), on bytes submitted here " +
	"instead of two files. Every task the workflow would otherwise call is replaced: a stub answers with " +
	"its `returns:`, or fails the way its `fails:` describes, and any task this case invokes with no " +
	"matching stub is refused rather than run for real, naming the task and how many stubs were declared " +
	"for it. Time is virtual, so a case with `sleep: 24h` resolves in under a second, and a " +
	"wait_for_signal step is answered by `signals:` scripted for a chosen offset from the run's start.\n\n" +
	"Needs no egress policy and no operator opt-in, unlike flowstate_run_local: a stubbed run never " +
	"invokes a real task's implementation at all — not `http`, not a plugin task registered by " +
	"--plugin-dir — so there is no network for a policy to govern, and no secret this tool could resolve " +
	"even where one is configured. Reach for this first, while authoring: it proves conditions, retries, " +
	"`undo:` compensation, and data-flow expressions without ever touching a network. Reach for " +
	"flowstate_run_local afterward, once egress is configured, to rehearse the real effect of whichever " +
	"task you deliberately left unstubbed.\n\n" +
	"What it does not prove: that a real task behaves the way a stub's `returns:` or `fails:` says it " +
	"does, or anything about durability — flowstate_run_local's own limits, on top of never running a " +
	"real task at all.\n\n" +
	"`tests` is a `*.test.yaml` document: `tests:` names one or more cases, each with an optional " +
	"`inputs:`, `stubs:`, `signals:`, `starter:`, and an `expect:` the run must satisfy: `expect.outputs` compares " +
	"the workflow's declared `outputs:`, `expect.failed`/`expect.error_contains` assert the run failing " +
	"outright, `expect.compensated` the undo log, and `expect.ran`/`expect.skipped` step presence. A " +
	"case's own `workflow:` field is accepted, for compatibility with a file written to disk, but is " +
	"never consulted: every case here runs against the `workflow` argument, not a sibling file.\n\n" +
	"To exercise a workflow's `signals:` policy: a scripted signal's `sender:` names who the delivery " +
	"stands in for and `starter:` names who the run started as, each carrying `subject:`/`issuer:` " +
	"together, `namespace:` and `claims:`, and both checked by the same policy function the server " +
	"calls, so `distinct_from_starter:` refuses a sender who is the run's own starter here exactly as " +
	"production would. Neither is attested: a delivery stands in for its sender, which is why a gate's " +
	"own `sender.local` output reads true, and `starter:` never reaches `run.identity`.\n\n" +
	"Answers with the same v1.TestReport `flow test -o json` writes: one verdict per case, and for a case " +
	"that did not pass, its unmet expectations as positioned diagnostics. A case that never reached a " +
	"verdict at all — the workflow failed to compile, a stub named a task with no matching invocation, or " +
	"the run failed in a way the case did not declare with `expect.failed` — reports why in `error` " +
	"instead of `failures`. `refused` is set instead of any case running at all when the submitted " +
	"`tests` document itself does not parse."

// testTool declares the tool.
func testTool() *mcp.Tool {
	return &mcp.Tool{
		Name:        testToolName,
		Description: testToolDescription,
		InputSchema: testInputSchema(),
	}
}

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
// package comment above [testToolName].
func testToolHandler() mcp.ToolHandler {
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
				return toolError(fmt.Errorf("arguments do not match %s: %w", testToolName, err)), nil
			}
		}

		if strings.TrimSpace(args.Workflow) == "" {
			return toolError(errors.New(
				"workflow is required: pass the Flowfile YAML under test, e.g. " +
					"\"edition: v2026.2\\nname: demo\\nsteps:\\n- id: hi\\n  log:\\n    message: hello\"")), nil
		}
		if strings.TrimSpace(args.Tests) == "" {
			return toolError(errors.New(
				"tests is required: pass a *.test.yaml document naming at least one case, e.g. " +
					"\"tests:\\n  - name: it runs\\n    expect:\\n      failed: false\"")), nil
		}

		report := flowtest.RunSource("<submitted>", []byte(args.Workflow), []byte(args.Tests))

		encoded, err := renderTestResult(report)
		if err != nil {
			return toolError(err), nil
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

// renderTestResult brings a v1.TestReport under maxMCPResultBytes —
// [renderRunLocalResult]'s own bound and its own discipline, reused rather
// than reinvented: stop at a document that still parses, and say what left
// rather than truncating bytes into something a caller cannot decode. The
// steps differ because what a workflow can make large differs between the two
// answers — run_local's is step outputs and log lines, this one is
// diagnostic messages built by comparing them — but the bound, the shape of
// the ladder, and the floor that is returned whether or not it fits are the
// same ones [renderRunLocalResult] already established.
func renderTestResult(report *v1.TestReport) ([]byte, error) {
	encoded, err := marshalJSON(report, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the report: %w", err)
	}
	if len(encoded) <= maxMCPResultBytes {
		return encoded, nil
	}

	// First, cap every failure's own message: a mismatch's %v of a large
	// stubbed or computed value is the one part of this document a case
	// controls the size of, and capping it keeps every case, every verdict,
	// and every field/step/value a diagnostic named.
	trimmed, ok := proto.Clone(report).(*v1.TestReport)
	if !ok {
		return nil, errors.New("rendering the report: the report is not a TestReport")
	}
	for _, c := range trimmed.GetCases() {
		for _, f := range c.GetFailures() {
			if len(f.GetMessage()) > maxTestFailureMessageBytes {
				f.Message = f.GetMessage()[:maxTestFailureMessageBytes] +
					fmt.Sprintf("... (truncated, exceeded %d bytes)", maxTestFailureMessageBytes)
			}
		}
	}
	encoded, err = marshalJSON(trimmed, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the report: %w", err)
	}
	if len(encoded) <= maxMCPResultBytes {
		return encoded, nil
	}

	// Still too big — enough cases with enough failures each that even capped
	// messages do not fit. Report per-case verdicts only, dropping the
	// diagnostics themselves down to a count: a report with no verdicts at
	// all is worse than no answer, so this floor is returned whether or not
	// it fits, the same reasoning [renderRunLocalResult]'s own last rung
	// gives for the fields nothing further can drop.
	summary := &v1.TestReport{File: report.GetFile(), Refused: report.GetRefused()}
	for _, c := range trimmed.GetCases() {
		caseError := c.GetError()
		if caseError == "" && len(c.GetFailures()) > 0 {
			caseError = fmt.Sprintf(
				"%d failure(s); their diagnostics were dropped because the answer exceeded %d bytes",
				len(c.GetFailures()), maxMCPResultBytes)
		}
		summary.Cases = append(summary.Cases, &v1.TestCase{
			Name:     c.GetName(),
			Passed:   c.GetPassed(),
			Duration: c.GetDuration(),
			Error:    caseError,
		})
	}
	encoded, err = marshalJSON(summary, false)
	if err != nil {
		return nil, fmt.Errorf("rendering the report: %w", err)
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
	if len(encoded) <= maxMCPResultBytes {
		return encoded, nil
	}

	// First the logs, which are commentary on the outputs.
	result := runLocalResult{
		Run:  run,
		Note: fmt.Sprintf("logs were dropped: the answer exceeded %d bytes", maxMCPResultBytes),
	}
	encoded, err = json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("rendering the answer: %w", err)
	}
	if len(encoded) <= maxMCPResultBytes {
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
		maxMCPResultBytes))
	if err != nil {
		return nil, err
	}
	if len(encoded) <= maxMCPResultBytes {
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
		maxMCPResultBytes))
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
