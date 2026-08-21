// Package mcp is the protocol surface `flow mcp` serves: tool names and
// descriptions derived from the schema, the RPC dispatch table, and
// registration of both onto an *mcp.Server.
//
// Split out of cmd/flow's own package by #410: this is the half of the old
// mcp.go that has no CLI-specific dependency — no flags, no egress policy, no
// secrets — and is therefore the part with a plausible non-CLI caller (an
// embedder serving MCP, #380's task-invocation tool, #241's agent surface).
// What stays in cmd/flow is the two tools that are not RPCs
// (flowstate_run_local and flowstate_test), because both execute against this
// binary's own flags — egress, secrets, plugins — which only the command line
// can supply; see cmd/flow/mcp.go.
//
// The one seam this package needs back from the CLI is redaction: an agent's
// context is an untrusted-consumer surface exactly like a terminal, so a
// GetResponse answered here must be narrowed the way `flow get` narrows one,
// which is a decision cmd/flow owns (--reveal-sensitive). See [Deps].
//
// # The agent surface is the machine surface, taken seriously
//
// `flow mcp` serves the control plane to a model the way `--output json`
// serves it to a pipe: the same schema messages, projected. Every
// WorkflowService RPC becomes one tool, discovered by walking the service
// descriptor rather than kept in a list — so an RPC added to the schema is a
// tool the day the code is regenerated, and there is no tool list to fall
// behind the engine. docs/DSL.md wrote this down as a rule before the surface
// existed: MCP is generated, not written.
package mcp

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// ToolPrefix namespaces the tools, since a client may aggregate servers.
const ToolPrefix = "flowstate_"

// WorkflowServiceName addresses the service whose prose this surface reads.
const WorkflowServiceName protoreflect.FullName = "flowstate.v1.WorkflowService"

// MaxResultBytes bounds what any tool on this surface may answer with.
//
// An agent-facing surface is an untrusted-consumer surface: a run's outputs
// are whatever the submitted workflow chose to produce, and a workflow
// producing a megabyte of step outputs would otherwise spend it all in a
// model's context window. [v1.MaxRunStateBytes] is the wrong number here — it
// bounds what Temporal can carry, which is nearly two megabytes and has
// nothing to do with what is useful to read.
//
// Exported because cmd/flow's own two tools (flowstate_run_local,
// flowstate_test) hold their answers to the identical bound; see
// [ServiceMethod]'s handler for how the RPC-projected tools enforce it.
const MaxResultBytes = 256 << 10

// ToolDescription is the sentence a model chooses a tool by, read from the
// schema that declares the RPC.
//
// It used to be a hand-written map here, one entry per RPC, which is the
// written-twice defect this repository keeps refinding: the schema's service
// section describes the same RPCs, so every description existed in two places
// and only one of them moved when the behavior did. The prose now lives in
// proto/flowstate/v1/service.proto and arrives through
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
// TestEveryToolHasADescription fails rather than an agent being handed a mute
// tool.
func ToolDescription(rpc string) string {
	return toolDescription(rpc, false)
}

// toolDescription is [ToolDescription] with the reduced surface's answer
// available.
//
// reduced is [AddLocalCapabilities]'s registration: `flow mcp serve`, where
// several of the notes below are simply false. A description is what a model
// chooses a tool by and is the only account of the surface it ever reads, so
// one describing behavior this surface does not have is a diagnostic that
// lies — the failure "Diagnostics are a feature" names, pointed at a
// non-human reader. Reported by Codex on picatz/flowstate#807.
func toolDescription(rpc string, reduced bool) string {
	description, ok := protodoc.Method(WorkflowServiceName, protoreflect.Name(rpc))
	if !ok {
		return ""
	}

	note := toolNotes[rpc]
	if reduced {
		note = reducedToolNotes[rpc]
	}

	for _, extra := range []string{note, localToolNote(rpc)} {
		if extra != "" {
			description += "\n\n" + extra
		}
	}

	return description
}

// reducedToolNotes replaces [toolNotes] on the surface [AddLocalCapabilities]
// registers, for the tools whose stdio note is untrue there.
//
// One entry, and the absence of the others is the point: a tool with no entry
// here gets no surface note rather than an inherited one, because an empty
// map would silently reintroduce every note this exists to suppress.
// GetCatalog's stdio note describes dispatching to a deployment named by
// --address, a flag `flow mcp serve` does not have and a dispatch it
// deliberately does not do (see cmd/flow/mcpserve.go on why no tool here
// reaches a deployment).
var reducedToolNotes = map[string]string{
	"GetCatalog": "This surface always answers from this binary's own build: its task registry and " +
		"any plugins this process started. It never dispatches to another deployment, so what is " +
		"reported here is what this process can validate and rehearse against, which may differ " +
		"from what the deployment that will eventually run a submitted workflow can execute.",
}

// toolNotes are the per-tool paragraphs that are about this surface rather
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
var toolNotes = map[string]string{
	"Get": "On this surface that call is flowstate_signal, with this run's workflowId, name set to the gate's " +
		"signalName, and payload.namedValues.approved set to {\"literal\": {\"boolValue\": true}} or false.\n\n" +
		"Over stdio the signal is delivered as this process's own identity, not as the identity of whoever " +
		"asked for it. Nothing on this transport can attest that a particular human approved anything, and an " +
		"interactive card rendering this result changes none of that; an attested approver waits on the remote " +
		"MCP surface.",

	// A fact about where this surface dispatches the call, which the schema's
	// GetCatalog cannot know: by default runMCP answers it from the in-process
	// server, but with --address (or FLOWSTATE_ADDRESS) explicitly naming a
	// deployment, it dispatches there instead — see [Deps.RemoteCatalogAddress]
	// and TestTheGetCatalogToolDispatchesToAnAddressedDeployment. Refuses rather
	// than falling back to the local answer when that deployment cannot be
	// reached, because a silent fallback is this same defect one level up: an
	// answer that looks authoritative and is not.
	"GetCatalog": "Without --address (and without FLOWSTATE_ADDRESS) this answers locally: this binary's " +
		"own build (its task registry and any plugins this process started), no server or Temporal needed. " +
		"With --address or FLOWSTATE_ADDRESS explicitly naming a deployment, this dispatches to that " +
		"deployment's own GetCatalog instead — the deployment is what will actually run a submitted " +
		"workflow, and may have plugins or a version this binary does not. If that deployment cannot be " +
		"reached, the call is refused rather than silently answering from this binary's build.",
}

// localToolNote says that a tool needs nothing stood up, for the tools where
// that is true.
//
// Derived from [LocalTools] rather than written into each description,
// because which side a tool answers on is one decision and the reference table
// already renders it from there. A model reading a description is the other
// reader of that same decision, and the two must not be able to disagree.
func localToolNote(rpc string) string {
	if !LocalTools[rpc] {
		return ""
	}

	// GetCatalog's own toolNotes entry already states when this holds and
	// when it does not; the blanket sentence below is only true of it by
	// default, and appending an unconditional "answers locally" after a
	// paragraph that says "unless --address names a deployment" would
	// contradict its own preceding sentence.
	if rpc == "GetCatalog" {
		return ""
	}

	return "Answers locally, in this process. No server and no Temporal needed."
}

// LocalTools names the RPCs that answer in-process rather than over the wire:
// the server's own handlers take a nil Temporal client, which is the proof —
// see [WorkflowServiceMethods].
var LocalTools = map[string]bool{
	"Validate":   true,
	"Compile":    true,
	"GetCatalog": true,
}

// ToolViews names the tool each UI resource renders, by RPC.
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
var ToolViews = map[string]string{
	"Get": ApprovalCardURI,
}

// Deps is what registration needs back from the binary that embeds this
// package — kept to the smallest seam that crosses the boundary; see the
// package doc for why redaction is the one thing that has to.
type Deps struct {
	// Redact narrows a GetResponse to what this surface may show, the way
	// `flow get` narrows a spec-less answer (workflow is always nil here:
	// this dispatch has no specification in reach). Required; a nil field
	// means nothing is ever withheld.
	Redact func(response *v1.GetResponse) *v1.GetResponse

	// RemoteCatalogAddress, when non-empty, routes flowstate_get_catalog to
	// the deployment named here instead of answering from this binary's own
	// build. The caller sets it only when the operator named a deployment
	// explicitly — --address or FLOWSTATE_ADDRESS — so the in-process answer
	// stays the default an agent gets with nothing else stood up (see
	// [LocalTools]).
	//
	// It is a value rather than a bool so a failure to reach that deployment
	// can name it: the tool refuses rather than falling back to the local
	// answer, because a silent fallback is the defect this field exists to
	// fix, one level up — an answer that looks like the deployment's and
	// is not. See remoteCatalogCall.
	RemoteCatalogAddress string

	// WrapHandler, when set, wraps every tool handler [AddLocalCapabilities]
	// registers — derived and caller-supplied alike — with the tool's own
	// name in hand.
	//
	// Read only there, deliberately. It exists for a serving surface with
	// several callers at once, where a tool that mutates process-wide state
	// for the duration of one call needs that call serialized against every
	// other tool that reads the same state — `flow mcp serve`'s guard around
	// [v1.DefaultRegistry] (cmd/flow/mcpserve.go) is the whole reason it is
	// here. Stdio has one caller and needs none of it, so [AddTools] ignores
	// this field and the surface an agent host launches is byte for byte the
	// one it always was.
	WrapHandler func(tool string, next mcp.ToolHandler) mcp.ToolHandler

	// WrapResourceHandler is [WrapHandler] for the read-only half of the
	// surface, with the resource's URI in hand.
	//
	// Separate because the two handler types are: a resource is read through
	// [mcp.ResourceHandler], not [mcp.ToolHandler]. It exists for the same
	// reason and would be pointless without it — flowstate://catalog/tasks
	// answers from [v1.DefaultRegistry] exactly as flowstate_get_catalog
	// does, so a guard applied only to tools leaves the identical read
	// reachable one request away. Also read only by [AddLocalCapabilities].
	WrapResourceHandler func(uri string, next mcp.ResourceHandler) mcp.ResourceHandler

	// reduced marks the registration [AddLocalCapabilities] performs, where
	// several tools and resources the full surface serves are absent. It
	// selects the descriptions that are true there — see [toolDescription]
	// and addResources — and is set by that function rather than by a
	// caller, because a caller who had to remember would eventually not.
	reduced bool
}

// ToolRegistration is one tool this package does not itself derive from the
// service descriptor — flowstate_run_local and flowstate_test, both supplied
// by the caller, since both execute against flags only the caller has. See
// the package doc.
type ToolRegistration struct {
	Tool    *mcp.Tool
	Handler mcp.ToolHandler
}

// NewServer constructs the server an agent connects to, capabilities and all.
//
// One constructor, shared with the tests, for the reason [AddCapabilities] is
// one registration: a second construction is a second set of capabilities, and
// the one an agent negotiates against would eventually stop being the one the
// tests negotiate against. The extension declared here is what a host reads to
// learn that this server serves views at all.
func NewServer(version string) *mcp.Server {
	return mcp.NewServer(
		&mcp.Implementation{Name: "flowstate", Version: version},
		&mcp.ServerOptions{Capabilities: uiServerCapabilities()},
	)
}

// ServeTools registers one tool per RPC, plus any extra tools the caller
// supplies, and runs the server on stdio.
func ServeTools(
	ctx context.Context,
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	deps Deps,
	extra ...ToolRegistration,
) error {
	AddCapabilities(srv, local, remote, deps, extra...)

	return srv.Run(ctx, &mcp.StdioTransport{})
}

// AddCapabilities is the one registration, shared with the tests so what they
// exercise is what an agent connects to — two registration sites would be the
// two-copies defect this repository keeps refinding, on a new surface.
//
// Two halves, and the split is what each is for: tools are the verbs, resources
// are what an agent reads before choosing one. See resources.go.
func AddCapabilities(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	deps Deps,
	extra ...ToolRegistration,
) {
	AddTools(srv, local, remote, deps, extra...)
	addResources(srv, local, deps)
	addUIResources(srv)
}

// AddLocalCapabilities registers only what answers in *this* process:
// the [LocalTools] RPCs, plus whatever extra tools the caller supplies, plus
// the read-only reference resources. It is what `flow mcp serve` — the
// token-gated HTTP surface, picatz/flowstate#558 — builds its server from.
//
// Three differences from [AddCapabilities], each of them a thing that surface
// must not have:
//
//   - No RPC tool that dispatches to a deployment. Those call through a
//     client this process authenticates as *itself*, so serving them to a
//     caller whose own authority nothing here checks yet would make this
//     process a deputy for whoever holds a token. That is why there is no
//     remote parameter to pass: a registration that cannot name a client
//     cannot dispatch to one.
//   - No UI resources. The one card this surface publishes renders
//     flowstate_get ([ToolViews]), which is one of the tools above, so
//     mounting it would advertise a view of a tool that is not there.
//   - Nothing derived from [Deps.RemoteCatalogAddress]: GetCatalog answers
//     from this binary's own build, which is the only answer available when
//     no deployment is addressed.
func AddLocalCapabilities(
	srv *mcp.Server,
	local *server.FlowstateServer,
	deps Deps,
	extra ...ToolRegistration,
) {
	// Set here rather than asked of the caller: this function *is* the
	// reduced surface, so a caller that had to remember to say so could
	// forget, and the failure would be a tool description promising a
	// capability that is not there. deps is a value, so nothing the caller
	// holds is changed.
	deps.reduced = true

	for _, method := range WorkflowServiceMethods() {
		if !LocalTools[method.Name] {
			continue
		}

		name := ToolName(method.Name)
		srv.AddTool(&mcp.Tool{
			Name:        name,
			Description: toolDescription(method.Name, deps.reduced),
			InputSchema: SchemaForMessage(method.Input),
			// No Meta: [ToolViews] names no local tool today, and a view
			// declared here would point at a resource this function does not
			// mount. If that ever changes, it changes here deliberately.
		}, wrapToolHandler(deps, name, dispatch(method, local, noRemoteClient, deps)))
	}

	for _, reg := range extra {
		srv.AddTool(reg.Tool, wrapToolHandler(deps, reg.Tool.Name, reg.Handler))
	}

	addResources(srv, local, deps)
}

// wrapToolHandler applies [Deps.WrapHandler] when one was given, and is the
// identity otherwise.
func wrapToolHandler(deps Deps, name string, handler mcp.ToolHandler) mcp.ToolHandler {
	if deps.WrapHandler == nil {
		return handler
	}

	return deps.WrapHandler(name, handler)
}

// wrapResourceHandler applies [Deps.WrapResourceHandler] when one was given,
// and is the identity otherwise.
func wrapResourceHandler(deps Deps, uri string, handler mcp.ResourceHandler) mcp.ResourceHandler {
	if deps.WrapResourceHandler == nil {
		return handler
	}

	return deps.WrapResourceHandler(uri, handler)
}

// noRemoteClient is the client [AddLocalCapabilities] hands its dispatchers.
// It is never called: every method registered there is a [LocalTools] one,
// whose Call closure takes the local server and ignores this argument
// entirely (see [WorkflowServiceMethods]). It panics rather than returning
// nil so that a future method added to LocalTools which *does* dial fails
// loudly in a test rather than nil-dereferencing in production — the
// registration filter is the guarantee, and this is the alarm on it.
func noRemoteClient() flowstatev1connect.WorkflowServiceClient {
	panic("mcp: a tool registered by AddLocalCapabilities tried to dispatch to a deployment; " +
		"only LocalTools may be registered there, because this process would dispatch as itself")
}

// AddTools registers one tool per RPC, plus whatever tools the caller passes
// as extra — flowstate_run_local and flowstate_test, in cmd/flow's own
// wiring.
func AddTools(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	deps Deps,
	extra ...ToolRegistration,
) {
	for _, method := range WorkflowServiceMethods() {
		tool := &mcp.Tool{
			Name:        ToolName(method.Name),
			Description: ToolDescription(method.Name),
			InputSchema: SchemaForMessage(method.Input),
		}
		if view, ok := ToolViews[method.Name]; ok {
			tool.Meta = uiToolMeta(view)
		}

		// GetCatalog is the one RPC whose dispatch a caller may override at
		// registration time rather than always answering from the in-process
		// server — see [Deps.RemoteCatalogAddress] for why, and
		// [remoteCatalogCall] for the refusal this substitutes in.
		if method.Name == "GetCatalog" && deps.RemoteCatalogAddress != "" {
			method.Call = remoteCatalogCall(deps.RemoteCatalogAddress)
		}

		srv.AddTool(tool, dispatch(method, local, remote, deps))
	}

	for _, reg := range extra {
		srv.AddTool(reg.Tool, reg.Handler)
	}
}

// ToolName renders an RPC name as a tool name: GetCatalog becomes
// flowstate_get_catalog, which is the casing MCP tools conventionally use.
func ToolName(rpc string) string {
	var b strings.Builder
	b.WriteString(ToolPrefix)
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

// ServiceMethod is one RPC, as the tool derivation needs it.
type ServiceMethod struct {
	Name  string
	Input protoreflect.MessageDescriptor
	Call  func(ctx context.Context, local *server.FlowstateServer,
		remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error)
}

// WorkflowServiceMethods enumerates the service.
//
// The names and shapes come from the descriptor — asserted against it by test,
// in both directions — while the dispatch is written out, because Go generics
// cannot rank over connect's typed methods without reflection that would cost
// more clarity than these lines do. A method added to the service without a row
// here fails TestEveryRPCIsATool.
func WorkflowServiceMethods() []ServiceMethod {
	return []ServiceMethod{
		{
			Name:  "Validate",
			Input: (&v1.ValidateRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.Validate(ctx, connect.NewRequest(in.(*v1.ValidateRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Compile",
			Input: (&v1.CompileRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.Compile(ctx, connect.NewRequest(in.(*v1.CompileRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "GetCatalog",
			Input: (&v1.GetCatalogRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.GetCatalog(ctx, connect.NewRequest(in.(*v1.GetCatalogRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Run",
			Input: (&v1.RunRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Run(ctx, connect.NewRequest(in.(*v1.RunRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Get",
			Input: (&v1.GetRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Get(ctx, connect.NewRequest(in.(*v1.GetRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Signal",
			Input: (&v1.SignalRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
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
			Name:  "SignalWithStart",
			Input: (&v1.SignalWithStartRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().SignalWithStart(ctx, connect.NewRequest(in.(*v1.SignalWithStartRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "List",
			Input: (&v1.ListRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().List(ctx, connect.NewRequest(in.(*v1.ListRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Cancel",
			Input: (&v1.CancelRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Cancel(ctx, connect.NewRequest(in.(*v1.CancelRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "Terminate",
			Input: (&v1.TerminateRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
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
			Name:  "CreateSchedule",
			Input: (&v1.CreateScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().CreateSchedule(ctx, connect.NewRequest(in.(*v1.CreateScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "ListSchedules",
			Input: (&v1.ListSchedulesRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().ListSchedules(ctx, connect.NewRequest(in.(*v1.ListSchedulesRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "DescribeSchedule",
			Input: (&v1.DescribeScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().DescribeSchedule(ctx, connect.NewRequest(in.(*v1.DescribeScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "DeleteSchedule",
			Input: (&v1.DeleteScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().DeleteSchedule(ctx, connect.NewRequest(in.(*v1.DeleteScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "PauseSchedule",
			Input: (&v1.PauseScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().PauseSchedule(ctx, connect.NewRequest(in.(*v1.PauseScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "ResumeSchedule",
			Input: (&v1.ResumeScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().ResumeSchedule(ctx, connect.NewRequest(in.(*v1.ResumeScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			Name:  "TriggerSchedule",
			Input: (&v1.TriggerScheduleRequest{}).ProtoReflect().Descriptor(),
			Call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().TriggerSchedule(ctx, connect.NewRequest(in.(*v1.TriggerScheduleRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
	}
}

// remoteCatalogCall builds the GetCatalog dispatch AddTools substitutes in
// place of [WorkflowServiceMethods]'s own in-process entry, when
// [Deps.RemoteCatalogAddress] names a deployment.
//
// address is carried only to name it in the refusal below — the RPC itself
// dials through remote(), which already carries the address the client was
// built with. Fails closed: an error dialling or answering from that
// deployment is returned as-is rather than falling back to local()'s answer,
// which is the behavior #439 exists to remove. A silent fallback here would
// be the identical defect one level up — a caller asking a named deployment
// and receiving, without being told, an answer describing something else.
func remoteCatalogCall(address string) func(
	ctx context.Context, _ *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message,
) (proto.Message, error) {
	return func(ctx context.Context, _ *server.FlowstateServer,
		remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
		resp, err := remote().GetCatalog(ctx, connect.NewRequest(in.(*v1.GetCatalogRequest)))
		if err != nil {
			return nil, fmt.Errorf("getting the task catalog from the deployment at %s: %w\n  "+
				"this refuses rather than falling back to this binary's own build, which could describe "+
				"different tasks; fix --address/FLOWSTATE_ADDRESS, or start that deployment, then retry",
				address, err)
		}

		return resp.Msg, nil
	}
}

// dispatch adapts one RPC into a tool handler.
//
// Arguments arrive as JSON and leave as protojson of the response message —
// the same bytes `--output json` prints, from the same schema, which is what
// keeps this surface from being a second dialect.
func dispatch(
	method ServiceMethod,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
	deps Deps,
) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in := NewMessage(method.Input)

		// DiscardUnknown stays false on purpose: the schema advertised
		// additionalProperties false, and honouring a field the schema does not
		// have would make the tool "work" while doing something other than what
		// was asked.
		if raw := req.Params.Arguments; len(raw) > 0 {
			if err := protojson.Unmarshal(raw, in); err != nil {
				return ToolError(fmt.Errorf("arguments do not match %s: %w", method.Input.FullName(), err)), nil
			}
		}

		out, err := method.Call(ctx, local, remote, in)
		if err != nil {
			return ToolError(err), nil
		}

		// An agent's context is an untrusted-consumer surface exactly like a
		// terminal, so `flowstate_get` honours `sensitive:` too. This tool
		// addresses a run by id alone, over a generic RPC dispatch shared by
		// every method in the service — there is no workflow specification
		// anywhere in reach here, which is the fail-closed case cmd/flow's
		// sensitive.go names for `flow get`: workflow is nil, so every
		// declared output is withheld unless the caller's Deps.Redact says
		// otherwise.
		if response, ok := out.(*v1.GetResponse); ok {
			out = deps.Redact(response)
		}

		encode := func(message proto.Message) ([]byte, error) {
			return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
		}

		// The bound this surface holds every answer to — see [MaxResultBytes].
		//
		// A GetResponse is reduced rather than refused, because it is the one
		// answer here whose size the *workload* chooses and the one this
		// surface therefore knows how to shed: see [getResponseLadder] for the
		// order and for why no other response message has one. `flow get` is
		// also the tool an agent reaches for when a run has gone wrong, which
		// is exactly when its transcript is largest — refusing then answers
		// "your run is too big to look at" to the question "what happened".
		//
		// Everything else is still refused, and the comment that used to sit
		// here is still the reason: nothing knows which field of an arbitrary
		// response could be dropped without changing what it says. Two of
		// those refusals are load-bearing rather than residual:
		//
		//   - A **listing** must not shed runs. ListResponse.next_page_token
		//     addresses where the server's scan stopped, not where a reduction
		//     here stopped, and this package cannot mint one — so returning
		//     fewer runs beside the server's token would leave the dropped
		//     ones behind a cursor that has already moved past them, absent
		//     from every later page rather than delayed. That is the defect
		//     server/list.go bounds its own batch size to make
		//     unrepresentable, and it must not be reintroduced one layer up.
		//     Returning fewer runs with an *empty* token is worse still: a
		//     truncated listing claiming to be the whole of it. `page_size` is
		//     the caller's own honest lever, which is what the refusal names.
		//   - A **ValidationReport** must not shed diagnostics, for the reason
		//     diagnostics exist: a file reported with some of its problems
		//     tells an author to fix what they were shown and ship the rest.
		if response, ok := out.(*v1.GetResponse); ok {
			rungs, notes := getResponseLadder(response, encode)

			encoded, rung, err := FitResult(rungs...)
			if err != nil {
				return ToolError(err), nil
			}

			content := []mcp.Content{&mcp.TextContent{Text: string(encoded)}}

			// What left, as a second content block rather than a field in the
			// document. The first block stays exactly the protojson of a
			// GetResponse — the same bytes `--output json` prints, which is
			// what keeps this surface from being a second dialect — so a
			// caller that parses the answer is never handed a shape the schema
			// does not describe. An MCP result is a list of blocks precisely
			// so an annotation need not be smuggled into the payload.
			if notes[rung] != "" {
				content = append(content, &mcp.TextContent{Text: notes[rung]})
			}

			return &mcp.CallToolResult{Content: content}, nil
		}

		encoded, err := encode(out)
		if err != nil {
			return ToolError(err), nil
		}

		if len(encoded) > MaxResultBytes {
			return ToolError(fmt.Errorf(
				"%s answered with %d bytes, over this surface's %d byte limit, so nothing was returned "+
					"rather than a document cut short; ask for less: a single run by id rather than a "+
					"listing, a smaller page, or a narrower filter",
				ToolName(method.Name), len(encoded), MaxResultBytes)), nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// ToolError reports a failure as the tool's result rather than a protocol
// error, which is what lets a model read the reason and correct itself.
func ToolError(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
	}
}

// The two tools that are not RPCs — flowstate_run_local and flowstate_test —
// are declared here (name, description, schema) because their metadata is
// part of this surface's derivation, walked by cmd/flow's docs generator
// exactly like every RPC's. Their *handlers* stay in cmd/flow: both execute
// against this binary's own flags (egress, secrets, plugins for run_local;
// nothing for test, which is why its handler needs no posture at all — see
// cmd/flow/mcp.go).

// RunLocalToolName is the tool an agent calls to execute what it just wrote.
const RunLocalToolName = ToolPrefix + "run_local"

// RunLocalToolDescription is written for the model that has to decide whether
// this is the tool it wants, and what it will and will not have proved by using
// it.
const RunLocalToolDescription = "Execute a Flowfile immediately, in this process, with no server and no Temporal, " +
	"the same rehearsal `flow run local` performs. Use it to verify a workflow you just authored: " +
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
	"Bounded: `sleep: 24h` is a legal Flowfile, and this call holds this turn open for as long as the " +
	"workflow runs, so the operator's --run-local-timeout (default 2m) stops execution and reports the " +
	"run as timed out rather than letting an untrusted workflow hold the call forever.\n\n" +
	"A source declaring `inputs:` is given them in the `inputs` object of this call, keyed by declared " +
	"name and typed as declared; a required one left out, an undeclared name, or a mistyped value is " +
	"refused before any step runs. What the source declares under `outputs:` comes back as `runOutputs`.\n\n" +
	"Answers with {\"run\": <GetResponse>, \"logs\": [...]}: the run's status, timing and step outputs, " +
	"plus whatever `log:` steps emitted. Invalid sources come back as an error carrying positioned " +
	"diagnostics (line:column) to correct against."

// RunLocalTool declares the tool.
func RunLocalTool() *mcp.Tool {
	return &mcp.Tool{
		Name:        RunLocalToolName,
		Description: RunLocalToolDescription,
		InputSchema: runLocalInputSchema(),
	}
}

// TestToolName is the tool an agent calls to rehearse what it just wrote.
const TestToolName = ToolPrefix + "test"

// TestToolDescription is written for the model choosing between this tool and
// flowstate_run_local.
//
// Assembled from three parts rather than written as one string, because the
// middle part is the only one that is not true everywhere: a surface that does
// not serve flowstate_run_local (see [AddLocalCapabilities]) must not tell a
// model to reach for it afterward. Derived rather than duplicated, so the four
// paragraphs both surfaces share cannot drift into two versions — see
// [ReducedTestToolDescription]. Reported by Codex on picatz/flowstate#807.
const TestToolDescription = testToolDescriptionOpening + testToolRunLocalComparison + testToolDescriptionRest

// ReducedTestToolDescription is [TestToolDescription] with the two paragraphs
// that compare this tool against flowstate_run_local replaced by the one
// sentence that is true where that tool is not served: it is not there.
//
// Replaced rather than dropped, because "reach for the other tool afterward"
// is real advice a model needs an answer to, and silence would leave it
// looking for one.
const ReducedTestToolDescription = testToolDescriptionOpening + testToolNoRunLocal + testToolDescriptionRest

// testToolNoRunLocal is the reduced surface's replacement for
// [testToolRunLocalComparison].
const testToolNoRunLocal = "Needs no egress policy and no operator opt-in: a stubbed run never invokes a real " +
	"task's implementation at all (not `http`, not a plugin task registered by --plugin-dir) so there " +
	"is no network for a policy to govern, and no secret this tool could resolve even where one is " +
	"configured. That is why it is served here at all: this surface deliberately serves no tool that " +
	"executes a submitted workflow for real, so there is nothing to reach for afterward — rehearse " +
	"here, and run the workflow where you would run it in earnest.\n\n" +
	"What it does not prove: that a real task behaves the way a stub's `returns:` or `fails:` says it " +
	"does, or anything about durability.\n\n"

// testToolRunLocalComparison is the pair of paragraphs that only make sense
// where flowstate_run_local is also served.
const testToolRunLocalComparison = "Needs no egress policy and no operator opt-in, unlike flowstate_run_local: a stubbed run never " +
	"invokes a real task's implementation at all (not `http`, not a plugin task registered by " +
	"--plugin-dir) so there is no network for a policy to govern, and no secret this tool could resolve " +
	"even where one is configured. Reach for this first, while authoring: it proves conditions, retries, " +
	"`undo:` compensation, and data-flow expressions without ever touching a network. Reach for " +
	"flowstate_run_local afterward, once egress is configured, to rehearse the real effect of whichever " +
	"task you deliberately left unstubbed.\n\n" +
	"What it does not prove: that a real task behaves the way a stub's `returns:` or `fails:` says it " +
	"does, or anything about durability: flowstate_run_local's own limits, on top of never running a " +
	"real task at all.\n\n"

// testToolDescriptionOpening is what the tool is, true on every surface.
const testToolDescriptionOpening = "Run a Flowfile against inline test cases the way `flow test` runs a *.test.yaml " +
	"beside a workflow on disk: the identical machinery (flowtest.RunSource), on bytes submitted here " +
	"instead of two files. Every task the workflow would otherwise call is replaced: a stub answers with " +
	"its `returns:`, or fails the way its `fails:` describes, and any task this case invokes with no " +
	"matching stub is refused rather than run for real, naming the task and how many stubs were declared " +
	"for it. Time is virtual, so a case with `sleep: 24h` resolves in under a second, and a " +
	"wait_for_signal step is answered by `signals:` scripted for a chosen offset from the run's start.\n\n"

// testToolDescriptionRest is the argument and answer reference, likewise true
// everywhere.
const testToolDescriptionRest = "`tests` is a `*.test.yaml` document: `tests:` names one or more cases, each with an optional " +
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
	"verdict at all (the workflow failed to compile, a stub named a task with no matching invocation, or " +
	"the run failed in a way the case did not declare with `expect.failed`) reports why in `error` " +
	"instead of `failures`. `refused` is set instead of any case running at all when the submitted " +
	"`tests` document itself does not parse."

// TestTool declares the tool.
func TestTool() *mcp.Tool {
	return &mcp.Tool{
		Name:        TestToolName,
		Description: TestToolDescription,
		InputSchema: testInputSchema(),
	}
}

// ReducedTestTool declares the tool for a surface that serves no
// flowstate_run_local — see [ReducedTestToolDescription]. Identical in every
// other respect, and deliberately built from the same schema, because the two
// differ in what they say and never in what they accept.
func ReducedTestTool() *mcp.Tool {
	tool := TestTool()
	tool.Description = ReducedTestToolDescription

	return tool
}

// NewMessage constructs an empty message for a descriptor.
func NewMessage(md protoreflect.MessageDescriptor) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		// Unreachable for the compiled-in schema; loud if it ever is not.
		panic("flow mcp: no type for " + string(md.FullName()))
	}

	return mt.New().Interface()
}
