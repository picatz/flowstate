package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// attachSourceWorkflow is the program a durable run was submitted from.
const attachSourceWorkflow = `edition: v2026.4
name: attach-lines
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
`

// attachSourceMoved is the same program with its lines moved: it compiles to
// the same steps, and only its bytes say it is not the file the run came from.
const attachSourceMoved = `edition: v2026.4
name: attach-lines
# a comment that moves every step down a line
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
`

// heldRunServer answers an attach as a durable run held at its first step,
// reporting the program it executes by digest, and accepts any breakpoint set.
type heldRunServer struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	irDigest string
	sent     []*v1.DebugBreakpoint
}

func (h *heldRunServer) snapshot() *v1.DebugSnapshot {
	return &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, IrDigest: h.irDigest}
}

func (h *heldRunServer) DebugAttach(_ context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	return connect.NewResponse(&v1.DebugAttachResponse{SessionId: "s", Snapshot: h.snapshot(),
		Receipt: &v1.DebugReceipt{RequestId: req.Msg.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}}), nil
}

func (h *heldRunServer) DebugSetBreakpoints(_ context.Context, req *connect.Request[v1.DebugSetBreakpointsRequest]) (*connect.Response[v1.DebugSetBreakpointsResponse], error) {
	h.sent = req.Msg.GetBreakpoints()

	return connect.NewResponse(&v1.DebugSetBreakpointsResponse{Snapshot: h.snapshot(),
		Receipt: &v1.DebugReceipt{RequestId: req.Msg.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}}), nil
}

func (h *heldRunServer) DebugGet(context.Context, *connect.Request[v1.DebugGetRequest]) (*connect.Response[v1.DebugGetResponse], error) {
	return connect.NewResponse(&v1.DebugGetResponse{Snapshot: h.snapshot()}), nil
}

func (h *heldRunServer) DebugResume(_ context.Context, req *connect.Request[v1.DebugResumeRequest]) (*connect.Response[v1.DebugResumeResponse], error) {
	return connect.NewResponse(&v1.DebugResumeResponse{Snapshot: h.snapshot(),
		Receipt: &v1.DebugReceipt{RequestId: req.Msg.GetRequestId(), Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}}), nil
}

// TestADAPAttachMapsLinesOnlyFromTheFileTheRunCameFrom: an editor attaching
// to a durable run with the file the run was submitted from gets its lines,
// and a line breakpoint names the step on it. The same program with its
// lines moved is a different file, and its lines are not used, where before
// its steps' identical compilation made it indistinguishable.
func TestADAPAttachMapsLinesOnlyFromTheFileTheRunCameFrom(t *testing.T) {
	t.Parallel()

	submitted, err := loadWorkflow(writeWorkflowFile(t, attachSourceWorkflow))
	require.NoError(t, err)

	for name, test := range map[string]struct {
		program  string
		verified bool
		reason   string
	}{
		"the file the run came from": {program: attachSourceWorkflow, verified: true},
		"the same program, its lines moved": {program: attachSourceMoved,
			reason: "the source map does not match the program this run executes"},
		"no program": {reason: "no program was given to map lines from"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server := &heldRunServer{irDigest: v1.WorkflowIRDigest(submitted)}
			mux := http.NewServeMux()
			mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(server))
			httpServer := httptest.NewServer(mux)
			t.Cleanup(httpServer.Close)

			cmd := &cobra.Command{}
			addServerFlags(cmd)
			require.NoError(t, cmd.Flags().Set("address", httpServer.URL))

			args := flowdap.AttachArguments{WorkflowID: "attach-lines"}
			if test.program != "" {
				args.Program = writeWorkflowFile(t, test.program)
			}
			attachment, err := attachDebuggedRun(t.Context(), cmd, args)
			require.NoError(t, err)
			t.Cleanup(func() { _ = attachment.Target.Close() })
			assert.Equal(t, test.verified, attachment.SourceMap != nil, "whether the attach carries the program's lines")

			// `second` is on line 8 of the file the run came from.
			line := uint32(8)
			if test.program == attachSourceMoved {
				line = 9
			}
			response, err := attachment.Target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
				Breakpoints: []*v1.DebugBreakpoint{{Id: "b1", Line: &v1.DebugSourceLine{
					Uri: args.Program, Line: line}}},
			})
			require.NoError(t, err)
			require.Len(t, response.GetBreakpoints(), 1)
			state := response.GetBreakpoints()[0]
			if test.verified {
				require.Len(t, server.sent, 1, "a verified line breakpoint was not sent to the run")
				assert.Equal(t, "second", server.sent[0].GetStep(), "the line named another step")

				return
			}
			assert.Empty(t, server.sent, "an unverified line breakpoint reached the run")
			assert.Contains(t, state.GetMessage(), test.reason)
		})
	}
}

// TestADAPAttachProgramThatCannotBeUsedSaysWhy: a `program` whose file cannot
// be read says so, and one that does not compile fails the attach without its
// diagnostics, which could quote a value its declarations were to withhold.
// Neither reaches the server.
func TestADAPAttachProgramThatCannotBeUsedSaysWhy(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	addServerFlags(cmd)
	require.NoError(t, cmd.Flags().Set("address", "http://127.0.0.1:1"))

	_, err := attachDebuggedRun(t.Context(), cmd, flowdap.AttachArguments{WorkflowID: "w", Program: "/nonexistent/workflow.yaml"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading the attach configuration's `program`", "an unreadable file was reported as something else")
	assert.NotContains(t, err.Error(), "does not compile")

	invalid := writeWorkflowFile(t, "edition: v2026.4\nname: broken\nsteps:\n  - id: hi\n    log:\n      message: hi\n    unknown-key-quoting-hunter2: 1\n")
	_, err = attachDebuggedRun(t.Context(), cmd, flowdap.AttachArguments{WorkflowID: "w", Program: invalid})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not compile")
	assert.NotContains(t, err.Error(), "hunter2", "the attach quoted the program's diagnostics")
}

// TestADAPAttachMapsAProgramWhosePluginsRunElsewhere: an attach reads its
// `program` for lines alone, and the run it attaches to executes on a worker
// that has the plugins its tasks name. So a program using a plugin task this
// process never loaded still maps, where validating it against what runs here
// refused the attach before it reached the server (Codex, #2223).
func TestADAPAttachMapsAProgramWhosePluginsRunElsewhere(t *testing.T) {
	t.Parallel()

	const plugged = `edition: v2026.4
name: attach-plugin
plugins:
  example: v1.0.0
steps:
  - id: greet
    example.greet:
      greeting: Hello
      name: attach
`
	program := writeWorkflowFile(t, plugged)
	submitted, _, err := flowfile.ParseAt([]byte(plugged), program)
	require.NoError(t, err)

	server := &heldRunServer{irDigest: v1.WorkflowIRDigest(submitted)}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(server))
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	cmd := &cobra.Command{}
	addServerFlags(cmd)
	require.NoError(t, cmd.Flags().Set("address", httpServer.URL))

	attachment, err := attachDebuggedRun(t.Context(), cmd, flowdap.AttachArguments{WorkflowID: "attach-plugin", Program: program})
	require.NoError(t, err, "a program whose plugin runs on the worker was refused here")
	t.Cleanup(func() { _ = attachment.Target.Close() })
	assert.NotNil(t, attachment.SourceMap, "the program's lines were not used")
}

// TestDebugAttachMapsAProgramWhosePluginsRunElsewhere is the same for `flow
// debug attach --program`, which reads its program for lines the same way: a
// program using a plugin task this process never loaded still maps, rather
// than failing validation here before the attach reaches the server.
func TestDebugAttachMapsAProgramWhosePluginsRunElsewhere(t *testing.T) {
	t.Parallel()

	const plugged = `edition: v2026.4
name: attach-plugin
plugins:
  example: v1.0.0
steps:
  - id: greet
    example.greet:
      greeting: Hello
      name: attach
`
	program := writeWorkflowFile(t, plugged)
	submitted, _, err := flowfile.ParseAt([]byte(plugged), program)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(&heldRunServer{irDigest: v1.WorkflowIRDigest(submitted)}))
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("quit\n"), 0o600))

	res := runFlow(t, "debug", "attach", "attach-plugin", "--program", program, "--script", script, "--address", httpServer.URL)
	require.NoError(t, res.Err, "a program whose plugin runs on the worker was refused here: %s", res.Output())
	assert.Contains(t, res.Output(), "attached to attach-plugin", "the attach did not reach the server")
	assert.NotContains(t, res.Output(), "does not match the program", "the program's lines were not used")
}
