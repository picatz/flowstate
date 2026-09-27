package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Retained debug sessions over MCP (#2127): an agent starts or attaches a
// session once, then observes, inspects and commands it across many calls,
// and ends it — the incremental counterpart to the one-shot flowstate_debug
// script, which stays.
//
// A session is leased. Every call on it renews the lease; one nobody calls
// lapses and is ended, which releases a durable run and lets a stubbed case
// finish. The number of sessions one server holds is bounded. A command
// carrying a request id is answered from memory when retried, so a lost
// response never moves a run twice, and a start carrying one never starts a
// second run: continuing a session is never silently replaced by restarting
// it.

const (
	maxDebugSessions      = 8
	debugSessionIdle      = 10 * time.Minute
	debugSessionLifetime  = time.Hour
	maxDebugSessionWait   = 30 * time.Second
	maxSessionReceipts    = 64
	debugSessionEndSettle = 5 * time.Second
)

// debugSessions is one server's retained sessions.
type debugSessions struct {
	remote func() flowstatev1connect.WorkflowServiceClient

	mu       sync.Mutex
	sessions map[string]*debugSessionEntry
	starts   map[string]string
}

type debugSessionEntry struct {
	id      string
	target  flowdebug.Target
	driver  *flowdebug.Driver
	local   *flowdebug.Session
	started time.Time

	// mu serializes calls on one session, so two commands never race to move
	// one run.
	mu       sync.Mutex
	expires  time.Time
	receipts map[string]json.RawMessage
	order    []string

	transcript *lockedTranscript
	cursor     int

	cancel context.CancelFunc
	done   chan struct{}
	// report is the case's verdict, written before done is closed and read
	// only after.
	report *v1.TestReport
}

// lockedTranscript is a transcript a run's goroutine writes while calls read.
type lockedTranscript struct {
	mu sync.Mutex
	debugTranscript
}

func (t *lockedTranscript) add(text string, tone flowdebug.Tone) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.debugTranscript.add(text, tone)
}

func (t *lockedTranscript) since(cursor int) ([]debugFragment, int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if cursor > len(t.fragments) {
		cursor = len(t.fragments)
	}

	return slices.Clone(t.fragments[cursor:]), len(t.fragments)
}

func newDebugSessions(remote func() flowstatev1connect.WorkflowServiceClient) *debugSessions {
	return &debugSessions{remote: remote, sessions: map[string]*debugSessionEntry{}, starts: map[string]string{}}
}

// sweep ends every session whose lease or lifetime has lapsed.
func (r *debugSessions) sweep() {
	now := time.Now()

	r.mu.Lock()
	var lapsed []*debugSessionEntry
	for id, entry := range r.sessions {
		entry.mu.Lock()
		expired := now.After(entry.expires) || now.Sub(entry.started) > debugSessionLifetime
		entry.mu.Unlock()
		if expired {
			lapsed = append(lapsed, entry)
			delete(r.sessions, id)
		}
	}
	r.mu.Unlock()

	for _, entry := range lapsed {
		entry.end(false)
	}
}

// register admits a new session, within the bound.
func (r *debugSessions) register(entry *debugSessionEntry, request string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.sessions) >= maxDebugSessions {
		return fmt.Errorf("this server already holds %d debug sessions; end one with %s", maxDebugSessions, debugSessionEndTool)
	}
	r.sessions[entry.id] = entry
	if request != "" {
		r.starts[request] = entry.id
	}

	return nil
}

// lookup returns a live session and renews its lease.
func (r *debugSessions) lookup(id string) (*debugSessionEntry, error) {
	r.sweep()

	r.mu.Lock()
	entry, ok := r.sessions[id]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no debug session %q: it ended, its lease lapsed, or it never existed; start or attach a new one", id)
	}

	entry.mu.Lock()
	entry.expires = time.Now().Add(debugSessionIdle)
	entry.mu.Unlock()

	return entry, nil
}

// end releases a session: a durable run is detached (or left attached when
// keep is set); a stubbed case is let finish, and cancelled if it cannot.
func (e *debugSessionEntry) end(keep bool) {
	if remote, ok := e.target.(*flowdebug.Remote); ok && keep {
		_ = remote.Disconnect()
	} else {
		_ = e.target.Close()
	}
	if e.done == nil {
		return
	}
	select {
	case <-e.done:
	case <-time.After(debugSessionEndSettle):
		e.cancel()
		<-e.done
	}
}

const (
	debugSessionStartTool   = flowmcp.ToolPrefix + "debug_session_start"
	debugSessionAttachTool  = flowmcp.ToolPrefix + "debug_session_attach"
	debugSessionObserveTool = flowmcp.ToolPrefix + "debug_session_observe"
	debugSessionCommandTool = flowmcp.ToolPrefix + "debug_session_command"
	debugSessionEndTool     = flowmcp.ToolPrefix + "debug_session_end"
)

// tools declares the retained-session tools.
func (r *debugSessions) tools() []flowmcp.ToolRegistration {
	str := func(description string) map[string]any {
		return map[string]any{"type": "string", "description": description}
	}
	object := func(properties map[string]any, required ...string) map[string]any {
		schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		if len(required) > 0 {
			req := make([]any, 0, len(required))
			for _, name := range required {
				req = append(req, name)
			}
			schema["required"] = req
		}

		return schema
	}
	session := str("The session id " + debugSessionStartTool + " or " + debugSessionAttachTool + " returned.")
	request := str("Optional retry key. A call repeated with the same key is answered with the first call's answer, " +
		"so a lost response never moves the run twice.")

	return []flowmcp.ToolRegistration{
		{Tool: &mcp.Tool{
			Name: debugSessionStartTool,
			Description: "Start a retained debug session over one test case of a Flowfile — the same stubbed, egress-free, " +
				"virtual-clock run flowstate_test uses — held at its first step. Drive it with " + debugSessionCommandTool +
				", read it with " + debugSessionObserveTool + ", and finish with " + debugSessionEndTool + ". The session " +
				"is leased: each call renews it, and one idle for 10 minutes is ended. Answers with the session id, its " +
				"typed snapshot (state, stop reason, occurrence address, frames, capabilities, revision), and the " +
				"transcript so far.",
			InputSchema: object(map[string]any{
				"workflow":   str("The Flowfile YAML to debug, including its `edition:` line."),
				"tests":      str("A `*.test.yaml` document whose case supplies inputs, stubs and signals."),
				"case":       str("The case to debug, when `tests` names more than one."),
				"request_id": request,
			}, "workflow", "tests"),
		}, Handler: r.start},
		{Tool: &mcp.Tool{
			Name: debugSessionAttachTool,
			Description: "Attach a retained debug session to a durable run on the configured server, holding it at its next " +
				"step boundary. Needs the run's `debug:` policy to name you and workload.debug (workload.debug_inspect to " +
				"evaluate). A hold never freezes work already dispatched. Pass session_id to rejoin a session.",
			InputSchema: object(map[string]any{
				"workflow_id": str("The durable run's workflow id."),
				"run_id":      str("Optional: the chain's first run id."),
				"session_id":  str("Optional: rejoin this session instead of attaching a new one."),
			}, "workflow_id"),
		}, Handler: r.attach},
		{Tool: &mcp.Tool{
			Name: debugSessionObserveTool,
			Description: "Read a retained session: its typed snapshot and the transcript since the last observe. Set " +
				"after_revision and wait_seconds (at most 30) to wait for the next stop instead of polling.",
			InputSchema: object(map[string]any{
				"session_id":     session,
				"after_revision": map[string]any{"type": "integer", "minimum": 0},
				"wait_seconds":   map[string]any{"type": "integer", "minimum": 0, "maximum": 30},
			}, "session_id"),
		}, Handler: r.observe},
		{Tool: &mcp.Tool{
			Name: debugSessionCommandTool,
			Description: "Run one debugger command in a retained session and answer with its typed result. Commands: " +
				"step, next, finish, continue, until <step>, pause, break <step> [hit <n>] [if <expr>], log <step> <msg>, " +
				"catch none|uncaught|all, delete <step>, breakpoints, inspect <expr>, expand <expr>, scope, backtrace, " +
				"detach, status. Movements answer with the next stop. Set expected_revision to the snapshot you acted on, " +
				"so a command meant for a stop the run has left is refused as stale.",
			InputSchema: object(map[string]any{
				"session_id":        session,
				"command":           str("One debugger command line."),
				"expected_revision": map[string]any{"type": "integer", "minimum": 0},
				"request_id":        request,
			}, "session_id", "command"),
		}, Handler: r.command},
		{Tool: &mcp.Tool{
			Name: debugSessionEndTool,
			Description: "End a retained session. A durable run is detached and continues; with keep, its session stays " +
				"attached for a later rejoin until its lease lapses. A stubbed case is let finish and its test report is " +
				"returned.",
			InputSchema: object(map[string]any{
				"session_id": session,
				"keep":       map[string]any{"type": "boolean"},
			}, "session_id"),
		}, Handler: r.end},
	}
}

// decode reads a tool's arguments, refusing unknown fields.
func decode(req *mcp.CallToolRequest, into any) error {
	raw := req.Params.Arguments
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	return decoder.Decode(into)
}

func toolJSON(value any) *mcp.CallToolResult {
	encoded, err := json.Marshal(value)
	if err != nil {
		return flowmcp.ToolError(err)
	}

	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}
}

// schemaJSON renders a proto message the way every other tool answer does.
func schemaJSON(message proto.Message) json.RawMessage {
	if message == nil {
		return nil
	}
	encoded, err := v1.MarshalSchemaJSON(message, false)
	if err != nil {
		return nil
	}

	return encoded
}

type sessionAnswer struct {
	SessionID  string          `json:"session_id"`
	Expires    string          `json:"lease_expires_at,omitempty"`
	Command    string          `json:"command,omitempty"`
	Text       string          `json:"text,omitempty"`
	Receipt    json.RawMessage `json:"receipt,omitempty"`
	Snapshot   json.RawMessage `json:"snapshot,omitempty"`
	Inspect    json.RawMessage `json:"inspect,omitempty"`
	Transcript []debugFragment `json:"transcript,omitempty"`
	Report     json.RawMessage `json:"report,omitempty"`
	Note       string          `json:"note,omitempty"`
}

func (e *debugSessionEntry) answer(ctx context.Context) sessionAnswer {
	answer := sessionAnswer{SessionID: e.id, Expires: e.expires.UTC().Format(time.RFC3339)}
	if snapshot, err := e.target.Snapshot(ctx); err == nil {
		answer.Snapshot = schemaJSON(snapshot)
	}
	if e.transcript != nil {
		answer.Transcript, e.cursor = e.transcript.since(e.cursor)
		answer.Note = e.transcript.note()
	}

	return answer
}

func (r *debugSessions) start(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		Workflow  string `json:"workflow"`
		Tests     string `json:"tests"`
		Case      string `json:"case"`
		RequestID string `json:"request_id"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionStartTool, err)), nil
	}
	r.sweep()

	if args.RequestID != "" {
		r.mu.Lock()
		existing, started := r.starts[args.RequestID]
		r.mu.Unlock()
		if started {
			entry, err := r.lookup(existing)
			if err != nil {
				return flowmcp.ToolError(err), nil
			}
			entry.mu.Lock()
			defer entry.mu.Unlock()
			answer := entry.answer(ctx)
			answer.Note = strings.TrimSpace("this request id already started this session; it was not started again. " + answer.Note)

			return toolJSON(answer), nil
		}
	}

	oneShot := debugToolArguments{Workflow: args.Workflow, Tests: args.Tests, Case: args.Case}
	if err := checkDebugSource(&oneShot); err != nil {
		return flowmcp.ToolError(err), nil
	}
	selected, err := debugCaseSelector([]byte(args.Tests), args.Case)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	transcript := &lockedTranscript{}
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Emit: transcript.add})
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	entry := &debugSessionEntry{
		id: uuid.NewString(), target: session, driver: flowdebug.NewDriver(session), local: session,
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
		transcript: transcript, cancel: cancel, done: make(chan struct{}),
	}
	entry.driver.Wait = maxDebugSessionWait
	if err := r.register(entry, args.RequestID); err != nil {
		cancel()

		return flowmcp.ToolError(err), nil
	}

	go func() {
		defer close(entry.done)
		defer cancel()

		result := flowtest.RunSourceWith(runCtx, "<submitted>", []byte(args.Workflow), []byte(args.Tests),
			flowtest.RunOptions{Select: selected, Debugger: session})
		// Published by closing done, which is what a reader waits on.
		entry.report = result.Report
		if testReportFailed(result.Report) {
			session.Finished(errors.New("the case did not pass"))
		} else {
			session.Finished(nil)
		}
		_ = session.Close()
	}()

	// The first stop, or the end of a case with no steps to hold at.
	waitCtx, stop := context.WithTimeout(ctx, maxDebugSessionWait)
	defer stop()
	for after := uint64(0); ; {
		snapshot, err := session.WaitSnapshot(waitCtx, after)
		if err != nil || snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_RUNNING {
			break
		}
		after = snapshot.GetRevision()
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return toolJSON(entry.answer(ctx)), nil
}

func (r *debugSessions) attach(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		WorkflowID string `json:"workflow_id"`
		RunID      string `json:"run_id"`
		SessionID  string `json:"session_id"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionAttachTool, err)), nil
	}
	if args.WorkflowID == "" {
		return flowmcp.ToolError(errors.New("workflow_id is required")), nil
	}
	r.sweep()

	remote, receipt, err := flowdebug.AttachRemote(ctx, r.remote(), args.WorkflowID, args.RunID,
		flowdebug.RemoteOptions{SessionID: args.SessionID, Wait: 5 * time.Second})
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	entry := &debugSessionEntry{
		id: remote.SessionID(), target: remote, driver: flowdebug.NewDriver(remote),
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
	}
	entry.driver.Wait = maxDebugSessionWait
	if err := r.register(entry, ""); err != nil {
		_ = remote.Disconnect()

		return flowmcp.ToolError(err), nil
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	answer := entry.answer(ctx)
	answer.Receipt = schemaJSON(receipt)

	return toolJSON(answer), nil
}

func (r *debugSessions) observe(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		SessionID     string `json:"session_id"`
		AfterRevision uint64 `json:"after_revision"`
		WaitSeconds   int    `json:"wait_seconds"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionObserveTool, err)), nil
	}
	entry, err := r.lookup(args.SessionID)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if args.WaitSeconds > 0 {
		wait := min(time.Duration(args.WaitSeconds)*time.Second, maxDebugSessionWait)
		waitCtx, cancel := context.WithTimeout(ctx, wait)
		_, _ = entry.target.WaitSnapshot(waitCtx, args.AfterRevision)
		cancel()
	}

	return toolJSON(entry.answer(ctx)), nil
}

func (r *debugSessions) command(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		SessionID        string `json:"session_id"`
		Command          string `json:"command"`
		ExpectedRevision uint64 `json:"expected_revision"`
		RequestID        string `json:"request_id"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionCommandTool, err)), nil
	}
	if len(args.Command) > flowdebug.MaxCommandBytes || strings.ContainsAny(args.Command, "\r\n") {
		return flowmcp.ToolError(fmt.Errorf("a command is one line of at most %d bytes", flowdebug.MaxCommandBytes)), nil
	}
	entry, err := r.lookup(args.SessionID)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if cached, ok := entry.receipts[args.RequestID]; ok && args.RequestID != "" {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(cached)}}}, nil
	}

	answer := sessionAnswer{SessionID: entry.id, Command: args.Command, Expires: entry.expires.UTC().Format(time.RFC3339)}
	if args.ExpectedRevision != 0 {
		current, err := entry.target.Snapshot(ctx)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		if current.GetRevision() != args.ExpectedRevision {
			answer.Receipt = schemaJSON(&v1.DebugReceipt{
				RequestId: args.RequestID,
				Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
				Revision:  current.GetRevision(),
				Message:   fmt.Sprintf("the command was meant for revision %d, and the session is at %d", args.ExpectedRevision, current.GetRevision()),
			})
			answer.Snapshot = schemaJSON(current)

			return toolJSON(answer), nil
		}
	}

	result, err := entry.driver.Do(ctx, args.Command)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	answer.Text = result.Text
	answer.Receipt = schemaJSON(result.Receipt)
	answer.Snapshot = schemaJSON(result.Snapshot)
	answer.Inspect = schemaJSON(result.Inspect)
	if entry.transcript != nil {
		answer.Transcript, entry.cursor = entry.transcript.since(entry.cursor)
	}

	encoded, err := json.Marshal(answer)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	if args.RequestID != "" {
		entry.receipts[args.RequestID] = encoded
		entry.order = append(entry.order, args.RequestID)
		if len(entry.order) > maxSessionReceipts {
			delete(entry.receipts, entry.order[0])
			entry.order = entry.order[1:]
		}
	}

	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}, nil
}

func (r *debugSessions) end(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		SessionID string `json:"session_id"`
		Keep      bool   `json:"keep"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionEndTool, err)), nil
	}
	entry, err := r.lookup(args.SessionID)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	r.mu.Lock()
	delete(r.sessions, entry.id)
	for request, id := range r.starts {
		if id == entry.id {
			delete(r.starts, request)
		}
	}
	r.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.end(args.Keep)
	answer := entry.answer(ctx)
	if entry.done != nil && entry.report != nil {
		answer.Report = schemaJSON(entry.report)
	}

	return toolJSON(answer), nil
}
