package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
// lapses and is ended by the server's sweeper, which releases a durable run
// and lets a stubbed case finish. The number of sessions one server holds is
// bounded. A command carrying a request id is answered from memory when
// retried, and the id reaches the target too, so a response lost after the
// target accepted it never moves a run twice; a start carrying one never
// starts a second run: continuing a session is never silently replaced by
// restarting it.

const (
	maxDebugSessions      = 8
	debugSessionIdle      = 10 * time.Minute
	debugSessionLifetime  = time.Hour
	maxDebugSessionWait   = 30 * time.Second
	maxSessionReceipts    = 64
	debugSessionEndSettle = 5 * time.Second
	debugSessionSweep     = time.Minute
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

	// calls serializes commands on one session, so two never race to move
	// one run. An observe waits outside it, so a command can produce the
	// revision the observe is waiting for.
	calls sync.Mutex

	// mu guards the fields below it. It is never held across a call on the
	// target.
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

// note is the embedded note, read under the lock the run's goroutine writes
// under.
func (t *lockedTranscript) note() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.debugTranscript.note()
}

func newDebugSessions(remote func() flowstatev1connect.WorkflowServiceClient) *debugSessions {
	return &debugSessions{remote: remote, sessions: map[string]*debugSessionEntry{}, starts: map[string]string{}}
}

// keep ends lapsed sessions on its own clock until ctx ends, so a session
// nobody calls again is still released: a lease enforced only by the next
// call is no lease on an idle server.
func (r *debugSessions) keep(ctx context.Context) {
	ticker := time.NewTicker(debugSessionSweep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweep()
		}
	}
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
			r.forgetLocked(id)
		}
	}
	r.mu.Unlock()

	for _, entry := range lapsed {
		entry.end(false)
	}
}

// forgetLocked drops a session and the start request ids that name it. The
// caller holds r.mu.
func (r *debugSessions) forgetLocked(id string) {
	delete(r.sessions, id)
	for request, started := range r.starts {
		if started == id {
			delete(r.starts, request)
		}
	}
}

// remove drops a session, reporting whether this caller was the one to, so a
// session is ended once.
func (r *debugSessions) remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.sessions[id]
	r.forgetLocked(id)

	return ok
}

// register admits a new session, within the bound. A start request id is
// reserved in the same critical section that admits the session, so two
// starts under one id never both launch a run: the second is handed the
// session the first admitted, and must discard its own.
func (r *debugSessions) register(entry *debugSessionEntry, request string) (*debugSessionEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if request != "" {
		if existing, ok := r.sessions[r.starts[request]]; ok {
			return existing, nil
		}
	}
	if len(r.sessions) >= maxDebugSessions {
		return nil, fmt.Errorf("this server already holds %d debug sessions; end one with %s", maxDebugSessions, debugSessionEndTool)
	}
	// A stubbed case holds the process-wide task registry for as long as it
	// runs, so a second one would wait, uncancellably, for the first to end.
	// Refused here instead, before anything is launched; durable sessions
	// hold no registry and are not counted.
	if entry.local != nil {
		for _, open := range r.sessions {
			if open.local != nil {
				return nil, fmt.Errorf("this server runs one stubbed debug session at a time, and %s is open; "+
					"end it with %s before starting another", open.id, debugSessionEndTool)
			}
		}
	}
	r.sessions[entry.id] = entry
	if request != "" {
		r.starts[request] = entry.id
	}

	return nil, nil
}

// lookup returns a live session and renews its lease.
func (r *debugSessions) lookup(id string) (*debugSessionEntry, error) {
	r.sweep()

	r.mu.Lock()
	entry, ok := r.sessions[id]
	r.mu.Unlock()
	if !ok {
		return nil, errNoDebugSession(id)
	}

	entry.mu.Lock()
	entry.expires = time.Now().Add(debugSessionIdle)
	entry.mu.Unlock()

	return entry, nil
}

func errNoDebugSession(id string) error {
	return fmt.Errorf("no debug session %q: it ended, its lease lapsed, or it never existed; start or attach a new one", id)
}

// end releases a session: a durable run is detached (or left attached when
// keep is set); a stubbed case is let finish, and cancelled if it cannot. It
// reports whether the case has finished, and never waits longer than twice
// [debugSessionEndSettle]: a case blocked where cancellation does not reach —
// waiting on the process-wide registry lock — is left to finish on its own
// rather than hang the caller.
func (e *debugSessionEntry) end(keep bool) bool {
	if remote, ok := e.target.(*flowdebug.Remote); ok && keep {
		_ = remote.Disconnect()
	} else {
		_ = e.target.Close()
	}
	if e.done == nil {
		return true
	}
	select {
	case <-e.done:
		return true
	case <-time.After(debugSessionEndSettle):
		e.cancel()
	}
	select {
	case <-e.done:
		return true
	case <-time.After(debugSessionEndSettle):
		return false
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
				"catch none|uncaught|all, delete <step>, clear, breakpoints, inspect <expr>, expand <expr>, scope, backtrace, " +
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

// answer reads the session: its snapshot, and the transcript since the last
// answer. It takes e.mu itself, and only around the fields it guards.
func (e *debugSessionEntry) answer(ctx context.Context) sessionAnswer {
	answer := sessionAnswer{SessionID: e.id}
	if snapshot, err := e.target.Snapshot(ctx); err == nil {
		answer.Snapshot = schemaJSON(snapshot)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	answer.Expires = e.expires.UTC().Format(time.RFC3339)
	if e.transcript != nil {
		answer.Transcript, e.cursor = e.transcript.since(e.cursor)
		answer.Note = e.transcript.note()
	}

	return answer
}

// startedAgain answers a start whose request id already started entry.
func startedAgain(ctx context.Context, entry *debugSessionEntry) *mcp.CallToolResult {
	answer := entry.answer(ctx)
	answer.Note = strings.TrimSpace("this request id already started this session; it was not started again. " + answer.Note)

	return toolJSON(answer)
}

// targetRequestID is the request id a retained session's command carries to
// its target: derived from the caller's retry key, so a retry reaches the
// target under the same id and is answered from its receipts, and spelled in
// the characters a target's request id allows. Empty stays empty.
func targetRequestID(session, request string) string {
	if request == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(session + "\x00" + request))

	return "mcp-" + hex.EncodeToString(sum[:16])
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

			return startedAgain(ctx, entry), nil
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
	existing, err := r.register(entry, args.RequestID)
	if err != nil || existing != nil {
		// Nothing was launched: this session never ran.
		cancel()
		_ = session.Close()
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		if _, err := r.lookup(existing.id); err != nil {
			return flowmcp.ToolError(err), nil
		}

		return startedAgain(ctx, existing), nil
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
	if _, err := r.register(entry, ""); err != nil {
		_ = remote.Disconnect()

		return flowmcp.ToolError(err), nil
	}

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

	// No lock is held while waiting: the command that produces the revision
	// this waits for must be able to run.
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
	entry.calls.Lock()
	defer entry.calls.Unlock()

	entry.mu.Lock()
	cached, ok := entry.receipts[args.RequestID]
	expires := entry.expires
	entry.mu.Unlock()
	if ok && args.RequestID != "" {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(cached)}}}, nil
	}

	// The retry key reaches the target as its request id, so a retry after a
	// response lost here — the target accepted the command and this call
	// ended before answering — is answered from the target's receipts rather
	// than moving the run again. The expected revision goes with it, and the
	// target answers a remembered id before it judges staleness.
	result, err := entry.driver.DoWith(ctx, args.Command, flowdebug.DoOptions{
		RequestID:        targetRequestID(entry.id, args.RequestID),
		ExpectedRevision: args.ExpectedRevision,
	})
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	answer := sessionAnswer{SessionID: entry.id, Command: args.Command, Expires: expires.UTC().Format(time.RFC3339)}
	answer.Text = result.Text
	answer.Receipt = schemaJSON(result.Receipt)
	answer.Snapshot = schemaJSON(result.Snapshot)
	answer.Inspect = schemaJSON(result.Inspect)

	entry.mu.Lock()
	defer entry.mu.Unlock()
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
	if !r.remove(entry.id) {
		// The sweeper or another end got there first, and ended it.
		return flowmcp.ToolError(errNoDebugSession(entry.id)), nil
	}

	finished := entry.end(args.Keep)
	answer := entry.answer(ctx)
	switch {
	case entry.done == nil:
	case finished && entry.report != nil:
		answer.Report = schemaJSON(entry.report)
	case !finished:
		answer.Note = strings.TrimSpace("the case was cancelled and has not yet stopped, so it has no report. " + answer.Note)
	}

	return toolJSON(answer), nil
}
