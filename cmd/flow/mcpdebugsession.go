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
	"sync/atomic"
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
	maxDebugSessions     = 8
	debugSessionIdle     = 10 * time.Minute
	debugSessionLifetime = time.Hour
	maxDebugSessionWait  = 30 * time.Second
	maxSessionReceipts   = 64
	// maxSessionReceiptBytes bounds what one session's retry cache holds,
	// whatever its count: each answer is fitted under
	// [flowmcp.MaxResultBytes], and this keeps sixty-four of them from
	// being sixteen megabytes a session.
	maxSessionReceiptBytes = 4 << 20
	debugSessionEndSettle  = 5 * time.Second
	debugSessionSweep      = time.Minute
)

// debugSessions is one server's retained sessions.
type debugSessions struct {
	remote func() flowstatev1connect.WorkflowServiceClient

	mu       sync.Mutex
	sessions map[string]*debugSessionEntry
	starts   map[string]string
	// ending are sessions no longer answered for, still being ended: a
	// durable one's detach not yet sent, a stubbed case still holding the
	// registry lock. A session is here from the moment it is forgotten until
	// its end returns and its case, if any, has stopped.
	ending map[string]*debugSessionEntry
}

type debugSessionEntry struct {
	id      string
	target  flowdebug.Target
	driver  *flowdebug.Driver
	local   *flowdebug.Session
	started time.Time

	// calls serializes commands on one session, so two never race to move
	// one run, and serializes them with the session's end. An observe waits
	// outside it, so a command can produce the revision the observe is
	// waiting for.
	calls sync.Mutex
	// retired is set when the session is forgotten, before it is ended, so
	// a command that found the entry just before is refused once it holds
	// calls rather than acting on a closed session — a durable pause then
	// would attach the run anew, held by a session nobody holds.
	retired atomic.Bool

	// mu guards the fields below it. It is never held across a call on the
	// target.
	mu       sync.Mutex
	expires  time.Time
	receipts map[string]json.RawMessage
	order    []string
	// receiptBytes is what receipts holds, for [maxSessionReceiptBytes].
	receiptBytes int

	transcript *lockedTranscript

	// run is the durable run a session attached to, so a rejoin under its
	// id is checked to name the same run. Empty for a stubbed case.
	workflowID, runID string

	cancel context.CancelFunc
	done   chan struct{}
	// released is closed once the session has finished ending; see
	// [debugSessions.ending].
	released chan struct{}
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

// take hands over every fragment not yet answered with, and frees the room
// they held: a retained session is read many times over its life, so what an
// answer has carried must stop counting against the transcript's bound, or a
// long session would drop everything after its first few thousand fragments
// however often it was read. The dropped count stays the session's total.
func (t *lockedTranscript) take() []debugFragment {
	t.mu.Lock()
	defer t.mu.Unlock()

	taken := t.fragments
	t.fragments, t.bytes = nil, 0

	return taken
}

// note is the embedded note, read under the lock the run's goroutine writes
// under.
func (t *lockedTranscript) note() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.debugTranscript.note()
}

func newDebugSessions(remote func() flowstatev1connect.WorkflowServiceClient) *debugSessions {
	return &debugSessions{
		remote: remote, sessions: map[string]*debugSessionEntry{}, starts: map[string]string{},
		ending: map[string]*debugSessionEntry{},
	}
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
			r.retireLocked(id)
		}
	}
	r.mu.Unlock()

	// Ended on their own goroutines: a stubbed case may take twice
	// [debugSessionEndSettle] to stop, and the call that happened to sweep —
	// about some other session — must not wait for it. Until each is ended
	// it stays in [debugSessions.ending], where what depends on it waits.
	for _, entry := range lapsed {
		go func() {
			entry.end(false)
			r.release(entry)
		}()
	}
}

// retireLocked forgets a session and moves it to ending, returning it, or nil
// when the server holds no session by that id. The caller holds r.mu.
func (r *debugSessions) retireLocked(id string) *debugSessionEntry {
	entry, ok := r.sessions[id]
	if !ok {
		return nil
	}
	r.forgetLocked(id)
	entry.retired.Store(true)
	entry.released = make(chan struct{})
	r.ending[id] = entry

	return entry
}

// release takes an ended session out of ending once its case, if it has one,
// has stopped holding the registry lock.
func (r *debugSessions) release(entry *debugSessionEntry) {
	if entry.done != nil {
		<-entry.done
	}

	r.mu.Lock()
	if r.ending[entry.id] == entry {
		delete(r.ending, entry.id)
	}
	r.mu.Unlock()
	close(entry.released)
}

// debugSessionEndWait bounds how long a call waits for a session that is
// ending before it gives up and says so: the end itself takes at most this.
const debugSessionEndWait = 2 * debugSessionEndSettle

// settle waits, bounded, for every ending session that matches to finish
// ending, and names one that has not.
func (r *debugSessions) settle(ctx context.Context, match func(*debugSessionEntry) bool) error {
	r.mu.Lock()
	var waiting []*debugSessionEntry
	for _, entry := range r.ending {
		if match(entry) {
			waiting = append(waiting, entry)
		}
	}
	r.mu.Unlock()

	bound := time.NewTimer(debugSessionEndWait)
	defer bound.Stop()
	for _, entry := range waiting {
		select {
		case <-entry.released:
		case <-bound.C:
			return fmt.Errorf("debug session %s is still ending; try again shortly", entry.id)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func stubbedEntry(entry *debugSessionEntry) bool { return entry.local != nil }

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

// remove retires a session, reporting whether this caller was the one to, so
// a session is ended once. The caller ends it and then releases it.
func (r *debugSessions) remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.retireLocked(id) != nil
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
	// A rejoin of a session this server already holds: one entry, and one
	// driver, per session, on one run. The caller discards the entry it
	// built.
	if existing, ok := r.sessions[entry.id]; ok {
		if existing.workflowID != entry.workflowID ||
			(existing.runID != "" && entry.runID != "" && existing.runID != entry.runID) {
			return nil, fmt.Errorf("debug session %s is attached to workflow %s here, not %s", entry.id, existing.workflowID, entry.workflowID)
		}

		return existing, nil
	}
	if _, ending := r.ending[entry.id]; ending {
		return nil, fmt.Errorf("debug session %s is still ending; try again shortly", entry.id)
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
		for _, ending := range r.ending {
			if ending.local != nil {
				return nil, fmt.Errorf("stubbed debug session %s is still ending; try again shortly", ending.id)
			}
		}
	}
	r.sessions[entry.id] = entry
	if request != "" {
		r.starts[request] = entry.id
	}

	return nil, nil
}

// stubbed names the open stubbed session, if there is one.
func (r *debugSessions) stubbed() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, open := range r.sessions {
		if open.local != nil {
			return open.id, true
		}
	}

	return "", false
}

// unlessStubbed refuses handler's call while a retained stubbed session is
// open: that session's case holds the process-wide task registry lock for as
// long as it runs, and a stubbed run of handler's own would wait on it,
// uncancellably, until the session ends. A session started while handler
// runs waits instead, bounded by handler's own run.
func (r *debugSessions) unlessStubbed(handler mcp.ToolHandler) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// A stubbed session that is ending still holds the lock until its
		// case stops: waited for, bounded, rather than run into.
		if err := r.settle(ctx, stubbedEntry); err != nil {
			return flowmcp.ToolError(err), nil
		}
		if id, open := r.stubbed(); open {
			return flowmcp.ToolError(fmt.Errorf("retained debug session %s is running a stubbed case, and this "+
				"server runs one at a time; end it with %s first", id, debugSessionEndTool)), nil
		}

		return handler(ctx, req)
	}
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
	// After any command in flight, and before any that found this entry:
	// those see retired once they hold calls.
	e.calls.Lock()
	defer e.calls.Unlock()

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

// result is the answer as a tool result, fitted by [sessionAnswer.encode].
func (a sessionAnswer) result() *mcp.CallToolResult {
	encoded, err := a.encode()
	if err != nil {
		return flowmcp.ToolError(err)
	}

	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}
}

// encode renders the answer under [flowmcp.MaxResultBytes]. A retained
// session's answer is sized by the run — a transcript of every arrival, a
// snapshot's observations and breakpoint definitions — so it is fitted as
// every other answer on this surface is, dropping first what a caller can
// most afford to lose and saying what went: the transcript's oldest fragments,
// then the transcript, then the rendered text and the snapshot's observations,
// and at the floor the snapshot and inspection themselves, which an observe
// reads again. An ended case's report is the verdict, so it is kept to the
// floor and there re-rendered within what the rest of the answer leaves it.
func (a sessionAnswer) encode() ([]byte, error) {
	encode := func() ([]byte, error) { return json.Marshal(a) }
	note := func(format string, args ...any) {
		a.Note = strings.TrimSpace(a.Note + " " + fmt.Sprintf(format, args...))
	}

	encoded, _, err := flowmcp.FitResult(
		encode,
		func() ([]byte, error) {
			if len(a.Transcript) > 1 {
				kept := max(1, len(a.Transcript)/4)
				note("The first %d transcript fragments were dropped, keeping the most recent %d: the answer exceeded %d bytes.",
					len(a.Transcript)-kept, kept, flowmcp.MaxResultBytes)
				a.Transcript = a.Transcript[len(a.Transcript)-kept:]
			}

			return encode()
		},
		func() ([]byte, error) {
			if len(a.Transcript) > 0 {
				note("The transcript was dropped: the answer exceeded %d bytes.", flowmcp.MaxResultBytes)
				a.Transcript = nil
			}

			return encode()
		},
		func() ([]byte, error) {
			a.Text = ""
			if len(a.snapshot.GetObservations()) > 0 {
				trimmed := proto.CloneOf(a.snapshot)
				trimmed.ObservationsDropped += uint64(len(trimmed.GetObservations()))
				trimmed.Observations = nil
				a.Snapshot = schemaJSON(trimmed)
			}
			note("The rendered text and the snapshot's observations were dropped: the answer exceeded %d bytes.", flowmcp.MaxResultBytes)

			return encode()
		},
		func() ([]byte, error) {
			a.Snapshot, a.Inspect = nil, nil
			note("The snapshot and any inspection were dropped: the answer exceeded %d bytes even reduced; "+
				"observe the session to read its snapshot alone.", flowmcp.MaxResultBytes)

			encoded, err := encode()
			if a.report == nil {
				return encoded, err
			}
			// The report gets what the rest of the answer leaves it, and each
			// pass takes the measured overshoot back out of that budget, as
			// the one-shot debug tool's floor does: the encoded length is the
			// only length, since the answer's encoding compacts and escapes
			// the report it carries. The passes are bounded rather than the
			// convergence argued ([maxDebugFloorPasses]).
			budget := flowmcp.MaxResultBytes - (len(encoded) - len(a.Report))
			for range maxDebugFloorPasses {
				if err != nil {
					return nil, err
				}
				if len(encoded) <= flowmcp.MaxResultBytes || budget < 1 {
					break
				}
				reduced, renderErr := renderTestResultWithin(a.report, budget)
				if renderErr != nil {
					return nil, renderErr
				}
				a.Report = json.RawMessage(reduced)
				encoded, err = encode()
				budget -= max(0, len(encoded)-flowmcp.MaxResultBytes)
			}

			return encoded, err
		},
	)

	return encoded, err
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

	// snapshot and report are what Snapshot and Report render, kept so
	// [sessionAnswer.encode] can reduce them.
	snapshot *v1.DebugSnapshot
	report   *v1.TestReport
}

// answer reads the session: its snapshot, and the transcript since the last
// answer. It takes e.mu itself, and only around the fields it guards.
//
// A snapshot that cannot be read is the answer's error, not an answer without
// a snapshot: a caller acting on a session whose state it was not told would
// act on nothing. The transcript is then left for the answer that can read the
// run, so no fragment is handed out beside an error and lost.
func (e *debugSessionEntry) answer(ctx context.Context) (sessionAnswer, error) {
	answer := sessionAnswer{SessionID: e.id}
	snapshot, err := e.target.Snapshot(ctx)

	e.mu.Lock()
	defer e.mu.Unlock()

	answer.Expires = e.expires.UTC().Format(time.RFC3339)
	if err != nil {
		return answer, fmt.Errorf("reading session %s's run: %w", e.id, err)
	}
	answer.snapshot, answer.Snapshot = snapshot, schemaJSON(snapshot)
	if e.transcript != nil {
		answer.Transcript = e.transcript.take()
		answer.Note = e.transcript.note()
	}

	return answer, nil
}

// answerAfter is the answer of a call that has already acted on the session —
// started, attached, rejoined or ended it. The action stands whether or not the
// run can be read afterwards, so a failed read is said in the note rather than
// returned as the call's error, which would hide the session id the caller
// needs to observe it again.
func (e *debugSessionEntry) answerAfter(ctx context.Context) sessionAnswer {
	answer, err := e.answer(ctx)
	if err != nil {
		answer.Note = strings.TrimSpace(answer.Note + " " + err.Error() + "; observe the session to read it again.")
	}

	return answer
}

// startedAgain answers a start whose request id already started entry.
func startedAgain(ctx context.Context, entry *debugSessionEntry) *mcp.CallToolResult {
	answer := entry.answerAfter(ctx)
	answer.Note = strings.TrimSpace("this request id already started this session; it was not started again. " + answer.Note)

	return answer.result()
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
	// A stubbed session that is ending still holds the registry lock.
	if err := r.settle(ctx, stubbedEntry); err != nil {
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
			session.Finished(caseFailure(result.Report))
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

	return entry.answerAfter(ctx).result(), nil
}

// caseFailure is why a case did not pass, for the session's snapshot: a case
// refused before it ran — a stub naming no step, an expectation naming none —
// otherwise fails at start with nothing but "did not pass", and the reason only
// in the report the end answers with. The text is the report's own, which
// flowtest has already redacted under the case's posture; the session redacts
// it again and bounds it as it does any failure.
func caseFailure(report *v1.TestReport) error {
	reason := report.GetRefused()
	for _, c := range report.GetCases() {
		if reason != "" {
			break
		}
		if c.GetPassed() {
			continue
		}
		reason = c.GetError()
		if reason == "" && len(c.GetFailures()) > 0 {
			reason = c.GetFailures()[0].GetMessage()
		}
	}
	if reason == "" {
		return errors.New("the case did not pass")
	}

	return fmt.Errorf("the case did not pass: %s", reason)
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
	// A rejoin of a session this server is still ending waits for the end,
	// so the answer says what the run did rather than racing its detach.
	if args.SessionID != "" {
		if err := r.settle(ctx, func(entry *debugSessionEntry) bool { return entry.id == args.SessionID }); err != nil {
			return flowmcp.ToolError(err), nil
		}
	}

	remote, receipt, err := flowdebug.AttachRemote(ctx, r.remote(), args.WorkflowID, args.RunID,
		flowdebug.RemoteOptions{SessionID: args.SessionID, Wait: 5 * time.Second})
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	entry := &debugSessionEntry{
		id: remote.SessionID(), target: remote, driver: flowdebug.NewDriver(remote),
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
		workflowID: args.WorkflowID, runID: args.RunID,
	}
	entry.driver.Wait = maxDebugSessionWait
	existing, err := r.register(entry, "")
	if err != nil || existing != nil {
		switch {
		case err != nil && args.SessionID == "":
			// A session this call attached, which no entry will hold: it is
			// detached, so a refusal never leaves a run held by nobody
			// until its lease lapses.
			_ = remote.Close()
		default:
			// A session someone else holds — the entry already here, or
			// the client that left it to be rejoined — stays attached.
			_ = remote.Disconnect()
		}
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		if _, err := r.lookup(existing.id); err != nil {
			return flowmcp.ToolError(err), nil
		}
		answer := existing.answerAfter(ctx)
		answer.Receipt = schemaJSON(receipt)
		answer.Note = strings.TrimSpace("this server already holds this session; it was rejoined, not attached again. " + answer.Note)

		return answer.result(), nil
	}

	answer := entry.answerAfter(ctx)
	answer.Receipt = schemaJSON(receipt)

	return answer.result(), nil
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
	answer, err := entry.answer(ctx)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	return answer.result(), nil
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
	if entry.retired.Load() {
		return flowmcp.ToolError(errNoDebugSession(entry.id)), nil
	}

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
	answer.snapshot, answer.Snapshot = result.Snapshot, schemaJSON(result.Snapshot)
	answer.Inspect = schemaJSON(result.Inspect)

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.transcript != nil {
		answer.Transcript = entry.transcript.take()
	}

	encoded, err := answer.encode()
	if err != nil {
		return flowmcp.ToolError(err), nil
	}
	if args.RequestID != "" {
		entry.rememberLocked(args.RequestID, encoded)
	}

	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}, nil
}

// rememberLocked keeps a command's answer for a retry under its request id,
// dropping the oldest past [maxSessionReceipts] answers or
// [maxSessionReceiptBytes] bytes. The caller holds e.mu.
func (e *debugSessionEntry) rememberLocked(request string, encoded []byte) {
	if previous, ok := e.receipts[request]; ok {
		e.receiptBytes -= len(previous)
		e.order = slices.DeleteFunc(e.order, func(id string) bool { return id == request })
	}
	e.receipts[request] = encoded
	e.order = append(e.order, request)
	e.receiptBytes += len(encoded)
	for len(e.order) > maxSessionReceipts || (e.receiptBytes > maxSessionReceiptBytes && len(e.order) > 1) {
		oldest := e.order[0]
		e.receiptBytes -= len(e.receipts[oldest])
		delete(e.receipts, oldest)
		e.order = e.order[1:]
	}
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
	go r.release(entry)
	answer, err := entry.answer(ctx)
	if err != nil {
		// Ended either way; the session is gone, so there is nothing to
		// observe again, only the failed read to report.
		answer.Note = strings.TrimSpace(answer.Note + " the session ended, and " + err.Error() + ".")
	}
	switch {
	case entry.done == nil:
	case finished && entry.report != nil:
		rendered, err := renderTestResult(entry.report)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		answer.report, answer.Report = entry.report, json.RawMessage(rendered)
	case !finished:
		answer.Note = strings.TrimSpace("the case was cancelled and has not yet stopped, so it has no report. " + answer.Note)
	}

	return answer.result(), nil
}
