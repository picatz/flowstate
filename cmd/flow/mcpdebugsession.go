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
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
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
	// secret is drawn once per process, so the session and request ids an
	// attach derives from a caller's retry key ([debugSessions.attachIDs])
	// are the same on a retry and unpredictable to anyone else.
	secret string

	mu       sync.Mutex
	sessions map[string]*debugSessionEntry
	starts   map[string]string
	// registry is shared by every call that reads the process-wide task
	// registry, for as long as it runs, and held whole by a stubbed session
	// for as long as its case can register a synthetic task in it: a lock
	// spanning both, rather than a check that a session is open followed by
	// a read that a session starting in between could see mid-swap. A
	// semaphore rather than a sync.RWMutex so a start waits for readers
	// within a bound, and a reader never waits at all.
	registry *semaphore.Weighted

	// ending are sessions no longer answered for, still being ended: a
	// durable one's detach not yet sent, a stubbed case still holding the
	// registry lock. A session is here from the moment it is forgotten until
	// its end returns and its case, if any, has stopped.
	ending map[string]*debugSessionEntry
}

type debugSessionEntry struct {
	id     string
	target flowdebug.Target
	driver *flowdebug.Driver
	// stubbed is set for a stubbed case, which runs in this process under a
	// [flowdebug.Reversible]; empty of a durable run.
	stubbed bool
	// stop ends a stubbed case's runs, the replays a rewind left behind
	// included. Nil for a durable session.
	stop func()
	// torn is closed once a stubbed session's runs have all stopped and its
	// share of the registry is returned, which [debugSessions.release] waits
	// for after done. Nil for a durable session.
	torn    chan struct{}
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
	// commands is, for each request id in receipts, the [commandDigest] of
	// the command its answer answers, so the id reused for another command
	// is refused rather than answered with the first command's result.
	commands map[string]string
	order    []string
	// receiptBytes is what receipts holds, for [maxSessionReceiptBytes].
	receiptBytes int
	// stopCall cancels the command in flight, if any, so retiring the
	// session ends it at once rather than leaving the end to wait out its
	// wait for the next stop.
	stopCall context.CancelFunc

	transcript *lockedTranscript

	// run is the durable run a session attached to, so a rejoin under its
	// id is checked to name the same run. Empty for a stubbed case.
	workflowID, runID string
	// inputs is a stubbed case's [startInputs], so a start retried under
	// its request id is checked to submit the same case. Empty for a
	// durable session.
	inputs string

	cancel context.CancelFunc
	done   chan struct{}
	// ready is closed once the start that registered this session has its
	// first answer, or has failed to launch the case, startErr saying why:
	// a retry of that start waits on it rather than answering with a
	// session that has not run yet or is about to be removed. Nil for a
	// session that is ready once registered.
	ready    chan struct{}
	startErr error
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
		remote: remote, secret: uuid.NewString(), sessions: map[string]*debugSessionEntry{}, starts: map[string]string{},
		ending: map[string]*debugSessionEntry{}, registry: semaphore.NewWeighted(registryReaders),
	}
}

// registryReaders is [debugSessions.registry]'s weight: how many calls may
// read the task registry at once. Only ever one or all of it, so its value
// decides nothing but that a stubbed session excludes every reader.
const registryReaders = 1 << 20

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
			// Nobody is there to tell of a detach that failed: the lease
			// that lapsed is the one that now ends the hold.
			_, _ = entry.end(false)
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
	entry.mu.Lock()
	if entry.stopCall != nil {
		entry.stopCall()
	}
	entry.mu.Unlock()
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
	if entry.torn != nil {
		<-entry.torn
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

func stubbedEntry(entry *debugSessionEntry) bool { return entry.stubbed }

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

// register admits a new session, within the bound. A call's retry key
// ([retryKey]) is reserved in the same critical section that admits the
// session, so two starts under one key never both launch a run: the second is
// handed the session the first admitted, and must discard its own.
func (r *debugSessions) register(entry *debugSessionEntry, request string) (*debugSessionEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if request != "" {
		if existing, ok := r.sessions[r.starts[request]]; ok {
			// An attach racing another under its key: answered with the
			// first only when both asked for the same run.
			if !entry.stubbed && !existing.onRun(entry.workflowID, entry.runID) {
				return nil, errReusedAttachKey
			}
			// And a start only when both submitted the same case.
			if entry.stubbed && existing.inputs != entry.inputs {
				return nil, errReusedStartKey
			}

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
	if entry.stubbed {
		for _, open := range r.sessions {
			if open.stubbed {
				return nil, fmt.Errorf("this server runs one stubbed debug session at a time, and %s is open; "+
					"end it with %s before starting another", open.id, debugSessionEndTool)
			}
		}
		for _, ending := range r.ending {
			if ending.stubbed {
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
		if open.stubbed {
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
			return flowmcp.ToolError(fmt.Errorf("retained debug session %s is running a stubbed case that holds "+
				"this process's task registry; end it with %s first", id, debugSessionEndTool)), nil
		}

		return handler(ctx, req)
	}
}

// readingRegistry takes a reader's share of the task registry for as long as
// the caller holds it, or says why it cannot: a retained stubbed session holds
// the registry, a synthetic task registered in it, for as long as its case
// runs. A session that is ending is waited for, bounded, first.
func (r *debugSessions) readingRegistry(ctx context.Context) (release func(), err error) {
	if err := r.settle(ctx, stubbedEntry); err != nil {
		return nil, err
	}
	if !r.registry.TryAcquire(1) {
		if id, open := r.stubbed(); open {
			return nil, fmt.Errorf("retained debug session %s is running a stubbed case that holds this "+
				"process's task registry; end it with %s first", id, debugSessionEndTool)
		}

		// A stubbed session between taking the registry and registering,
		// or one whose case is still stopping.
		return nil, errors.New("a retained debug session is starting or stopping a stubbed case, which " +
			"holds this process's task registry; try again")
	}

	return func() { r.registry.Release(1) }, nil
}

// claimRegistry takes the whole task registry for a stubbed session, waiting
// within [maxDebugSessionWait] for readers in flight.
func (r *debugSessions) claimRegistry(ctx context.Context) error {
	if r.registry.TryAcquire(registryReaders) {
		return nil
	}
	claim, stop := context.WithTimeout(ctx, maxDebugSessionWait)
	defer stop()
	if err := r.registry.Acquire(claim, registryReaders); err != nil {
		return fmt.Errorf("a tool reading this process's task registry is still running, and a stubbed "+
			"session needs it alone; try again: %w", err)
	}

	return nil
}

// readsRegistry wraps a tool that answers from the task registry, so it runs
// holding a reader's share of it: refused while a stubbed session holds the
// registry, and never overlapped by one starting.
func (r *debugSessions) readsRegistry(handler mcp.ToolHandler) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		release, err := r.readingRegistry(ctx)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		defer release()

		return handler(ctx, req)
	}
}

// guardRegistryReaders is stdio's [flowmcp.Deps.WrapHandler]: the tools that
// answer from this process's task registry — the RPCs this process answers
// itself ([flowmcp.LocalTools]) — read it under [debugSessions.readsRegistry],
// so none advertises or compiles a task a stubbed session registered and that
// vanishes when it ends. Every other tool dispatches to the deployment and is
// left as it is.
func (r *debugSessions) guardRegistryReaders(tool string, next mcp.ToolHandler) mcp.ToolHandler {
	for method := range flowmcp.LocalTools {
		if flowmcp.ToolName(method) == tool {
			return r.readsRegistry(next)
		}
	}

	return next
}

// guardRegistryResource is stdio's [flowmcp.Deps.WrapResourceHandler]: the
// catalog resource answers from the same registry the catalog tool does, and
// is refused on the same terms.
func (r *debugSessions) guardRegistryResource(uri string, next mcp.ResourceHandler) mcp.ResourceHandler {
	if uri != flowmcp.CatalogResourceURI {
		return next
	}

	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		release, err := r.readingRegistry(ctx)
		if err != nil {
			return nil, err
		}
		defer release()

		return next(ctx, req)
	}
}

// lookup returns a live session and renews its lease.
func (r *debugSessions) lookup(id string) (*debugSessionEntry, error) {
	if err := checkSessionID(id); err != nil {
		return nil, err
	}
	r.sweep()

	r.mu.Lock()
	entry, ok := r.sessions[id]
	r.mu.Unlock()
	if !ok {
		return nil, errNoDebugSession(id)
	}

	// A stubbed session is registered before its case is launched, and is
	// given its target once it is: until the start that registered it answers,
	// nothing may act on it, whoever learned its id.
	if entry.ready != nil {
		<-entry.ready
		if entry.startErr != nil {
			return nil, fmt.Errorf("%w: the start that opened it failed: %w", errNoDebugSession(id), entry.startErr)
		}
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
// reports whether the case has finished, and the error of a detach that did
// not go through — the run is then still held, until its lease lapses or a
// rejoin ends it — and never waits longer than twice
// [debugSessionEndSettle]: a case blocked where cancellation does not reach —
// waiting on the process-wide registry lock — is left to finish on its own
// rather than hang the caller.
func (e *debugSessionEntry) end(keep bool) (bool, error) {
	// A session ended, by the sweeper or by another call that found its id,
	// before the start that registered it has launched its case has no target
	// yet: it is ended once it has.
	if e.ready != nil {
		<-e.ready
	}
	// After any command in flight, and before any that found this entry:
	// those see retired once they hold calls.
	e.calls.Lock()
	defer e.calls.Unlock()
	if e.stop != nil {
		// After the case has had its chance to finish, and off this call:
		// it ends the replays a rewind left and waits for them.
		defer func() { go e.stop() }()
	}

	var detach error
	if remote, ok := e.target.(*flowdebug.Remote); ok && keep {
		_ = remote.Disconnect()
	} else {
		detach = e.target.Close()
	}
	if e.done == nil {
		return true, detach
	}
	select {
	case <-e.done:
		return true, detach
	case <-time.After(debugSessionEndSettle):
		e.cancel()
	}
	select {
	case <-e.done:
		return true, detach
	case <-time.After(debugSessionEndSettle):
		return false, detach
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
	request["maxLength"] = v1.MaxDebugRequestIDBytes
	session["maxLength"] = v1.MaxDebugSessionIDBytes
	rejoin := str("Optional: rejoin this session instead of attaching a new one.")
	rejoin["maxLength"] = v1.MaxDebugSessionIDBytes

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
				"session_id":  rejoin,
				"request_id":  request,
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
				flowdebug.DriverCommandList() + ". back, reverse-continue (rc) and goto <point> (a point of the snapshot's timeline, counted from 0) " +
				"need a stubbed session that can step back; any other says so and does not move. Movements answer with the next stop. Set expected_revision to the " +
				"snapshot you acted on, " +
				"so a command meant for a stop the run has left is refused as stale: a movement or an inspection is " +
				"judged by the run in the same step as the command; any other command is checked just before it is sent.",
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
			if len(a.snapshot.GetObservations()) > 0 || len(a.snapshot.GetTimeline().GetPoints()) > 0 {
				trimmed := proto.CloneOf(a.snapshot)
				trimmed.ObservationsDropped += uint64(len(trimmed.GetObservations()))
				trimmed.Observations = nil
				if trimmed.GetTimeline() != nil {
					trimmed.Timeline.Dropped += uint32(len(trimmed.Timeline.GetPoints()))
					trimmed.Timeline.Points, trimmed.Timeline.Current = nil, -1
				}
				a.Snapshot = schemaJSON(trimmed)
			}
			note("The rendered text and the snapshot's observations were dropped, with any timeline points: the answer exceeded %d bytes.", flowmcp.MaxResultBytes)

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
	if entry.ready != nil {
		select {
		case <-entry.ready:
		case <-ctx.Done():
			return flowmcp.ToolError(fmt.Errorf("the start this request id names is still starting: %w", ctx.Err()))
		}
		if entry.startErr != nil {
			return flowmcp.ToolError(fmt.Errorf("the start this request id names failed: %w", entry.startErr))
		}
	}
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
	if err := checkRequestID(args.RequestID); err != nil {
		return flowmcp.ToolError(err), nil
	}
	r.sweep()

	key, inputs := retryKey(debugSessionStartTool, args.RequestID), startInputs(args.Workflow, args.Tests, args.Case)
	if key != "" {
		r.mu.Lock()
		existing, started := r.starts[key]
		r.mu.Unlock()
		if started {
			entry, err := r.lookup(existing)
			if err != nil {
				return flowmcp.ToolError(err), nil
			}
			if entry.inputs != inputs {
				return flowmcp.ToolError(errReusedStartKey), nil
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

	// The program the case runs, so the session judges a target against it:
	// with no program and no inventory it refuses nothing, and `until typo`
	// would release the case to its end. A source that does not parse here
	// fails the run itself, which says why.
	var program *v1.Workflow
	var steps []flowdebug.Step
	if workflow, _, err := flowfile.Parse([]byte(args.Workflow)); err == nil {
		program, steps = workflow, stepList(workflow)
	}
	transcript := &lockedTranscript{}

	// The case's runs live as long as the session, not as long as the call
	// that started it or the one that rewinds it.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	entry := &debugSessionEntry{
		id: uuid.NewString(), stubbed: true,
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
		transcript: transcript, cancel: cancel, done: make(chan struct{}), ready: make(chan struct{}), inputs: inputs,
		torn: make(chan struct{}),
	}
	existing, err := r.register(entry, key)
	if err != nil || existing != nil {
		// Nothing was launched: this session never ran.
		cancel()
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		if _, err := r.lookup(existing.id); err != nil {
			return flowmcp.ToolError(err), nil
		}

		return startedAgain(ctx, existing), nil
	}
	// A retry registered behind this start waits for its answer, which the
	// return below has settled by the time this runs.
	defer close(entry.ready)

	// The whole registry, held until the case stops: after every reader in
	// flight, and before any that would overlap the case's registrations.
	// Claimed once admitted, so a second start under this request id — or a
	// second stubbed session — is answered by register rather than left
	// waiting here on a registry this session will hold for its whole life.
	// Nothing touches the registry before the case launches below, and a
	// reader never waits for the claim, so the wait is bounded by theirs.
	if err := r.claimRegistry(ctx); err != nil {
		entry.startErr = err
		cancel()
		close(entry.done)
		close(entry.torn)
		if r.remove(entry.id) {
			r.release(entry)
		}

		return flowmcp.ToolError(err), nil
	}

	// The case's first end, whichever run is shown when it comes, is its
	// verdict. The registry is not released with it: a rewind can still launch
	// a replay, which swaps the process registry while it is set up, so the
	// share is returned when the session is torn down, once no run is left.
	var finishOnce sync.Once
	finish := func(report *v1.TestReport) {
		finishOnce.Do(func() {
			entry.report = report
			close(entry.done)
		})
	}
	launch := stubbedCase{
		Program: program, Steps: steps, Speak: transcript.add, Finish: finish,
		Run: func(ctx context.Context, debugger v1.Debugger) flowtest.RunResult {
			return flowtest.RunSourceWith(ctx, "<submitted>", []byte(args.Workflow), []byte(args.Tests),
				flowtest.RunOptions{Select: selected, Debugger: debugger})
		},
	}.launcher(runCtx)
	reversible, err := flowdebug.NewReversible(runCtx, launch)
	if err != nil {
		entry.startErr = err
		cancel()
		close(entry.done)
		r.registry.Release(registryReaders)
		close(entry.torn)
		if r.remove(entry.id) {
			r.release(entry)
		}

		return flowmcp.ToolError(err), nil
	}
	entry.target, entry.driver = reversible, flowdebug.NewDriver(reversible)
	var tearOnce sync.Once
	entry.stop = func() {
		tearOnce.Do(func() {
			// Every run has stopped when Stop returns, so what the case never
			// reported — a live run stopped before it ended — is settled now,
			// and the registry has no user left.
			reversible.Stop()
			finish(nil)
			r.registry.Release(registryReaders)
			close(entry.torn)
		})
	}
	entry.driver.Wait = maxDebugSessionWait

	// The first stop, or the end of a case with no steps to hold at.
	waitCtx, stop := context.WithTimeout(ctx, maxDebugSessionWait)
	defer stop()
	for after := uint64(0); ; {
		snapshot, err := reversible.WaitSnapshot(waitCtx, after)
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
		RequestID  string `json:"request_id"`
	}
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionAttachTool, err)), nil
	}
	if args.WorkflowID == "" {
		return flowmcp.ToolError(errors.New("workflow_id is required")), nil
	}
	if err := errors.Join(checkSessionID(args.SessionID), checkRequestID(args.RequestID)); err != nil {
		return flowmcp.ToolError(err), nil
	}
	r.sweep()

	// A retry of an attach whose answer was lost is answered with the session
	// it attached — whose id the caller never learned — rather than
	// attaching again beside it.
	key := retryKey(debugSessionAttachTool, args.RequestID)
	if key != "" {
		r.mu.Lock()
		id, attached := r.starts[key]
		r.mu.Unlock()
		if attached {
			entry, err := r.lookup(id)
			if err != nil {
				return flowmcp.ToolError(err), nil
			}
			if !entry.onRun(args.WorkflowID, args.RunID) {
				return flowmcp.ToolError(errReusedAttachKey), nil
			}
			answer := entry.answerAfter(ctx)
			answer.Note = strings.TrimSpace("this request id already attached this session; it was not attached again. " + answer.Note)

			return answer.result(), nil
		}
	}
	// A rejoin of a session this server is still ending waits for the end,
	// so the answer says what the run did rather than racing its detach.
	if args.SessionID != "" {
		if err := r.settle(ctx, func(entry *debugSessionEntry) bool { return entry.id == args.SessionID }); err != nil {
			return flowmcp.ToolError(err), nil
		}
	}

	opts := flowdebug.RemoteOptions{SessionID: args.SessionID, Wait: 5 * time.Second}
	if args.RequestID != "" {
		opts.NewSessionID, opts.RequestID = r.attachIDs(args.WorkflowID, args.RunID, args.SessionID, args.RequestID)
	}
	remote, receipt, err := flowdebug.AttachRemote(ctx, r.remote(), args.WorkflowID, args.RunID, opts)
	if err != nil {
		// The run remembers the answer under this request id, so a retry
		// under it gets the same one.
		if receipt != nil && args.RequestID != "" {
			err = fmt.Errorf("%w; a retry under this request_id gets the same answer, so use a new one to try again", err)
		}

		return flowmcp.ToolError(err), nil
	}

	entry := &debugSessionEntry{
		id: remote.SessionID(), target: remote, driver: flowdebug.NewDriver(remote),
		started: time.Now(), expires: time.Now().Add(debugSessionIdle), receipts: map[string]json.RawMessage{},
		workflowID: args.WorkflowID, runID: args.RunID,
	}
	entry.driver.Wait = maxDebugSessionWait
	existing, err := r.register(entry, key)
	if err != nil || existing != nil {
		// made is whether this call created the session. A duplicate is the
		// run answering an attach it applied before — a keyed retry, whose
		// session may be one the caller ended with keep to rejoin later —
		// and is never this call's to detach.
		made := args.SessionID == "" && receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE
		switch {
		case made && (err != nil || existing.id != remote.SessionID()):
			// A session this call attached, which no entry will hold — the
			// server refused it, or a concurrent retry under this request
			// id attached first: it is detached, so a run is never left
			// held by nobody until its lease lapses.
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
		said := "this server already holds this session; it was rejoined, not attached again. "
		if existing.id != remote.SessionID() {
			said = "this request id already attached this session; it was not attached again. "
		}
		answer.Note = strings.TrimSpace(said + answer.Note)

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

// sessionCommand is a command call's arguments.
type sessionCommand struct {
	SessionID        string `json:"session_id"`
	Command          string `json:"command"`
	ExpectedRevision uint64 `json:"expected_revision"`
	RequestID        string `json:"request_id"`
}

func (r *debugSessions) command(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args sessionCommand
	if err := decode(req, &args); err != nil {
		return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", debugSessionCommandTool, err)), nil
	}
	if err := checkRequestID(args.RequestID); err != nil {
		return flowmcp.ToolError(err), nil
	}
	if len(args.Command) > flowdebug.MaxCommandBytes || strings.ContainsAny(args.Command, "\r\n") {
		return flowmcp.ToolError(fmt.Errorf("a command is one line of at most %d bytes", flowdebug.MaxCommandBytes)), nil
	}
	entry, err := r.lookup(args.SessionID)
	if err != nil {
		return flowmcp.ToolError(err), nil
	}

	return r.commandOn(ctx, entry, args)
}

// commandOn runs a command on a session the call has already found — which
// may have been retired since.
func (r *debugSessions) commandOn(ctx context.Context, entry *debugSessionEntry, args sessionCommand) (*mcp.CallToolResult, error) {
	entry.calls.Lock()
	defer entry.calls.Unlock()

	// Registered before retired is read, so a retirement either is seen here
	// or cancels the call: the end that follows it waits for calls, and never
	// for more than a cancelled call takes to return.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	entry.mu.Lock()
	entry.stopCall = stop
	entry.mu.Unlock()
	defer func() {
		entry.mu.Lock()
		entry.stopCall = nil
		entry.mu.Unlock()
	}()
	if entry.retired.Load() {
		return flowmcp.ToolError(errNoDebugSession(entry.id)), nil
	}

	command := commandDigest(args.Command, args.ExpectedRevision)
	entry.mu.Lock()
	cached, ok := entry.receipts[args.RequestID]
	answered := entry.commands[args.RequestID]
	expires := entry.expires
	entry.mu.Unlock()
	if ok && args.RequestID != "" {
		if answered != command {
			return flowmcp.ToolError(errReusedCommandKey), nil
		}

		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(cached)}}}, nil
	}

	// The retry key reaches the target as its request id, so a retry after a
	// response lost here — the target accepted the command and this call
	// ended before answering — is answered from the target's receipts rather
	// than moving the run again. The expected revision goes with it, and the
	// target answers a remembered id before it judges staleness. The id
	// names the command too, so a key reused for another one after its
	// answer here was evicted is never answered from the first's receipt.
	result, err := entry.driver.DoWith(ctx, args.Command, flowdebug.DoOptions{
		RequestID:        commandRequestID(entry.id, args.RequestID, command),
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
		entry.rememberLocked(args.RequestID, command, encoded)
	}

	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}, nil
}

// commandDigest identifies a retained command — its line and the revision it
// expects — so a request id names one command.
func commandDigest(command string, expected uint64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d", strings.TrimSpace(command), expected))

	return hex.EncodeToString(sum[:16])
}

// commandRequestID is the request id a retained command carries to its
// target: its session, the caller's retry key, and the command it names.
// Empty when the caller sent no key.
func commandRequestID(session, request, command string) string {
	if request == "" {
		return ""
	}

	return targetRequestID(session, request+"\x00"+command)
}

// errReusedCommandKey refuses a command whose request id already answered
// another command.
var errReusedCommandKey = errors.New("this request_id already answered another command or expected_revision; use a new request id")

// attachIDs are the session id a new attach under the caller's retry key
// creates, and the request id it sends: derived from the key, the run, and
// the process's secret. An attach whose answer was lost — the server applied
// it, the response never came, so nothing here registered it — is retried by
// sending the same attach, which the run answers from its receipts with the
// session the first made, rather than refusing a second session while the
// first holds it until its lease lapses.
func (r *debugSessions) attachIDs(workflowID, runID, rejoin, request string) (session, target string) {
	sum := sha256.Sum256([]byte(strings.Join([]string{r.secret, workflowID, runID, rejoin, request}, "\x00")))
	id := hex.EncodeToString(sum[:16])

	return "mcp-" + id, targetRequestID(id, "attach")
}

// retryKey is the key a request id is remembered under for tool: each tool's
// keys are its own, so one tool's key never answers another's call — a
// start is never handed the durable session an attach made. Empty when
// request is.
func retryKey(tool, request string) string {
	if request == "" {
		return ""
	}

	return tool + "\x00" + request
}

// onRun reports whether this durable session is on the run workflowID and
// runID name, a run id either side leaves empty matching any, as a rejoin
// judges it.
func (e *debugSessionEntry) onRun(workflowID, runID string) bool {
	return !e.stubbed && e.workflowID == workflowID && (e.runID == "" || runID == "" || e.runID == runID)
}

// startInputs identifies what a start submitted — the workflow, the tests,
// and the case chosen — so a request id names one start of one case.
func startInputs(workflow, tests, testCase string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{workflow, tests, testCase}, "\x00")))

	return hex.EncodeToString(sum[:])
}

// errReusedStartKey refuses a start whose request id already started another
// case: the key names that call, not this one.
var errReusedStartKey = errors.New("this request_id already started a session for another workflow, tests, or case; use a new request id")

// errReusedAttachKey refuses an attach whose request id already attached a
// session to another run: the key names that call, not this one.
var errReusedAttachKey = errors.New("this request_id already attached a session to another run; use a new request id")

// checkSessionID refuses a session id longer than any session's: one that
// matches nothing is echoed in the refusal, so it is bounded before it is.
func checkSessionID(id string) error {
	if len(id) > v1.MaxDebugSessionIDBytes {
		return fmt.Errorf("session_id is at most %d bytes", v1.MaxDebugSessionIDBytes)
	}

	return nil
}

// checkRequestID refuses a retry key longer than a target's own request id may
// be: each is kept, with its answer, for as long as the session is, so its
// length is bounded where it is spent.
func checkRequestID(id string) error {
	if len(id) > v1.MaxDebugRequestIDBytes {
		return fmt.Errorf("request_id is at most %d bytes", v1.MaxDebugRequestIDBytes)
	}

	return nil
}

// rememberLocked keeps a command's answer for a retry under its request id,
// dropping the oldest past [maxSessionReceipts] answers or
// [maxSessionReceiptBytes] bytes. The caller holds e.mu.
func (e *debugSessionEntry) rememberLocked(request, command string, encoded []byte) {
	if e.commands == nil {
		e.commands = map[string]string{}
	}
	e.commands[request] = command
	if previous, ok := e.receipts[request]; ok {
		e.receiptBytes -= len(request) + len(previous)
		e.order = slices.DeleteFunc(e.order, func(id string) bool { return id == request })
	}
	e.receipts[request] = encoded
	e.order = append(e.order, request)
	e.receiptBytes += len(request) + len(encoded)
	for len(e.order) > maxSessionReceipts || (e.receiptBytes > maxSessionReceiptBytes && len(e.order) > 1) {
		oldest := e.order[0]
		e.receiptBytes -= len(oldest) + len(e.receipts[oldest])
		delete(e.receipts, oldest)
		delete(e.commands, oldest)
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

	finished, detach := entry.end(args.Keep)
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

	if detach != nil {
		// The session is forgotten either way, and the run is still held
		// until its lease lapses: said as a failure, with the way to end it
		// sooner, rather than reported as the end it was not.
		answer.Note = strings.TrimSpace(fmt.Sprintf("the run was not detached: %v. It stays held until the "+
			"session's lease lapses; rejoin it with %s (session_id %s) and end it again to release it now. %s",
			detach, debugSessionAttachTool, entry.id, answer.Note))
	}
	result := answer.result()
	// A case that did not pass is a failed call, as flowstate_test and the
	// one-shot flowstate_debug report it, not a success whose report says
	// otherwise; so is an end that left the run held. The report is read
	// only once the case has finished: until then its goroutine may still
	// write it.
	if (finished && entry.report != nil && testReportFailed(entry.report)) || detach != nil {
		result.IsError = true
	}

	return result, nil
}
