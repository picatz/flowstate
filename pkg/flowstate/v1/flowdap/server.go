package flowdap

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// runThreadID is the one thread a run is. A run is not a set of threads: a
// parallel branch is a container a stop is inside, and the stack says which,
// rather than a second thread an editor would expect to pause on its own.
const runThreadID = 1

// MaxScopeVariables bounds how many variables one expansion answers with.
const MaxScopeVariables = 500

// MaxVariableHandles bounds the variables references one stop holds. A
// reference is reused for an expression already issued at the stop, so an
// editor refreshing the same scopes does not add to it; distinct expressions
// past the bound come back without a reference, and so cannot be expanded,
// until the run moves and the table is cleared.
const MaxVariableHandles = 4096

// MaxVariableHandleBytes bounds the expression text the references one stop
// holds, which [MaxVariableHandles] alone does not: each expression may be as
// long as a command.
const MaxVariableHandleBytes = 4 << 20

// MaxBreakpointBytes bounds the text the adapter keeps for the breakpoints it
// holds — each source's path and every condition, hit condition and log
// message — across all sources, which the count bound alone does not.
const MaxBreakpointBytes = 1 << 20

// Server is one editor's debug session, over the Debug Adapter Protocol.
//
// It is a translation and nothing more: every request becomes a call on a
// [flowdebug.Target] — a local [flowdebug.Session] for a launch, a
// [flowdebug.Remote] for an attach to a durable run — and every capability it
// advertises is one the target reports. What the target does not do, the
// adapter refuses by name rather than answering success for.
type Server struct {
	stream Stream

	launcher LaunchFunc
	attacher AttachFunc

	mu  sync.Mutex
	seq int

	// order keeps a movement's response ahead of the stop it causes: DAP has
	// the response to `next` precede the `stopped` event, and a run can reach
	// its next stop before the command that moved it has been answered.
	order sync.Mutex

	// out serializes outbound messages: their numbering and their writing,
	// and hungUp, set once the client is gone and nothing more is written.
	out    sync.Mutex
	hungUp atomic.Bool

	target flowdebug.Target

	// completeMu guards driver: the completer is one value read by whichever
	// request arrives, and it keeps a cache.
	completeMu   sync.Mutex
	driver       *flowdebug.Driver
	sourceMap    *v1.DebugSourceMap
	capabilities *v1.DebugCapabilities
	start        func()
	terminate    func()
	remote       bool

	program         string
	revealSensitive bool
	stopOnEntry     bool

	// linesFrom0 and columnsFrom0 are the client's coordinates when its
	// initialize said they start at 0 rather than DAP's default of 1. Every
	// position is 1-based inside the adapter and translated at the edge.
	linesFrom0, columnsFrom0 bool

	// uriPaths is set when the client's initialize asked for source paths as
	// URIs rather than file system paths.
	uriPaths bool

	// lost is closed once a write to the client fails: the conversation is
	// over, whether or not its input has noticed.
	lost     chan struct{}
	loseOnce sync.Once

	// bound is set once a launch or attach has given the session its program;
	// a second is refused rather than replacing a target nobody would close.
	bound bool

	// pending are the breakpoints a durable run accepted but has not yet
	// applied — it installs a replacement at its next step boundary — by id,
	// with the source line each was set on. Each is reported to the editor
	// once a snapshot shows it applied.
	pending map[string]uint32
	// pendingAfter is the revision the pending replacement was accepted at:
	// the first hold past it is the run's answer to all of it.
	pendingAfter uint64

	launched chan struct{}
	once     sync.Once

	// nonce names this adapter in the request IDs it sends. A client's
	// sequence numbers restart with each connection, and a durable run keeps
	// the receipts of the commands it applied: an editor reconnecting to the
	// same session would otherwise send an ID the run already answered, and
	// have a new movement taken for a retry of an old one.
	nonce string

	// running counts the launched run the adapter started, so [Server.Wait]
	// can outlive the client of a run it let go of.
	running sync.WaitGroup

	// entered is closed once the first stop is announced (or the run ends),
	// so a movement that arrives before it waits for the stop it moves from.
	entered   chan struct{}
	enteredAt sync.Once

	// The stop the editor is looking at: its revision, its frames, and the
	// handles issued for it. A handle from an earlier stop answers nothing.
	revision uint64
	held     *v1.DebugSnapshot
	handles  map[int]handle
	issued   map[handle]int
	// issuedBytes is the expression text handles holds.
	issuedBytes int
	next        int
	observed    uint64

	lines       map[string][]lineBreakpoint
	functions   []functionBreakpoint
	failureMode v1.DebugFailureMode
	// heldMode is the failure filter in force when the held stop was
	// recorded, so exceptionInfo answers about that stop whatever the editor
	// changes while it is stopped.
	heldMode v1.DebugFailureMode
	ids      map[string]int
	idSeq    int

	ended sync.Once
	exit  int

	// pauses are the pause requests the run has taken but not yet held for,
	// kept as no more than their answer needs and at most [maxPendingPauses].
	// DAP answers a pause with a success that a `stopped` event follows, so
	// each is answered at the next stop, just before its event, or refused
	// when the run ends without one (#1297).
	pauses []inbound

	// endState is how the run ended, as the adapter last saw it, which words
	// the refusal of a pause the run ended before holding for.
	endState v1.DebugRunState
}

// maxPendingPauses bounds the pause requests waiting on one stop: each gets
// the same answer, and a client repeating a request it is still waiting on
// must not grow the adapter without limit (Codex, #2220).
const maxPendingPauses = 64

// pauseRefusal is how a pause the run ended before holding for is refused:
// in the words both drivers use for a run that completed first
// ([flowdebug.MissedPauseNotice]), or saying what ended instead.
func pauseRefusal(state v1.DebugRunState, exit int) string {
	switch {
	case state == v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
		state == v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED && exit == 0:
		return flowdebug.MissedPauseNotice
	case state == v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, state == v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED:
		return "the debug session ended before the run reached a step boundary to pause at"
	default:
		return "the run ended before it reached a step boundary to pause at"
	}
}

// recordEnd notes how the run ended, for [pauseRefusal].
func (s *Server) recordEnd(state v1.DebugRunState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.endState = state
}

type handle struct {
	revision   uint64
	expression string
}

type lineBreakpoint struct {
	line                             int
	condition, hitCondition, logText *string
}

type functionBreakpoint struct {
	name                    string
	condition, hitCondition *string
}

// LaunchArguments are a launch request's arguments.
type LaunchArguments struct {
	Program         string `json:"program"`
	RevealSensitive bool   `json:"revealSensitive"`
	StopOnEntry     *bool  `json:"stopOnEntry"`
	// Reverse asks for a run that can step back. The program runs again each
	// time it does, so it repeats every effect its steps have: the host
	// decides what to offer, and a launch that does not ask never does.
	Reverse bool `json:"reverse"`
	// Inputs are the run's arguments, keyed by the name the workflow
	// declares under `inputs:`, each a JSON value.
	Inputs map[string]json.RawMessage `json:"inputs"`
	Raw    json.RawMessage            `json:"-"`
}

// Reverser is a [flowdebug.Target] that can step back, as
// [flowdebug.Reversible] does. A server offers stepBack and reverseContinue
// only for a target that is one and says so in its capabilities.
type Reverser = flowdebug.Reverser

// capable is a target that reports its own capabilities before it has a
// snapshot, as a [flowdebug.Session] and a [flowdebug.Reversible] do.
type capable interface {
	Capabilities() *v1.DebugCapabilities
}

// Launch is what a [LaunchFunc] prepared: the session to drive, the source
// map it was built with, and how to start and stop the run.
type Launch struct {
	Target    flowdebug.Target
	SourceMap *v1.DebugSourceMap

	// Start runs the program. The adapter calls it once, at
	// configurationDone, so no step runs before the editor has set its
	// breakpoints.
	Start func()

	// Terminate ends the run. Nil means the adapter can only detach.
	Terminate func()
}

// LaunchFunc prepares a launch.
type LaunchFunc func(ctx context.Context, args LaunchArguments) (*Launch, error)

// AttachArguments are an attach request's arguments.
type AttachArguments struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
	SessionID  string `json:"sessionId"`
	Program    string `json:"program"`
	// History walks the run's recorded history instead of attaching a live
	// session: forward and back between its points, with nothing executing.
	History bool            `json:"history"`
	Raw     json.RawMessage `json:"-"`
}

// Attachment is what an [AttachFunc] attached to.
type Attachment struct {
	Target    flowdebug.Target
	SourceMap *v1.DebugSourceMap
}

// AttachFunc attaches to a durable run.
type AttachFunc func(ctx context.Context, args AttachArguments) (*Attachment, error)

// Option configures a [Server].
type Option func(*Server)

// WithLaunch makes launch prepare its own session and run.
func WithLaunch(launch LaunchFunc) Option { return func(s *Server) { s.launcher = launch } }

// WithAttach makes attach reach a durable run.
func WithAttach(attach AttachFunc) Option { return func(s *Server) { s.attacher = attach } }

// NewServer returns an adapter over stream. target, when non-nil, is a local
// session a caller already built and runs itself once [Server.Launched] is
// closed; otherwise [WithLaunch] or [WithAttach] supplies one.
func NewServer(target flowdebug.Target, stream Stream, opts ...Option) *Server {
	s := &Server{
		stream:      stream,
		target:      target,
		launched:    make(chan struct{}),
		lost:        make(chan struct{}),
		entered:     make(chan struct{}),
		handles:     map[int]handle{},
		issued:      map[handle]int{},
		lines:       map[string][]lineBreakpoint{},
		stopOnEntry: true,
		nonce:       rand.Text(),
	}
	if session, ok := target.(capable); ok {
		s.capabilities = session.Capabilities()
	}
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Launched is closed once the client has finished configuring a launch.
func (s *Server) Launched() <-chan struct{} { return s.launched }

// Program is the workflow a launch named.
func (s *Server) Program() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.program
}

// RevealSensitive reports whether the launch configuration asked to show
// values the workflow declares sensitive.
func (s *Server) RevealSensitive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.revealSensitive
}

// Output relays text to the editor's debug console.
func (s *Server) Output(text string) {
	if text == "" {
		return
	}

	s.emit("output", map[string]string{"category": "stdout", "output": text})
}

// Exited records the run's exit code, reported when it finishes.
func (s *Server) Exited(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.exit = code
}

// Finished reports the run's end to the editor, once.
func (s *Server) Finished() {
	// After any movement's response, for the reason [Server.order] gives: a
	// run can end before the command that let it go has been answered.
	s.order.Lock()
	defer s.order.Unlock()

	s.ended.Do(func() {
		s.mu.Lock()
		code, state := s.exit, s.endState
		// A finished run holds no stop: nothing more can be asked about it.
		s.held = nil
		s.mu.Unlock()

		for _, request := range s.takePauses() {
			s.fail(request, pauseRefusal(state, code))
		}
		s.enteredAt.Do(func() { close(s.entered) })
		s.emit("terminated", nil)
		s.emit("exited", exitedBody{ExitCode: code})
	})
}

// Wait blocks until a run the adapter launched has returned, and at once when
// it launched none. A client that disconnects without terminating detaches
// from a local run rather than ending it, so the process serving the adapter
// calls Wait after [Server.Serve] to let that run finish with the resources it
// was started with.
func (s *Server) Wait() { s.running.Wait() }

// Serve answers requests until the client disconnects or ctx ends.
func (s *Server) Serve(ctx context.Context) error {
	// Whatever ends the conversation, nothing more is written to it: a run the
	// session detached from goes on narrating, and on stdio a write to a
	// client that has gone is a broken pipe that kills the process under it.
	defer s.hangUp()

	// Ending ctx closes the stream, for a stream whose Close interrupts what
	// is blocked on it; the loop below does not depend on that.
	stop := context.AfterFunc(ctx, func() {
		s.hangUp()
		_ = s.stream.Close()
	})
	defer stop()

	// Reads happen on a goroutine of their own, so a read blocked on the
	// client never keeps ctx's end from detaching the session: an editor's
	// stdin is a blocking pipe, and closing it does not interrupt a read.
	reads := make(chan received)
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		for {
			var got received
			got.err = s.stream.ReadObject(&got.request)
			select {
			case reads <- got:
			case <-quit:
				return
			}
			if got.err != nil {
				return
			}
		}
	}()

	for {
		// A lost output ends the conversation before anything else is read.
		select {
		case <-s.lost:
			return s.leaveLost(ctx)
		default:
		}

		select {
		case <-s.lost:
			return s.leaveLost(ctx)

		case <-ctx.Done():
			// Ended from outside: the session detaches as it does when the
			// client goes, rather than leave a durable target renewing its
			// lease for a conversation that is over.
			s.hangUp()
			s.end(false)

			return ctx.Err()

		case got := <-reads:
			if got.err != nil {
				// A client gone without a disconnect is one: the session
				// detaches, so a run it left paused goes on rather than
				// waiting for a command nobody can send, and [Server.Wait]
				// returns. Nobody is left to read what the detach says.
				s.hangUp()
				s.end(false)

				return ctx.Err()
			}
			if got.request.Type != "request" {
				continue
			}
			if done := s.dispatch(ctx, got.request); done {
				return nil
			}
		}
	}
}

// fileURI is the file URI naming path: slashed, and with a drive letter's
// path rooted as a URI's must be ("C:\\x" is "file:///C:/x").
func fileURI(path string) string {
	slashed := filepath.ToSlash(path)
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}

	return (&url.URL{Scheme: "file", Path: slashed}).String()
}

// leaveLost ends a conversation whose output is gone: the session detaches
// and the stream is closed. It answers ctx's error when ctx has ended too, so
// a caller sees the same result whichever of the two Serve noticed first.
func (s *Server) leaveLost(ctx context.Context) error {
	s.end(false)
	_ = s.stream.Close()

	return ctx.Err()
}

// received is one read from the client: a request, or why there was none.
type received struct {
	request inbound
	err     error
}

func (s *Server) currentTarget() flowdebug.Target {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.target
}

func (s *Server) dispatch(ctx context.Context, request inbound) (done bool) {
	switch request.Command {
	case "initialize":
		var asked struct {
			LinesStartAt1   *bool  `json:"linesStartAt1"`
			ColumnsStartAt1 *bool  `json:"columnsStartAt1"`
			PathFormat      string `json:"pathFormat"`
		}
		_ = json.Unmarshal(request.Arguments, &asked)
		s.mu.Lock()
		s.linesFrom0 = asked.LinesStartAt1 != nil && !*asked.LinesStartAt1
		s.columnsFrom0 = asked.ColumnsStartAt1 != nil && !*asked.ColumnsStartAt1
		s.uriPaths = asked.PathFormat == "uri"
		s.mu.Unlock()
		s.reply(request, s.capabilitiesBody())
		s.emit("initialized", nil)

	case "launch":
		s.launch(ctx, request)

	case "attach":
		s.attach(ctx, request)

	case "configurationDone":
		s.reply(request, nil)
		s.release(ctx)

	case "setBreakpoints":
		s.setLineBreakpoints(ctx, request)

	case "setFunctionBreakpoints":
		s.setFunctionBreakpoints(ctx, request)

	case "setExceptionBreakpoints":
		s.setExceptionBreakpoints(ctx, request)

	case "threads":
		s.reply(request, threadsBody{Threads: []thread{{ID: runThreadID, Name: "run"}}})

	case "stackTrace":
		s.reply(request, s.stackTrace(request.Arguments))

	case "exceptionInfo":
		s.exceptionInfo(request)

	case "scopes":
		s.reply(request, s.scopeList(ctx, request.Arguments))

	case "variables":
		s.reply(request, s.variables(ctx, request.Arguments))

	case "evaluate":
		s.evaluate(ctx, request)

	case "completions":
		s.completions(ctx, request)

	case "pause":
		s.pause(ctx, request)

	case "continue":
		s.move(ctx, request, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE)

	case "next":
		s.move(ctx, request, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER)

	case "stepIn":
		s.move(ctx, request, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)

	case "stepOut":
		s.move(ctx, request, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT)

	case "stepBack":
		s.back(ctx, request, false)

	case "reverseContinue":
		s.back(ctx, request, true)

	case "gotoTargets":
		s.gotoTargets(ctx, request)

	case "goto":
		s.goTo(ctx, request)

	case "terminate":
		s.mu.Lock()
		owned := s.terminate != nil
		s.mu.Unlock()
		if !owned {
			// Never report ending a run this adapter did not start: it detaches,
			// and says the run goes on.
			s.end(false)
			s.fail(request, "flowdap: this adapter attached to the run rather than starting it, so it cannot end it; the session detached and the run continues")

			return true
		}
		// The run this adapter started is cancelled and the conversation
		// goes on: the run's end arrives as `terminated` and `exited`, which a
		// client waits for before it sends the `disconnect` that closes it.
		//
		// Under order, so the response precedes the events the cancellation
		// causes, as a movement's does; with the exit code recorded first, so
		// whichever path reports the end reports a run that did not finish.
		s.order.Lock()
		s.Exited(1)
		s.end(true)
		s.reply(request, nil)
		s.order.Unlock()
		// A run never released has no goroutine to report its end.
		select {
		case <-s.launched:
		default:
			s.Finished()
		}

	case "disconnect":
		// Answered once the session is released, so a client that reads the
		// response may rely on the run being detached or ended.
		var asked struct {
			TerminateDebuggee bool `json:"terminateDebuggee"`
		}
		_ = json.Unmarshal(request.Arguments, &asked)
		s.end(asked.TerminateDebuggee)
		s.reply(request, nil)

		return true

	default:
		s.fail(request, fmt.Sprintf("flowdap: %q is not something this adapter answers", request.Command))
	}

	return false
}

// end leaves the session: terminating the run when asked and able, and
// otherwise detaching, which lets it finish unattended. Disconnecting never
// silently ends a durable run.
func (s *Server) end(terminate bool) {
	s.mu.Lock()
	target, stop := s.target, s.terminate
	s.held = nil
	s.mu.Unlock()

	if terminate && stop != nil {
		stop()
	}
	if target != nil {
		_ = target.Close()
	}
}

func (s *Server) capabilitiesBody() capabilities {
	s.mu.Lock()
	caps := s.capabilities
	// Termination is the run owner's to offer. An adapter that launched the
	// run owns it, and before either request one that can launch offers it,
	// a launch being the common case; one that attached to a durable run does
	// not, whatever else it could have done.
	terminable := s.terminate != nil || (s.target == nil && !s.remote && s.launcher != nil)
	s.mu.Unlock()
	if caps == nil {
		// Before a launch or attach has said which backend this is, the
		// local session's set: a launch is the common case, and an attach
		// narrows it with a capabilities event once it knows.
		caps = localCapabilities()
	}

	body := capabilities{
		SupportsConfigurationDoneRequest:  true,
		SupportsFunctionBreakpoints:       true,
		SupportsConditionalBreakpoints:    caps.GetConditionalBreakpoints(),
		SupportsHitConditionalBreakpoints: caps.GetHitConditions(),
		SupportsLogPoints:                 caps.GetLogpoints(),
		SupportsEvaluateForHovers:         caps.GetInspect(),
		SupportsCompletionsRequest:        caps.GetInspect(),
		SupportsTerminateRequest:          terminable || caps.GetTerminate(),
		SupportTerminateDebuggee:          terminable || caps.GetTerminate(),
		SupportsDelayedStackTraceLoading:  true,
		SupportsStepBack:                  (caps.GetReverse() || caps.GetHistory()) && s.canStepBack(),
		SupportsGotoTargetsRequest:        (caps.GetReverse() || caps.GetHistory()) && s.canTravel(),
		ExceptionBreakpointFilters:        []exceptionFilter{},
	}
	body.SupportsExceptionInfoRequest = caps.GetFailureBreakpoints()
	if body.SupportsCompletionsRequest {
		body.CompletionTriggerCharacters = []string{"."}
	}
	if caps.GetFailureBreakpoints() {
		body.ExceptionBreakpointFilters = []exceptionFilter{
			{Filter: "uncaught", Label: "Failed steps", Description: "Stop where a step fails and its failure will propagate"},
			{Filter: "all", Label: "All step failures", Description: "Also stop where continue_on_error tolerates a failure"},
		}
	}

	return body
}

// localCapabilities is what a controlled local session does.
func localCapabilities() *v1.DebugCapabilities {
	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	if err != nil {
		// Nothing is advertised: every getter reads a nil set as false, and
		// only flowdebug and the durable driver construct one.
		return nil
	}
	defer func() { _ = session.Close() }()

	return session.Capabilities()
}

func (s *Server) launch(ctx context.Context, request inbound) {
	var asked LaunchArguments
	// Refused rather than read in part: a field of the wrong shape — an
	// `inputs` that is a list, a `stopOnEntry` that is a string — would
	// otherwise be dropped while the rest launches, running the program with
	// arguments nobody gave it.
	if len(request.Arguments) > 0 {
		if err := json.Unmarshal(request.Arguments, &asked); err != nil {
			s.fail(request, fmt.Sprintf("flowdap: the launch configuration could not be read: %v", err))

			return
		}
	}
	asked.Raw = request.Arguments

	s.mu.Lock()
	if s.bound && s.launcher != nil {
		s.mu.Unlock()
		// Refused before anything is recorded: a second launch must not
		// change the first one's options, such as its stop on entry.
		s.fail(request, errOneProgram.Error())

		return
	}
	launcher := s.launcher
	if launcher == nil {
		s.adoptLocked(asked)
		s.mu.Unlock()
		s.reply(request, nil)

		return
	}
	s.mu.Unlock()

	// The options are adopted only once the launch is taken: one the launcher
	// refused, a missing input say, must not leave its `stopOnEntry` behind
	// for the retry that corrects it.
	launched, err := launcher(ctx, asked)
	if err != nil {
		s.fail(request, err.Error())

		return
	}

	s.mu.Lock()
	s.adoptLocked(asked)
	s.bound = true
	s.target = launched.Target
	s.sourceMap = launched.SourceMap
	s.start = launched.Start
	s.terminate = launched.Terminate
	if session, ok := launched.Target.(capable); ok {
		s.capabilities = session.Capabilities()
	}
	reverse := s.capabilities.GetReverse() || s.capabilities.GetHistory()
	s.mu.Unlock()

	s.reply(request, nil)
	if reverse {
		// initialize answered before this launch said it could step back.
		s.emit("capabilities", map[string]any{"capabilities": s.capabilitiesBody()})
	}
	s.reapply(ctx)
}

// adoptLocked records a taken launch's options. Callers hold s.mu.
func (s *Server) adoptLocked(asked LaunchArguments) {
	s.program = asked.Program
	s.revealSensitive = asked.RevealSensitive
	if asked.StopOnEntry != nil {
		s.stopOnEntry = *asked.StopOnEntry
	}
}

func (s *Server) attach(ctx context.Context, request inbound) {
	if s.attacher == nil {
		s.fail(request, "flowdap: this adapter has no server to attach to; start it with `flow dap --address <server>`")

		return
	}

	var asked AttachArguments
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.WorkflowID == "" {
		s.fail(request, "flowdap: attach needs a workflowId")

		return
	}
	asked.Raw = request.Arguments
	s.mu.Lock()
	bound := s.bound
	s.mu.Unlock()
	if bound {
		s.fail(request, errOneProgram.Error())

		return
	}

	attached, err := s.attacher(ctx, asked)
	if err != nil {
		s.fail(request, err.Error())

		return
	}

	snapshot, err := attached.Target.Snapshot(ctx)
	if err != nil {
		_ = attached.Target.Close()
		s.fail(request, err.Error())

		return
	}

	s.mu.Lock()
	s.bound = true
	s.target = attached.Target
	s.sourceMap = attached.SourceMap
	s.capabilities = snapshot.GetCapabilities()
	s.remote = true
	// An exception filter an editor chose before it knew the backend is one
	// this backend may not offer. Kept, it would refuse every breakpoint set
	// sent with it; dropped, the rest of the configuration applies, and the
	// console says the filter did not.
	droppedFilter := !s.capabilities.GetFailureBreakpoints() && s.failureMode > v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
	if droppedFilter {
		s.failureMode = v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
	}
	s.mu.Unlock()

	s.reply(request, nil)
	if droppedFilter {
		s.emit("output", map[string]string{"category": "console",
			"output": "flowdap: this run cannot stop on failures, so the exception filter was dropped\n"})
	}
	// The durable driver does less than a local session; say so before the
	// editor configures anything it would then find ignored.
	s.emit("capabilities", map[string]any{"capabilities": s.capabilitiesBody()})
	s.reapply(ctx)
}

// reapply sends the breakpoints an editor set before there was anything to
// set them on, now that there is, and tells the editor what became of each.
func (s *Server) reapply(ctx context.Context) {
	s.mu.Lock()
	pending := len(s.lines) > 0 || len(s.functions) > 0 || s.failureMode > v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
	s.mu.Unlock()
	if !pending {
		return
	}

	states, err := s.applyBreakpoints(ctx)
	if err != nil {
		s.emit("output", map[string]string{"category": "stderr", "output": "flowdap: applying breakpoints: " + err.Error() + "\n"})

		return
	}
	for id, state := range states {
		answer := answerFor(state)
		answer.ID = s.breakpointID(id)
		s.emit("breakpoint", map[string]any{"reason": "changed", "breakpoint": answer})
	}
}

// breakpointID is the number an editor knows a breakpoint by, stable for as
// long as the breakpoint's slot is.
func (s *Server) breakpointID(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ids == nil {
		s.ids = map[string]int{}
	}
	if n, ok := s.ids[id]; ok {
		return n
	}
	s.idSeq++
	s.ids[id] = s.idSeq

	return s.idSeq
}

// forgetLines drops the editor numbers of path's line slots from the first
// one its set no longer has, so a client moving breakpoints across files
// does not grow the table with slots that are gone.
func (s *Server) forgetLines(path string, from int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := "line:" + path + ":"
	for id := range s.ids {
		slot, ok := strings.CutPrefix(id, prefix)
		if !ok {
			continue
		}
		if i, err := strconv.Atoi(slot); err == nil && i >= from {
			delete(s.ids, id)
		}
	}
}

// release starts the run once the editor has configured it, and begins
// reporting its stops.
func (s *Server) release(ctx context.Context) {
	s.once.Do(func() {
		close(s.launched)

		s.mu.Lock()
		start := s.start
		s.mu.Unlock()
		if start != nil {
			s.running.Go(start)
		}

		go s.watch(ctx)
	})
}

// How long watch keeps trying to read a target whose reads fail: a few
// attempts, each waiting a little longer, before it gives the run up.
const maxWatchRetries = 5

// watchRetryBackoff is the first wait between reads; each retry waits one
// more of it. A variable only so a test can shorten it.
var watchRetryBackoff = time.Second

// watch reports every stop the target reaches, in order, and the run's end.
func (s *Server) watch(ctx context.Context) {
	target := s.currentTarget()
	if target == nil {
		return
	}

	var (
		after    uint64
		failures int
	)
	for {
		snapshot, err := target.WaitSnapshot(ctx, after)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, flowdebug.ErrRunOver) {
				s.enteredAt.Do(func() { close(s.entered) })

				return
			}
			// A read that failed is not a session that ended: a durable
			// run's server can be briefly unreachable. Try again a few times,
			// and if it stays unreachable, let the run go rather than leave
			// it held behind a lease this adapter keeps renewing while the
			// editor hears nothing.
			failures++
			if failures == 1 {
				s.emit("output", map[string]string{"category": "stderr", "output": "flowdap: " + err.Error() + "; retrying\n"})
			}
			if failures <= maxWatchRetries {
				select {
				case <-time.After(time.Duration(failures) * watchRetryBackoff):
					continue
				case <-ctx.Done():
					s.enteredAt.Do(func() { close(s.entered) })

					return
				}
			}
			s.emit("output", map[string]string{"category": "stderr",
				"output": "flowdap: the run could not be read, so the session detached and the run continues\n"})
			_ = target.Close()
			s.enteredAt.Do(func() { close(s.entered) })
			s.Exited(1)
			s.Finished()

			return
		}
		failures = 0
		after = snapshot.GetRevision()
		s.relayObservations(snapshot)
		s.relayBreakpoints(snapshot)

		switch state := snapshot.GetState(); {
		case state == v1.DebugRunState_DEBUG_RUN_STATE_HELD:
			if snapshot.GetReason() == v1.DebugStopReason_DEBUG_STOP_REASON_ENTRY && !s.stopsOnEntry() {
				_, _ = target.Resume(ctx, &v1.DebugResumeRequest{
					Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, ExpectedRevision: snapshot.GetRevision(),
				})

				continue
			}
			s.stopped(snapshot)

		case terminalState(state):
			s.recordEnd(state)
			s.enteredAt.Do(func() { close(s.entered) })
			if s.isRemote() {
				code := exitCodeFor(state)
				if message := snapshot.GetMessage(); message != "" {
					s.Output(message + "\n")
				}
				s.Exited(code)
				s.Finished()
			}

			return
		}
	}
}

func (s *Server) stopsOnEntry() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stopOnEntry
}

func (s *Server) isRemote() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.remote
}

// exitCodeFor is the exit code a durable run's terminal state reports: zero
// for a run that completed or that the debugger let go, one otherwise.
func exitCodeFor(state v1.DebugRunState) int {
	switch state {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return 0
	default:
		return 1
	}
}

func terminalState(state v1.DebugRunState) bool {
	switch state {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return true
	default:
		return false
	}
}

// relayObservations sends a durable run's observations to the console. A
// local session prints its own account as it goes, so it is relayed only for
// a remote one.
func (s *Server) relayObservations(snapshot *v1.DebugSnapshot) {
	if !s.isRemote() {
		return
	}
	for _, observation := range snapshot.GetObservations() {
		s.mu.Lock()
		fresh := observation.GetSequence() > s.observed
		if fresh {
			s.observed = observation.GetSequence()
		}
		s.mu.Unlock()
		if fresh {
			s.emit("output", map[string]string{"category": "console", "output": "  " + observation.GetText() + "\n"})
		}
	}
}

// stopped records a new stop and tells the editor.
func (s *Server) stopped(snapshot *v1.DebugSnapshot) {
	s.order.Lock()
	defer s.order.Unlock()

	s.mu.Lock()
	s.revision = snapshot.GetRevision()
	s.held = snapshot
	s.heldMode = s.failureMode
	clear(s.handles)
	clear(s.issued)
	s.issuedBytes = 0
	s.mu.Unlock()

	body := stoppedBody{
		Reason:            stopReason(snapshot.GetReason()),
		Description:       snapshot.GetOccurrence().GetSite().GetKind(),
		ThreadID:          runThreadID,
		AllThreadsStopped: true,
	}
	switch snapshot.GetReason() {
	case v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE:
		body.Description = "step failed"
		body.Text = snapshot.GetFailure()
	case v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY:
		body.Description = "autopsy"
	}
	// A pause taken while running is answered by this stop, whatever held
	// the run first, and its answer goes ahead of the event.
	for _, request := range s.takePauses() {
		s.reply(request, nil)
	}
	s.emit("stopped", body)
	s.enteredAt.Do(func() { close(s.entered) })
}

func stopReason(reason v1.DebugStopReason) string {
	switch reason {
	case v1.DebugStopReason_DEBUG_STOP_REASON_ENTRY:
		return "entry"
	case v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT:
		return "breakpoint"
	case v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY:
		return "pause"
	case v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE:
		return "exception"
	default:
		return "step"
	}
}

// move resumes the run. The response says the command was applied; the next
// stop arrives as its own stopped event.
func (s *Server) move(ctx context.Context, request inbound, action v1.DebugResumeAction) {
	select {
	case <-s.entered:
	case <-ctx.Done():
		return
	}

	target := s.currentTarget()
	if target == nil {
		s.fail(request, "flowdap: nothing is running")

		return
	}

	s.order.Lock()
	defer s.order.Unlock()

	s.mu.Lock()
	revision := s.revision
	s.mu.Unlock()

	receipt, err := target.Resume(ctx, &v1.DebugResumeRequest{
		RequestId:        s.requestID(request.Seq),
		Action:           action,
		ExpectedRevision: revision,
	})
	if err != nil {
		s.fail(request, err.Error())

		return
	}

	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		s.mu.Lock()
		clear(s.handles)
		clear(s.issued)
		s.issuedBytes = 0
		s.held = nil
		s.mu.Unlock()
		if action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE {
			s.reply(request, map[string]bool{"allThreadsContinued": true})
		} else {
			s.reply(request, nil)
		}
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED:
		// A durable run's end is reported with the code its final state
		// gives, not the default, whichever of this and the watcher gets
		// there first. A local run's code is recorded before its session
		// reads as ended.
		if s.isRemote() {
			if final, err := target.Snapshot(ctx); err == nil && terminalState(final.GetState()) {
				s.recordEnd(final.GetState())
				s.Exited(exitCodeFor(final.GetState()))
			}
		}
		s.reply(request, nil)
		go s.Finished()
	default:
		s.fail(request, receiptText(receipt))
	}
}

// canStepBack is whether the bound target can be asked to go back.
func (s *Server) canStepBack() bool {
	_, ok := s.currentTarget().(Reverser)

	return ok
}

// canTravel is whether the bound target can be asked to go to a timeline point.
func (s *Server) canTravel() bool {
	_, ok := s.currentTarget().(flowdebug.Traveler)

	return ok
}

// back answers stepBack and, with toBreakpoint, reverseContinue: the target
// goes to the previous stop, or the nearest earlier one a breakpoint decided.
// The stop it lands on reaches the editor through the same watch as any other,
// so the response is ordered ahead of it the way a movement's is.
func (s *Server) back(ctx context.Context, request inbound, toBreakpoint bool) {
	select {
	case <-s.entered:
	case <-ctx.Done():
		return
	}

	reverser, ok := s.currentTarget().(Reverser)
	if !ok || !s.capabilitiesBody().SupportsStepBack {
		s.fail(request, "flowdap: this session cannot step back; launch with \"reverse\": true to run one that can, or attach with \"history\": true to walk a recorded run")

		return
	}
	rewind := reverser.Back
	if toBreakpoint {
		rewind = reverser.BackToBreakpoint
	}
	s.travel(ctx, request, rewind)
}

// travel answers a request that moves the target to a stop it showed: the
// target goes there, and the stop it lands on reaches the editor through the
// same watch as any other, so the response is ordered ahead of it the way a
// movement's is.
func (s *Server) travel(ctx context.Context, request inbound, move func(ctx context.Context, requestID string, expected uint64) (*v1.DebugReceipt, error)) {
	s.order.Lock()
	defer s.order.Unlock()

	// A run that has ended has no stop to go back to: the editor's session
	// ends with it, and a rewind would start a run nobody is attached to.
	if snapshot, err := s.currentTarget().Snapshot(ctx); err == nil && terminalState(snapshot.GetState()) {
		s.fail(request, "flowdap: the run has ended, so there is no stop to go back to")

		return
	}

	s.mu.Lock()
	revision := s.revision
	s.mu.Unlock()

	receipt, err := move(ctx, s.requestID(request.Seq), revision)
	if err != nil {
		s.fail(request, err.Error())

		return
	}

	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE:
		s.mu.Lock()
		clear(s.handles)
		clear(s.issued)
		s.issuedBytes = 0
		s.held = nil
		s.mu.Unlock()
		s.reply(request, nil)
	default:
		s.fail(request, receiptText(receipt))
	}
}

// gotoTargets answers the points of the timeline an editor may jump to from a
// source position: each point a travel would be tried for. A point whose step
// the verified source map places on another line, or in another document, is
// left out; one it cannot place is offered at the line asked about and named by
// its address, so it can be chosen without being drawn somewhere it is not.
func (s *Server) gotoTargets(ctx context.Context, request inbound) {
	select {
	case <-s.entered:
	case <-ctx.Done():
		return
	}

	var asked struct {
		Source *source `json:"source"`
		Line   int     `json:"line"`
	}
	if len(request.Arguments) != 0 {
		if err := json.Unmarshal(request.Arguments, &asked); err != nil {
			s.fail(request, "flowdap: gotoTargets needs a source and a line")

			return
		}
	}
	target := s.currentTarget()
	if target == nil || !s.capabilitiesBody().SupportsGotoTargetsRequest {
		s.reply(request, gotoTargetsBody{Targets: []gotoTarget{}})

		return
	}
	snapshot, err := target.Snapshot(ctx)
	if err != nil {
		s.fail(request, err.Error())

		return
	}

	s.mu.Lock()
	sourceMap := s.sourceMap
	s.mu.Unlock()
	places := map[string]*v1.DebugSourceLocation{}
	for _, entry := range sourceMap.GetEntries() {
		if key := v1.DebugSiteKey(entry.GetSite()); places[key] == nil {
			places[key] = entry.GetLocation()
		}
	}

	lineBase, _ := s.clientBases()
	targets := []gotoTarget{}
	for i, point := range snapshot.GetTimeline().GetPoints() {
		if !point.GetReachable() {
			continue
		}
		line := asked.Line
		if location := places[v1.DebugSiteKey(point.GetOccurrence().GetSite())]; location != nil && location.GetRange() != nil {
			here := toClient(location.GetRange().GetStartLine(), lineBase)
			documents := sourceMap.GetDocuments()
			elsewhere := asked.Source != nil && asked.Source.Path != "" && int(location.GetDocument()) < len(documents) &&
				!flowdebug.SameSourceURI(documents[location.GetDocument()].GetUri(), asked.Source.Path)
			if elsewhere || (asked.Line > 0 && here != asked.Line) {
				continue
			}
			line = here
		}
		targets = append(targets, gotoTarget{
			ID:    i,
			Label: fmt.Sprintf("%d: %s", i, point.GetOccurrence().GetAddress()),
			Line:  line,
		})
	}
	s.reply(request, gotoTargetsBody{Targets: targets})
}

// goTo answers goto: the target travels to the timeline point the target id
// names, which is the index gotoTargets offered it under.
func (s *Server) goTo(ctx context.Context, request inbound) {
	select {
	case <-s.entered:
	case <-ctx.Done():
		return
	}

	var asked struct {
		TargetID *int `json:"targetId"`
	}
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.TargetID == nil || *asked.TargetID < 0 || *asked.TargetID > math.MaxInt32 {
		s.fail(request, "flowdap: goto needs the targetId of a target gotoTargets offered")

		return
	}
	traveler, ok := s.currentTarget().(flowdebug.Traveler)
	if !ok || !s.capabilitiesBody().SupportsGotoTargetsRequest {
		s.fail(request, "flowdap: this session cannot go to a point on its timeline; launch with \"reverse\": true to run one that can, or attach with \"history\": true to walk a recorded run")

		return
	}
	point := int32(*asked.TargetID)
	s.travel(ctx, request, func(ctx context.Context, requestID string, expected uint64) (*v1.DebugReceipt, error) {
		return traveler.Travel(ctx, requestID, expected, point)
	})
}

func receiptText(receipt *v1.DebugReceipt) string {
	status := strings.ToLower(strings.TrimPrefix(receipt.GetStatus().String(), "DEBUG_COMMAND_STATUS_"))
	if receipt.GetMessage() == "" {
		return "flowdap: " + status
	}

	return "flowdap: " + status + ": " + receipt.GetMessage()
}

// pause asks the run to hold at its next boundary. The stopped event follows
// when it does; work already under way is not interrupted.
func (s *Server) pause(ctx context.Context, request inbound) {
	target := s.currentTarget()
	if target == nil {
		s.fail(request, "flowdap: nothing is running")

		return
	}

	// Under the order lock, as every movement is: the stop a pause causes
	// can be ready before this handler answers, and its response must still
	// reach the client first.
	s.order.Lock()
	defer s.order.Unlock()

	// Refused before it reaches the run, so a client repeating a pause it is
	// still waiting on sends the run nothing more (Codex, #2220). Under
	// order, as every change to the waiting pauses is, so none is answered
	// between this check and the append.
	s.mu.Lock()
	full := len(s.pauses) >= maxPendingPauses
	s.mu.Unlock()
	if full {
		s.fail(request, "flowdap: a pause is already waiting on the run's next step boundary")

		return
	}
	receipt, err := target.Pause(ctx, s.requestID(request.Seq))
	if err != nil {
		s.fail(request, err.Error())

		return
	}
	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		// Answered by the stop it causes, or refused by the end of a run
		// that reached no boundary to hold at: a success no stop follows is
		// the one answer DAP does not allow.
		s.mu.Lock()
		s.pauses = append(s.pauses, inbound{Seq: request.Seq, Command: request.Command})
		s.mu.Unlock()
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE:
		s.reply(request, nil)
	default:
		s.fail(request, receiptText(receipt))
	}
}

// takePauses is the pause requests still waiting on the run, which the
// caller answers.
func (s *Server) takePauses() []inbound {
	s.mu.Lock()
	defer s.mu.Unlock()

	pauses := s.pauses
	s.pauses = nil

	return pauses
}

func (s *Server) currentStop() (*v1.DebugSnapshot, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.held, s.revision
}

// exceptionInfo answers why the held step failed from the held snapshot's
// failure, which the backend already rendered, redacted and bounded; nothing is
// re-evaluated. It fails closed: a run that is not held at a failure stop, or
// whose stop has since moved on, has no exception to describe.
func (s *Server) exceptionInfo(request inbound) {
	var asked struct {
		ThreadID int `json:"threadId"`
	}
	// Unmarshalled even when absent: threadId is required, so a request
	// without arguments is as malformed as one naming another thread.
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.ThreadID != runThreadID {
		s.fail(request, "flowdap: exceptionInfo names the run's thread")

		return
	}

	s.mu.Lock()
	held, mode := s.held, s.heldMode
	s.mu.Unlock()
	if held == nil || held.GetReason() != v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE {
		s.fail(request, "flowdap: the run is not stopped at a step failure")

		return
	}

	address := held.GetOccurrence().GetAddress()
	if address == "" {
		address = strings.Join(held.GetOccurrence().GetSite().GetPath(), "/")
	}
	breakMode := "unhandled"
	if mode == v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL {
		breakMode = "always"
	}
	s.reply(request, exceptionInfoBody{
		ExceptionID: address,
		Description: "step failed",
		BreakMode:   breakMode,
		Details:     exceptionDetails{Message: held.GetFailure()},
	})
}

func (s *Server) stackTrace(arguments json.RawMessage) stackTraceBody {
	var asked struct {
		StartFrame int `json:"startFrame"`
		Levels     int `json:"levels"`
	}
	if len(arguments) != 0 {
		if err := json.Unmarshal(arguments, &asked); err != nil || asked.StartFrame < 0 || asked.Levels < 0 {
			return stackTraceBody{StackFrames: []stackFrame{}}
		}
	}

	held, _ := s.currentStop()
	if held == nil {
		return stackTraceBody{StackFrames: []stackFrame{}}
	}
	if held.GetReason() == v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY {
		frames := []stackFrame{{ID: 1, Name: "after the run"}}

		return stackTraceBody{StackFrames: frameWindow(frames, asked.StartFrame, asked.Levels), TotalFrames: 1}
	}

	lineBase, columnBase := s.clientBases()
	frames := make([]stackFrame, 0, len(held.GetFrames()))
	for _, frame := range held.GetFrames() {
		entry := stackFrame{ID: int(frame.GetId()), Name: frame.GetLabel()}
		if !frame.GetScoped() {
			entry.PresentationHint = "subtle"
		}
		if location := frame.GetSource(); location != nil {
			if source := s.sourceOf(location); source != nil {
				entry.Source = source
				entry.Line = toClient(location.GetRange().GetStartLine(), lineBase)
				entry.Column = toClient(location.GetRange().GetStartColumn(), columnBase)
			}
		}
		frames = append(frames, entry)
	}

	return stackTraceBody{
		StackFrames: frameWindow(frames, asked.StartFrame, asked.Levels),
		TotalFrames: len(frames),
	}
}

// sourceOf is a location's document as an editor names it.
func (s *Server) sourceOf(location *v1.DebugSourceLocation) *source {
	s.mu.Lock()
	sourceMap, uriPaths := s.sourceMap, s.uriPaths
	s.mu.Unlock()

	documents := sourceMap.GetDocuments()
	index := int(location.GetDocument())
	if index < 0 || index >= len(documents) || location.GetRange() == nil {
		return nil
	}
	path := strings.TrimPrefix(documents[index].GetUri(), "file://")
	if uriPaths {
		// In the form the client asked for, so a frame and a breakpoint it
		// set name one document.
		return &source{Name: filepath.Base(path), Path: fileURI(path)}
	}

	return &source{Name: filepath.Base(path), Path: path}
}

func frameWindow(frames []stackFrame, start, levels int) []stackFrame {
	if start >= len(frames) {
		return []stackFrame{}
	}
	end := len(frames)
	if levels > 0 && levels < len(frames)-start {
		end = start + levels
	}

	return frames[start:end]
}

// toClient translates a 1-based position to the client's base. Zero is an
// unknown position and stays zero, the value the specification requires in
// place of an absent one.
func toClient(position uint32, base int) int {
	if position == 0 {
		return 0
	}

	return int(position) - 1 + base
}

// requestID is the retry key for the command a client request carries: stable
// for that request, and distinct from every other adapter's.
func (s *Server) requestID(seq int) string { return fmt.Sprintf("dap-%s-%d", s.nonce, seq) }

// clientBases is the first line and column number in the client's
// coordinates: 1 unless its initialize said 0.
func (s *Server) clientBases() (line, column int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	line, column = 1, 1
	if s.linesFrom0 {
		line = 0
	}
	if s.columnsFrom0 {
		column = 0
	}

	return line, column
}

// issue hands out a variables reference for an expression at a revision: the
// one already issued for it at this stop, or a new one while the stop holds
// fewer than [MaxVariableHandles] and [MaxVariableHandleBytes] allows it, and
// otherwise none.
func (s *Server) issue(revision uint64, expression string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := handle{revision: revision, expression: expression}
	if reference, ok := s.issued[key]; ok {
		return reference
	}
	if len(s.handles) >= MaxVariableHandles || s.issuedBytes+len(expression) > MaxVariableHandleBytes {
		return 0
	}
	s.next++
	s.handles[s.next] = key
	s.issued[key] = s.next
	s.issuedBytes += len(expression)

	return s.next
}

func (s *Server) scopeList(ctx context.Context, arguments json.RawMessage) scopesBody {
	var asked struct {
		FrameID int `json:"frameId"`
	}
	held, revision := s.currentStop()
	if err := json.Unmarshal(arguments, &asked); err != nil || held == nil || !scopedFrame(held, asked.FrameID) {
		return scopesBody{Scopes: []scope{}}
	}

	target := s.currentTarget()
	roots, err := target.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision, Limit: MaxScopeVariables})
	if err != nil {
		return scopesBody{Scopes: []scope{}}
	}

	scopes := make([]scope, 0, len(roots.GetChildren()))
	for _, group := range roots.GetChildren() {
		scopes = append(scopes, scope{
			Name:               group.GetName(),
			VariablesReference: s.issue(revision, group.GetValue().GetExpression()),
			NamedVariables:     int(group.GetValue().GetChildren()),
		})
	}

	return scopesBody{Scopes: scopes}
}

// scopedFrame reports whether a frame id names the stop's readable frame: the
// innermost one, whose scope is the run's own. A container's frame names
// where the stop is, and has no bindings of its own to read.
func scopedFrame(held *v1.DebugSnapshot, id int) bool {
	if held.GetReason() == v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY {
		return id == 1
	}
	for _, frame := range held.GetFrames() {
		if int(frame.GetId()) == id {
			return frame.GetScoped()
		}
	}

	return false
}

func (s *Server) variables(ctx context.Context, arguments json.RawMessage) variablesBody {
	var asked struct {
		VariablesReference int `json:"variablesReference"`
	}
	_ = json.Unmarshal(arguments, &asked)

	s.mu.Lock()
	reference, known := s.handles[asked.VariablesReference]
	current := s.revision
	s.mu.Unlock()
	if !known || reference.revision != current {
		return variablesBody{Variables: []variable{}}
	}

	answer, err := s.currentTarget().Inspect(ctx, &v1.DebugInspectRequest{
		Revision:   reference.revision,
		Expression: reference.expression,
		Children:   true,
		Limit:      MaxScopeVariables,
	})
	if err != nil || answer.GetError() != "" {
		return variablesBody{Variables: []variable{}}
	}

	variables := make([]variable, 0, len(answer.GetChildren())+1)
	for _, child := range answer.GetChildren() {
		value := child.GetValue()
		entry := variable{Name: child.GetName(), Value: value.GetRendered(), Type: value.GetType()}
		if value.GetType() == "error" {
			entry.Value = "(" + value.GetRendered() + ")"
		}
		if !strings.HasPrefix(value.GetExpression(), "@") {
			entry.EvaluateName = value.GetExpression()
		}
		if value.GetChildren() > 0 && value.GetExpression() != "" {
			entry.VariablesReference = s.issue(reference.revision, value.GetExpression())
		}
		variables = append(variables, entry)
	}
	if omitted := int(answer.GetTotal()) - len(answer.GetChildren()); omitted > 0 {
		variables = append(variables, variable{Name: "…", Value: fmt.Sprintf("%d more, not rendered", omitted)})
	}

	return variablesBody{Variables: variables}
}

func (s *Server) evaluate(ctx context.Context, request inbound) {
	var asked struct {
		Expression string `json:"expression"`
		FrameID    *int   `json:"frameId"`
	}
	if err := json.Unmarshal(request.Arguments, &asked); err != nil {
		s.fail(request, "invalid evaluate arguments")

		return
	}

	held, revision := s.currentStop()
	if held == nil {
		s.fail(request, "the run is not stopped, so there is nothing to evaluate against")

		return
	}
	if asked.FrameID != nil && !scopedFrame(held, *asked.FrameID) {
		s.fail(request, "that stack frame has no readable scope")

		return
	}

	answer, err := s.currentTarget().Inspect(ctx, &v1.DebugInspectRequest{Revision: revision, Expression: asked.Expression})
	if err != nil {
		s.fail(request, err.Error())

		return
	}
	if answer.GetError() != "" {
		s.fail(request, answer.GetError())

		return
	}

	body := evaluateBody{Result: answer.GetValue().GetRendered(), Type: answer.GetValue().GetType()}
	if answer.GetValue().GetChildren() > 0 {
		body.VariablesReference = s.issue(revision, asked.Expression)
	}
	s.reply(request, body)
}

// maxCompletionText bounds the text a `completions` request is answered for: an
// expression typed at a console, not a document.
const maxCompletionText = 4096

// completions answers the names an expression may continue with at the cursor.
//
// It offers the same names, and withholds the same ones, as `evaluate` would
// answer for the text before it: it asks the target to inspect scope, and reads
// only the names from what comes back. A run that is not stopped, or a target
// that refuses the inspection, has nothing to offer, and says so with an empty
// list rather than an error, because an editor asks on every keystroke.
func (s *Server) completions(ctx context.Context, request inbound) {
	var asked struct {
		FrameID *int   `json:"frameId"`
		Text    string `json:"text"`
		Line    *int   `json:"line"`
		Column  int    `json:"column"`
	}
	if err := json.Unmarshal(request.Arguments, &asked); err != nil {
		s.fail(request, "invalid completions arguments")

		return
	}
	empty := completionsBody{Targets: []completionItem{}}

	// A frame with no readable scope has no names to offer, as `evaluate` has
	// nothing to read there.
	held, _ := s.currentStop()
	if held == nil || len(asked.Text) > maxCompletionText || asked.FrameID != nil && !scopedFrame(held, *asked.FrameID) {
		s.reply(request, empty)

		return
	}

	s.mu.Lock()
	columnsFrom0, linesFrom0 := s.columnsFrom0, s.linesFrom0
	s.mu.Unlock()

	// The console may send several lines and name the one the cursor is on;
	// the column counts from the start of that line.
	text := asked.Text
	if asked.Line != nil {
		index := *asked.Line
		if !linesFrom0 {
			index--
		}
		lines := strings.Split(text, "\n")
		if index < 0 || index >= len(lines) {
			s.reply(request, empty)

			return
		}
		text = lines[index]
	}
	cursor := asked.Column
	if !columnsFrom0 {
		cursor--
	}
	text = text[:byteOffset(text, cursor)]

	answer, err := s.completer(s.currentTarget()).CompleteExpression(ctx, text)
	if err != nil {
		s.reply(request, empty)

		return
	}

	// Both positions are in the UTF-16 units an editor counts in, and `start`
	// is in the client's column origin like the `column` it answers.
	start := utf16Len(text[:len(text)-len(answer.Prefix)])
	if !columnsFrom0 {
		start++
	}
	body := empty
	for _, candidate := range answer.Candidates {
		kind := "field"
		if candidate.Continues {
			kind = "module"
		}
		body.Targets = append(body.Targets, completionItem{
			Label: candidate.Text, Text: candidate.Text, Type: kind, Start: start, Length: utf16Len(answer.Prefix),
		})
	}
	s.reply(request, body)
}

// byteOffset is the index in text of the given UTF-16 offset, clamped to the
// text and never inside a character.
func byteOffset(text string, units int) int {
	for i, r := range text {
		if units <= 0 {
			return i
		}
		units -= utf16.RuneLen(r)
	}

	return len(text)
}

func utf16Len(text string) int {
	n := 0
	for _, r := range text {
		n += utf16.RuneLen(r)
	}

	return n
}

// completer is the driver completions are read through. A server is bound to
// one target for its life — a second launch or attach is refused — so the
// driver, and the per-revision cache it keeps, is made once. Targets are not
// compared: nothing requires one to be comparable.
func (s *Server) completer(target flowdebug.Target) *flowdebug.Driver {
	s.completeMu.Lock()
	defer s.completeMu.Unlock()
	if s.driver == nil {
		s.driver = flowdebug.NewDriver(target)
	}

	return s.driver
}

// errInvalidBreakpoints is the one text a malformed breakpoint request is
// refused with, which never quotes what was submitted.
var errInvalidBreakpoints = errors.New("invalid breakpoint arguments")

// errOneProgram refuses a second launch or attach in one session: the first
// target would be replaced without being closed, and a run it held would
// wait on a debugger nobody can reach.
var errOneProgram = errors.New("flowdap: this session already launched or attached to a program; start another debug session for another")

// errTooManyBreakpoints fails a request that alone names more breakpoints than
// a session holds. It fails whole, before anything is built from it, rather
// than answering each entry: a frame can carry far more compact entries than a
// session will ever hold, and one refusal apiece would make the response many
// times the request. A set within the bound that only overflows with the other
// sources installed is still answered entry by entry.
var errTooManyBreakpoints = fmt.Errorf("a breakpoint request may name at most %d breakpoints", flowdebug.MaxBreakpoints)

func (s *Server) setLineBreakpoints(ctx context.Context, request inbound) {
	var asked struct {
		Source *struct {
			Path string `json:"path"`
		} `json:"source"`
		Breakpoints []*struct {
			Line         *int    `json:"line"`
			Condition    *string `json:"condition"`
			HitCondition *string `json:"hitCondition"`
			LogMessage   *string `json:"logMessage"`
		} `json:"breakpoints"`
		SourceModified bool `json:"sourceModified"`
	}
	// A missing array is malformed, not an empty replacement: only an explicit
	// empty set clears a source's breakpoints.
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.Source == nil || asked.Source.Path == "" ||
		len(asked.Source.Path) > maxSourcePathBytes || asked.Breakpoints == nil {
		s.fail(request, errInvalidBreakpoints.Error())

		return
	}
	if len(asked.Breakpoints) > flowdebug.MaxBreakpoints {
		s.fail(request, errTooManyBreakpoints.Error())

		return
	}
	// Lines in a file edited since the program was compiled are not the lines
	// the source map knows: set through it, a breakpoint would stop on
	// another step or never. They are answered unverified, and the set
	// already installed stands. An empty set names no line, and clears the
	// source's breakpoints as it would for an unedited file.
	if asked.SourceModified && len(asked.Breakpoints) > 0 {
		s.reply(request, breakpointsBody{Breakpoints: refused(len(asked.Breakpoints),
			"the file changed since the program was compiled, so its lines no longer name the steps that run; restart the debug session to break on the edited file")})

		return
	}

	lineBase, _ := s.clientBases()
	wanted := make([]lineBreakpoint, 0, len(asked.Breakpoints))
	for _, want := range asked.Breakpoints {
		// Positions are uint32 past this edge: a larger line would wrap onto
		// a small one rather than name no line.
		if want == nil || want.Line == nil || *want.Line < lineBase || int64(*want.Line)-int64(lineBase)+1 > math.MaxUint32 {
			s.fail(request, errInvalidBreakpoints.Error())

			return
		}
		wanted = append(wanted, lineBreakpoint{line: *want.Line - lineBase + 1, condition: want.Condition, hitCondition: want.HitCondition, logText: want.LogMessage})
	}
	if s.retainedBytes(asked.Source.Path, false)+lineBytes(asked.Source.Path, wanted) > MaxBreakpointBytes {
		s.fail(request, errBreakpointBytes.Error())

		return
	}
	if s.totalBreakpoints(asked.Source.Path, len(wanted), -1) > flowdebug.MaxBreakpoints {
		s.reply(request, breakpointsBody{Breakpoints: refused(len(wanted),
			fmt.Sprintf("a session may hold at most %d breakpoints", flowdebug.MaxBreakpoints))})

		return
	}

	s.mu.Lock()
	previous, had := s.lines[asked.Source.Path]
	if len(wanted) == 0 {
		delete(s.lines, asked.Source.Path)
	} else {
		s.lines[asked.Source.Path] = wanted
	}
	s.mu.Unlock()

	states, err := s.applyBreakpoints(ctx)
	if err != nil {
		s.mu.Lock()
		if had {
			s.lines[asked.Source.Path] = previous
		} else {
			delete(s.lines, asked.Source.Path)
		}
		s.mu.Unlock()
		s.reply(request, breakpointsBody{Breakpoints: refused(len(wanted), err.Error())})

		return
	}

	s.forgetLines(asked.Source.Path, len(wanted))
	answers := make([]breakpoint, 0, len(wanted))
	for i, want := range wanted {
		answer := answerFor(states[lineID(asked.Source.Path, i)])
		answer.ID = s.breakpointID(lineID(asked.Source.Path, i))
		answer.Line = new(want.line - 1 + lineBase)
		answers = append(answers, answer)
	}
	s.reply(request, breakpointsBody{Breakpoints: answers})
}

func (s *Server) setFunctionBreakpoints(ctx context.Context, request inbound) {
	var asked struct {
		Breakpoints []*struct {
			Name         *string `json:"name"`
			Condition    *string `json:"condition"`
			HitCondition *string `json:"hitCondition"`
		} `json:"breakpoints"`
	}
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.Breakpoints == nil {
		// A malformed replacement must not clear the installed set.
		s.fail(request, errInvalidBreakpoints.Error())

		return
	}
	if len(asked.Breakpoints) > flowdebug.MaxBreakpoints {
		s.fail(request, errTooManyBreakpoints.Error())

		return
	}

	wanted := make([]functionBreakpoint, 0, len(asked.Breakpoints))
	for _, want := range asked.Breakpoints {
		if want == nil || want.Name == nil {
			s.fail(request, errInvalidBreakpoints.Error())

			return
		}
		wanted = append(wanted, functionBreakpoint{name: strings.TrimSpace(*want.Name), condition: want.Condition, hitCondition: want.HitCondition})
	}
	if s.retainedBytes("", true)+functionBytes(wanted) > MaxBreakpointBytes {
		s.fail(request, errBreakpointBytes.Error())

		return
	}
	if s.totalBreakpoints("", 0, len(wanted)) > flowdebug.MaxBreakpoints {
		s.reply(request, breakpointsBody{Breakpoints: refused(len(wanted),
			fmt.Sprintf("a session may hold at most %d breakpoints", flowdebug.MaxBreakpoints))})

		return
	}

	s.mu.Lock()
	previous := s.functions
	s.functions = wanted
	s.mu.Unlock()

	states, err := s.applyBreakpoints(ctx)
	if err != nil {
		s.mu.Lock()
		s.functions = previous
		s.mu.Unlock()
		s.reply(request, breakpointsBody{Breakpoints: refused(len(wanted), err.Error())})

		return
	}

	answers := make([]breakpoint, 0, len(wanted))
	for i, want := range wanted {
		if want.name == "" {
			answers = append(answers, breakpoint{Message: "a breakpoint here is a step id, and this one is empty"})

			continue
		}
		answer := answerFor(states[functionID(i)])
		answer.ID = s.breakpointID(functionID(i))
		answers = append(answers, answer)
	}
	s.reply(request, breakpointsBody{Breakpoints: answers})
}

func (s *Server) setExceptionBreakpoints(ctx context.Context, request inbound) {
	var asked struct {
		Filters []string `json:"filters"`
	}
	// A missing array is malformed, not a request to clear the filters: only
	// an explicit empty array turns failure stops off.
	if err := json.Unmarshal(request.Arguments, &asked); err != nil || asked.Filters == nil {
		s.fail(request, errInvalidBreakpoints.Error())

		return
	}

	mode := v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
	for _, filter := range asked.Filters {
		switch filter {
		case "all":
			mode = v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL
		case "uncaught":
			if mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL {
				mode = v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT
			}
		default:
			s.fail(request, fmt.Sprintf("flowdap: no exception filter is named %q", filter))

			return
		}
	}

	s.mu.Lock()
	caps := s.capabilities
	previous := s.failureMode
	s.failureMode = mode
	s.mu.Unlock()

	if mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE && caps != nil && !caps.GetFailureBreakpoints() {
		s.mu.Lock()
		s.failureMode = previous
		s.mu.Unlock()
		s.fail(request, "flowdap: this backend cannot stop where a step fails")

		return
	}
	if _, err := s.applyBreakpoints(ctx); err != nil {
		s.mu.Lock()
		s.failureMode = previous
		s.mu.Unlock()
		s.fail(request, err.Error())

		return
	}
	s.reply(request, nil)
}

// maxSourcePathBytes is the longest source path a line breakpoint may name:
// the bound the debug contract puts on a source line's URI, enforced where the
// adapter first keeps the path rather than where the backend later refuses it.
const maxSourcePathBytes = 4096

// errBreakpointBytes fails a request whose breakpoints would take the text the
// adapter keeps past [MaxBreakpointBytes].
var errBreakpointBytes = fmt.Errorf("flowdap: the breakpoints' paths, conditions and log messages may total at most %d bytes", MaxBreakpointBytes)

// retainedBytes is the breakpoint text held for every source but skipPath,
// and for the function breakpoints unless skipFunctions.
func (s *Server) retainedBytes(skipPath string, skipFunctions bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for path, set := range s.lines {
		if path != skipPath {
			total += lineBytes(path, set)
		}
	}
	if !skipFunctions {
		total += functionBytes(s.functions)
	}

	return total
}

// lineBytes is the text a source's line breakpoints hold. The path is
// charged once for the source and again for each breakpoint, whose identity
// carries it.
func lineBytes(path string, set []lineBreakpoint) int {
	total := len(path)
	for _, b := range set {
		total += len(path) + textBytes(b.condition) + textBytes(b.hitCondition) + textBytes(b.logText)
	}

	return total
}

// functionBytes is the text a set of function breakpoints holds.
func functionBytes(set []functionBreakpoint) int {
	total := 0
	for _, b := range set {
		total += len(b.name) + textBytes(b.condition) + textBytes(b.hitCondition)
	}

	return total
}

func textBytes(text *string) int {
	if text == nil {
		return 0
	}

	return len(*text)
}

func (s *Server) totalBreakpoints(path string, lines, functions int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := lines
	for other, set := range s.lines {
		if other != path {
			total += len(set)
		}
	}
	if functions >= 0 {
		return total + functions
	}

	return total + len(s.functions)
}

func lineID(path string, i int) string { return "line:" + path + ":" + strconv.Itoa(i) }

func functionID(i int) string { return "function:" + strconv.Itoa(i) }

// applyBreakpoints sends the whole set — every source's lines, the function
// breakpoints, and the failure mode — as one replacement, which is what the
// target's contract is. It returns each breakpoint's state by id.
func (s *Server) applyBreakpoints(ctx context.Context) (map[string]*v1.DebugBreakpointState, error) {
	s.mu.Lock()
	var set []*v1.DebugBreakpoint
	for path, lines := range s.lines {
		for i, line := range lines {
			set = append(set, &v1.DebugBreakpoint{
				Id:           lineID(path, i),
				Line:         &v1.DebugSourceLine{Uri: path, Line: uint32(line.line)},
				Condition:    deref(line.condition),
				HitCondition: deref(line.hitCondition),
				LogMessage:   deref(line.logText),
			})
		}
	}
	for i, function := range s.functions {
		if function.name == "" {
			continue
		}
		set = append(set, &v1.DebugBreakpoint{
			Id:           functionID(i),
			Step:         function.name,
			Condition:    deref(function.condition),
			HitCondition: deref(function.hitCondition),
		})
	}
	mode := s.failureMode
	target := s.target
	s.mu.Unlock()

	states := map[string]*v1.DebugBreakpointState{}
	if target == nil {
		for _, want := range set {
			states[want.GetId()] = &v1.DebugBreakpointState{Id: want.GetId(), Message: "pending until the program is launched or attached"}
		}

		return states, nil
	}

	if mode == v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED {
		mode = v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
	}
	response, err := target.ReplaceBreakpoints(ctx, &v1.DebugSetBreakpointsRequest{Breakpoints: set, FailureMode: mode})
	if err != nil {
		return nil, err
	}
	if status := response.GetReceipt().GetStatus(); status != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED &&
		status != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSPECIFIED && status != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		return nil, errors.New(receiptText(response.GetReceipt()))
	}
	for i, state := range response.GetBreakpoints() {
		id := state.GetId()
		if id == "" && i < len(set) {
			id = set[i].GetId()
		}
		states[id] = state
	}

	// A replacement the run accepted but will install only at its next step
	// boundary: what it has not applied yet is remembered, so the editor
	// hears when it has rather than showing it unverified for good.
	//
	// Every breakpoint of the replacement is pending, not only those the
	// answer shows unverified: the states the run returned are of the set it
	// has installed, and a verified one under a reused id is the old
	// definition, not this one. One the target refused before sending keeps
	// its reason now, and is settled with the rest.
	pending := map[string]uint32{}
	if response.GetReceipt().GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		for _, want := range set {
			pending[want.GetId()] = want.GetLine().GetLine()
			if states[want.GetId()].GetVerified() {
				states[want.GetId()] = &v1.DebugBreakpointState{Id: want.GetId(), Message: "the run applies this breakpoint at its next step boundary"}
			}
		}
	}
	s.mu.Lock()
	s.pending = pending
	s.pendingAfter = response.GetReceipt().GetRevision()
	s.mu.Unlock()

	return states, nil
}

// relayBreakpoints reports a pending replacement once the run has answered it.
// A durable run installs a replacement whole, at its next step boundary,
// before it holds there, and a hold moves its revision: so a hold past the
// acceptance shows exactly the set the run installed. A run that ends shows
// its final set whatever its revision, which ending need not move. Either
// way each breakpoint of the replacement is there, with its state, or was not
// installed — refused, or never reached. A snapshot of a run still moving
// shows the set it had, and settles nothing.
func (s *Server) relayBreakpoints(snapshot *v1.DebugSnapshot) {
	held := snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD
	ended := terminalState(snapshot.GetState())
	s.mu.Lock()
	if len(s.pending) == 0 || !(ended || held && snapshot.GetRevision() > s.pendingAfter) {
		s.mu.Unlock()

		return
	}
	// A run that ended at the revision it accepted the replacement at may
	// have ended before the boundary that would have installed it, and then
	// still lists its old set under the same slot ids: its states say nothing
	// of the replacement, which is reported not applied rather than read off
	// a definition it replaced.
	installed := map[string]*v1.DebugBreakpointState{}
	unapplied := "the run did not install this breakpoint"
	if ended && snapshot.GetRevision() <= s.pendingAfter {
		unapplied = "the run ended before it applied this breakpoint"
	} else {
		for _, state := range snapshot.GetBreakpoints() {
			installed[state.GetId()] = state
		}
	}
	type applied struct {
		id    string
		line  uint32
		state *v1.DebugBreakpointState
	}
	ready := make([]applied, 0, len(s.pending))
	for id, line := range s.pending {
		state, ok := installed[id]
		if !ok {
			state = &v1.DebugBreakpointState{Id: id, Message: unapplied}
		}
		ready = append(ready, applied{id: id, line: line, state: state})
	}
	s.pending = nil
	s.mu.Unlock()

	lineBase, _ := s.clientBases()
	for _, bp := range ready {
		answer := answerFor(bp.state)
		answer.ID = s.breakpointID(bp.id)
		if bp.line > 0 {
			answer.Line = new(toClient(bp.line, lineBase))
		}
		s.emit("breakpoint", map[string]any{"reason": "changed", "breakpoint": answer})
	}
}

func deref(text *string) string {
	if text == nil {
		return ""
	}

	return *text
}

func answerFor(state *v1.DebugBreakpointState) breakpoint {
	if state == nil {
		return breakpoint{Message: "the backend did not report this breakpoint"}
	}

	return breakpoint{Verified: state.GetVerified(), Message: state.GetMessage()}
}

// refused answers every entry of a request refused whole: none is verified.
func refused(n int, message string) []breakpoint {
	answers := make([]breakpoint, n)
	for i := range answers {
		answers[i] = breakpoint{Message: message}
	}

	return answers
}

// send numbers one outbound message and writes it under one lock, so the
// sequence the client reads is the order the messages were sent in: a number
// taken under one lock and written under another could reach the client
// behind a later one.
func (s *Server) send(message func(seq int) any) {
	if s.hungUp.Load() {
		return
	}
	s.out.Lock()
	defer s.out.Unlock()

	// Again under the lock: a hang-up while this waited for it stands.
	if s.hungUp.Load() {
		return
	}
	s.seq++
	if err := s.stream.WriteObject(message(s.seq)); err != nil {
		// Nobody can hear the session any more: Serve detaches it rather than
		// go on reading from a client whose output is gone.
		s.hungUp.Store(true)
		s.loseOnce.Do(func() { close(s.lost) })
	}
}

// hangUp stops every later write to the client.
//
// It takes no lock, so ending a conversation never waits behind a write that
// is blocked on a client that stopped reading.
func (s *Server) hangUp() { s.hungUp.Store(true) }

func (s *Server) reply(request inbound, body any) {
	s.send(func(seq int) any {
		return response{
			Seq:        seq,
			Type:       "response",
			RequestSeq: request.Seq,
			Success:    true,
			Command:    request.Command,
			Body:       body,
		}
	})
}

func (s *Server) fail(request inbound, message string) {
	s.send(func(seq int) any {
		return response{
			Seq:        seq,
			Type:       "response",
			RequestSeq: request.Seq,
			Success:    false,
			Command:    request.Command,
			Message:    message,
		}
	})
}

func (s *Server) emit(name string, body any) {
	s.send(func(seq int) any {
		return event{
			Seq:   seq,
			Type:  "event",
			Event: name,
			Body:  body,
		}
	})
}
