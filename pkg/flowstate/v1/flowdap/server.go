package flowdap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The one thread a run has. Named rather than repeated, because it appears in
// the thread list, in every stopped event, and as the frame's owner, and three
// literals is three chances to disagree.
const runThreadID = 1

// MaxScopeVariables bounds how many names one `variables` answer renders.
//
// The names themselves are bounded by nothing — [flowdebug.Session.Scope]
// deliberately returns every name a run can reach, because a value surface
// narrower than the run is a worse lie than a long list — and this is the
// surface that turns each of them into a *rendered value*, which costs an
// evaluation apiece. A run inside a long loop can name thousands of steps, and
// an editor asks for a scope's variables every time it repaints a pane.
//
// So the bound is here rather than there, on the work rather than on the
// knowledge, and what is dropped is said out loud in a final entry instead of
// being silently missing.
const MaxScopeVariables = 500

// Server answers DAP requests by driving one [flowdebug.Session].
//
// It holds no debugging state of its own beyond what a client needs addresses
// for: which scope a `variablesReference` names, and the sequence number for
// its own messages. Everything else is asked of the session, so this adapter
// and the prompt cannot come to disagree about where a run is.
type Server struct {
	session *flowdebug.Session

	// mu guards everything below, because a DAP client is free to send while
	// the adapter is emitting an event for something it did earlier — and
	// `stopped` events are emitted from the goroutine that moved the run.
	mu sync.Mutex

	seq int

	// reference numbers variables, counted separately from seq. They are both
	// integers a client sees and they mean unrelated things — one orders the
	// conversation, the other addresses a scope — so sharing a counter would
	// couple the number of messages sent to the addresses handed out, and read
	// as though it meant something.
	reference int

	// scopes maps a variablesReference back to the group it was minted for.
	// Rebuilt at every stop: a reference is only meaningful for the pause it
	// was handed out during, and answering a stale one with the current scope
	// would report the run's position as the answer to a question about a
	// different one.
	scopes map[int]string

	// stream is where responses and events go.
	stream Stream

	// launched reports that configurationDone has been seen, so the run may
	// start. See [Server.Launched].
	launched chan struct{}
	once     sync.Once

	// program is what the client's launch configuration named, read once the
	// launch request arrives and only meaningful after [Server.Launched].
	program string

	// ended guards the terminated/exited pair, because two things can learn
	// the run is over — a movement that meets [flowdebug.ErrRunOver], and
	// whoever owns the run watching it return — and a client told twice puts
	// its session away twice.
	ended sync.Once
}

// NewServer returns a server that drives session over stream.
func NewServer(session *flowdebug.Session, stream Stream) *Server {
	return &Server{
		session:  session,
		stream:   stream,
		scopes:   map[int]string{},
		launched: make(chan struct{}),
	}
}

// Launched is closed once the client has finished configuring — its
// `configurationDone`, or its `launch` where it declared no interest in
// configuring first.
//
// A caller starts the run when this fires and not before. Breakpoints arrive
// *after* launch in DAP's own order, so a run started at the launch request is
// a run already past the step somebody set a breakpoint on, and the person is
// left watching a session that will never stop.
func (s *Server) Launched() <-chan struct{} { return s.launched }

// Program is what the client's launch configuration named, or "" where it
// named nothing.
//
// Read after [Server.Launched] fires: a client sends `launch` before
// `configurationDone`, so by then it is set or was never coming.
func (s *Server) Program() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.program
}

// Output puts text in the client's debug console.
//
// It is where the session's own prose goes, because the adapter's standard
// output is the protocol stream and a debugger that printed onto it would
// corrupt the conversation with its own account of the run.
func (s *Server) Output(text string) {
	if text == "" {
		return
	}

	s.emit("output", map[string]string{"category": "stdout", "output": text})
}

// Finished tells the client the run is over.
//
// Called by whoever owns the run when it returns, because the adapter cannot
// see that for itself: [v1.Debugger] fires before each step and
// [v1.RunObserver] after each one, and neither says "that was the last".
// Idempotent, since a movement outstanding when the run ends learns the same
// thing through [flowdebug.ErrRunOver] and a client told twice puts its session
// away twice.
func (s *Server) Finished() {
	s.ended.Do(func() {
		s.emit("terminated", nil)
		s.emit("exited", exitedBody{})
	})
}

// Serve reads requests until the stream ends or ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var request inbound
		if err := s.stream.ReadObject(&request); err != nil {
			// The client hung up, which is an ordinary end rather than a
			// failure: an editor closing a debug session closes the pipe.
			return nil
		}

		if request.Type != "request" {
			// Responses and events flow the other way. A client that sends one
			// is confused, and answering it would be inventing a conversation.
			continue
		}

		if done := s.dispatch(ctx, request); done {
			return nil
		}
	}
}

// dispatch answers one request, reporting whether the conversation is over.
func (s *Server) dispatch(ctx context.Context, request inbound) (done bool) {
	switch request.Command {
	case "initialize":
		// The `initialized` event is what tells a client it may start sending
		// breakpoints, and it must follow the response rather than precede it.
		s.reply(request, capabilities{
			SupportsFunctionBreakpoints:      true,
			SupportsConfigurationDoneRequest: true,
			SupportsEvaluateForHovers:        true,
		})
		s.emit("initialized", nil)

	case "launch", "attach":
		// What to run, as the client's launch configuration names it. Read
		// here rather than taken from this process's arguments because one
		// adapter serves whatever the editor points it at, which is the whole
		// shape of a `launch.json`.
		var asked struct {
			Program string `json:"program"`
		}
		_ = json.Unmarshal(request.Arguments, &asked)

		s.mu.Lock()
		s.program = asked.Program
		s.mu.Unlock()

		s.reply(request, nil)

	case "configurationDone":
		s.reply(request, nil)
		s.once.Do(func() { close(s.launched) })

	case "setFunctionBreakpoints":
		s.reply(request, s.setBreakpoints(request.Arguments))

	case "setBreakpoints":
		// Answered, and answered honestly. A client sends this for any source
		// it has breakpoints in, and refusing the request outright makes the
		// session look broken; reporting every one unverified with the reason
		// puts the truth where the person is already looking.
		s.reply(request, s.refuseLineBreakpoints(request.Arguments))

	case "threads":
		s.reply(request, threadsBody{Threads: []thread{{ID: runThreadID, Name: "run"}}})

	case "stackTrace":
		s.reply(request, s.stackTrace())

	case "scopes":
		s.reply(request, s.scopeList())

	case "variables":
		s.reply(request, s.variables(ctx, request.Arguments))

	case "evaluate":
		s.evaluate(ctx, request)

	case "next", "stepIn", "stepOut":
		// One granularity: a run's steps are its steps, and there is nothing
		// inside one for a debugger to descend into. Answering stepIn and
		// stepOut as `next` is what a client's buttons then do, rather than
		// leaving two of the three greyed out or silently dead.
		s.reply(request, nil)
		go s.move(ctx, s.session.Step, "step")

	case "continue":
		s.reply(request, map[string]bool{"allThreadsContinued": true})
		go s.move(ctx, s.session.Continue, "breakpoint")

	case "disconnect", "terminate":
		s.reply(request, nil)
		// Closing releases the run to finish rather than leaving it held by a
		// debugger that has gone: an editor closing its debug session must not
		// strand a workflow at a breakpoint.
		_ = s.session.Close()

		return true

	default:
		s.fail(request, fmt.Sprintf("flowdap: %q is not something this adapter answers", request.Command))
	}

	return false
}

// move runs one movement verb and reports where the run stopped.
//
// On its own goroutine, because a DAP request is answered immediately and the
// stop is an *event* that follows — a client that had to wait for the response
// would show a frozen UI for as long as the step takes, and one that timed out
// would give up on a run that was working.
func (s *Server) move(ctx context.Context, step func(context.Context) (flowdebug.Position, error), reason string) {
	at, err := step(ctx)
	if err != nil {
		if errors.Is(err, flowdebug.ErrRunOver) {
			// The run is over, which is not an error to report as one: it is
			// the ordinary end, and `terminated` is how a client learns the
			// session is finished and puts its buttons away.
			s.Finished()

			return
		}

		s.emit("output", map[string]string{
			"category": "stderr",
			"output":   "flowdap: " + err.Error() + "\n",
		})

		return
	}

	s.newStop()
	s.emit("stopped", stoppedBody{
		Reason:            reason,
		Description:       at.Kind,
		ThreadID:          runThreadID,
		AllThreadsStopped: true,
	})
}

// stackTrace is where the run is, as the one frame it has.
func (s *Server) stackTrace() stackTraceBody {
	at, paused := s.session.Paused()
	if !paused {
		// An empty stack rather than a refusal: a client asks for this
		// speculatively, and "the run is not stopped" is exactly what no frames
		// means.
		return stackTraceBody{StackFrames: []stackFrame{}}
	}

	name := at.Step
	switch {
	case at.Autopsy:
		// The run is over and its scope is still readable, which is a real
		// position to be at and one no step id describes.
		name = "after the run"
	case at.Kind != "":
		name = fmt.Sprintf("%s (%s)", at.Step, at.Kind)
	}

	return stackTraceBody{
		StackFrames: []stackFrame{{ID: runThreadID, Name: name}},
		TotalFrames: 1,
	}
}

// scopeList is what the paused run can name, one DAP scope per group.
func (s *Server) scopeList() scopesBody {
	groups, err := s.session.Scope()
	if err != nil {
		return scopesBody{Scopes: []scope{}}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	scopes := make([]scope, 0, len(groups))
	for _, group := range groups {
		// Never zero, which DAP reserves for "this has no children": a scope
		// handed out with reference zero is one a client will not ask about.
		s.reference++
		reference := s.reference
		s.scopes[reference] = group.Group
		scopes = append(scopes, scope{
			Name:               group.Group,
			VariablesReference: reference,
			Expensive:          false,
		})
	}

	return scopesBody{Scopes: scopes}
}

// variables renders one scope's names, each evaluated for its value.
func (s *Server) variables(ctx context.Context, arguments json.RawMessage) variablesBody {
	var asked struct {
		VariablesReference int `json:"variablesReference"`
	}
	_ = json.Unmarshal(arguments, &asked)

	s.mu.Lock()
	group, known := s.scopes[asked.VariablesReference]
	s.mu.Unlock()

	if !known {
		return variablesBody{Variables: []variable{}}
	}

	groups, err := s.session.Scope()
	if err != nil {
		return variablesBody{Variables: []variable{}}
	}

	var names []string
	root := ""
	for _, candidate := range groups {
		if candidate.Group == group {
			names = candidate.Names
			root = rootOf(candidate.Group)

			break
		}
	}

	variables := make([]variable, 0, min(len(names), MaxScopeVariables)+1)
	for i, name := range names {
		if i == MaxScopeVariables {
			variables = append(variables, variable{
				Name:  "…",
				Value: fmt.Sprintf("%d more, not rendered", len(names)-MaxScopeVariables),
			})

			break
		}

		expression := name
		if root != "" {
			expression = root + "." + name
		}

		text, _, evalErr := s.session.Evaluate(ctx, expression)
		if evalErr != nil {
			// The name is real — the run told us so — and only its value could
			// not be produced. Saying so beats dropping the row, which would
			// make the pane disagree with the scope listing beside it.
			text = "(" + evalErr.Error() + ")"
		}

		variables = append(variables, variable{Name: name, Value: text})
	}

	return variablesBody{Variables: variables}
}

// rootOf is the expression prefix a group's names hang from, or "" where they
// are bound bare.
//
// The mapping is the prompt's own: `scope` prints `inspect steps.` beside the
// rooted groups and nothing beside the bare ones, and this is the same fact
// read for a different renderer.
func rootOf(group string) string {
	switch group {
	case "steps":
		return "steps"
	case "inputs":
		return "inputs"
	case "workflow vars":
		return "vars"
	case "run":
		return "run"
	case "trigger":
		return "trigger"
	default:
		// `vars` (a loop's `as:`, a step's own `vars:`) and the autopsy's bare
		// bindings, which resolve under no root at all.
		return ""
	}
}

// evaluate answers the debug console and hover.
func (s *Server) evaluate(ctx context.Context, request inbound) {
	var asked struct {
		Expression string `json:"expression"`
	}
	_ = json.Unmarshal(request.Arguments, &asked)

	text, _, err := s.session.Evaluate(ctx, asked.Expression)
	if err != nil {
		// A failed evaluation is a failed *request* in DAP, which is what puts
		// the message in the console beside what was typed. It is not a failure
		// of the session: somebody asking questions will ask some that do not
		// parse, exactly as at the prompt.
		s.fail(request, err.Error())

		return
	}

	s.reply(request, evaluateBody{Result: text})
}

// setBreakpoints applies a client's function breakpoints, which for this
// adapter are step ids.
func (s *Server) setBreakpoints(arguments json.RawMessage) breakpointsBody {
	var asked struct {
		Breakpoints []struct {
			Name string `json:"name"`
		} `json:"breakpoints"`
	}
	_ = json.Unmarshal(arguments, &asked)

	// Through [flowdebug.Session.SetBreakpoints] and *not* through a command
	// line, which is the difference between this working and deadlocking. A
	// command waits for a boundary to deliver it into, and a client sets
	// breakpoints before the run starts — that is what `configurationDone`
	// orders — so there is no boundary to wait for and never will be one until
	// the breakpoints are in place.
	//
	// It replaces the set for the same reason the method does: a client sends
	// everything it has each time one changes.
	names := make([]string, 0, len(asked.Breakpoints))
	answers := make([]breakpoint, 0, len(asked.Breakpoints))
	for _, want := range asked.Breakpoints {
		name := strings.TrimSpace(want.Name)
		if name == "" {
			answers = append(answers, breakpoint{Message: "a breakpoint here is a step id, and this one is empty"})

			continue
		}

		names = append(names, name)
		answers = append(answers, breakpoint{Verified: true})
	}

	if err := s.session.SetBreakpoints(names); err != nil {
		// The set was refused whole, so no entry may claim to be verified: the
		// alternative is a person watching for stops at breakpoints the session
		// never took.
		return breakpointsBody{Breakpoints: refused(len(asked.Breakpoints), err.Error())}
	}

	return breakpointsBody{Breakpoints: answers}
}

// refuseLineBreakpoints answers a source-line request with the reason it cannot
// be honoured, one entry per breakpoint asked for.
func (s *Server) refuseLineBreakpoints(arguments json.RawMessage) breakpointsBody {
	var asked struct {
		Breakpoints []json.RawMessage `json:"breakpoints"`
	}
	_ = json.Unmarshal(arguments, &asked)

	return breakpointsBody{Breakpoints: refused(len(asked.Breakpoints),
		"this adapter breaks on step ids rather than lines, because the debugger seam is "+
			"handed steps and not files; add a function breakpoint named after the step")}
}

// refused is n unverified breakpoints carrying one reason.
func refused(n int, reason string) []breakpoint {
	answers := make([]breakpoint, 0, n)
	for range n {
		answers = append(answers, breakpoint{Message: reason})
	}

	return answers
}

// newStop forgets the addresses handed out for the pause that just ended.
func (s *Server) newStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.scopes)
}

// reply sends a successful response.
func (s *Server) reply(request inbound, body any) {
	s.send(response{
		Type:       "response",
		RequestSeq: request.Seq,
		Success:    true,
		Command:    request.Command,
		Body:       body,
	})
}

// fail sends an unsuccessful response carrying why.
func (s *Server) fail(request inbound, message string) {
	s.send(response{
		Type:       "response",
		RequestSeq: request.Seq,
		Success:    false,
		Command:    request.Command,
		Message:    message,
	})
}

// emit sends an event.
func (s *Server) emit(name string, body any) {
	s.send(event{Type: "event", Event: name, Body: body})
}

// send stamps a sequence number and writes.
//
// Every outbound message goes through here, so the numbering is one counter
// rather than one per kind — a client is entitled to treat `seq` as increasing
// across everything the adapter says.
func (s *Server) send(message any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	switch stamped := message.(type) {
	case response:
		stamped.Seq = s.seq
		_ = s.stream.WriteObject(stamped)
	case event:
		stamped.Seq = s.seq
		_ = s.stream.WriteObject(stamped)
	}
}
