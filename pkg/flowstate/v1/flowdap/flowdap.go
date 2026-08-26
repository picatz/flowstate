// Package flowdap speaks the Debug Adapter Protocol over a paused
// [flowdebug.Session], so an editor's step and continue buttons drive a real
// flowstate run.
//
// # Why this can exist now
//
// It could not before. A session's whole control surface was a *line of text*
// and the run parks inside the debugger blocked reading one, so an adapter had
// nobody to type: it would have had to compose command strings and parse the
// human-readable output back. [flowdebug.Session.Control] and the value surface
// beside it are what this maps onto, and every DAP request below is one call
// into them.
//
// That is the whole design. This package holds no idea about stepping, no
// breakpoint set, no scope of its own — a second implementation of any of those
// would be free to disagree with the one people type at, which is this
// repository's most-paid-for shape. It is a translation and nothing else.
//
// # What the seam cannot say, and what that costs
//
// [v1.Debugger] is handed a [v1.Node], and a node carries an `id` and no source
// position. Neither does anything else the session sees: it is given steps, not
// files. Two consequences, both visible to a person in an editor, both stated
// here rather than discovered:
//
//   - **Breakpoints are by step id, not by line.** DAP's `setBreakpoints` is
//     addressed by source line, which this cannot honour without inventing a
//     mapping it has no basis for. `setFunctionBreakpoints` is addressed by
//     *name*, and a step id is a name, so that is the request this answers.
//     In VS Code they appear under the Breakpoints view's function-breakpoint
//     section rather than as red dots in the gutter.
//   - **Stack frames carry no source.** A client shows the frame's name and
//     cannot navigate to it.
//
// Both are answerable by parsing the workflow the run is executing and mapping
// step ids to the positions the parser already records for diagnostics. That is
// a separate slice: it needs the file, which only a launch configuration knows,
// and it is a different kind of work from speaking the protocol. Doing it here
// would mean guessing at a position when the parse and the run disagree, which
// is worse than admitting there is none.
//
// # One thread, deliberately
//
// A run has one position — that is [flowdebug.Session]'s own contract, and why
// its movement commands serialize. So this reports exactly one thread. A
// `parallel:` block genuinely runs several steps at once and the debugger
// deliberately does not stop inside one (the engine refuses to suspend there
// for the same reason), so a second thread would be a fiction the run cannot
// back.
package flowdap

import (
	"encoding/json"
)

// Stream is the framed transport a client speaks over: DAP frames its messages
// exactly as LSP does, `Content-Length` and a JSON body.
//
// An interface rather than a concrete reader, because the bounded framing this
// repository already owns lives in the language server's package
// (`lsp.NewBoundedStream`, whose doc records the 512 MiB an unbounded header
// parse cost when it was measured). A second framer here would be a second
// place to get that bound wrong; taking the stream as a parameter lets the
// command hand over the one that is already bounded and fuzzed, and keeps this
// package from depending on the Flowfile language server to speak a protocol
// that has nothing to do with Flowfiles.
type Stream interface {
	ReadObject(v any) error
	WriteObject(v any) error
	Close() error
}

// inbound is a message read from the client.
//
// Read and written through separate types on purpose. A response must carry
// `success` even when it is false, and a single struct with `omitempty` on that
// field would silently drop it from every failure — a client would see a
// response with no verdict in it. Splitting the direction makes that
// unrepresentable rather than a tag somebody has to keep right.
type inbound struct {
	Seq       int             `json:"seq"`
	Type      string          `json:"type"`
	Command   string          `json:"command"`
	Arguments json.RawMessage `json:"arguments"`
}

// response is a reply to one request.
type response struct {
	Seq        int    `json:"seq"`
	Type       string `json:"type"`
	RequestSeq int    `json:"request_seq"`
	Success    bool   `json:"success"`
	Command    string `json:"command"`
	Message    string `json:"message,omitempty"`
	Body       any    `json:"body,omitempty"`
}

// event is something the adapter says without being asked.
type event struct {
	Seq   int    `json:"seq"`
	Type  string `json:"type"`
	Event string `json:"event"`
	Body  any    `json:"body,omitempty"`
}

// The bodies this adapter sends. Only the fields it fills are here: a struct
// mirroring the whole specification would be mostly zero values, and every one
// of them is a claim to a client about what this adapter supports.

// capabilities is the initialize response body.
type capabilities struct {
	// Breakpoints by name, because a step id is a name and the seam has no
	// lines. See the package comment.
	SupportsFunctionBreakpoints bool `json:"supportsFunctionBreakpoints"`

	// Told about configurationDone, so breakpoints set before the run starts
	// are in place when it does. Without it a client launches and the run is
	// already past the step somebody meant to stop at.
	SupportsConfigurationDoneRequest bool `json:"supportsConfigurationDoneRequest"`

	// `evaluate` answers in the REPL and on hover, which is the debug console
	// this package exists to make useful.
	SupportsEvaluateForHovers bool `json:"supportsEvaluateForHovers"`
}

type stoppedBody struct {
	Reason            string `json:"reason"`
	Description       string `json:"description,omitempty"`
	ThreadID          int    `json:"threadId"`
	AllThreadsStopped bool   `json:"allThreadsStopped"`
}

type exitedBody struct {
	ExitCode int `json:"exitCode"`
}

type thread struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type threadsBody struct {
	Threads []thread `json:"threads"`
}

type stackFrame struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// Zero, and sent rather than omitted: the specification requires both, and
	// a client reading a missing line as line 1 would point at the wrong place
	// with more confidence than "nowhere".
	Line   int `json:"line"`
	Column int `json:"column"`
}

type stackTraceBody struct {
	StackFrames []stackFrame `json:"stackFrames"`
	TotalFrames int          `json:"totalFrames"`
}

type scope struct {
	Name               string `json:"name"`
	VariablesReference int    `json:"variablesReference"`
	Expensive          bool   `json:"expensive"`
}

type scopesBody struct {
	Scopes []scope `json:"scopes"`
}

type variable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	// Always zero: every value this adapter reports is rendered, and a non-zero
	// reference is a promise that `variables` will expand it. Handing out a
	// reference this adapter would then refuse is worse than a flat value.
	VariablesReference int `json:"variablesReference"`
}

type variablesBody struct {
	Variables []variable `json:"variables"`
}

type evaluateBody struct {
	Result             string `json:"result"`
	VariablesReference int    `json:"variablesReference"`
}

type breakpoint struct {
	// Verified says the session took it.
	//
	// It is not a claim that the run will reach it, and cannot be: breakpoints
	// are set before the run starts, and nothing this adapter holds knows what
	// steps a workflow has — the session is handed each node as the engine
	// reaches it. So a step id that is simply misspelled verifies here and
	// never stops anything, which is the cost of setting breakpoints early
	// enough to be useful. Saying otherwise would be a promise made by the one
	// component with no way to check it; naming the steps is what the source
	// mapping in this package's second slice would buy.
	//
	// What it does mean is that the session is holding it. A set refused whole
	// — too many, or a name it will not take — verifies nothing, because a
	// breakpoint that looks set and was never taken is a person waiting for a
	// stop that cannot come.
	Verified bool   `json:"verified"`
	Message  string `json:"message,omitempty"`
}

type breakpointsBody struct {
	Breakpoints []breakpoint `json:"breakpoints"`
}
