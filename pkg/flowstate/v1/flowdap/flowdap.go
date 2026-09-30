// Package flowdap speaks the Debug Adapter Protocol over a
// [flowdebug.Target], so an editor's step and continue buttons drive a real
// flowstate run: a local [flowdebug.Session] the adapter launched, or a durable
// run reached through [flowdebug.Remote].
//
// # A translation, nothing else
//
// Every DAP request is one call into the target's typed contract — its
// snapshot, its breakpoint set, its movements and its value reads — which is
// the same contract the terminal debugger, the CLI and the MCP tools drive.
// This package holds no idea about stepping, no breakpoint semantics and no
// scope of its own: a second implementation of any of those would be free to
// disagree with the one people type at, which is this repository's
// most-paid-for shape.
//
// # Where source positions come from
//
// A step carries an `id` and no position, and so does the compiled program a
// run executes. Lines come from a [v1.DebugSourceMap] built from the bytes the
// launch compiled, bound to the program by its IR digest. With one, line
// breakpoints (`setBreakpoints`) resolve to the step sites on that line, and
// stack frames name their file and line. Function breakpoints are addressed by
// step address (`build`, `pages/page`, `pages[2]/page`) and need no map.
//
// A durable attach has no map. The IR carries no positions, so a local file
// whose lines moved since the run was submitted compiles to the same digest and
// would put frames and breakpoints on the wrong lines; the run records no
// digest of its source to check a file against. An attached session therefore
// shows step addresses, answers line breakpoints unverified, and narrows the
// capabilities it offers (no logpoints, no failure stops) with a capabilities
// event once it knows the backend.
//
// # One thread, deliberately
//
// A run has one position — that is [flowdebug.Session]'s own contract, and why
// its movement commands serialize. So this reports exactly one thread. The
// local driver runs a `parallel:` block's branches one at a time, so a stop
// inside a branch is still the run's one position, and a second thread would
// be a fiction the run cannot back.
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
	SupportsConfigurationDoneRequest  bool              `json:"supportsConfigurationDoneRequest"`
	SupportsFunctionBreakpoints       bool              `json:"supportsFunctionBreakpoints"`
	SupportsConditionalBreakpoints    bool              `json:"supportsConditionalBreakpoints"`
	SupportsHitConditionalBreakpoints bool              `json:"supportsHitConditionalBreakpoints"`
	SupportsLogPoints                 bool              `json:"supportsLogPoints"`
	SupportsEvaluateForHovers         bool              `json:"supportsEvaluateForHovers"`
	SupportsTerminateRequest          bool              `json:"supportsTerminateRequest"`
	SupportTerminateDebuggee          bool              `json:"supportTerminateDebuggee"`
	SupportsDelayedStackTraceLoading  bool              `json:"supportsDelayedStackTraceLoading"`
	ExceptionBreakpointFilters        []exceptionFilter `json:"exceptionBreakpointFilters"`
}

// exceptionFilter is one kind of failure stop an editor offers as a checkbox.
type exceptionFilter struct {
	Filter      string `json:"filter"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

// source names a document the way an editor opens it.
type source struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type stoppedBody struct {
	Reason            string `json:"reason"`
	Description       string `json:"description,omitempty"`
	Text              string `json:"text,omitempty"`
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

	// Source is where the frame's step is written, when a verified source map
	// says so.
	Source *source `json:"source,omitempty"`

	// PresentationHint is "subtle" for a container frame: a loop iteration, a
	// parallel branch, a switch arm or a call the stop is inside.
	PresentationHint string `json:"presentationHint,omitempty"`
}

type stackTraceBody struct {
	StackFrames []stackFrame `json:"stackFrames"`
	TotalFrames int          `json:"totalFrames"`
}

type scope struct {
	Name               string `json:"name"`
	VariablesReference int    `json:"variablesReference"`
	NamedVariables     int    `json:"namedVariables,omitempty"`
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

	// Type is the value's CEL type.
	Type string `json:"type,omitempty"`

	// EvaluateName is the expression that reads this value again.
	EvaluateName string `json:"evaluateName,omitempty"`
}

type variablesBody struct {
	Variables []variable `json:"variables"`
}

type evaluateBody struct {
	Result             string `json:"result"`
	Type               string `json:"type,omitempty"`
	VariablesReference int    `json:"variablesReference"`
}

type breakpoint struct {
	// ID is the number an editor knows the breakpoint by across updates.
	ID int `json:"id,omitempty"`

	// Verified says the session took it.
	//
	// It is not a claim that the run will reach it, and cannot be: breakpoints
	// are set before the run starts, and nothing this adapter holds knows what
	// steps a workflow has. A session given [flowdebug.Options.Steps] reports an
	// id it does not declare, which comes back unverified with the reason; the
	// session `flow dap` builds has no such inventory, so there a step id that
	// is simply misspelled verifies and never stops anything, which is the cost
	// of setting breakpoints early enough to be useful. Saying otherwise would be a promise made by the one
	// component with no way to check it; naming the steps is what the source
	// mapping in this package's second slice would buy.
	//
	// What it does mean is that the session is holding it. A set refused whole
	// — too many, or a name it will not take — verifies nothing, because a
	// breakpoint that looks set and was never taken is a person waiting for a
	// stop that cannot come.
	Verified bool   `json:"verified"`
	Message  string `json:"message,omitempty"`

	// Line is the line a source breakpoint was set on.
	// A pointer so a zero-based client's first line is sent, not omitted.
	Line *int `json:"line,omitempty"`
}

type breakpointsBody struct {
	Breakpoints []breakpoint `json:"breakpoints"`
}
