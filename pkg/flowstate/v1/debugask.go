package flowstatev1

import (
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
)

// The typed durable debug protocol (#2126): the asks `DebugAttach`,
// `DebugResume` and `DebugSetBreakpoints` deliver on [DebugSignal].
//
// They travel on the one reserved channel the lease mechanics already read, so
// ordering still comes from one FIFO in history, and they are told apart from
// the earlier identity-fenced asks by the session they carry: an ask with a
// [DebugSessionInput] is typed, and every command it carries is fenced by that
// session, by the attested sender, and — for a resume — by the revision the
// caller last saw. An ask without one keeps the legacy semantics exactly, so a
// history recorded before this protocol replays unchanged.

const (
	// DebugProtocol is the durable debug protocol version this build's
	// interpreter speaks, reported by [DebugQuery]. Version 1 was the
	// identity-fenced pause and resume; version 2 adds sessions, receipts,
	// stepping, breakpoints, and inspection.
	DebugProtocol = 2

	// DebugQuery is the query a run answers its debug state on.
	DebugQuery = "flowstate.debug"

	// DebugInspectQuery is the query a held run answers an inspection on.
	DebugInspectQuery = "flowstate.debug.inspect"

	// DebugSessionInput names the session a typed ask is fenced by.
	DebugSessionInput = "session"

	// DebugRequestInput names the request id a typed ask is receipted under.
	DebugRequestInput = "request"

	// DebugRevisionInput is the revision a resume expects the session to be
	// at.
	DebugRevisionInput = "revision"

	// DebugActionInput is a resume's [DebugResumeAction], by name.
	DebugActionInput = "action"

	// DebugUntilInput is a run-until's target.
	DebugUntilInput = "until"

	// DebugBreakpointsInput is a breakpoint replacement, as the protobuf JSON
	// of a [DebugSetBreakpointsRequest] carrying only its breakpoints and
	// failure mode.
	DebugBreakpointsInput = "breakpoints"

	// DebugVerbRenew extends a session's lease without asking for a hold.
	DebugVerbRenew = "renew"

	// DebugVerbBreakpoints replaces a session's breakpoints.
	DebugVerbBreakpoints = "breakpoints"
)

// DebugConditionHeader is the response header a debug RPC's refusal names its
// condition in, so a client tells a stale snapshot from any other failed
// precondition by value rather than by reading the message.
const DebugConditionHeader = "Flowstate-Debug-Condition"

// DebugConditionStale is [DebugConditionHeader]'s value for a question about
// a revision the session has left.
const DebugConditionStale = "stale"

// MaxDebugReceipts bounds the receipts a run keeps for retries.
const MaxDebugReceipts = 64

// The bounds a typed ask is read under, the schema's own for the fields a
// receipt keeps: a run carries its receipts across Continue-As-New, so what one
// may hold is decided where the run reads it, not only where an RPC validated
// it. With [MaxDebugReceipts] they bound a run's receipts to under 80 KiB:
// 64 × (128 + 1024) bytes of id and message, plus a few bytes of framing each.
const (
	MaxDebugSessionIDBytes      = 256
	MaxDebugRequestIDBytes      = 128
	MaxDebugReceiptMessageBytes = 1024
)

// DebugAsk is one typed debug ask.
type DebugAsk struct {
	Verb        string
	Session     string
	Request     string
	Revision    uint64
	Lease       time.Duration
	Action      DebugResumeAction
	Until       string
	Breakpoints *DebugSetBreakpointsRequest
}

// NewTypedDebugAsk encodes a typed ask as the payload [DebugSignal] carries.
func NewTypedDebugAsk(ask *DebugAsk) (*Node_Outputs, error) {
	if ask.Session == "" || ask.Request == "" {
		return nil, errors.New("a typed debug ask names its session and its request")
	}

	values := map[string]*Value{
		DebugVerbInput:    NewLiteral(ask.Verb),
		DebugSessionInput: NewLiteral(ask.Session),
		DebugRequestInput: NewLiteral(ask.Request),
	}
	if ask.Lease > 0 {
		values[DebugLeaseInput] = NewLiteral(ask.Lease.String())
	}
	if ask.Revision > 0 {
		values[DebugRevisionInput] = NewLiteral(strconv.FormatUint(ask.Revision, 10))
	}
	if ask.Action != DebugResumeAction_DEBUG_RESUME_ACTION_UNSPECIFIED {
		values[DebugActionInput] = NewLiteral(ask.Action.String())
	}
	if ask.Until != "" {
		values[DebugUntilInput] = NewLiteral(ask.Until)
	}
	if ask.Breakpoints != nil {
		encoded, err := protojson.Marshal(&DebugSetBreakpointsRequest{
			Breakpoints: ask.Breakpoints.GetBreakpoints(),
			FailureMode: ask.Breakpoints.GetFailureMode(),
		})
		if err != nil {
			return nil, fmt.Errorf("encoding breakpoints: %w", err)
		}
		values[DebugBreakpointsInput] = NewLiteral(string(encoded))
	}

	return &Node_Outputs{NamedValues: values}, nil
}

// ParseTypedDebugAsk reads a typed ask out of a [DebugSignal] payload. It
// reports false for a legacy ask, which carries no session. A typed ask that
// cannot be read is an error, and the engine receipts it as refused rather
// than guessing at what it meant.
func ParseTypedDebugAsk(payload *Node_Outputs) (*DebugAsk, bool, error) {
	values := payload.GetNamedValues()
	text := func(name string) string { return values[name].GetLiteral().GetStringValue() }

	session := text(DebugSessionInput)
	if session == "" {
		return nil, false, nil
	}
	if len(session) > MaxDebugSessionIDBytes || len(text(DebugRequestInput)) > MaxDebugRequestIDBytes {
		// Typed, and refused with nothing to answer under: a request id this
		// long is not one the run will keep a receipt for.
		return &DebugAsk{Session: truncateBytes(session, MaxDebugSessionIDBytes)}, true,
			fmt.Errorf("a typed debug ask's session id may be %d bytes and its request id %d", MaxDebugSessionIDBytes, MaxDebugRequestIDBytes)
	}

	ask := &DebugAsk{
		Verb:    text(DebugVerbInput),
		Session: session,
		Request: text(DebugRequestInput),
		Until:   text(DebugUntilInput),
		Lease:   DebugLeaseRequested(payload),
	}
	if ask.Request == "" {
		return ask, true, errors.New("a typed debug ask carries no request id")
	}
	if revision := text(DebugRevisionInput); revision != "" {
		parsed, err := strconv.ParseUint(revision, 10, 64)
		if err != nil {
			return ask, true, fmt.Errorf("revision %q is not a number", revision)
		}
		ask.Revision = parsed
	}
	if action := text(DebugActionInput); action != "" {
		parsed, ok := DebugResumeAction_value[action]
		if !ok || parsed == 0 {
			return ask, true, fmt.Errorf("resume action %q is not one this build knows", action)
		}
		ask.Action = DebugResumeAction(parsed)
	}
	if encoded := text(DebugBreakpointsInput); encoded != "" {
		breakpoints := &DebugSetBreakpointsRequest{}
		if err := protojson.Unmarshal([]byte(encoded), breakpoints); err != nil {
			return ask, true, fmt.Errorf("breakpoints: %w", err)
		}
		ask.Breakpoints = breakpoints
	}

	switch ask.Verb {
	case DebugVerbPause, DebugVerbRenew, DebugVerbBreakpoints:
	case DebugVerbResume:
		// A resume says how to resume. One that does not is malformed, and a
		// held run is never let go on a guess at what it meant.
		if ask.Action == DebugResumeAction_DEBUG_RESUME_ACTION_UNSPECIFIED {
			return ask, true, errors.New("a typed resume names no action")
		}
	default:
		return ask, true, fmt.Errorf("debug verb %q is not one this build knows", ask.Verb)
	}

	return ask, true, nil
}

// DurableDebugCapabilities is what the durable driver does.
//
// It holds only where a run has one position — the top level of the run and of
// a called workflow, and inside a `loop:`, a `switch:` arm and a `for_each:`
// running one iteration at a time — so step-in reaches a callee's steps and a
// body's, but never a `parallel:` branch or a concurrent `for_each:`, which
// run as a unit durably. Failure
// stops and logpoints are local-only: the durable driver has no place to hold
// a run after a failure is recorded, and a logpoint's expressions would be a
// second, unaudited inspection channel.
func DurableDebugCapabilities() *DebugCapabilities {
	return &DebugCapabilities{
		StepIn:                 true,
		StepOver:               true,
		StepOut:                true,
		Pause:                  true,
		RunUntil:               true,
		ConditionalBreakpoints: true,
		HitConditions:          true,
		Inspect:                true,
		ValueExpansion:         true,
		Observations:           true,
	}
}

// truncateBytes cuts text to at most limit bytes on a rune boundary.
func truncateBytes(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}

	return text[:cut]
}

// TruncateDebugReceiptMessage cuts message to [MaxDebugReceiptMessageBytes] on
// a rune boundary, ending in an elision when anything was cut.
func TruncateDebugReceiptMessage(message string) string {
	if len(message) <= MaxDebugReceiptMessageBytes {
		return message
	}
	const elision = "…"

	return truncateBytes(message, MaxDebugReceiptMessageBytes-len(elision)) + elision
}
