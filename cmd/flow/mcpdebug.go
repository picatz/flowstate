package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The debugger's MCP front (#928 slice 3): the third adapter over the one
// session core, after the CLI's console and ahead of DAP.
//
// The whole adapter is two translations, and that it *is* only two is the
// point of #928's "one core, three thin fronts": a script becomes the
// session's command stream, and the session's toned output becomes a JSON
// array. Nothing here re-implements stepping, breakpoints, inspection or the
// autopsy — a second session model is exactly the drift the decision refused.
//
// What MCP takes away is the console: no prompt, nobody to type at it, and a
// handler that blocked waiting for one would be a request that never returns.
// What replaces it is the property slice 1 built for replay — commands are a
// *stream*, so a finite script is a complete session. A script that runs out
// mid-run is not a hang either: the session resumes and the run finishes,
// which is [flowdebug.Session]'s own answer to a vanished console.

// maxDebugCommands bounds one script.
//
// A session's own bounds already cover what a *run* can do (MaxBreakpoints,
// MaxScriptCommands) and what one command may weigh (MaxCommandBytes). This
// bounds the argument, which is the part an untrusted caller submits in one
// request: a hundred commands is a long debugging conversation and a small
// document, and the refusal names the number rather than truncating a script
// into a session that answers different questions than the one submitted.
const maxDebugCommands = 100

// maxDebugScriptBytes bounds a script's total size, which the per-command
// bound does not: a hundred commands each just under [flowdebug.MaxCommandBytes]
// is six megabytes of argument, and the answer echoes the accepted script back
// (Codex, #1109). A debugging conversation is questions, not a payload — the
// longest sensible `inspect` is a line — so this is generous at 64 KiB across
// the whole script and still an order of magnitude under what the per-command
// bound alone would have allowed for one command.
const maxDebugScriptBytes = 64 << 10

// maxDebugCaseBytes bounds the `case` argument.
//
// A refusal naming an unknown case quotes what was asked for, which is right —
// a diagnostic that will not say what it did not find is not much of one — and
// that puts a caller-controlled string into an error returned outside the
// answer ladder, where a 300 KiB "name" becomes an oversized result (Codex,
// #1109). Bounded at the door instead of trimmed in the message, because a
// case name is a name: this is two orders of magnitude more than any real one
// and still a rounding error against the cap.
const maxDebugCaseBytes = 256

// maxDebugFragments and maxDebugTranscriptBytes bound the answer at the point
// it is collected, rather than trimming it afterward.
//
// The transcript is the answer here — dropping it to fit would leave a
// document with nothing in it a caller asked for — so it is bounded where the
// bytes appear, the same shape [runLocalLogs] uses for `log:` lines and for
// the same reason: a submitted workflow chooses how much a step's account
// weighs, and a `continue` over a thousand steps is a legal script.
const (
	maxDebugFragments       = 2000
	maxDebugTranscriptBytes = flowmcp.MaxResultBytes / 2
)

// maxDebugFloorPasses bounds the floor rung's re-measuring in
// [renderDebugResult].
//
// The loop it bounds converges by construction — every pass that does not fit
// takes its own overshoot out of the next pass's budget, so the budget falls
// strictly and [renderTestResultWithin]'s three rungs are exhausted long
// before this — but a loop whose termination is an argument about another
// function's ladder is one that a change to that ladder can turn into a hang.
// The count is the bound; the argument is why it is never reached.
const maxDebugFloorPasses = 4

// debugToolArguments is the tool's whole input surface.
type debugToolArguments struct {
	// Workflow is the Flowfile YAML being debugged.
	Workflow string `json:"workflow"`

	// Tests is the `*.test.yaml` document whose case supplies the run.
	Tests string `json:"tests"`

	// Case names which case to debug. Optional where the document declares
	// exactly one, required where it declares more.
	Case string `json:"case"`

	// Commands is the script, in order.
	Commands []string `json:"commands"`
}

// debugFragment is one piece of the session's output, with the tone a
// terminal would have coloured it.
//
// The tone travels as a word rather than being dropped, because it is the
// same distinction a person reads as colour: which line is a stop, which is a
// warning, which is a failure. A model reading a flat transcript has to infer
// that from prose; reading it as data it does not.
type debugFragment struct {
	Text string `json:"text"`
	Tone string `json:"tone"`
}

// debugResult is the document the tool answers with.
type debugResult struct {
	// Session is the transcript, in order.
	Session []debugFragment `json:"session"`

	// Script is what the session accepted, which is the input to a longer
	// session rather than a copy of the argument: a mistyped command is
	// answered and not recorded, so replaying this script re-runs the
	// questions and not the typing mistakes.
	Script []string `json:"script"`

	// Report is the case's ordinary verdict — the same v1.TestReport
	// flowstate_test answers with, because a debugged run is the run.
	Report json.RawMessage `json:"report"`

	// Note says what was dropped, when anything was.
	Note string `json:"note,omitempty"`
}

// debugTranscript collects the session's output under a bound.
type debugTranscript struct {
	fragments []debugFragment
	bytes     int
	seen      int
	dropped   int
}

// add records one fragment, or counts it as dropped.
func (t *debugTranscript) add(text string, tone flowdebug.Tone) {
	t.seen++
	if len(t.fragments) >= maxDebugFragments || t.bytes+len(text) > maxDebugTranscriptBytes {
		t.dropped++

		return
	}
	t.bytes += len(text)
	t.fragments = append(t.fragments, debugFragment{Text: text, Tone: debugToneName(tone)})
}

// note is what the answer says about a bounded transcript.
func (t *debugTranscript) note() string {
	if t.dropped == 0 {
		return ""
	}

	return fmt.Sprintf("%d of this session's %d output fragments were dropped: the transcript "+
		"exceeded %d fragments or %d bytes. Drive the run with a shorter script — `break <step-id>` "+
		"and `continue` reach a step without narrating every one before it",
		t.dropped, t.seen, maxDebugFragments, maxDebugTranscriptBytes)
}

// debugToneName is the wire spelling of a tone.
//
// A switch rather than a Stringer on the type, deliberately: these words are
// this tool's answer schema and a caller may match on them, while
// [flowdebug.Tone] is a rendering hint whose Go-side names belong to the
// package. Naming them here keeps a rename of one from silently becoming a
// change to the other.
func debugToneName(tone flowdebug.Tone) string {
	switch tone {
	case flowdebug.ToneBreak:
		return "break"
	case flowdebug.TonePrompt:
		return "prompt"
	case flowdebug.ToneWarning:
		return "warning"
	case flowdebug.ToneDanger:
		return "danger"
	default:
		return "info"
	}
}

// debugToolHandler drives one case under a scripted session.
//
// timeout is the serving surface's, exactly as [testToolHandler] takes it and
// for the identical reason: a case that never completes is a legal Flowfile,
// and a debug script cannot rescue one — the session only holds at step
// boundaries a run reaches.
func debugToolHandler(timeout time.Duration) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args debugToolArguments

		if raw := req.Params.Arguments; len(raw) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))

			// The mirror of the schema's additionalProperties:false, as every
			// other tool here does it.
			decoder.DisallowUnknownFields()

			if err := decoder.Decode(&args); err != nil {
				return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", flowmcp.DebugToolName, err)), nil
			}
		}

		if err := checkDebugArguments(&args); err != nil {
			return flowmcp.ToolError(err), nil
		}

		// Which case, established by reading the document rather than by
		// hoping — the same check `flow test --debug` makes before it opens a
		// console, and for the same reason: a session drives one run, and a
		// script driving three would be answering about a run it cannot name.
		selected, err := debugCaseSelector([]byte(args.Tests), args.Case)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		transcript := &debugTranscript{}
		session, err := flowdebug.New(flowdebug.Options{
			// The script, as the stream slice 1 already reads. A trailing
			// newline so the last command is a line like every other.
			In:   strings.NewReader(strings.Join(args.Commands, "\n") + "\n"),
			Emit: transcript.add,
		})
		if err != nil {
			return flowmcp.ToolError(err), nil
		}
		// The session's reader outlives this call otherwise: it parks on a
		// send nobody will take, holding the scanner and the script for the
		// life of a process that, on the serving surface, does not exit
		// (Codex, #1109).
		defer func() { _ = session.Close() }()

		// The request's own context is the base on every surface, not just
		// where a timeout is configured. A client that cancels a debug call —
		// an agent that gave up, a closed connection — is asking for the run
		// to stop, and a run rooted at context.Background() would not hear it:
		// `continue` into an unbounded wait would keep a goroutine parked with
		// nobody left to answer (Codex, #1109). The timeout, where a surface
		// sets one, layers on top.
		runCtx := ctx
		if timeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		result := flowtest.RunSourceWith(runCtx, "<submitted>", []byte(args.Workflow), []byte(args.Tests),
			flowtest.RunOptions{Select: selected, Debugger: session})

		// A serving deadline must never read as the workflow's own failure —
		// [testToolHandler] states the whole argument, and it applies
		// identically here: a case that expected failure would pass on a run
		// that never completed.
		if runCtx.Err() != nil {
			return flowmcp.ToolError(fmt.Errorf(
				"the debugged case did not finish within %s and was stopped, so no verdict is "+
					"reported. A debugger cannot rescue a run that never reaches another step: this "+
					"is usually a `wait_for_signal:` with no `timeout:` and no stub scripting its "+
					"signal, which parks the virtual clock with no deadline to advance to", timeout)), nil
		}

		encoded, err := renderDebugResult(result.Report, transcript, session.Script(), session.ScriptTruncated())
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		return &mcp.CallToolResult{
			// The verdict this tool did not change, flagged the way
			// flowstate_test flags it: a model that cannot tell a case that
			// failed from one that passed will report success.
			IsError: testReportFailed(result.Report),
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// checkDebugArguments refuses what cannot be a session, naming the fix.
func checkDebugArguments(args *debugToolArguments) error {
	if strings.TrimSpace(args.Workflow) == "" {
		return errors.New("workflow is required: pass the Flowfile YAML to debug, e.g. " +
			"\"edition: v2026.3\\nname: demo\\nsteps:\\n- id: hi\\n  log:\\n    message: hello\"")
	}
	if strings.TrimSpace(args.Tests) == "" {
		return errors.New("tests is required: a debug session runs a case, so pass a *.test.yaml " +
			"document naming one, e.g. \"tests:\\n  - name: it runs\\n    expect:\\n      failed: false\"")
	}
	if len(args.Case) > maxDebugCaseBytes {
		return fmt.Errorf("the `case` argument is %d bytes and a case name may be at most %d: "+
			"it names one of the cases in the `tests` document, so this is not one of them",
			len(args.Case), maxDebugCaseBytes)
	}
	if len(args.Commands) == 0 {
		return errors.New("commands is required: pass the script that drives the session, e.g. " +
			"[\"step\", \"inspect steps.hi\", \"continue\"]. An empty script would run the case " +
			"unattended, which is what " + flowmcp.TestToolName + " already does")
	}
	if len(args.Commands) > maxDebugCommands {
		return fmt.Errorf("a session takes at most %d commands and this script has %d; "+
			"submit the first %d, read the transcript, and send the rest with the answers in hand "+
			"— which is what a debugging conversation looks like anyway",
			maxDebugCommands, len(args.Commands), maxDebugCommands)
	}
	total := 0
	for _, command := range args.Commands {
		total += len(command)
	}
	if total > maxDebugScriptBytes {
		return fmt.Errorf("this script is %d bytes and a session takes at most %d across all of its "+
			"commands: the answer echoes the script back, so a script this large is an answer nobody "+
			"can read. Ask shorter questions, or compute the value in the file",
			total, maxDebugScriptBytes)
	}
	for i, command := range args.Commands {
		if len(command) > flowdebug.MaxCommandBytes {
			return fmt.Errorf("commands[%d] is %d bytes, and a command may be at most %d: an "+
				"expression that long is one to compute in the file rather than at a prompt",
				i, len(command), flowdebug.MaxCommandBytes)
		}
		if strings.ContainsAny(command, "\n\r") {
			return fmt.Errorf("commands[%d] contains a line break, and each entry is one command: "+
				"split it into separate entries, which is the order they run in", i)
		}
	}

	return nil
}

// debugCaseSelector decides which case the session drives.
//
// It loads the document a second time — the run loads it again itself — for
// the reason `flow test --debug` pays the same cost: a refusal that names the
// case count is worth more than a session that silently debugs the first of
// three. Loading is cheap and bounded ([flowtest.LoadSource]); guessing is
// not recoverable.
func debugCaseSelector(testSource []byte, name string) (func(string) bool, error) {
	file, err := flowtest.LoadSource(testSource)
	if err != nil {
		// Deliberately not this function's refusal to make: the run loads the
		// same document and answers with a v1.TestReport whose `refused`
		// carries this error, which is the shape a caller of flowstate_test
		// already handles. Refusing here as well would give one mistake two
		// spellings, and the selector has nothing to say about a document
		// that has no cases to select from.
		return nil, nil
	}

	names := make([]string, 0, len(file.Tests))
	for _, test := range file.Tests {
		names = append(names, test.Name)
	}

	if name != "" {
		// Counted rather than found, because a match is not a selection. The
		// predicate this returns is asked about a *name*, so it matches every
		// case carrying that name — and nothing in [flowtest.LoadSource]
		// requires names to be unique, so two same-named cases both run,
		// against one command stream: the first consumes the script and the
		// second is debugged by an exhausted session, under a transcript that
		// names one case twice with no way to tell which said what (Codex,
		// #1109). A session drives one run; this is the same refusal the
		// count below makes, for the same reason.
		matches := 0
		for _, have := range names {
			if have == name {
				matches++
			}
		}

		switch matches {
		case 1:
			return func(candidate string) bool { return candidate == name }, nil

		case 0:
			return nil, fmt.Errorf("no case is named %q; this document declares %s",
				name, quotedList(names))

		default:
			return nil, fmt.Errorf("%d of this document's cases are named %q, and a session drives "+
				"one run: a script sent to both would be consumed by the first, and the transcript "+
				"could not say which case answered what. Give them distinct names",
				matches, name)
		}
	}

	if len(names) != 1 {
		return nil, fmt.Errorf("a session drives one run, and this document declares %d cases: %s. "+
			"Name one with `case`", len(names), quotedList(names))
	}

	return nil, nil
}

// renderDebugResult assembles the answer and brings it under the cap.
//
// Measured on the rendered document, not on its parts. Each part is bounded —
// the transcript where it is collected, the report by [renderTestResult]'s own
// ladder, the script by [maxDebugScriptBytes] — and three bounded parts still
// add up to more than the cap, which is the arithmetic the first cut got wrong
// (Codex, #1109). It is also the same lesson the run_local ladder learned one
// slice earlier: JSON escaping expands a control-heavy transcript past whatever
// its raw bytes said, so only the encoded length is the length.
//
// The rungs are ordered by what a caller can most afford to lose, and the first
// two are cheap for a reason worth stating: the script is the caller's own
// input coming back, and the transcript's oldest fragments are the ones a
// caller has usually already read in an earlier call.
//
// It takes the script and the truncation flag rather than the session they
// came from, so the arithmetic above can be tested for what it is — three
// bounded parts against one cap — without standing up a run whose parser
// bounds fight the fixture. Assembling an answer is not something a session
// should have to exist for.
func renderDebugResult(report *v1.TestReport, transcript *debugTranscript, script []string, scriptTruncated bool) ([]byte, error) {
	encodedReport, err := renderTestResult(report)
	if err != nil {
		return nil, err
	}

	note := transcript.note()
	if scriptTruncated {
		note = strings.TrimSpace(note + fmt.Sprintf(
			" This session accepted more than %d commands, so the script above is a prefix of what ran.",
			flowdebug.MaxScriptCommands))
	}

	answer := debugResult{
		Session: transcript.fragments,
		Script:  script,
		Report:  json.RawMessage(encodedReport),
		Note:    note,
	}

	encode := func(a debugResult) ([]byte, error) {
		encoded, err := json.Marshal(a)
		if err != nil {
			return nil, fmt.Errorf("rendering the session: %w", err)
		}

		return encoded, nil
	}

	// Appended to the answer itself rather than to a copy of it, so a document
	// that lost two things says both: the rungs are cumulative, and a note
	// that lived only in the value one rung encoded left the next rung
	// reporting the transcript reduction while silently omitting the script it
	// had already dropped (Codex, #1109).
	addNote := func(said string) {
		answer.Note = strings.TrimSpace(strings.TrimSpace(answer.Note) + " " + said)
	}

	encoded, _, err := flowmcp.FitResult(
		func() ([]byte, error) { return encode(answer) },

		// First the script: it is what the caller sent, so it is the one part
		// of this document they already have.
		func() ([]byte, error) {
			answer.Script = nil
			addNote(fmt.Sprintf(
				"The accepted script was dropped: the answer exceeded %d bytes. It was the commands "+
					"you sent, in the order they were accepted.", flowmcp.MaxResultBytes))

			return encode(answer)
		},

		// Then the transcript's front. The tail is what a debugging call is
		// for — the last answers, and the autopsy if the case failed — so the
		// oldest fragments go first and the count says how many.
		func() ([]byte, error) {
			kept := len(answer.Session) / 4
			dropped := len(answer.Session) - kept
			answer.Session = answer.Session[dropped:]
			addNote(fmt.Sprintf(
				"The first %d transcript fragments were dropped, keeping the most recent %d: the "+
					"answer exceeded %d bytes.", dropped, kept, flowmcp.MaxResultBytes))

			return encode(answer)
		},

		// The floor: the verdict, which is the one thing this call shares with
		// the flowstate_test call a caller could have made instead. Returned
		// whether or not it fits, which is [flowmcp.FitResult]'s contract for a
		// last rung.
		//
		// The report is re-rendered here rather than reused, because the one
		// above was fitted to the whole cap and this document is not only a
		// report. A case whose diagnostics render just under the cap survives
		// every rung above untouched — nothing here can shrink a report — and
		// the floor then adds the object, its keys and the notes saying what
		// left, so the floor came back oversized *by construction* while
		// reporting itself as the answer (Codex, #1109). What the wrapper
		// costs has to come out of the report's budget.
		//
		// Converged rather than computed, and the difference is not fussiness:
		// what the report costs *here* is not what it measured over there.
		// [json.Marshal] compacts a json.RawMessage and escapes `<`, `>` and
		// `&` inside it, and protojson's own spacing is not fixed across
		// processes — so the report shrinks by a few hundred bytes in one run
		// and grows in another, from the same report. A reserve computed from
		// the wrapper alone is a prediction about all of that. Measuring the
		// finished document is not a prediction, and this file already holds
		// the lesson: only the encoded length is the length.
		//
		// Each pass takes the real overshoot back out of the report's budget,
		// so the budget strictly falls and the report ladder's own floor —
		// per-case verdicts — is reached in two or three. That floor bounds
		// every string it keeps by a share of the budget it was handed, so a
		// smaller budget really does produce a smaller report; before it did,
		// a refusal quoting a megabyte of submitted document was carried whole
		// by every rung and this loop had nothing to converge on (Codex,
		// #1109). The bound is on passes rather than on convergence all the
		// same, because a loop whose termination is an argument about another
		// function's ladder is one a change to that ladder can turn into a
		// hang — see [maxDebugFloorPasses].
		func() ([]byte, error) {
			answer.Session = nil
			addNote(fmt.Sprintf(
				"The transcript was dropped entirely: the answer exceeded %d bytes even reduced. "+
					"Drive the run with a shorter script — `break <step-id>` and `continue` reach a "+
					"step without narrating every one before it.", flowmcp.MaxResultBytes))

			budget := flowmcp.MaxResultBytes

			var encoded []byte

			for pass := 0; pass < maxDebugFloorPasses; pass++ {
				reduced, err := renderTestResultWithin(report, budget)
				if err != nil {
					return nil, err
				}
				answer.Report = json.RawMessage(reduced)

				encoded, err = encode(answer)
				if err != nil {
					return nil, err
				}

				over := len(encoded) - flowmcp.MaxResultBytes
				if over <= 0 {
					return encoded, nil
				}

				budget -= over
				if budget < 1 {
					break
				}
			}

			return encoded, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}
