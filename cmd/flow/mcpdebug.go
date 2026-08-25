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

		runCtx := context.Background()
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

		encoded, err := renderDebugResult(result.Report, transcript, session)
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
		for _, have := range names {
			if have == name {
				return func(candidate string) bool { return candidate == name }, nil
			}
		}

		return nil, fmt.Errorf("no case is named %q; this document declares %s",
			name, quotedList(names))
	}

	if len(names) != 1 {
		return nil, fmt.Errorf("a session drives one run, and this document declares %d cases: %s. "+
			"Name one with `case`", len(names), quotedList(names))
	}

	return nil, nil
}

// renderDebugResult assembles the answer and brings it under the cap.
//
// The transcript is already bounded at collection, so the ladder here is the
// report's: the same [renderTestResult] rungs flowstate_test settles on,
// reused rather than rewritten — this document embeds that one.
func renderDebugResult(report *v1.TestReport, transcript *debugTranscript, session *flowdebug.Session) ([]byte, error) {
	encodedReport, err := renderTestResult(report)
	if err != nil {
		return nil, err
	}

	script := session.Script()
	note := transcript.note()
	if session.ScriptTruncated() {
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

	encoded, err := json.Marshal(answer)
	if err != nil {
		return nil, fmt.Errorf("rendering the session: %w", err)
	}

	return encoded, nil
}
