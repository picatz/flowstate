// Package hook reads and writes the Claude Code hook contract for the three
// guards under tools/hooks (#482).
//
// A hook receives one JSON document on stdin. The fields these guards read,
// per https://code.claude.com/docs/en/hooks:
//
//	hook_event_name  "PreToolUse" or "PostToolUse"
//	tool_name        "Edit", "Write", "Bash", ...
//	cwd              the session's working directory
//	tool_input       the tool's arguments: file_path for Edit/Write,
//	                 command for Bash
//
// A PreToolUse hook refuses a call by printing a permissionDecision of
// "deny" (Deny); a PostToolUse hook informs without blocking by printing
// additionalContext (Advise). Both exit 0; exit codes are left out of the
// contract on purpose, because exit 2 blocks unconditionally and these
// guards want their reasons read as structured output.
//
// Parsing is deliberately lenient, and lenient means silent: these guards
// are advisory, so an input the parser does not recognize allows rather
// than blocks. They are not a security boundary (a session that can edit
// files can edit anything); they encode habits from CLAUDE.md at the moment
// a mistake is cheapest to undo, with a message naming the right move.
package hook

import (
	"encoding/json"
	"io"
	"os"
)

// maxInput bounds what is read from stdin before parsing. The peer is
// Claude Code itself, but CLAUDE.md's rule is that every reader of input an
// outside party chooses gets an explicit bound, and a Write tool_input
// carries an entire file's content.
const maxInput = 16 << 20

// Input is the slice of the hook stdin document the guards use.
type Input struct {
	HookEventName string         `json:"hook_event_name"`
	ToolName      string         `json:"tool_name"`
	CWD           string         `json:"cwd"`
	ToolInput     map[string]any `json:"tool_input"`
}

// Read parses one hook input document from r, bounded by maxInput.
func Read(r io.Reader) (*Input, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxInput))
	if err != nil {
		return nil, err
	}
	var in Input
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

// str reads a string field from tool_input, tolerating absence.
func (in *Input) str(key string) string {
	if in == nil || in.ToolInput == nil {
		return ""
	}
	s, _ := in.ToolInput[key].(string)
	return s
}

// FilePath is the file a file-editing tool targets. Edit, MultiEdit and
// Write all spell it file_path; notebook_path and path cover the notebook
// tool and any future spelling, in that order.
func (in *Input) FilePath() string {
	for _, key := range []string{"file_path", "notebook_path", "path"} {
		if s := in.str(key); s != "" {
			return s
		}
	}
	return ""
}

// Command is the shell command the Bash tool would run.
func (in *Input) Command() string {
	return in.str("command")
}

// Deny prints the PreToolUse JSON that refuses the tool call with reason.
// Both the current shape (hookSpecificOutput.permissionDecision) and the
// older top-level decision/block pair are emitted, so the refusal lands on
// any Claude Code version that reads either.
func Deny(reason string) {
	writeJSON(map[string]any{
		"decision": "block",
		"reason":   reason,
		"hookSpecificOutput": map[string]any{
			"hookEventName":            "PreToolUse",
			"permissionDecision":       "deny",
			"permissionDecisionReason": reason,
		},
	})
}

// Advise prints the PostToolUse JSON that surfaces context to the model
// without blocking anything.
func Advise(context string) {
	writeJSON(map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":     "PostToolUse",
			"additionalContext": context,
		},
	})
}

func writeJSON(v any) {
	// An encoding failure here would mean the maps above stopped being
	// marshalable, which a test would catch; at run time there is nothing
	// useful to do with it, and stderr would be noise on every edit.
	_ = json.NewEncoder(os.Stdout).Encode(v)
}
