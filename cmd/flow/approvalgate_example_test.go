package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestApprovalGateHelpExamplesActuallyRun is a targeted regression for a P2
// Codex found on #208: making approval-gate's `version`, `environment`,
// `requested_by`, and `expected_approver` inputs required made the two
// `Example:` invocations under `flow run local --help` and `flow signal
// --help` fail, because both showed the workflow run with a signal and no
// inputs at all. `checkRunInputs` rejects a missing required input before a
// signal is ever consulted, so both examples stopped being runnable the
// moment the inputs became required.
//
// This does not walk the cobra tree the way [TestEverySubcommandHasAWorkedExample]
// and [TestExamplesArePlausibleInvocations] do — those check that an Example
// looks like a command, not that it succeeds, and building a general "resolve
// and execute every Example: line in the tree" harness would need to fake a
// server for the commands that talk to one. `approval-gate` runs entirely
// locally, so it is cheap to actually run; this test does exactly that,
// pinned to the two lines that regressed, rather than generalizing.
func TestApprovalGateHelpExamplesActuallyRun(t *testing.T) {
	for _, path := range []string{"run local", "signal"} {
		t.Run(path, func(t *testing.T) {
			// findCommand (mcp_test.go) builds its own tree per call, so this
			// is not the tree the actual invocation below mutates.
			cmd := findCommand(t, path)
			line := approvalGateExampleLine(t, cmd.Example)

			// The example is written the way an author would paste it in
			// front of their own repo checkout, i.e. paths relative to the
			// repo root; this test runs from cmd/flow, one level under that.
			line = strings.ReplaceAll(line, "examples/approval-gate/", filepath.Join("..", "..", "examples", "approval-gate")+string(filepath.Separator))

			args := splitShellish(t, line)
			if args[0] != "flow" {
				t.Fatalf("expected the example line to start with %q, got %q", "flow", line)
			}
			args = args[1:]

			root := newRootCommand()
			var out, errOut strings.Builder
			root.SetOut(&out)
			root.SetErr(&errOut)
			root.SetArgs(args)

			if err := execute(t.Context(), root); err != nil {
				t.Fatalf("the help example\n\t%s\ndid not run cleanly: %v\nstdout:\n%s\nstderr:\n%s",
					line, err, out.String(), errOut.String())
			}
		})
	}
}

// approvalGateExampleLine finds the one line in example that runs the
// approval-gate workflow, failing loudly if the example no longer contains
// one — so a future rewrite of the Example: text does not silently stop
// exercising anything.
func approvalGateExampleLine(t *testing.T, example string) string {
	t.Helper()

	for _, line := range strings.Split(example, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "examples/approval-gate/workflow.yaml") {
			return trimmed
		}
	}
	t.Fatalf("no line in this Example: runs examples/approval-gate/workflow.yaml:\n%s", example)
	return ""
}

// splitShellish is a minimal, single-quote-aware word splitter — enough for
// the one shape these examples use (`--flag value` and `--flag 'json with
// spaces'`), not a shell parser. Good enough here because the input is this
// repo's own Example: text, not anything untrusted.
func splitShellish(t *testing.T, line string) []string {
	t.Helper()

	var (
		fields []string
		cur    strings.Builder
		inQuo  bool
	)
	flush := func() {
		if cur.Len() > 0 {
			fields = append(fields, cur.String())
			cur.Reset()
		}
	}
	for _, r := range line {
		switch {
		case r == '\'':
			inQuo = !inQuo
		case r == ' ' && !inQuo:
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	if inQuo {
		t.Fatalf("unterminated quote splitting example line: %q", line)
	}
	return fields
}
