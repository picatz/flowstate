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
// The two `Example:` lines this pins to now run `examples/expense-approval/`
// rather than `examples/approval-gate/` (#207 slice 3): once approval-gate
// declared a `signals:` policy, `--signal` — always
// [v1.LocalSignalSender], never an attested identity — could no longer
// satisfy it, by the same design that makes the gate real in production
// (see signals.go's withLocalSignals doc comment). Demonstrating "answer a
// gate locally, up front" needs a workflow with a `wait_for_signal:` and no
// signal policy to attest a sender against, which is exactly what
// expense-approval already is and approval-gate, by design, no longer can
// be. The name stuck; what it pins did not.
//
// "No longer can be" held for exactly as long as a local delivery had no way
// to say who it was from. #349 gave it one - --signal-as-subject and its
// siblings, a rehearsal identity the durable path refuses outright - so
// approval-gate has a `run local` Example again, pinned by
// [TestPolicedGateExampleRehearsesLocally] below. This test is unchanged and
// still pins the two lines that regressed, which are still expense-approval's:
// a gate with no `signals:` policy is the shape "answer a gate up front"
// demonstrates with the least in the way.
//
// This does not walk the cobra tree the way [TestEverySubcommandHasAWorkedExample]
// and [TestExamplesArePlausibleInvocations] do — those check that an Example
// looks like a command, not that it succeeds, and building a general "resolve
// and execute every Example: line in the tree" harness would need to fake a
// server for the commands that talk to one. `expense-approval` runs entirely
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
			line = strings.ReplaceAll(line, "examples/expense-approval/", filepath.Join("..", "..", "examples", "expense-approval")+string(filepath.Separator))

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
// expense-approval workflow, failing loudly if the example no longer
// contains one — so a future rewrite of the Example: text does not silently
// stop exercising anything.
func approvalGateExampleLine(t *testing.T, example string) string {
	t.Helper()

	for _, line := range strings.Split(example, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "examples/expense-approval/workflow.yaml") {
			return trimmed
		}
	}
	t.Fatalf("no line in this Example: runs examples/expense-approval/workflow.yaml:\n%s", example)
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

// TestPolicedGateExampleRehearsesLocally runs the `flow run local` Example line
// that answers examples/approval-gate's gate, in CI, against the real example.
//
// It is the reachability half of #349, and the reason it is a CLI test rather
// than another `flow test` case: `flow test` has had a scripted `sender:` all
// along, so a green test file there says nothing about whether an author with
// a terminal can rehearse the same gate. What this pins is the whole path a
// person takes - flags, policy resolved against this run's own inputs, the
// delivery, and a gate that actually opens.
//
// The comment on [TestApprovalGateHelpExamplesActuallyRun] above records why
// approval-gate stopped appearing in these Example: lines: `--signal` attested
// nobody, so the one workflow in the corpus with a `signals:` policy could not
// be answered locally at all. That is what --signal-as-subject changed, and
// this is the line that came back.
func TestPolicedGateExampleRehearsesLocally(t *testing.T) {
	cmd := findCommand(t, "run local")

	var line string
	for _, candidate := range strings.Split(cmd.Example, "\n") {
		trimmed := strings.TrimSpace(candidate)
		if strings.Contains(trimmed, "examples/approval-gate/workflow.yaml") {
			line = trimmed
			break
		}
	}
	if line == "" {
		t.Fatalf("no line in `run local`'s Example: rehearses examples/approval-gate/workflow.yaml:\n%s",
			cmd.Example)
	}

	if !strings.Contains(line, "--signal-as-subject") {
		t.Fatalf("the approval-gate example no longer names an approver, so it rehearses the "+
			"refusal rather than the gate:\n\t%s", line)
	}

	// Paths are written as an author would paste them at the repo root; this
	// test runs one level under it.
	line = strings.ReplaceAll(line, "examples/", filepath.Join("..", "..", "examples")+string(filepath.Separator))

	args := splitShellish(t, line)
	if args[0] != "flow" {
		t.Fatalf("expected the example line to start with %q, got %q", "flow", line)
	}

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args[1:])

	if err := execute(t.Context(), root); err != nil {
		t.Fatalf("rehearsing the policed gate did not run cleanly: %v\nstdout:\n%s\nstderr:\n%s",
			err, out.String(), errOut.String())
	}

	// The gate opened for the rehearsed approver, which is the whole claim.
	// `decision` is approval-gate's own name for the branch it took, and
	// "deployed" is reachable only through a delivery the policy admitted.
	if !strings.Contains(out.String(), "deployed") {
		t.Fatalf("the rehearsal did not reach the deploy branch, so a policed gate is still "+
			"unreachable locally:\nstdout:\n%s\nstderr:\n%s", out.String(), errOut.String())
	}

	// And it said it was a rehearsal, on the way past.
	if !strings.Contains(out.String(), "rehearsing --signal deliveries as") {
		t.Fatalf("nothing in the output marks this as a rehearsal identity:\nstdout:\n%s", out.String())
	}
}
