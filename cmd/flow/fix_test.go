package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// These tests are about the one thing `flow fix` has to earn: enough trust to be
// run over a whole repository. That trust is not that the rewrite is clever — it
// is that a file it has nothing to say about is not touched at all, that
// `--check` only looks, that a shape it refuses is left exactly as it was and
// said so in the exit status, and that what it does write still compiles. Each of
// those is asserted on the bytes on disk rather than on what the command said it
// did.

// oldStyleGreeter is a Flowfile written before the task was flattened onto the
// step, with a comment above a step and another between a step's own keys.
//
// The comments are load-bearing: both sit outside the run of lines the rewrite
// replaces, so a rewriter that edits the source keeps them and one that
// re-renders the document loses them.
//
// It declares no `edition:`, which is the same age showing from another side: the
// marker became required in the sweep that flattened the task onto the step, so a
// file written before one was written before the other. That makes this fixture
// the pre-edition case as well as the pre-flattening one, and every test using it
// covers the stamp `flow fix` now has to supply.
//
// The tasks are the two that still exist. A retired name here would pin `flow fix`
// to producing a spelling the language refuses, which is a migration test asserting
// the migration is not finished.
const oldStyleGreeter = `# A greeter written before the task was flattened onto the step.
name: greeter
steps:
  # Fetch the line to greet whoever is listening with.
  - id: greet
    # The task this step runs.
    task:
      name: http
      inputs:
        url: https://example.com/greeting
  - id: shout
    task:
      name: log
      inputs:
        message: ${greet.body}
`

// currentGreeter is oldStyleGreeter in the current edition.
//
// Derived from the transformations fix.go documents rather than from a run of it.
// Three of them apply here, which is the point of keeping this exact: `task:` and
// `name:` go away and `inputs:` becomes the task's own key, with everything under
// it dedenting by the two columns `inputs:` used to add; the reference in the last
// line is rooted, because a step is named `steps.<id>` now; and an `edition:` is
// stamped in, because a file that declares none is a file this build refuses.
// Every line no edit covers, comments included, is copied through — including the
// header comment, which the stamp goes *under* rather than above.
//
// The first two together are worth a fixture of their own: they are different
// kinds of edit — one replaces a run of lines, the other substitutes inside one —
// and the first version of this got them in an order where the block replacement
// stepped over the substitution and produced a file the validator refused.
const currentGreeter = `# A greeter written before the task was flattened onto the step.
edition: v2026.2
name: greeter
steps:
  # Fetch the line to greet whoever is listening with.
  - id: greet
    # The task this step runs.
    http:
      url: https://example.com/greeting
  - id: shout
    log:
      message: ${steps.greet.body}
`

// The 1-based lines oldStyleGreeter writes `task:` on. A report has to name
// them: an author reads `file:line:` and jumps there.
const (
	greeterFirstTaskLine  = 7
	greeterSecondTaskLine = 12
)

// oldStyleSingle is the smallest pre-flattening file, for tests about which
// files are picked up rather than about what the rewrite produces.
const oldStyleSingle = `edition: v2026.2
name: single
steps:
  - id: greet
    task:
      name: log
      inputs:
        message: hello
`

// oldStyleNested reaches every place a task can be written: at the top level, in
// a loop body, inside a parallel branch, and with a description that belongs to
// the step once the task is no longer a block of its own.
//
// The last step reads a step bare, inside a block the rewrite replaces whole, so
// the two kinds of edit have to compose here at every depth rather than only at
// the top of a file.
const oldStyleNested = `edition: v2026.2
name: nested-example
vars:
  targets: [alpha, beta]
steps:
  - id: announce
    task:
      name: log
      description: says what the run is about to do
      inputs:
        message: processing every target
  - id: process
    for_each:
      items: ${vars.targets}
      as: target
      max_parallel: 2
      steps:
        - id: label
          task:
            name: log
            inputs:
              message: ${'processing %s'.format([target])}
  - id: checks
    parallel:
      - steps:
          - id: check_config
            task:
              name: log
              inputs:
                message: config ok
      - steps:
          - id: check_quota
            task:
              name: log
              inputs:
                message: quota ok
  - id: summary
    task:
      name: log
      inputs:
        message: ${'processed %d target(s)'.format([size(process.results)])}
`

// oldStyleMixed has one step the rewriter can act on and one it must refuse, so
// the two halves of an unfinished run can be told apart: what it could do, it
// did, and the run still failed.
const oldStyleMixed = `edition: v2026.2
name: mixed
steps:
  - id: greet
    task:
      name: log
      inputs:
        message: hello
  - id: shout
    task: {name: log, inputs: {message: "hi!"}}
`

// partlyFixedMixed is oldStyleMixed after a run: the block-style step rewritten,
// the flow-style one left exactly as it was written.
const partlyFixedMixed = `edition: v2026.2
name: mixed
steps:
  - id: greet
    log:
      message: hello
  - id: shout
    task: {name: log, inputs: {message: "hi!"}}
`

// The 1-based line the refused step is on *after* the run — line 8 of
// partlyFixedMixed, not line 10 of oldStyleMixed.
//
// Which is the line worth naming, and it used to be the other one. `flow fix`
// rewrites until nothing changes and reports the last round's refusals, so a
// diagnostic describes the file as it now sits on disk. Before that it described
// the file as read: the rewrite above it removed two lines, and the author was
// sent to line 10 of a file whose refused step had moved to line 8.
//
// Nobody had noticed, because the number was only ever compared against itself.
const mixedRefusedLine = 8

// runFixCommand runs `flow fix` the way a shell does — through the command, so
// the flag spellings are part of what is under test — and returns its two
// streams separately along with the error that becomes the exit status.
//
// Separately, because which stream a report goes to is itself a property: under
// `--stdout` the document is the output, and everything else has to go somewhere
// a pipe will not pick it up.
func runFixCommand(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	cmd := newFixCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)

	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// writeFixture writes contents into dir and returns the path.
func writeFixture(t *testing.T, dir, name, contents string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("making the fixture's directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}
	return path
}

// readFixture reads a file back as bytes, because every property here is about
// bytes rather than about a document that happens to mean the same thing.
func readFixture(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s back: %v", path, err)
	}
	return data
}

// A report is one line `flow fix` printed about a file: the position it named
// and what it said there.
type report struct {
	line    int
	column  int // zero when the report names only a line
	message string
}

// reportsFor picks out the lines out printed about path, dropping the path
// itself.
//
// Dropping it is the point. Every fixture here lives in a temporary directory
// named after the test, so an assertion like "the refusal mentions an alias"
// would otherwise pass by matching the directory rather than the diagnostic —
// a test that cannot fail for the reason it was written.
func reportsFor(t *testing.T, out, path string) []report {
	t.Helper()

	var reports []report
	for _, text := range strings.Split(out, "\n") {
		rest, named := strings.CutPrefix(text, path+":")
		if !named {
			continue
		}

		field, remainder, _ := strings.Cut(rest, ":")
		line, err := strconv.Atoi(field)
		if err != nil {
			t.Errorf("a report does not begin with the line it is about, so nothing can jump to it: %q", text)
			continue
		}

		found := report{line: line, message: strings.TrimSpace(remainder)}
		if field, tail, hasMore := strings.Cut(remainder, ":"); hasMore {
			if column, err := strconv.Atoi(field); err == nil {
				found.column, found.message = column, strings.TrimSpace(tail)
			}
		}
		reports = append(reports, found)
	}
	return reports
}

// reportAt returns the report made at a line, or fails saying what was reported
// instead.
func reportAt(t *testing.T, reports []report, line int, out string) report {
	t.Helper()

	for _, r := range reports {
		if r.line == line {
			return r
		}
	}
	t.Fatalf("nothing was reported at line %d; the command said:\n%s", line, out)
	return report{}
}

// copyExamplesInto copies the shipped examples into dir, returning what each
// copy held so a later read can be compared to it byte for byte.
//
// The examples are the closest thing to a repository of current files, and CI
// already keeps them honest — including one that ends without a trailing
// newline, which is exactly the sort of detail a careless rewriter normalizes.
func copyExamplesInto(t *testing.T, dir string) map[string][]byte {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*", "workflow.yaml"))
	if err != nil {
		t.Fatalf("finding the examples: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples were found, so this test proves nothing")
	}

	copied := make(map[string][]byte, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		// Named for the example it came from, so a failure says which one.
		dest := writeFixture(t, dir, filepath.Base(filepath.Dir(path))+".yaml", string(data))
		copied[dest] = data
	}
	return copied
}

// TestFixLeavesACurrentFileByteForByte is what makes running this over a
// directory safe.
//
// Not "parses the same" and not "means the same": the same bytes. A rewriter
// that reformats what it had nothing to say about turns a one-line migration
// into a diff nobody can review, and the first person that happens to stops
// trusting the command on everything else too.
func TestFixLeavesACurrentFileByteForByte(t *testing.T) {
	dir := t.TempDir()
	before := copyExamplesInto(t, dir)

	out, _, err := runFixCommand(t, dir)
	if err != nil {
		t.Fatalf("fixing a directory of current files failed: %v\n%s", err, out)
	}

	for path, want := range before {
		if got := readFixture(t, path); !bytes.Equal(got, want) {
			t.Errorf("%s was rewritten although it is already current:\n--- before\n%s\n--- after\n%s",
				path, want, got)
		}
	}
}

// TestFixLeavesOddWhitespaceAlone covers the same property on the details a
// formatter is most tempted by.
func TestFixLeavesOddWhitespaceAlone(t *testing.T) {
	// No trailing newline, a blank line carrying spaces, and a quoting style
	// nobody would choose: all legal, none of it this command's business.
	//
	// Current in every other way, which is what leaves the whitespace as the only
	// thing a run could touch. A step spelled in a retirement's old key would have
	// this asserting that a refusal writes nothing, which is a different property
	// and one already covered.
	const odd = "edition: v2026.2\nname: odd\n\nsteps:\n  \n  - id: greet\n    log:\n      message:   'hello'"

	dir := t.TempDir()
	path := writeFixture(t, dir, "odd.yaml", odd)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fixing a current file failed: %v\n%s", err, out)
	}

	if got := string(readFixture(t, path)); got != odd {
		t.Errorf("whitespace was normalized in a file with nothing to change:\n--- before\n%q\n--- after\n%q",
			odd, got)
	}
}

// TestFixRewritesTheStepAndKeepsEverythingElse is the transformation itself,
// asserted on the whole file rather than on the lines that changed.
//
// The comments are the point. One sits above a step and one between a step's own
// keys, and both survive because the rewrite replaces a run of lines and copies
// the rest through — which is the difference between a migration someone reviews
// and a reformat of their file.
func TestFixRewritesTheStepAndKeepsEverythingElse(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("a run that refused nothing still failed: %v\n%s", err, out)
	}

	if got := string(readFixture(t, path)); got != currentGreeter {
		t.Errorf("the rewrite is not what the transformation says it should be:\n--- want\n%s\n--- got\n%s",
			currentGreeter, got)
	}

	// A report an editor can jump into, naming the task each step now runs.
	reports := reportsFor(t, out, path)
	for line, task := range map[int]string{
		greeterFirstTaskLine:  "http",
		greeterSecondTaskLine: "log",
	} {
		if got := reportAt(t, reports, line, out); !strings.Contains(got.message, task) {
			t.Errorf("the report at line %d does not say the step now runs %q: %q", line, task, got.message)
		}
	}
}

// TestFixKeepsCommentsWrittenInsideTheTaskBlock covers the comments that are not
// merely nearby but inside the run of lines being replaced.
//
// A comment above `name:` describes the task, and `name:` is the key going away,
// so it is carried up to the task's new key rather than deleted along with the
// line it annotated. Comments among the inputs travel with the inputs and dedent
// with them. This is a migration tool: dropping an author's comment is losing
// their work, and it is the kind of loss nobody notices until the explanation is
// already gone.
func TestFixKeepsCommentsWrittenInsideTheTaskBlock(t *testing.T) {
	const commented = `edition: v2026.2
name: commented
steps:
  - id: greet
    task:
      # which task this is
      name: log
      inputs:
        # what to say
        message: hello
        # and a note after it
`

	// Derived from the transformation: the comment about the task moves up to the
	// key replacing `name:`, and the two among the inputs keep their place within
	// the block as it dedents by the two columns `inputs:` used to add.
	const want = `edition: v2026.2
name: commented
steps:
  - id: greet
    # which task this is
    log:
      # what to say
      message: hello
      # and a note after it
`

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", commented)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	fixed := readFixture(t, path)
	if string(fixed) != want {
		t.Errorf("comments inside the rewritten block did not survive as written:\n--- want\n%s\n--- got\n%s",
			want, fixed)
	}

	// And what it wrote is still a Flowfile: a comment carried to the wrong
	// indentation can change what the line after it belongs to.
	diagnostics, err := flowfile.ValidateSource(fixed)
	if err != nil {
		t.Fatalf("the rewritten file does not parse: %v\n%s", err, fixed)
	}
	if len(diagnostics) != 0 {
		t.Fatalf("the rewritten file does not validate: %s\n--- file\n%s", diagnostics.Error(), fixed)
	}
}

// TestFixCheckReportsWithoutWriting is the form CI runs, and the property that
// makes it usable: a --check that mutates is a --check nobody can put in a
// pipeline.
func TestFixCheckReportsWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, "--check", path)
	if err == nil {
		t.Error("--check found work to do and still exited zero, so CI would never see it")
	}

	if got := string(readFixture(t, path)); got != oldStyleGreeter {
		t.Errorf("--check wrote to the file it was only asked to report on:\n--- before\n%s\n--- after\n%s",
			oldStyleGreeter, got)
	}

	reports := reportsFor(t, out, path)
	reportAt(t, reports, greeterFirstTaskLine, out)
	reportAt(t, reports, greeterSecondTaskLine, out)
}

// TestFixCheckOnACurrentTreeExitsZero is the other direction, and the reason it
// is worth writing separately: a --check that always failed would satisfy the
// test above perfectly.
func TestFixCheckOnACurrentTreeExitsZero(t *testing.T) {
	dir := t.TempDir()
	before := copyExamplesInto(t, dir)

	out, _, err := runFixCommand(t, "--check", dir)
	if err != nil {
		t.Fatalf("--check reported work on a tree that is already current: %v\n%s", err, out)
	}

	for path, want := range before {
		if got := readFixture(t, path); !bytes.Equal(got, want) {
			t.Errorf("--check modified %s", path)
		}
	}
}

// TestFixExitsNonZeroWhenAnythingWasRefused is the status a migration script
// reads.
//
// `flow fix . && git commit` must not succeed while steps are still in a
// spelling that no longer compiles, so a refusal fails the run whether or not
// --check was asked for, and whether or not other steps in the same file were
// rewritten successfully.
func TestFixExitsNonZeroWhenAnythingWasRefused(t *testing.T) {
	for _, test := range []struct {
		name     string
		contents string
	}{
		{
			// A shape the rewriter will not guess at.
			name: "a task written in flow style",
			contents: `edition: v2026.2
name: flow-style
steps:
  - id: greet
    task: {name: log, inputs: {message: hi}}
`,
		},
		{
			// Not YAML at all, which is certainly not the current edition either.
			name:     "a file that does not parse",
			contents: "edition: v2026.2\nname: x\n  steps: [\n",
		},
		{
			// The one easiest to get wrong: something *was* rewritten, so a status
			// derived from "did anything change" would call this a success.
			name:     "one step rewritten and one refused",
			contents: oldStyleMixed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeFixture(t, dir, "workflow.yaml", test.contents)

			out, _, err := runFixCommand(t, path)
			if err == nil {
				t.Errorf("a run that refused something exited zero, so `flow fix . && git commit` would commit it:\n%s", out)
			}
			if !strings.Contains(out, path) {
				t.Errorf("the run failed without naming the file it could not finish:\n%s", out)
			}
		})
	}

	// The other direction, without which a command that always failed would pass
	// every case above.
	t.Run("nothing refused", func(t *testing.T) {
		dir := t.TempDir()
		path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

		out, _, err := runFixCommand(t, path)
		if err != nil {
			t.Errorf("a run that rewrote everything it was given still failed: %v\n%s", err, out)
		}
	})
}

// notAFlowfile is a document with a top-level shape this repo's real policy
// files share — a mapping declaring neither `steps:` nor `tests:` — without
// depending on the shipped examples existing at a particular relative path.
const notAFlowfile = `egress:
  schemes: [https]
`

// TestFixRefusesANonFlowfileAndExitsNonZero is issue #203's fix at the CLI
// boundary: a document `flow fix` does not recognize is refused, left byte for
// byte, and the run still fails — because "silently accepted" is exactly the
// defect, and a refusal that exited zero would be no safer than the bug it
// replaces.
func TestFixRefusesANonFlowfileAndExitsNonZero(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "policy.yaml", notAFlowfile)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Fatalf("a document that is not a Flowfile was silently accepted:\n%s", out)
	}

	if got := string(readFixture(t, path)); got != notAFlowfile {
		t.Errorf("a refused document was rewritten anyway:\n--- before\n%s\n--- after\n%s", notAFlowfile, got)
	}

	report := reportAt(t, reportsFor(t, out, path), 1, out)
	if !strings.Contains(report.message, "does not look like a Flowfile") {
		t.Errorf("the refusal does not say what the file looks like instead of a Flowfile: %q", report.message)
	}
}

// TestFixExitCodesDistinguishRefusalFromChangesNeeded is the property #203's
// correction comment calls out by name: once CI hands `flow fix --check` a
// whole directory rather than a hand-picked glob, a refusal that reads the
// same as "changes are needed" would turn every policy file in that directory
// into a permanent red build. The three outcomes are checked side by side so
// a future change cannot make two of them read alike without a test noticing.
//
// All three exit non-zero except "already current" — refusal and "changes
// needed" share that exit status today, which is deliberate: `flow fix . &&
// git commit` must not succeed while *anything* is unfinished, a refusal
// included. What has to be distinguishable, and is asserted here, is what the
// run *says*: a refusal names what the file looks like instead of a Flowfile,
// a pending change names the edit that would be made, and neither is the
// "already current" message the clean case prints. A caller — CI, or a person
// reading the log — tells the three apart from the message, not the number.
func TestFixExitCodesDistinguishRefusalFromChangesNeeded(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		dir := t.TempDir()
		path := writeFixture(t, dir, "policy.yaml", notAFlowfile)

		out, _, err := runFixCommand(t, "--check", path)
		if err == nil {
			t.Fatalf("--check accepted a document that is not a Flowfile:\n%s", out)
		}
		if strings.Contains(out, "already current") {
			t.Errorf("a refusal printed the same message as a clean file:\n%s", out)
		}
		if strings.Contains(out, "added, which is now required") {
			t.Errorf("a refusal read like a pending edition stamp:\n%s", out)
		}
		if !strings.Contains(out, "does not look like a Flowfile") {
			t.Errorf("a refusal did not say what the file looks like instead:\n%s", out)
		}
	})

	t.Run("changes needed", func(t *testing.T) {
		dir := t.TempDir()
		path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

		out, _, err := runFixCommand(t, "--check", path)
		if err == nil {
			t.Fatalf("--check found nothing to do in a pre-edition file:\n%s", out)
		}
		if strings.Contains(out, "already current") {
			t.Errorf("pending changes printed the same message as a clean file:\n%s", out)
		}
		if strings.Contains(out, "does not look like a Flowfile") {
			t.Errorf("pending changes were reported as a refusal:\n%s", out)
		}
		if got := string(readFixture(t, path)); got != oldStyleGreeter {
			t.Errorf("--check wrote to a file it was only asked to report on:\n%s", got)
		}
	})

	t.Run("already current", func(t *testing.T) {
		dir := t.TempDir()
		path := writeFixture(t, dir, "workflow.yaml", currentGreeter)

		out, _, err := runFixCommand(t, "--check", path)
		if err != nil {
			t.Fatalf("--check failed on a file with nothing to change: %v\n%s", err, out)
		}
		if !strings.Contains(out, "already current") {
			t.Errorf("a clean file under --check did not say so:\n%s", out)
		}
	})
}

// TestFixDirectoryWalkSkipsPolicyFilesSilently is the reason widening CI's
// `flow fix --check` coverage to the whole examples/ tree (#203) does not turn
// every policy file into a permanent red build: a directory walk selects files
// by [flowfile.LooksLikeFlowfile] before any of them ever reaches [flowfile.Fix],
// so a policy file sitting beside a Flowfile is passed over rather than handed
// to the rewriter and refused. Refusal is still what happens to a policy file
// *named directly* — see TestFixRefusesANonFlowfileAndExitsNonZero — this test
// is about the other path into the same file, a sweep that never named it.
func TestFixDirectoryWalkSkipsPolicyFilesSilently(t *testing.T) {
	dir := t.TempDir()
	flowfilePath := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)
	policyPath := writeFixture(t, dir, "policy.yaml", notAFlowfile)

	out, _, err := runFixCommand(t, dir)
	if err != nil {
		t.Fatalf("a directory holding a Flowfile and a policy file failed: %v\n%s", err, out)
	}

	if got := string(readFixture(t, flowfilePath)); got == oldStyleGreeter {
		t.Errorf("the Flowfile in the same directory was not picked up by the walk")
	}
	if got := string(readFixture(t, policyPath)); got != notAFlowfile {
		t.Errorf("a directory walk touched a file that is not a Flowfile:\n--- before\n%s\n--- after\n%s",
			notAFlowfile, got)
	}
	if strings.Contains(out, "policy.yaml") {
		t.Errorf("a directory walk reported on a file it correctly never touched:\n%s", out)
	}
}

// malformedYAML is not a Flowfile, a policy file, or anything else recognized
// YAML — it does not parse at all. [flowfile.LooksLikeFlowfile] answers false
// for it the same as for notAFlowfile, which is exactly the ambiguity this
// fix resolves: unlike notAFlowfile, this is not "recognizably something
// else", it is broken, and a directory sweep must say so rather than pass
// over it in silence the way it correctly does for a policy file.
const malformedYAML = `steps:
  - id: broken
    task: [unterminated
`

// TestFixDirectoryWalkReportsMalformedFlowfilesRatherThanSkippingThem is
// Codex's P2 on #209: `collectFlowfiles` prefilters a directory sweep with
// [flowfile.LooksLikeFlowfile], which answers false both for "this is a
// policy file" and for "this does not parse at all" — so a syntactically
// broken workflow.yaml used to vanish from `flow fix --check examples/`
// silently, the one file in a sweep whose author most needs told. A named
// path already reports this (`fixOne` hands a parse error straight to Fix);
// a swept one now must too.
func TestFixDirectoryWalkReportsMalformedFlowfilesRatherThanSkippingThem(t *testing.T) {
	dir := t.TempDir()
	brokenPath := writeFixture(t, dir, "workflow.yaml", malformedYAML)
	validPath := writeFixture(t, dir, filepath.Join("other", "workflow.yaml"), currentGreeter)

	out, _, err := runFixCommand(t, dir)
	if err == nil {
		t.Fatalf("a directory holding a broken workflow.yaml was silently accepted:\n%s", out)
	}
	if !strings.Contains(out, "workflow.yaml") {
		t.Errorf("the broken file was not named in the report at all:\n%s", out)
	}
	if got := string(readFixture(t, brokenPath)); got != malformedYAML {
		t.Errorf("a file that failed to parse was rewritten anyway:\n--- before\n%s\n--- after\n%s", malformedYAML, got)
	}
	// The valid Flowfile beside it must still be picked up by the same walk —
	// one broken sibling must not make the walk stop looking at the rest.
	if got := string(readFixture(t, validPath)); got != currentGreeter {
		t.Errorf("a valid Flowfile beside a broken one was not left alone as expected (already current):\n%s", got)
	}
}

// TestFixDirectoryWalkStillSkipsPolicyAndObservabilityFilesSilently pins the
// exact count #209 introduced the walk to hit: the shipped examples/ tree
// holds 3 real policy files and 7 non-Flowfile YAML documents under
// examples/observability/ (docker-compose.yaml and each collector/dashboard
// config), all of which parse as YAML fine and are none of them a Flowfile —
// so all 10 must still be skipped silently by the walk, both before and after
// teaching it to report a file that does not parse at all. This is the
// regression the malformed-YAML fix must not cause: reporting on unparseable
// files must not start reporting on files that parse fine into some other
// recognized shape.
func TestFixDirectoryWalkStillSkipsPolicyAndObservabilityFilesSilently(t *testing.T) {
	out, _, err := runFixCommand(t, "--check", "../../examples")
	if err != nil {
		t.Logf("flow fix --check examples/ exited non-zero (%v); fine as long as the count and skip list below hold", err)
	}

	const wantReported = 44
	got := 0
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if line != "" {
			got++
		}
	}
	if got != wantReported {
		t.Errorf("flow fix --check examples/ reported on %d files, want %d "+
			"(54 total .yaml/.yml under examples/, minus 3 policy files and "+
			"7 non-Flowfile observability configs that must stay silently skipped)",
			got, wantReported)
	}

	for _, name := range []string{
		"egress-policy.yaml",
		"auth-policy.yaml",
		filepath.Join("observability", "docker-compose.yaml"),
		filepath.Join("observability", "otel-collector", "config.yaml"),
		filepath.Join("observability", "prometheus", "prometheus.yml"),
		filepath.Join("observability", "grafana", "provisioning", "datasources", "datasources.yaml"),
		filepath.Join("observability", "grafana", "provisioning", "dashboards", "dashboards.yaml"),
		filepath.Join("observability", "tempo", "tempo.yaml"),
		filepath.Join("observability", "loki", "loki.yaml"),
	} {
		if strings.Contains(out, name) {
			t.Errorf("the walk reported on %s, which is not a Flowfile and must be skipped silently:\n%s", name, out)
		}
	}
}

// TestFixRewritesWhatItCanBeforeGivingUp is why a refusal is a status rather
// than a stop.
//
// One unrewritable step must not block the other nine: the author wants the ones
// the tool understood done, and the one it did not left exactly as it was to fix
// by hand — with the run still failing so that nobody mistakes it for finished.
func TestFixRewritesWhatItCanBeforeGivingUp(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleMixed)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Error("a file with a refused step reported success")
	}

	if got := string(readFixture(t, path)); got != partlyFixedMixed {
		t.Errorf("either the step it could rewrite was skipped or the one it refused was touched:\n--- want\n%s\n--- got\n%s",
			partlyFixedMixed, got)
	}

	if refusal := reportAt(t, reportsFor(t, out, path), mixedRefusedLine, out); refusal.column == 0 {
		t.Errorf("the refusal names a line but no column: %q", refusal.message)
	}
}

// TestFixDoesNotPrintUsageWhenAFileNeedsWork keeps the report readable.
//
// A file that needs fixing is not a command someone typed wrongly, and a usage
// block after the diagnostics says it was — which sends the reader to check
// their flags instead of the line they were just told about.
func TestFixDoesNotPrintUsageWhenAFileNeedsWork(t *testing.T) {
	for _, test := range []struct {
		name     string
		contents string
		args     []string
	}{
		{name: "check finds work", contents: oldStyleGreeter, args: []string{"--check"}},
		{name: "a step is refused", contents: oldStyleMixed},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeFixture(t, dir, "workflow.yaml", test.contents)

			out, errOut, err := runFixCommand(t, append(test.args, path)...)
			if err == nil {
				t.Fatalf("the run was expected to report unfinished work:\n%s", out)
			}
			for stream, text := range map[string]string{"stdout": out, "stderr": errOut} {
				if strings.Contains(text, "Usage:") {
					t.Errorf("a file needing work drew a usage block on %s:\n%s", stream, text)
				}
			}
		})
	}
}

// TestFixWalksADirectoryForFlowfiles checks which files a directory hands over.
//
// Both extensions a Flowfile is written with are picked up, at any depth, and
// something that merely lives in the same directory is not: walking a tree and
// rewriting a file nobody described as a workflow is not what running `flow fix
// examples/` asks for.
func TestFixWalksADirectoryForFlowfiles(t *testing.T) {
	dir := t.TempDir()

	yamlPath := writeFixture(t, dir, "workflow.yaml", oldStyleSingle)
	ymlPath := writeFixture(t, dir, "other.yml", oldStyleSingle)
	deepPath := writeFixture(t, filepath.Join(dir, "nested"), "deep.yaml", oldStyleSingle)
	// The same contents, so the only thing that can leave it alone is its
	// extension.
	notAFlowfile := writeFixture(t, dir, "notes.txt", oldStyleSingle)

	out, _, err := runFixCommand(t, dir)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	for _, path := range []string{yamlPath, ymlPath, deepPath} {
		got := string(readFixture(t, path))
		if strings.Contains(got, "task:") {
			t.Errorf("%s was not picked up by the walk:\n%s", path, got)
		}
		if !strings.Contains(got, "log:") {
			t.Errorf("%s was changed into something other than the current spelling:\n%s", path, got)
		}
	}

	if got := string(readFixture(t, notAFlowfile)); got != oldStyleSingle {
		t.Errorf("a file that is not a Flowfile was rewritten by a directory walk:\n%s", got)
	}
}

// TestFixTakesANamedFileWhateverItIsCalled is the other half of that rule.
//
// Naming a file is saying what you mean, so the extension stops being the
// evidence.
func TestFixTakesANamedFileWhateverItIsCalled(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.flow", oldStyleSingle)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	got := string(readFixture(t, path))
	if strings.Contains(got, "task:") {
		t.Errorf("a file named explicitly was skipped for its extension:\n%s", got)
	}
	if !strings.Contains(got, "log:") {
		t.Errorf("the named file was not rewritten into the current spelling:\n%s", got)
	}
}

// TestFixWritesSomethingThatCompiles is the property a rewriter is worth nothing
// without.
//
// It crosses the boundary in both directions: the fixture has to genuinely fail
// the current language before, or the test proves nothing, and has to draw no
// diagnostics at all after. The shapes are every place a task can be written —
// top level, loop body, parallel branch — plus a description, which belongs to
// the step once the task is no longer a block of its own.
func TestFixWritesSomethingThatCompiles(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleNested)

	// The old spelling is gone from the language rather than deprecated in it, so
	// it has to be refused now. Without this the test would pass just as well on a
	// fixture that was already current.
	if diagnostics, err := flowfile.ValidateSource([]byte(oldStyleNested)); err == nil && len(diagnostics) == 0 {
		t.Fatal("the pre-flattening fixture still compiles, so this proves nothing about the rewrite")
	}

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	fixed := readFixture(t, path)
	diagnostics, err := flowfile.ValidateSource(fixed)
	if err != nil {
		t.Fatalf("the rewritten file does not parse: %v\n%s", err, fixed)
	}
	if len(diagnostics) != 0 {
		t.Fatalf("the rewritten file does not validate: %s\n--- file\n%s", diagnostics.Error(), fixed)
	}

	if strings.Contains(string(fixed), "task:") {
		t.Errorf("a `task:` block survived the rewrite:\n%s", fixed)
	}
	// The description moved to the step rather than being dropped: prose about a
	// step is the sort of loss an author only notices much later.
	if !strings.Contains(string(fixed), "says what the run is about to do") {
		t.Errorf("the task's description was lost:\n%s", fixed)
	}
}

// TestFixRefusesFlowStyleWithoutMangling covers the refusal that keeps the whole
// command trustworthy.
//
// Flow style has no line structure to rewrite, so acting on it would mean
// reflowing an author's file on a guess. All three halves are asserted: the
// position is reported, the run fails, and the file is exactly as it was — a file
// that looks fixed and is not is worse than one that was never touched.
func TestFixRefusesFlowStyleWithoutMangling(t *testing.T) {
	const inFlowStyle = `edition: v2026.2
name: flow-style
steps:
  - id: greet
    task: {name: log, inputs: {message: hi}}
`
	// The line the flow-style task is written on.
	const taskLine = 5

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", inFlowStyle)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Error("a refused file reported success")
	}

	if got := string(readFixture(t, path)); got != inFlowStyle {
		t.Errorf("a step the rewriter refused was edited anyway:\n--- before\n%s\n--- after\n%s",
			inFlowStyle, got)
	}

	refusal := reportAt(t, reportsFor(t, out, path), taskLine, out)
	if refusal.column == 0 {
		t.Errorf("the refusal names a line but no column, so it points at a line rather than at the shape: %q",
			refusal.message)
	}
	if !strings.Contains(strings.ToLower(refusal.message), "flow style") {
		t.Errorf("the refusal does not say what it could not act on: %q", refusal.message)
	}
}

// TestFixRefusesATaskBehindAnAliasWithoutMangling is the same refusal for a shape
// whose contents are not written where they are used.
//
// An alias cannot be rewritten without knowing what it will expand to, and the
// anchor it names is not a mapping written under `task:` either. Neither is
// guessed at, and the file comes back untouched.
func TestFixRefusesATaskBehindAnAliasWithoutMangling(t *testing.T) {
	const shared = `edition: v2026.2
name: shared-task
steps:
  - id: first
    task: &b
      name: log
      inputs:
        message: hi
  - id: second
    task: *b
`
	// The lines the anchor and the alias standing in for it are written on.
	const (
		anchorLine = 5
		aliasLine  = 10
	)

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", shared)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Error("a refused file reported success")
	}

	if got := string(readFixture(t, path)); got != shared {
		t.Errorf("a step standing behind an alias was edited anyway:\n--- before\n%s\n--- after\n%s",
			shared, got)
	}

	reports := reportsFor(t, out, path)
	for _, line := range []int{anchorLine, aliasLine} {
		if refusal := reportAt(t, reports, line, out); refusal.column == 0 {
			t.Errorf("the refusal at line %d names no column: %q", line, refusal.message)
		}
	}
	if refusal := reportAt(t, reports, aliasLine, out); !strings.Contains(strings.ToLower(refusal.message), "alias") {
		t.Errorf("the refusal does not say what it could not act on: %q", refusal.message)
	}
}

// TestFixRefusesAnEditionItDoesNotKnow keeps the migration honest about which
// direction it runs in.
//
// A marker this build has never heard of came from a newer `flow`, and rewriting
// it to the current edition would be this build stamping a claim to understand a
// grammar it does not have — on a file it just failed to interpret. The fix is to
// upgrade, so it says so and changes nothing.
func TestFixRefusesAnEditionItDoesNotKnow(t *testing.T) {
	const fromTheFuture = `edition: "2099.7"
name: from-the-future
steps:
  - id: greet
    log:
      message: hello
`

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", fromTheFuture)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Error("an edition this build does not know was reported as finished work")
	}

	if got := string(readFixture(t, path)); got != fromTheFuture {
		t.Errorf("an edition marker this build cannot interpret was rewritten anyway:\n--- before\n%s\n--- after\n%s",
			fromTheFuture, got)
	}

	// Line 1 is where the marker is written.
	refusal := reportAt(t, reportsFor(t, out, path), 1, out)
	if refusal.column == 0 {
		t.Errorf("the refusal names no column: %q", refusal.message)
	}
	if !strings.Contains(refusal.message, "edition") {
		t.Errorf("the refusal does not say which problem fired: %q", refusal.message)
	}
	if !strings.Contains(refusal.message, "2099.7") {
		t.Errorf("the refusal does not quote the marker it could not interpret: %q", refusal.message)
	}
}

// TestFixStampsAnEditionIntoAFileWithoutOne covers the one repair every file
// written before the sweep needs.
//
// The rule here reversed, and which way round it runs is the whole test. An absent
// `edition:` used to mean "the current grammar, unpinned", so writing one in would
// have been the rewriter acquiring an opinion the author did not have. Making the
// marker required turned the absence into the opinion instead: a file declaring
// none is refused now, so a `flow fix` that declined to stamp would leave the one
// defect it alone can repair — a migration tool that does not migrate the thing
// its own diagnostic names.
func TestFixStampsAnEditionIntoAFileWithoutOne(t *testing.T) {
	// Guarded, because this is a test that can go quiet without failing: an
	// `edition:` added to the shared fixture for some other test's sake would leave
	// the assertion below passing on a marker the author wrote rather than on one
	// this command supplied.
	if strings.Contains(oldStyleGreeter, "edition:") {
		t.Fatal("the fixture already declares an edition, so nothing here would be about stamping one in")
	}

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	fixed := string(readFixture(t, path))
	// A file that really was rewritten, so this is not passing because nothing was
	// written at all.
	if fixed == oldStyleGreeter {
		t.Fatal("the file was not rewritten, so this says nothing about what a rewrite adds")
	}
	if !strings.Contains(fixed, "edition: "+flowfile.CurrentEdition) {
		t.Errorf("no edition marker was stamped into a file that declares none:\n%s", fixed)
	}

	// And the file the command left behind is one this build accepts, which is the
	// whole reason the stamp exists: the marker is required now, so `flow fix` has to
	// supply the one thing every pre-edition file needs.
	if _, _, err := flowfile.Parse([]byte(fixed)); err != nil {
		t.Errorf("the rewritten file does not compile: %v\n%s", err, fixed)
	}
}

// TestFixBringsAStaleEditionForward is the case the edition marker exists for,
// and the one this build cannot exercise yet.
//
// An older edition is refused by the compiler with "run `flow fix`", so `flow
// fix` has to be what resolves it; answering "already current" while leaving the
// marker that caused the refusal would be a migration tool that does not migrate
// the thing its own diagnostic names. There is one known edition today, so this
// waits for the second rather than asserting nothing.
func TestFixBringsAStaleEditionForward(t *testing.T) {
	var stale string
	for _, edition := range flowfile.KnownEditions() {
		if edition != flowfile.CurrentEdition {
			stale = edition
			break
		}
	}
	if stale == "" {
		t.Skipf("this build knows only %s, so no file can declare an older edition yet", flowfile.CurrentEdition)
	}

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", "edition: "+strconv.Quote(stale)+`
name: stale-edition
steps:
  - id: greet
    log:
      message: hello
`)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	fixed := readFixture(t, path)
	if strings.Contains(string(fixed), stale) {
		t.Errorf("the stale edition marker survived, so the file is still refused after being fixed:\n%s", fixed)
	}
	if !strings.Contains(string(fixed), flowfile.CurrentEdition) {
		t.Errorf("the file no longer declares an edition this build knows:\n%s", fixed)
	}

	diagnostics, err := flowfile.ValidateSource(fixed)
	if err != nil {
		t.Fatalf("the rewritten file does not parse: %v\n%s", err, fixed)
	}
	if len(diagnostics) != 0 {
		t.Fatalf("the rewritten file does not validate: %s\n--- file\n%s", diagnostics.Error(), fixed)
	}
}

// TestFixPreservesFileMode keeps a migration from also being a permissions
// change.
//
// A rewriter run over a repository that widens a file everyone had forgotten was
// restricted has done something nobody asked for and nobody will notice.
func TestFixPreservesFileMode(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	const mode fs.FileMode = 0o600
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("setting the fixture's mode: %v", err)
	}

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("fix: %v\n%s", err, out)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("reading the mode back: %v", err)
	}
	if got := info.Mode().Perm(); got != mode {
		t.Errorf("mode = %v, want %v: fixing a file changed who can read it", got, mode)
	}

	// And it really was rewritten, so the mode survived a write rather than
	// surviving because nothing happened.
	if got := string(readFixture(t, path)); got == oldStyleGreeter {
		t.Error("the file was not rewritten, so this says nothing about writing through its mode")
	}
}

// TestFixStdoutWritesTheResultAndLeavesTheFile covers piping the result
// somewhere else, which is only useful if the original stays put.
func TestFixStdoutWritesTheResultAndLeavesTheFile(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, "--stdout", path)
	if err != nil {
		t.Fatalf("fix --stdout: %v\n%s", err, out)
	}

	if out != currentGreeter {
		t.Errorf("--stdout wrote something other than the rewritten document:\n--- want\n%s\n--- got\n%s",
			currentGreeter, out)
	}
	if got := string(readFixture(t, path)); got != oldStyleGreeter {
		t.Errorf("--stdout wrote back to the file as well:\n--- before\n%s\n--- after\n%s",
			oldStyleGreeter, got)
	}
}

// TestFixStdoutKeepsReportsOffTheDocument is what makes `flow fix --stdout
// old.yaml > new.yaml` safe on a file that is not perfectly rewritable.
//
// A tool that writes its complaints into its own output cannot be piped: the
// result is a new.yaml whose first line is a diagnostic about old.yaml — broken
// in a way that reads as though the rewriter mangled the document. So the
// document is the whole of stdout, and the refusal goes where a pipe will not
// pick it up.
func TestFixStdoutKeepsReportsOffTheDocument(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleMixed)

	out, errOut, err := runFixCommand(t, "--stdout", path)
	if err == nil {
		t.Error("a refused file reported success")
	}

	if out != partlyFixedMixed {
		t.Errorf("stdout is not exactly the document:\n--- want\n%s\n--- got\n%s", partlyFixedMixed, out)
	}
	if got := string(readFixture(t, path)); got != oldStyleMixed {
		t.Errorf("--stdout wrote back to the file as well:\n--- before\n%s\n--- after\n%s", oldStyleMixed, got)
	}

	// The refusal is still made, just not into the document.
	refusal := reportAt(t, reportsFor(t, errOut, path), mixedRefusedLine, errOut)
	if refusal.column == 0 {
		t.Errorf("the refusal on stderr names no column: %q", refusal.message)
	}
}

// TestFixStdoutRefusesMoreThanOneFile keeps two documents from being run
// together into a stream that is neither of them.
func TestFixStdoutRefusesMoreThanOneFile(t *testing.T) {
	dir := t.TempDir()
	first := writeFixture(t, dir, "first.yaml", oldStyleGreeter)
	second := writeFixture(t, dir, "second.yaml", oldStyleSingle)

	out, _, err := runFixCommand(t, "--stdout", first, second)
	if err == nil {
		t.Error("--stdout ran two documents together instead of refusing")
	}
	if strings.Contains(out, "message:") {
		t.Errorf("--stdout wrote a document it had refused to write:\n%s", out)
	}

	for path, want := range map[string]string{first: oldStyleGreeter, second: oldStyleSingle} {
		if got := string(readFixture(t, path)); got != want {
			t.Errorf("%s was rewritten by a refused invocation:\n%s", path, got)
		}
	}
}

// TestFixStdoutAndCheckAreRefused pins the one flag combination that cannot be
// honoured.
//
// One asks for the result and the other promises to produce nothing, so serving
// either reading silently gives the caller the opposite of what they asked for.
func TestFixStdoutAndCheckAreRefused(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, "--stdout", "--check", path)
	if err == nil {
		t.Error("--stdout and --check were accepted together")
	}
	if strings.Contains(out, "message:") {
		t.Errorf("a refused invocation wrote a document anyway:\n%s", out)
	}
	if got := string(readFixture(t, path)); got != oldStyleGreeter {
		t.Errorf("a refused invocation wrote to the file:\n%s", got)
	}
}
