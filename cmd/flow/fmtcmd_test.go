package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// These tests are about what `flow fmt` promises rather than what a text-editing
// rewriter like `flow fix` promises: it normalizes key order, quoting and layout
// rather than preserving them, but it is idempotent, `--check` only looks, a file
// that will not parse is never touched, and every comment in a file it rewrites
// comes out the other side (#381).

// runFmtCommand runs `flow fmt` through the command, the way a shell does, and
// returns its two streams separately along with the error that becomes the exit
// status — the same shape runFixCommand in fix_test.go uses, for the same reason:
// which stream a report lands on is part of what is under test.
func runFmtCommand(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer
	cmd := newFmtCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)

	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestFmtIsIdempotentAcrossExamples is the property a formatter has to have to
// be safe to run in a pre-commit hook: formatting an already-formatted file must
// not change it again.
//
// Every shipped example is real, and the round trip through Marshal changes at
// least one of them — sorted map keys, normalized quoting, an added trailing
// newline — so this is not passing because nothing was rewritten either time.
func TestFmtIsIdempotentAcrossExamples(t *testing.T) {
	dir := t.TempDir()
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*", "workflow.yaml"))
	if err != nil {
		t.Fatalf("finding the examples: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples were found, so this test proves nothing")
	}

	// Copied as `<name>/workflow.yaml` rather than flattened to one renamed
	// file, and joined by whatever a `call:` step in it reaches: `call-a-
	// workflow` names a sibling file by a relative path, and flattening it away
	// would leave that path resolving to nothing in the copy. Nothing else
	// under an example's own directory is copied — several examples ship a
	// policy file or a docker-compose lab beside `workflow.yaml` that is not
	// itself a Flowfile, and copying those in would ask this test to reformat
	// documents `flow fmt` was never going to be pointed at.
	var copies []string
	for _, src := range paths {
		exampleDir := filepath.Dir(src)
		name := filepath.Base(exampleDir)

		copies = append(copies, writeFixture(t, dir, filepath.Join(name, "workflow.yaml"), readFixtureString(t, src)))

		callees := map[string]bool{}
		collectCallFiles(t, src, callees)
		for abs := range callees {
			rel, err := filepath.Rel(exampleDir, abs)
			if err != nil || strings.HasPrefix(rel, "..") {
				t.Fatalf("%s calls %s, outside its own example directory", src, abs)
			}
			copies = append(copies, writeFixture(t, dir, filepath.Join(name, rel), readFixtureString(t, abs)))
		}
	}

	before := make(map[string][]byte, len(copies))
	for _, path := range copies {
		before[path] = readFixture(t, path)
	}

	// The exit status is not the assertion, because a real example may carry a
	// comment the rewrite cannot keep and is then refused by design (#381). What
	// has to hold is that a refused file is left exactly as it was, and that
	// everything else settles after one pass.
	out, _, _ := runFmtCommand(t, dir)

	firstPass := make(map[string][]byte, len(copies))
	for _, path := range copies {
		firstPass[path] = readFixture(t, path)
	}
	if !strings.Contains(out, "reformatted") {
		t.Fatal("nothing was reformatted on the first run, so this says nothing about idempotence")
	}

	refused := 0
	for _, path := range copies {
		if !fmtAccepts(t, path) {
			refused++
			if !bytes.Equal(before[path], firstPass[path]) {
				t.Errorf("%s was refused and rewritten anyway:\n--- before\n%s\n--- after\n%s",
					path, before[path], firstPass[path])
			}
		}
	}

	out2, _, _ := runFmtCommand(t, "--check", dir)
	if strings.Contains(out2, "reformatted") {
		t.Errorf("--check found work to do on the second run, so fmt(fmt(x)) != fmt(x):\n%s", out2)
	}

	for _, path := range copies {
		second := readFixture(t, path)
		if !bytes.Equal(firstPass[path], second) {
			t.Errorf("%s changed on a second --check run:\n--- first\n%s\n--- second\n%s",
				path, firstPass[path], second)
		}
	}

	// Refusals are the exception rather than the rule, and a change that made
	// every file refuse would satisfy everything above.
	if refused > len(copies)/4 {
		t.Errorf("%d of %d examples were refused, which is too many for refusal to still be the exception",
			refused, len(copies))
	}
}

// fmtAccepts reports whether `flow fmt` has an answer for the file at path, as
// opposed to refusing it: a file whose comments the rewrite cannot keep, or a
// workflow it cannot write back out.
func fmtAccepts(t *testing.T, path string) bool {
	t.Helper()

	workflow, _, err := flowfile.ParseFile(path)
	if err != nil {
		return false
	}
	_, err = flowfile.Format(readFixture(t, path), workflow)
	return err == nil
}

// readFixtureString reads path as a string, failing the test on error.
func readFixtureString(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// collectCallFiles compiles the Flowfile at path and records the absolute path
// of every file a `call:` step in it — or in any callee it reaches,
// transitively — resolves to.
//
// Each level is walked relative to *its own* directory, matching how the
// compiler itself resolves `call:`: a callee's own calls are relative to the
// callee's file, not to path's.
func collectCallFiles(t *testing.T, path string, into map[string]bool) {
	t.Helper()

	workflow, _, err := flowfile.ParseFile(path)
	if err != nil {
		t.Fatalf("compiling %s: %v", path, err)
	}
	walkCallNodes(workflow.GetSteps(), filepath.Dir(path), into)
}

func walkCallNodes(nodes []*v1.Node, dir string, into map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Call:
			source := kind.Call.GetSource()
			if source == "" {
				continue
			}
			abs := filepath.Clean(filepath.Join(dir, source))
			into[abs] = true
			walkCallNodes(kind.Call.GetWorkflow().GetSteps(), filepath.Dir(abs), into)
		case *v1.Node_ForEach:
			walkCallNodes(kind.ForEach.GetBody(), dir, into)
		case *v1.Node_Loop:
			// A `loop:` body, for the identical reason a `for_each` body is
			// walked: a call reached only from inside one is still a file this
			// test has to copy beside its caller, or the copy's `call:` resolves
			// to nothing. `progressive-rollout` is the first example whose only
			// call sits there, and it failed here rather than in the assertion —
			// a walk that knows about three of the four nesting constructs is a
			// walk that is wrong about the fourth.
			walkCallNodes(kind.Loop.GetBody(), dir, into)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				walkCallNodes(branch.GetSteps(), dir, into)
			}
		}
	}
}

// TestFmtProducesFormatsOutput checks the transformation itself against the
// function it wraps, so a bug in the command's plumbing cannot hide behind a
// bug in Format, or the reverse.
func TestFmtProducesFormatsOutput(t *testing.T) {
	const src = `edition: v2026.2
name: greeter
steps:
  # a comment flow fmt carries through
  - id: greet
    log:
      message: hello world
`

	workflow, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("the fixture does not compile: %v", err)
	}
	want, err := flowfile.Format([]byte(src), workflow)
	if err != nil {
		t.Fatalf("Format failed on the fixture directly: %v", err)
	}
	if !strings.Contains(string(want), "# a comment flow fmt carries through") {
		t.Fatalf("the fixture proves nothing about comments, because Format dropped it:\n%s", want)
	}

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, _, err := runFmtCommand(t, path)
	if err != nil {
		t.Fatalf("fmt: %v\n%s", err, out)
	}

	if got := string(readFixture(t, path)); got != string(want) {
		t.Errorf("the command's output does not match Format's own output:\n--- want\n%s\n--- got\n%s", want, got)
	}
}

// TestFmtCheckReportsWithoutWriting is the form CI runs, and the property that
// makes it usable there: a --check that mutates is a --check nobody can put in
// a pipeline.
func TestFmtCheckReportsWithoutWriting(t *testing.T) {
	const src = `edition: v2026.2
name: greeter
steps:
  - id: greet
    log:
      message: hello world
`
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, _, err := runFmtCommand(t, "--check", path)
	if err == nil {
		t.Error("--check found work to do and still exited zero, so CI would never see it")
	}
	if got := string(readFixture(t, path)); got != src {
		t.Errorf("--check wrote to the file it was only asked to report on:\n--- before\n%s\n--- after\n%s", src, got)
	}
	if !strings.Contains(out, path) {
		t.Errorf("the report does not name the file that needs formatting:\n%s", out)
	}
}

// TestFmtCheckOnFormattedFilesExitsZero is the other direction: a --check that
// always failed would satisfy the test above perfectly.
func TestFmtCheckOnFormattedFilesExitsZero(t *testing.T) {
	const src = `edition: v2026.2
name: greeter
steps:
  - id: greet
    log:
      message: hello world
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("the fixture does not compile: %v", err)
	}
	formatted, err := flowfile.Marshal(workflow)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", string(formatted))

	out, _, err := runFmtCommand(t, "--check", path)
	if err != nil {
		t.Fatalf("--check reported work on a file that is already formatted: %v\n%s", err, out)
	}
	if got := string(readFixture(t, path)); got != string(formatted) {
		t.Errorf("--check modified %s", path)
	}
}

// TestFmtLeavesAParseFailureUntouched is the refusal that keeps the command
// trustworthy: a file `flow fmt` cannot read into a workflow is left exactly as
// it was, byte for byte, rather than guessed at.
func TestFmtLeavesAParseFailureUntouched(t *testing.T) {
	const broken = "edition: v2026.2\nname: x\n  steps: [\n"

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", broken)

	out, _, err := runFmtCommand(t, path)
	if err == nil {
		t.Error("a file that does not parse was reported as formatted")
	}
	if got := string(readFixture(t, path)); got != broken {
		t.Errorf("a file that failed to parse was rewritten anyway:\n--- before\n%s\n--- after\n%s", broken, got)
	}
	if !strings.Contains(out, path) {
		t.Errorf("the failure does not name the file it could not format:\n%s", out)
	}
}

// TestFmtStdoutWritesTheResultAndLeavesTheFile covers piping the result
// somewhere else, which is only useful if the original stays put.
func TestFmtStdoutWritesTheResultAndLeavesTheFile(t *testing.T) {
	const src = `edition: v2026.2
name: greeter
steps:
  - id: greet
    log:
      message: hello world
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("the fixture does not compile: %v", err)
	}
	want, err := flowfile.Marshal(workflow)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, _, err := runFmtCommand(t, "--stdout", path)
	if err != nil {
		t.Fatalf("fmt --stdout: %v\n%s", err, out)
	}
	if out != string(want) {
		t.Errorf("--stdout wrote something other than Marshal's output:\n--- want\n%s\n--- got\n%s", want, out)
	}
	if got := string(readFixture(t, path)); got != src {
		t.Errorf("--stdout wrote back to the file as well:\n--- before\n%s\n--- after\n%s", src, got)
	}
}

// TestFmtStdoutKeepsReportsOffTheDocument checks that a parse failure under
// --stdout writes nothing to stdout, so a pipeline never receives a diagnostic
// where it expects a document.
func TestFmtStdoutKeepsReportsOffTheDocument(t *testing.T) {
	const broken = "edition: v2026.2\nname: x\n  steps: [\n"

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", broken)

	out, errOut, err := runFmtCommand(t, "--stdout", path)
	if err == nil {
		t.Error("a file that does not parse was reported as formatted")
	}
	if out != "" {
		t.Errorf("stdout is not empty for a file that failed to format:\n%s", out)
	}
	if !strings.Contains(errOut, path) {
		t.Errorf("the failure was not reported on stderr:\n%s", errOut)
	}
}

// TestFmtStdoutRefusesMoreThanOneFile keeps two documents from being run
// together into a stream that is neither of them.
func TestFmtStdoutRefusesMoreThanOneFile(t *testing.T) {
	dir := t.TempDir()
	first := writeFixture(t, dir, "first.yaml", oldStyleSingle)
	second := writeFixture(t, dir, "second.yaml", oldStyleSingle)

	out, _, err := runFmtCommand(t, "--stdout", first, second)
	if err == nil {
		t.Error("--stdout ran two documents together instead of refusing")
	}
	if out != "" {
		t.Errorf("--stdout wrote a document for a refused invocation:\n%s", out)
	}
}

// TestFmtStdoutAndCheckAreRefused pins the one flag combination that cannot be
// honored: one asks for the result and the other promises to produce nothing.
func TestFmtStdoutAndCheckAreRefused(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleSingle)

	out, _, err := runFmtCommand(t, "--stdout", "--check", path)
	if err == nil {
		t.Error("--stdout and --check were accepted together")
	}
	if out != "" {
		t.Errorf("a refused invocation wrote a document anyway:\n%s", out)
	}
}

// TestFmtDoesNotPrintUsageWhenAFileNeedsWork keeps the report readable: a file
// needing formatting is not a command someone typed wrong.
func TestFmtDoesNotPrintUsageWhenAFileNeedsWork(t *testing.T) {
	const src = `edition: v2026.2
name: greeter
steps:
  - id: greet
    log:
      message: hello world
`
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, errOut, err := runFmtCommand(t, "--check", path)
	if err == nil {
		t.Fatalf("the run was expected to report unfinished work:\n%s", out)
	}
	for stream, text := range map[string]string{"stdout": out, "stderr": errOut} {
		if strings.Contains(text, "Usage:") {
			t.Errorf("a file needing formatting drew a usage block on %s:\n%s", stream, text)
		}
	}
}

// currentStyleSingle is a small current-edition Flowfile with a comment and an
// indented list, so a directory walk over it has something to reformat and
// something to carry through.
const currentStyleSingle = `edition: v2026.2
name: single
steps:
  # a comment flow fmt carries through
  - id: greet
    log:
      message: hello
`

// TestFmtWalksADirectoryForFlowfiles checks which files a directory hands over,
// the same rule `flow fix` follows: both extensions, at any depth, and nothing
// else.
func TestFmtWalksADirectoryForFlowfiles(t *testing.T) {
	dir := t.TempDir()

	yamlPath := writeFixture(t, dir, "workflow.yaml", currentStyleSingle)
	ymlPath := writeFixture(t, dir, "other.yml", currentStyleSingle)
	deepPath := writeFixture(t, filepath.Join(dir, "nested"), "deep.yaml", currentStyleSingle)
	notAFlowfile := writeFixture(t, dir, "notes.txt", currentStyleSingle)

	out, _, err := runFmtCommand(t, dir)
	if err != nil {
		t.Fatalf("fmt: %v\n%s", err, out)
	}

	for _, path := range []string{yamlPath, ymlPath, deepPath} {
		got := string(readFixture(t, path))
		// The list comes back unindented, which is the reformatting; the comment
		// comes back with it, which is what the reformatting may not cost.
		if !strings.Contains(got, "\n- id: greet") {
			t.Errorf("%s was not reformatted, so it was not picked up by the walk:\n%s", path, got)
		}
		if !strings.Contains(got, "# a comment flow fmt carries through") {
			t.Errorf("%s lost its comment to the walk:\n%s", path, got)
		}
		if !strings.Contains(got, "log:") {
			t.Errorf("%s is no longer a Flowfile after formatting:\n%s", path, got)
		}
	}
	if got := string(readFixture(t, notAFlowfile)); got != currentStyleSingle {
		t.Errorf("a file that is not a Flowfile was rewritten by a directory walk:\n%s", got)
	}
}

// TestFmtKeepsEveryCommentInTheScaffoldItWasGiven is #381 as it was found: the
// formatter's first act on the file the CLI itself had just written was to
// report `reformatted` and delete every teaching comment in it.
//
// The scaffold is the fixture because it is the one Flowfile in this repository
// whose comments are the point of the file, and because it is what a first-time
// author runs `flow fmt` on.
func TestFmtKeepsEveryCommentInTheScaffoldItWasGiven(t *testing.T) {
	dir := t.TempDir()

	init := newInitCommand()
	init.SetOut(new(bytes.Buffer))
	init.SetErr(new(bytes.Buffer))
	init.SetArgs([]string{dir})
	if err := init.Execute(); err != nil {
		t.Fatalf("scaffolding: %v", err)
	}

	path := filepath.Join(dir, "workflow.yaml")
	scaffold := string(readFixture(t, path))

	var comments []string
	for _, line := range strings.Split(scaffold, "\n") {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "#") {
			comments = append(comments, trimmed)
		}
	}
	if len(comments) == 0 {
		t.Fatal("the scaffold has no comments, so this test proves nothing")
	}

	out, _, err := runFmtCommand(t, path)
	if err != nil {
		t.Fatalf("fmt refused the scaffold: %v\n%s", err, out)
	}

	formatted := string(readFixture(t, path))
	for _, comment := range comments {
		if !strings.Contains(formatted, comment) {
			t.Errorf("formatting the scaffold deleted a comment:\n%s\n--- the file it wrote\n%s", comment, formatted)
		}
	}

	// And the run really did rewrite the file, so the comments above did not
	// survive by nothing having happened.
	if formatted == scaffold {
		t.Fatal("the scaffold was already formatted, so nothing here was at risk")
	}
}

// TestFmtRefusesAFileWhoseCommentItCannotKeep is the fail-closed half at the
// command's own boundary.
//
// A mapping of expressions is written back as a single expression, so a comment
// inside one has no key left to sit above. The file is reported and left exactly
// as it was rather than rewritten without the comment: a formatter choosing
// between wrong output and no output chooses no output.
func TestFmtRefusesAFileWhoseCommentItCannotKeep(t *testing.T) {
	const src = `edition: v2026.2
name: report
steps:
  - id: report
    log:
      message: done
      fields:
        # which value survived escaping
        q: ${"x"}
`
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, _, err := runFmtCommand(t, path)
	if err == nil {
		t.Error("a file whose comment cannot be kept was reported as formatted")
	}
	if got := string(readFixture(t, path)); got != src {
		t.Errorf("a refused file was rewritten anyway:\n--- before\n%s\n--- after\n%s", src, got)
	}
	if !strings.Contains(out, path) {
		t.Errorf("the refusal does not name the file:\n%s", out)
	}
	if !strings.Contains(out, "8:9") {
		t.Errorf("the refusal does not position the comment that caused it:\n%s", out)
	}
	if !strings.Contains(out, "comment cannot be kept") {
		t.Errorf("the refusal does not say what stopped it:\n%s", out)
	}
}
