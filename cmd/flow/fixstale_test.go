package main

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `flow fix` leaves a pin alone and rewrites callees, which between them is how
// a run makes a pin stale without touching it. These tests are #640's second
// half: the run that caused the staleness says so, names the digest to adopt,
// and fails — rather than leaving the author to hear about it from whatever they
// happened to run next.
//
// The direction each of these has to get right is the one a "does it report"
// test does not: a pin that was *already* stale, or one that is still good, must
// produce nothing at all, because a report that fires on either is a report
// nobody can act on.

// pinnedCaller is a current-edition file calling ./callee.yaml and pinning it.
func pinnedCaller(pin string) string {
	return `edition: v2026.3
name: caller
steps:
  - id: run
    call: ./callee.yaml
    digest: ` + pin + `
`
}

// TestFixReportsThePinItsOwnRewriteInvalidated is the case the issue describes:
// a directory-wide run legitimately rewrites a callee, and the caller's pin
// names bytes that no longer exist anywhere.
func TestFixReportsThePinItsOwnRewriteInvalidated(t *testing.T) {
	dir := t.TempDir()
	callee := writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	caller := writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte(oldStyleGreeter))))

	out, _, err := runFixCommand(t, dir)
	if err == nil {
		t.Fatal("a run that invalidated a pin exited zero, so `flow fix . && git commit` would commit a tree whose call no longer compiles")
	}
	if !strings.Contains(err.Error(), "digest:") {
		t.Errorf("the exit reason does not say what is left to do: %v", err)
	}

	// The callee really was rewritten, so the staleness is a fact about the
	// tree rather than an artifact of the fixture.
	rewritten := readFixture(t, callee)
	if string(rewritten) == oldStyleGreeter {
		t.Fatal("premise: the callee was not rewritten, so nothing could have gone stale")
	}

	// Named where the pin is (line 6 of the caller), and carrying the digest to
	// adopt, so acting on it is a paste rather than a hash computed by hand.
	if !strings.Contains(out, caller+":6:") {
		t.Errorf("the report does not position the stale pin at the caller's `digest:` line:\n%s", out)
	}
	if !strings.Contains(out, v1.ContentDigest(rewritten)) {
		t.Errorf("the report does not name the digest to adopt:\n%s", out)
	}
	for _, want := range []string{"this run rewrote ./callee.yaml", `pin on step "run"`, "no longer names the bytes it pins"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not say %q:\n%s", want, out)
		}
	}

	// The caller itself is untouched: `flow fix` reports a pin it invalidated
	// and never re-stamps one, because a pin is the caller saying it read those
	// bytes.
	if got := string(readFixture(t, caller)); got != pinnedCaller(v1.ContentDigest([]byte(oldStyleGreeter))) {
		t.Errorf("the run rewrote the pin instead of reporting it:\n%s", got)
	}
}

// TestFixCheckReportsThePinItWouldInvalidate is the same finding in the tense
// `--check` earns: nothing was written, so nothing is stale yet.
func TestFixCheckReportsThePinItWouldInvalidate(t *testing.T) {
	dir := t.TempDir()
	callee := writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte(oldStyleGreeter))))

	out, _, err := runFixCommand(t, "--check", dir)
	if err == nil {
		t.Fatal("--check found work to do and exited zero")
	}
	if !strings.Contains(out, "this run would rewrite ./callee.yaml") {
		t.Errorf("--check reported a rewrite it did not make as one it did:\n%s", out)
	}
	if strings.Contains(out, "this run rewrote ./callee.yaml") {
		t.Errorf("--check claimed to have rewritten something:\n%s", out)
	}
	if got := string(readFixture(t, callee)); got != oldStyleGreeter {
		t.Error("--check wrote to the callee it was only asked to report on")
	}
}

// TestFixSaysNothingAboutAPinThatWasAlreadyStale is the negative direction that
// keeps the report actionable. A pin naming bytes the callee never had is
// somebody else's news, and attributing it to whoever next ran `flow fix` would
// make every long-broken pin in a tree look like their doing.
func TestFixSaysNothingAboutAPinThatWasAlreadyStale(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte("bytes this callee never had"))))

	out, _, err := runFixCommand(t, dir)
	if err != nil {
		t.Fatalf("a run whose own rewrite invalidated nothing failed: %v\n%s", err, out)
	}
	if strings.Contains(out, "digest:") {
		t.Errorf("a pin that was already stale was reported as this run's doing:\n%s", out)
	}
}

// TestFixSaysNothingWhenNothingWasRewritten is the other half of that: a
// current tree with a good pin in it produces no report and no cost — the scan
// stops before reading a single file when no file changed.
func TestFixSaysNothingWhenNothingWasRewritten(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "callee.yaml", currentGreeter)
	writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte(currentGreeter))))

	out, _, err := runFixCommand(t, dir)
	if err != nil {
		t.Fatalf("a run over a current tree failed: %v\n%s", err, out)
	}
	if strings.Contains(out, "digest:") {
		t.Errorf("a run that rewrote nothing reported a stale pin:\n%s", out)
	}
}

// TestFixReportsAStalePinInTheMachineFormatToo holds the rule the text and JSON
// forms are written under: they are two renderings of one run, and a fact only
// one of them carries is a report that has already drifted.
func TestFixReportsAStalePinInTheMachineFormatToo(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	caller := writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte(oldStyleGreeter))))

	out, _, err := runFixCommand(t, "--check", "-o", "json", dir)
	if err == nil {
		t.Fatal("--check found work to do and exited zero")
	}

	var reports struct {
		Files []struct {
			File      string `json:"file"`
			StalePins []struct {
				Line    uint32 `json:"line"`
				Message string `json:"message"`
			} `json:"stalePins"`
		} `json:"files"`
	}
	if err := json.Unmarshal([]byte(out), &reports); err != nil {
		t.Fatalf("the report is not JSON: %v\n%s", err, out)
	}

	var found bool
	for _, file := range reports.Files {
		if file.File != caller {
			if len(file.StalePins) != 0 {
				t.Errorf("%s carries a stale pin that belongs to another file", file.File)
			}
			continue
		}
		if len(file.StalePins) != 1 {
			t.Fatalf("the caller's report carries %d stale pins, want 1: %s", len(file.StalePins), out)
		}
		found = true
		if file.StalePins[0].Line != 6 {
			t.Errorf("the stale pin is reported at line %d, want the `digest:` line", file.StalePins[0].Line)
		}
		if !strings.Contains(file.StalePins[0].Message, "./callee.yaml") {
			t.Errorf("the stale pin does not name the callee: %s", file.StalePins[0].Message)
		}
	}
	if !found {
		t.Errorf("no report was written for %s:\n%s", caller, out)
	}
}

// TestFixReportsAStalePinWhenArgumentsOverlap is the finding on #833: naming a
// file both directly and through a directory containing it used to process it
// twice, and the second pass — reading the document the first pass had already
// rewritten — recorded "nothing to do" over the first pass's answer. The
// staleness scan reads that answer to know which callees moved, so the caller
// went unreported and the run exited zero on a tree whose call no longer
// compiles.
//
// Mutation proof: dropping the deduplication in collectFlowfiles fails this
// test with err == nil, which is exactly the shape the finding describes.
func TestFixReportsAStalePinWhenArgumentsOverlap(t *testing.T) {
	dir := t.TempDir()
	callee := writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	writeFixture(t, dir, "caller.yaml", pinnedCaller(v1.ContentDigest([]byte(oldStyleGreeter))))

	// The directory first, so the walk rewrites the callee before the explicit
	// occurrence of it is reached — the order the finding is about.
	out, _, err := runFixCommand(t, dir, callee)
	if err == nil {
		t.Fatalf("overlapping arguments hid the pin this run invalidated, and the run exited zero:\n%s", out)
	}
	if !strings.Contains(out, "this run rewrote ./callee.yaml") {
		t.Errorf("the stale pin was not reported when the callee was named twice:\n%s", out)
	}

	// And the callee is rewritten once rather than read a second time and
	// reported as needing nothing: a file named two ways is one file.
	if strings.Contains(out, callee+": already current") {
		t.Errorf("the callee was processed a second time, which is what hid the finding:\n%s", out)
	}
}

// TestFixWithStdoutAcceptsTheSameFileNamedTwice is the same deduplication seen
// from the flag that counts files: `--stdout` writes one document, and a file
// named twice is one file, not two.
func TestFixWithStdoutAcceptsTheSameFileNamedTwice(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", currentGreeter)

	out, _, err := runFixCommand(t, "--stdout", path, path)
	if err != nil {
		t.Fatalf("one file named twice was refused as two: %v", err)
	}
	if out != currentGreeter {
		t.Errorf("--stdout wrote something other than the one document:\n%s", out)
	}
}

// TestFixOutcomeHoldsNoDocumentBodies is the bound the third finding on #833
// asked for, written where it can be checked rather than only asserted in prose.
//
// One fixOutcome is retained per file for the whole invocation. A Flowfile is
// capped at a mebibyte, but the number of them in a directory is the user's tree
// to decide — the resource an outside party controls — so an outcome that held
// the document's bytes would make `flow fix` over a large generated tree scale
// with the whole tree at once. It holds two digests instead, and
// [findStalePins] reads one file at a time.
//
// Reflective because that is the property: not "these particular fields", but
// that nothing retained per file grows with the size of the file.
func TestFixOutcomeHoldsNoDocumentBodies(t *testing.T) {
	outcome := reflect.TypeOf(fixOutcome{})
	for i := range outcome.NumField() {
		field := outcome.Field(i)
		if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8 {
			t.Errorf("fixOutcome.%s holds a document body; one of these is kept per file for the "+
				"whole run, so it must hold only what does not grow with a file's size", field.Name)
		}
	}
}

// TestFixReportsStalePinsAcrossManyCallers is the functional half of the same
// change: the scan now re-reads each caller rather than being handed bytes it
// kept, so a run with several callers has to find all of them — and find them
// from what is on disk, which under a writing run is the rewritten document.
func TestFixReportsStalePinsAcrossManyCallers(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "callee.yaml", oldStyleGreeter)
	pin := v1.ContentDigest([]byte(oldStyleGreeter))
	const callers = 5
	for i := range callers {
		writeFixture(t, dir, "caller"+strconv.Itoa(i)+".yaml", pinnedCaller(pin))
	}

	out, _, err := runFixCommand(t, dir)
	if err == nil {
		t.Fatalf("a run that invalidated %d pins exited zero:\n%s", callers, out)
	}
	if got := strings.Count(out, "no longer names the bytes it pins"); got != callers {
		t.Errorf("reported %d stale pins, want %d:\n%s", got, callers, out)
	}
}
