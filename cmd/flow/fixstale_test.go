package main

import (
	"encoding/json"
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
