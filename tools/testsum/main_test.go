package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The fixtures under testdata/ are recorded `go test -json` streams from a
// throwaway module at /home/dev/fixture (module example.com/fixture) on Go
// 1.27, one per shape the tool has to read: a clean run, assertion failures,
// a panic, a timeout, and a package that does not build. Recorded rather than
// hand-written, because the shapes are the toolchain's — a timed-out test gets
// no terminal event, a panic's dump is attributed to the test, a build
// failure arrives as events keyed by ImportPath — and a stream written from
// memory would encode what the author believed rather than what `go test`
// does.
func fixtureOptions() options {
	return options{
		moduleDir: ".",
		cwd:       "/home/dev/fixture",
		module:    "example.com/fixture",
	}
}

func summarize(t *testing.T, fixture string, opts options) (int, string) {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", fixture))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out bytes.Buffer
	code, err := run(f, &out, opts)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return code, out.String()
}

func mustContain(t *testing.T, out string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func mustNotContain(t *testing.T, out string, unwanted ...string) {
	t.Helper()
	for _, s := range unwanted {
		if strings.Contains(out, s) {
			t.Errorf("output names %q, which it should not:\n%s", s, out)
		}
	}
}

// TestAPassingRunIsCountedNotListed is the shape of the common case: one line,
// the counts, exit 0, and not one test name.
func TestAPassingRunIsCountedNotListed(t *testing.T) {
	code, out := summarize(t, "pass.json", fixtureOptions())
	if code != 0 {
		t.Fatalf("exit %d for a passing run:\n%s", code, out)
	}
	mustContain(t, out, "testsum: 4 passed, 1 skipped, 0 failed; 2 package(s), 0 failed")
	mustNotContain(t, out, "TestOne", "TestTwo", "TestThree", "TestSub", "FAIL", "rerun:")
}

// TestAnAssertionFailureNamesTheFileAndLine: the assertion lines the test
// printed, with the package directory in front of the bare file name the
// stream carries, and a rerun line carrying the shuffle seed. The parent of a
// failing subtest is counted but not listed, since its entry would say only
// what the subtest's says; the passing test in the same package is not named.
func TestAnAssertionFailureNamesTheFileAndLine(t *testing.T) {
	code, out := summarize(t, "fail.json", fixtureOptions())
	if code != 1 {
		t.Fatalf("exit %d for a failing run:\n%s", code, out)
	}
	mustContain(t, out,
		"testsum: 1 passed, 0 skipped, 3 failed; 1 package(s), 1 failed",
		"FAIL example.com/fixture/fail TestFails",
		"fail/fail_test.go:9: want 1, got 2",
		"fail/fail_test.go:10: second problem",
		"rerun: go test -shuffle=1788697682299198473 -run '^TestFails$' ./fail/",
		"FAIL example.com/fixture/fail TestParent/child",
		"fail/fail_test.go:15: child broke",
	)
	// The t.Log line is context the stream does not mark as an error, so
	// it is not an assertion line; and the bare parent is not an entry.
	mustNotContain(t, out, "about to check", "TestPasses", "FAIL example.com/fixture/fail TestParent\n")
}

// TestAMultiLineAssertionKeepsItsContinuationLines is the shape an
// assert.Equal diff arrives in: the first line is `file.go:6: the document
// differs:` and everything a reader needs is on the continuation lines the
// stream marks "error-continue". The first version of this tool printed the
// first line alone, which for the repository's own docs-drift test was
// `cmd/flow/docs_test.go:91: ` and nothing else. A t.Log's continuation is
// not marked and is not kept.
func TestAMultiLineAssertionKeepsItsContinuationLines(t *testing.T) {
	code, out := summarize(t, "multiline.json", fixtureOptions())
	if code != 1 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	mustContain(t, out,
		"FAIL example.com/fixture/multi TestMultiLineMessage\n"+
			"    multi/multi_test.go:6: the document differs:\n"+
			"        --- want\n"+
			"        +++ got\n"+
			"        -alpha\n"+
			"        +beta\n"+
			"    multi/multi_test.go:8: and one more\n",
	)
	mustNotContain(t, out, "a log line", "with a continuation")

	// The same stream with the marks stripped is what an older toolchain
	// prints; the continuations are then kept by their indentation, and
	// the log line, which nothing distinguishes, is kept too rather than
	// risk dropping an assertion.
	data, err := os.ReadFile(filepath.Join("testdata", "multiline.json"))
	if err != nil {
		t.Fatal(err)
	}
	unmarked := regexp.MustCompile(`,"OutputType":"error(?:-continue)?"`).ReplaceAllString(string(data), "")
	var buf bytes.Buffer
	if code, err := run(strings.NewReader(unmarked), &buf, fixtureOptions()); err != nil || code != 1 {
		t.Fatalf("exit %d, %v:\n%s", code, err, buf.String())
	}
	mustContain(t, buf.String(),
		"    multi/multi_test.go:6: the document differs:\n        --- want\n",
		"    multi/multi_test.go:7: a log line\n        with a continuation\n",
		"    multi/multi_test.go:8: and one more\n",
	)
}

// TestAPanicIsAttributedToItsTestWithTheFrameInsideIt: the panic line, and the
// frame from the goroutine dump that is in the test itself rather than in
// testing.go, made relative to the module.
func TestAPanicIsAttributedToItsTestWithTheFrameInsideIt(t *testing.T) {
	code, out := summarize(t, "panic.json", fixtureOptions())
	if code != 1 {
		t.Fatalf("exit %d for a panicking run:\n%s", code, out)
	}
	mustContain(t, out,
		"testsum: 2 passed, 0 skipped, 1 failed; 1 package(s), 1 failed",
		"PANIC example.com/fixture/panic TestPanics",
		"panic: assignment to entry in nil map",
		"goroutine 9 [running] at panic/panic_test.go:9",
		"rerun: go test -shuffle=1788697682594452310 -run '^TestPanics$' ./panic/",
	)
	mustNotContain(t, out, "TestBefore", "TestAfter", "/home/dev/fixture", "testing.go")
}

// TestATimeoutIsTheTestThatNeverFinished is the shape that has to be inferred:
// the stream carries no fail event for the hanging test, only the panic text
// and the package's fail, so the test that was started and not finished is
// the one reported, with the goroutine that was blocked inside it.
func TestATimeoutIsTheTestThatNeverFinished(t *testing.T) {
	code, out := summarize(t, "timeout.json", fixtureOptions())
	if code != 1 {
		t.Fatalf("exit %d for a timed-out run:\n%s", code, out)
	}
	mustContain(t, out,
		"testsum: 1 passed, 0 skipped, 0 failed; 1 package(s), 1 failed",
		"TIMEOUT example.com/fixture/timeout TestHangs",
		"panic: test timed out after 1s",
		"goroutine 6 [sleep] at timeout/timeout_test.go:11",
		"rerun: go test -shuffle=1788697682909305391 -run '^TestHangs$' ./timeout/",
	)
	mustNotContain(t, out, "TestQuick")
}

// TestABuildFailureIsReportedWithTheDiagnostic: no test ran, the package
// failed, and the compiler's line is what the reader needs.
func TestABuildFailureIsReportedWithTheDiagnostic(t *testing.T) {
	code, out := summarize(t, "buildfail.json", fixtureOptions())
	if code != 1 {
		t.Fatalf("exit %d for a build failure:\n%s", code, out)
	}
	mustContain(t, out,
		"testsum: 0 passed, 0 skipped, 0 failed; 1 package(s), 1 failed",
		"BUILD example.com/fixture/buildfail",
		`buildfail/buildfail_test.go:6:14: cannot use "not an int"`,
	)
}

// TestGitHubAnnotationsAndSummary: under Actions, one ::error per failure
// carrying the repository-relative file and line, the lines as the message,
// and the seed; and a table appended to the step summary. The module
// directory is prefixed, since a plugin module's tests run from its own
// directory and the Files tab wants the repository's path.
func TestGitHubAnnotationsAndSummary(t *testing.T) {
	var summary bytes.Buffer
	opts := fixtureOptions()
	opts.moduleDir = "plugins/example"
	opts.github = true
	opts.summary = &summary

	code, out := summarize(t, "fail.json", opts)
	if code != 1 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	mustContain(t, out,
		"::error file=plugins/example/fail/fail_test.go,line=9,title=example.com/fixture/fail.TestFails fail::fail/fail_test.go:9: want 1, got 2%0Afail/fail_test.go:10: second problem%0Ashuffle seed 1788697682299198473\n",
		"::error file=plugins/example/fail/fail_test.go,line=15,title=example.com/fixture/fail.TestParent/child fail::",
	)
	if n := strings.Count(out, "::error "); n != 2 {
		t.Errorf("%d annotations, want 2 (one per listed failure):\n%s", n, out)
	}
	mustContain(t, summary.String(),
		"### go test: 1 passed, 0 skipped, 3 failed (1 package(s), 1 failed)",
		"| package | test | kind | where | shuffle seed |",
		"| `example.com/fixture/fail` | `TestFails` | fail | `plugins/example/fail/fail_test.go:9` | `1788697682299198473` |",
	)

	// A panic's annotation points at the frame inside the test, and a
	// timeout's at the blocked frame — the Files tab then shows the
	// failure on the line that hung.
	_, out = summarize(t, "timeout.json", opts)
	mustContain(t, out, "::error file=plugins/example/timeout/timeout_test.go,line=11,title=example.com/fixture/timeout.TestHangs timeout::")

	// And nothing of this outside Actions.
	plain := fixtureOptions()
	_, out = summarize(t, "fail.json", plain)
	mustNotContain(t, out, "::error")
}

// TestAPassingRunStillWritesTheSummaryCount: the step summary carries the
// count on a green run too, so "nothing listed" is distinguishable from
// "nothing ran".
func TestAPassingRunStillWritesTheSummaryCount(t *testing.T) {
	var summary bytes.Buffer
	opts := fixtureOptions()
	opts.github = true
	opts.summary = &summary
	if code, out := summarize(t, "pass.json", opts); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	mustContain(t, summary.String(), "### go test: 4 passed, 1 skipped, 0 failed (2 package(s), 0 failed)")
	mustNotContain(t, summary.String(), "| package |")
}

// TestAStreamCutOffMidTestIsNotAPass. A runner's job timeout, an OOM kill, a
// signal: the stream just stops. A test that was running and a package that
// never reported are a failure, not zero failures.
func TestAStreamCutOffMidTestIsNotAPass(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "fail.json"))
	if err != nil {
		t.Fatal(err)
	}
	cut := strings.Index(string(data), `"Action":"fail","Package":"example.com/fixture/fail","Test":"TestParent/child"`)
	if cut < 0 {
		t.Fatal("fixture no longer has the subtest fail event this test cuts before")
	}
	head := string(data)[:strings.LastIndex(string(data)[:cut], "\n")+1]

	var out bytes.Buffer
	code, err := run(strings.NewReader(head), &out, fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	if code != 1 {
		t.Fatalf("exit %d for a stream that stopped mid-test:\n%s", code, out.String())
	}
	mustContain(t, out.String(),
		"ABORTED example.com/fixture/fail TestParent/child",
		"the stream ended before this test finished",
	)
}

// TestNonJSONLinesPassThroughAndAreCounted: a line that is not an event is
// something upstream said, shown rather than hidden, and the count line says
// it happened.
func TestNonJSONLinesPassThroughAndAreCounted(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "pass.json"))
	if err != nil {
		t.Fatal(err)
	}
	stream := "go: downloading example.com/dep v1.2.3\n" + string(data)
	var out bytes.Buffer
	code, err := run(strings.NewReader(stream), &out, fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out.String())
	}
	mustContain(t, out.String(),
		"go: downloading example.com/dep v1.2.3\n",
		"1 non-JSON line(s) passed through above",
	)
}

// TestOutputHeldForARunningTestIsBounded. A test that logs in a loop before
// failing would otherwise be held whole until its fail event; the buffer
// keeps the head and the newest tail and says how much it dropped, and the
// assertion at the end survives.
func TestOutputHeldForARunningTestIsBounded(t *testing.T) {
	var stream strings.Builder
	const pkg = "example.com/fixture/loud"
	fmt.Fprintf(&stream, `{"Action":"start","Package":%q}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"run","Package":%q,"Test":"TestLoud"}`+"\n", pkg)
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&stream, `{"Action":"output","Package":%q,"Test":"TestLoud","Output":"    loud_test.go:7: line %d\n"}`+"\n", pkg, i)
	}
	fmt.Fprintf(&stream, `{"Action":"output","Package":%q,"Test":"TestLoud","Output":"    loud_test.go:9: the assertion\n","OutputType":"error"}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"fail","Package":%q,"Test":"TestLoud","Elapsed":0.1}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"fail","Package":%q,"Elapsed":0.1}`+"\n", pkg)

	var out bytes.Buffer
	code, err := run(strings.NewReader(stream.String()), &out, fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	if code != 1 {
		t.Fatalf("exit %d:\n%s", code, out.String())
	}
	mustContain(t, out.String(),
		"FAIL example.com/fixture/loud TestLoud",
		"loud/loud_test.go:9: the assertion",
		fmt.Sprintf("(%d output line(s) dropped past the %d-line bound)", 5000+1-maxLines, maxLines),
	)
	if lines := strings.Count(out.String(), "\n"); lines > maxLines {
		t.Errorf("the summary is %d lines for one failure; the bound did not hold", lines)
	}
}

// TestAFailuresLinesAreBounded: an assertion library that prints the whole
// expected document on one line, or a diff hundreds of lines long, is cut to
// a readable summary that says what it cut; the raw stream has the rest.
func TestAFailuresLinesAreBounded(t *testing.T) {
	var stream strings.Builder
	const pkg = "example.com/fixture/wide"
	long := strings.Repeat("x", 5000)
	fmt.Fprintf(&stream, `{"Action":"start","Package":%q}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"run","Package":%q,"Test":"TestWide"}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"output","Package":%q,"Test":"TestWide","Output":"    wide_test.go:9: not equal\n","OutputType":"error"}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"output","Package":%q,"Test":"TestWide","Output":"        expected: %s\n","OutputType":"error-continue"}`+"\n", pkg, long)
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&stream, `{"Action":"output","Package":%q,"Test":"TestWide","Output":"        -line %d\n","OutputType":"error-continue"}`+"\n", pkg, i)
	}
	fmt.Fprintf(&stream, `{"Action":"fail","Package":%q,"Test":"TestWide","Elapsed":0.1}`+"\n", pkg)
	fmt.Fprintf(&stream, `{"Action":"fail","Package":%q,"Elapsed":0.1}`+"\n", pkg)

	var out bytes.Buffer
	if code, err := run(strings.NewReader(stream.String()), &out, fixtureOptions()); err != nil || code != 1 {
		t.Fatalf("exit %d, %v:\n%s", code, err, out.String())
	}
	mustContain(t, out.String(),
		"wide/wide_test.go:9: not equal",
		fmt.Sprintf(" …(%d more bytes)", len("        expected: "+long)-len("        ")+4-maxLineLen),
		fmt.Sprintf("…(%d more line(s); the raw go test -json stream has them)", 2+100-maxFailureLines),
	)
	if lines := strings.Count(out.String(), "\n"); lines > maxFailureLines+6 {
		t.Errorf("one failure printed %d lines", lines)
	}
	for _, l := range strings.Split(out.String(), "\n") {
		if len(l) > maxLineLen+40 {
			t.Errorf("a printed line is %d bytes long", len(l))
		}
	}
}

// TestAPackageThatFailsOutsideAnyTestIsReported: a TestMain that exits, a
// panic in init — no test failed and none was running, and the package's own
// output is what there is to show.
func TestAPackageThatFailsOutsideAnyTestIsReported(t *testing.T) {
	const pkg = "example.com/fixture/init"
	stream := strings.Join([]string{
		`{"Action":"start","Package":"` + pkg + `"}`,
		`{"Action":"output","Package":"` + pkg + `","Output":"-test.shuffle 42\n"}`,
		`{"Action":"output","Package":"` + pkg + `","Output":"TestMain: refusing to start without FIXTURE_DB\n"}`,
		`{"Action":"output","Package":"` + pkg + `","Output":"FAIL\texample.com/fixture/init\t0.010s\n"}`,
		`{"Action":"fail","Package":"` + pkg + `","Elapsed":0.01}`,
	}, "\n") + "\n"
	var out bytes.Buffer
	code, err := run(strings.NewReader(stream), &out, fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	if code != 1 {
		t.Fatalf("exit %d:\n%s", code, out.String())
	}
	mustContain(t, out.String(),
		"PACKAGE example.com/fixture/init",
		"TestMain: refusing to start without FIXTURE_DB",
	)
	mustNotContain(t, out.String(), "-test.shuffle 42", "FAIL\texample.com")
}
