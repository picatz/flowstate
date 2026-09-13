// Command testsum reads `go test -json` on standard input and prints what a
// reader of a failing run actually needs: each failing test with its package,
// the assertion lines it printed and the file:line those lines name, and a
// count of everything that passed. Passing tests are counted, not listed.
//
//	go test -json -shuffle=on ./... | go run ./tools/testsum
//
// `make test` and the local gate both pipe through it, so the local loop and
// CI print the same shape (#1727). Under GitHub Actions — GITHUB_ACTIONS set —
// it also emits one `::error file=…,line=…,title=…::` annotation per failing
// test, which the Files tab renders inline, and appends a table to the file
// GITHUB_STEP_SUMMARY names, so a reviewer on the PR page sees the failure
// without opening a five-minute log.
//
// Three shapes a failing test takes in the stream, each handled and each
// covered by a recorded fixture in testdata/: an assertion (`t.Errorf` and
// friends), whose lines the stream marks and which name a file:line; a panic,
// which the test binary recovers and attributes to the test along with the
// goroutine dump; and a timeout, which is a panic the binary does *not*
// attribute — the running test gets no terminal event at all, only the
// `panic: test timed out` text and a package-level fail — so a test that was
// started and never finished when its package failed is reported as timed out,
// with the goroutine that was inside it. A package that fails to build, or
// fails with no test failing (a TestMain that exits, a panic in init), is
// reported at the package level with the output it produced.
//
// The shuffle seed `go test -shuffle=on` prints per package is recorded and
// printed beside every failure, so a failure that depends on order is
// reproducible from the annotation rather than from a log that has scrolled
// away.
//
// Exit status is 1 when anything failed and 0 otherwise. It reads one stream
// and holds output only for tests still running, bounded per test, so a
// five-minute suite costs a few kilobytes of memory here rather than its log.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	dir := flag.String("dir", ".", "the tested module's directory relative to the repository root, prefixed to file paths in annotations (a plugin module is not the repository root)")
	flag.Parse()

	// The working directory is what every file:line is made relative to,
	// and an annotation on the wrong path is worse than none: refused
	// rather than left empty.
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "testsum: reading the working directory:", err)
		os.Exit(2)
	}
	opts := options{
		moduleDir: *dir,
		cwd:       cwd,
		module:    modulePath(cwd),
		github:    os.Getenv("GITHUB_ACTIONS") != "",
	}
	if p := os.Getenv("GITHUB_STEP_SUMMARY"); p != "" && opts.github {
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "testsum:", err)
			os.Exit(2)
		}
		defer f.Close()
		opts.summary = f
	}

	code, err := run(os.Stdin, os.Stdout, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testsum:", err)
		os.Exit(2)
	}
	os.Exit(code)
}

// modulePath reads the module directive of the go.mod in dir, or "" when there
// is none. It is how an import path in the stream becomes a directory in the
// tree: the assertion lines `go test` prints name a bare file, and the file
// lives in the package's directory.
func modulePath(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return ""
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// options is everything run needs from its environment, passed in so the
// tests can hand it a recorded stream and a buffer.
type options struct {
	// moduleDir is where the tested module sits relative to the repository
	// root; "." for the root module.
	moduleDir string
	// cwd is the directory `go test` ran in, used to make the absolute paths
	// in a goroutine dump relative.
	cwd string
	// module is the tested module's path, used to map a package import path
	// to its directory.
	module string
	// github turns on the annotations; summary, when non-nil, receives the
	// step-summary table.
	github  bool
	summary io.Writer
}

// event is one line of `go test -json`, the fields this program reads. Build
// events (Go 1.24+) carry ImportPath rather than Package; the package-level
// fail that follows names the build in FailedBuild.
type event struct {
	Action      string
	Package     string
	Test        string
	Output      string
	OutputType  string
	Elapsed     float64
	ImportPath  string
	FailedBuild string
}

// A failure is one entry in the printed summary.
type failure struct {
	Package string
	Test    string // empty for a package-level failure
	Kind    string // "fail", "panic", "timeout", "build", "package", "aborted"
	Lines   []string
	// File and Line are where the failure points, File relative to the
	// tested module's directory; Line is 0 when nothing named one.
	File    string
	Line    int
	Seed    string
	Elapsed float64
}

// line is one buffered output line. marked is the stream's own word that
// this is what t.Error and friends printed (OutputType "error" for the first
// line of a message and "error-continue" for the rest, Go 1.27 and later),
// which separates an assertion from a t.Log beside it.
type line struct {
	text   string
	marked bool
}

// isContinuation reports whether a line is the rest of a multi-line message:
// the testing package indents a message's first line by four spaces after
// the file:line and every further line by eight. A diff in an assertion
// message — the common shape — is all continuation lines, and dropping them
// leaves `file.go:91: ` with nothing after the colon.
func isContinuation(text string) bool {
	return strings.HasPrefix(text, "        ")
}

// maxLines bounds the output held for one running test or one package. A test
// that logs in a loop would otherwise hold its whole log here until it ends; a
// reader of a failure needs the assertion lines, not the loop. The first and
// last halves are kept, since a failure's own lines are usually at the end and
// the context is at the start.
const maxLines = 400

// maxLineLen bounds one printed line of a failure, and maxFailureLines bounds
// the lines one failure prints. An assertion library that prints the whole
// expected document on one line — a rendered reference page, a catalog —
// makes the summary and the annotation unreadable, and the raw stream is
// what holds the rest: the artifact CI uploads, or the log locally.
const (
	maxLineLen      = 600
	maxFailureLines = 40
)

// bound applies those two limits to a failure's lines, saying what it cut.
func bound(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if len(l) > maxLineLen {
			l = l[:maxLineLen] + fmt.Sprintf(" …(%d more bytes)", len(l)-maxLineLen)
		}
		out = append(out, l)
	}
	// The marker takes the last slot, so a bounded failure prints exactly
	// maxFailureLines lines, the marker included, rather than one more.
	if len(out) > maxFailureLines {
		kept := maxFailureLines - 1
		more := len(out) - kept
		out = append(out[:kept], fmt.Sprintf("…(%d more line(s); the raw go test -json stream has them)", more))
	}
	return out
}

type packageState struct {
	seed    string
	running []string // tests started and not yet finished, in start order
	failed  map[string]bool
	output  map[string][]line // per test; "" for package-level lines
	dropped map[string]int    // lines dropped past maxLines, per test
}

type summarizer struct {
	opts     options
	packages map[string]*packageState
	builds   map[string][]string // build output by ImportPath

	passed, skipped, failed      int
	packagesSeen, packagesFailed int
	passthrough                  int

	failures []failure
}

// run reads the stream and writes the summary. The int is the exit status.
func run(r io.Reader, w io.Writer, opts options) (int, error) {
	s := &summarizer{opts: opts, packages: map[string]*packageState{}, builds: map[string][]string{}}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		raw := sc.Bytes()
		if len(raw) == 0 || raw[0] != '{' {
			// Not JSON: something upstream printed to stdout around
			// the stream. Shown, since hiding it would hide a build
			// error on an older toolchain, and counted.
			s.passthrough++
			fmt.Fprintln(w, string(raw))
			continue
		}
		var ev event
		if err := json.Unmarshal(raw, &ev); err != nil {
			s.passthrough++
			fmt.Fprintln(w, string(raw))
			continue
		}
		s.handle(ev)
	}
	if err := sc.Err(); err != nil {
		return 2, fmt.Errorf("reading go test -json: %w", err)
	}

	// A stream that ended with tests still running and no package verdict
	// is a run that was killed — by a signal, an OOM, a runner's timeout.
	// Not a pass.
	for _, pkg := range sortedPackages(s.packages) {
		st := s.packages[pkg]
		for _, test := range st.running {
			s.failures = append(s.failures, failure{Package: pkg, Test: test, Kind: "aborted", Seed: st.seed,
				Lines: []string{"the stream ended before this test finished and before its package reported"}})
		}
		if len(st.running) > 0 {
			s.packagesFailed++
		}
	}

	for i := range s.failures {
		s.failures[i].Lines = bound(s.failures[i].Lines)
	}

	s.print(w)
	if s.opts.github {
		s.annotate(w)
		if s.opts.summary != nil {
			s.stepSummary(s.opts.summary)
		}
	}
	if len(s.failures) > 0 || s.packagesFailed > 0 {
		return 1, nil
	}
	return 0, nil
}

func (s *summarizer) state(pkg string) *packageState {
	st, ok := s.packages[pkg]
	if !ok {
		st = &packageState{failed: map[string]bool{}, output: map[string][]line{}, dropped: map[string]int{}}
		s.packages[pkg] = st
	}
	return st
}

func (s *summarizer) handle(ev event) {
	switch ev.Action {
	case "build-output":
		s.builds[ev.ImportPath] = append(s.builds[ev.ImportPath], strings.TrimRight(ev.Output, "\n"))
		return
	case "build-fail":
		return
	}
	if ev.Package == "" {
		return
	}
	st := s.state(ev.Package)

	switch ev.Action {
	case "start":
		s.packagesSeen++

	case "output":
		text := strings.TrimRight(ev.Output, "\n")
		if ev.Test == "" {
			if seed, ok := strings.CutPrefix(strings.TrimSpace(text), "-test.shuffle "); ok {
				st.seed = seed
			}
		}
		l := line{text: text, marked: ev.OutputType == "error" || ev.OutputType == "error-continue"}
		if buf := st.output[ev.Test]; len(buf) >= maxLines {
			// Keep the head and the newest tail: drop from the
			// middle so both the setup and the assertion survive.
			copy(buf[maxLines/2:], buf[maxLines/2+1:])
			buf[maxLines-1] = l
			st.dropped[ev.Test]++
			return
		}
		st.output[ev.Test] = append(st.output[ev.Test], l)

	case "run":
		st.running = append(st.running, ev.Test)

	case "pass", "skip":
		if ev.Test == "" {
			delete(s.packages, ev.Package)
			return
		}
		if ev.Action == "pass" {
			s.passed++
		} else {
			s.skipped++
		}
		st.finish(ev.Test)
		delete(st.output, ev.Test)
		delete(st.dropped, ev.Test)

	case "fail":
		if ev.Test != "" {
			s.failed++
			st.failed[ev.Test] = true
			st.finish(ev.Test)
			s.testFailed(ev.Package, st, ev)
			delete(st.output, ev.Test)
			delete(st.dropped, ev.Test)
			return
		}
		s.packageFailed(ev.Package, st, ev)
		delete(s.packages, ev.Package)
	}
}

func (st *packageState) finish(test string) {
	for i, t := range st.running {
		if t == test {
			st.running = append(st.running[:i], st.running[i+1:]...)
			return
		}
	}
}

var (
	// An assertion line as the testing package prints it: indented, the
	// bare file name, the line, a colon. OutputType marks these on Go 1.27
	// and later; the pattern is what finds them before that.
	assertionLine = regexp.MustCompile(`^\s+([^\s:]+\.go):(\d+): `)
	// A goroutine-dump frame: a tab, an absolute or relative path, a line.
	frameLine = regexp.MustCompile(`^\t(\S+\.go):(\d+)(?: \+0x[0-9a-f]+)?$`)
	// A compiler diagnostic: path:line:col: message.
	buildLine = regexp.MustCompile(`^(\S+\.go):(\d+):\d+: `)
)

// testFailed records a test's own failure from the output it produced.
func (s *summarizer) testFailed(pkg string, st *packageState, ev event) {
	out := st.output[ev.Test]
	f := failure{Package: pkg, Test: ev.Test, Kind: "fail", Seed: st.seed, Elapsed: ev.Elapsed}

	// The lines the failure is made of. Where the stream marks what
	// t.Error and friends printed, those are the lines, continuations
	// included; an older stream marks nothing, and there every file:line
	// line is kept with the continuation lines under it, since dropping
	// the assertion is worse than keeping a t.Log beside it. The file name
	// is bare in the stream and is printed with the package's directory in
	// front, which is the path a reader opens; a continuation keeps four
	// spaces of indent under its first line.
	anyMarked := false
	for _, l := range out {
		anyMarked = anyMarked || l.marked
	}
	dir := s.packageDir(pkg)
	var assertions []string
	keeping := false // inside a kept message, so its continuations are kept
	for _, l := range out {
		if strings.HasPrefix(l.text, "panic: ") {
			if strings.Contains(l.text, "test timed out") {
				f.Kind = "timeout"
			} else {
				f.Kind = "panic"
			}
			f.Lines = append(f.Lines, l.text)
			keeping = false
			continue
		}
		if m := assertionLine.FindStringSubmatch(l.text); m != nil {
			keeping = !anyMarked || l.marked
			if !keeping {
				continue
			}
			file := path.Join(dir, m[1])
			assertions = append(assertions, file+strings.TrimPrefix(strings.TrimLeft(l.text, " \t"), m[1]))
			if f.File == "" {
				f.File = file
				f.Line, _ = strconv.Atoi(m[2])
			}
			continue
		}
		if keeping && isContinuation(l.text) && (!anyMarked || l.marked) {
			assertions = append(assertions, "    "+strings.TrimLeft(l.text, " \t"))
			continue
		}
		keeping = false
	}
	f.Lines = append(f.Lines, assertions...)

	if f.Kind != "fail" {
		// The frame inside the test itself, when the dump has one: it
		// is the line the panic came from, or the line the timed-out
		// test was blocked on, and it is worth more than a frame in
		// testing.go.
		if file, n, goroutine := s.frameIn(pkg, ev.Test, out); file != "" {
			f.File, f.Line = file, n
			f.Lines = append(f.Lines, fmt.Sprintf("%s at %s:%d", goroutine, file, n))
		}
	}
	if st.dropped[ev.Test] > 0 {
		f.Lines = append(f.Lines, fmt.Sprintf("(%d output line(s) dropped past the %d-line bound)", st.dropped[ev.Test], maxLines))
	}

	if len(f.Lines) == 0 {
		// A parent that failed only because a subtest did adds nothing
		// the subtest's own entry does not say. A test that failed and
		// printed nothing is still listed: a silent failure is a
		// finding.
		for failed := range st.failed {
			if strings.HasPrefix(failed, ev.Test+"/") {
				return
			}
		}
		f.Lines = []string{"(the test failed without printing anything)"}
	}
	s.failures = append(s.failures, f)
}

// packageFailed records what a package-level fail means once its tests have
// been accounted for: a build failure, tests that never finished, or a package
// that failed with no test failing.
func (s *summarizer) packageFailed(pkg string, st *packageState, ev event) {
	s.packagesFailed++

	if ev.FailedBuild != "" {
		f := failure{Package: pkg, Kind: "build", Lines: s.builds[ev.FailedBuild]}
		for _, text := range f.Lines {
			if m := buildLine.FindStringSubmatch(text); m != nil {
				f.File = m[1]
				f.Line, _ = strconv.Atoi(m[2])
				break
			}
		}
		if len(f.Lines) == 0 {
			f.Lines = []string{"build failed: " + ev.FailedBuild}
		}
		s.failures = append(s.failures, f)
		return
	}

	// Tests still running when the package failed never got a terminal
	// event: the binary died underneath them. A timeout is the common
	// cause and the stream says so; anything else is reported as what it
	// is, an abort, with whatever the package printed.
	pkgOut := st.output[""]
	if len(st.running) > 0 {
		for _, test := range st.running {
			out := append(append([]line(nil), st.output[test]...), pkgOut...)
			f := failure{Package: pkg, Test: test, Kind: "aborted", Seed: st.seed}
			for _, l := range out {
				if strings.HasPrefix(l.text, "panic: ") {
					if strings.Contains(l.text, "test timed out") {
						f.Kind = "timeout"
					} else {
						f.Kind = "panic"
					}
					f.Lines = append(f.Lines, l.text)
					break
				}
			}
			if file, n, goroutine := s.frameIn(pkg, test, out); file != "" {
				f.File, f.Line = file, n
				f.Lines = append(f.Lines, fmt.Sprintf("%s at %s:%d", goroutine, file, n))
			}
			if len(f.Lines) == 0 {
				f.Lines = []string{"the package failed while this test was still running"}
			}
			s.failures = append(s.failures, f)
		}
		return
	}

	if len(st.failed) > 0 {
		// Its tests' failures are already listed.
		return
	}

	// Nothing failed and nothing was running: the package itself did,
	// outside any test. Show what it said, minus the verdict lines.
	f := failure{Package: pkg, Kind: "package", Seed: st.seed, Elapsed: ev.Elapsed}
	for _, l := range pkgOut {
		if strings.HasPrefix(l.text, "FAIL") || strings.HasPrefix(l.text, "-test.shuffle ") {
			continue
		}
		f.Lines = append(f.Lines, strings.TrimLeft(l.text, " \t"))
		if f.File == "" {
			if m := assertionLine.FindStringSubmatch(l.text); m != nil {
				f.File = path.Join(s.packageDir(pkg), m[1])
				f.Line, _ = strconv.Atoi(m[2])
			}
		}
	}
	if len(f.Lines) > 20 {
		f.Lines = append([]string{fmt.Sprintf("(%d earlier line(s) omitted)", len(f.Lines)-20)}, f.Lines[len(f.Lines)-20:]...)
	}
	if len(f.Lines) == 0 {
		f.Lines = []string{"(the package failed without printing anything)"}
	}
	s.failures = append(s.failures, f)
}

// frameIn finds the goroutine-dump frame that is inside the named test — the
// line after `<package>.<Test>(` — and returns its file relative to the tested
// module, the line, and the `goroutine N [state]` header of the goroutine it
// was in. A dump names paths absolutely.
func (s *summarizer) frameIn(pkg, test string, out []line) (string, int, string) {
	// A subtest's function is a closure inside its parent's, so the frame
	// that matters is the parent's.
	top := test
	if before, _, ok := strings.Cut(test, "/"); ok {
		top = before
	}
	prefix := pkg + "." + top
	for i := 0; i+1 < len(out); i++ {
		if !strings.HasPrefix(out[i].text, prefix) {
			continue
		}
		m := frameLine.FindStringSubmatch(out[i+1].text)
		if m == nil {
			continue
		}
		n, _ := strconv.Atoi(m[2])
		goroutine := "goroutine"
		for j := i; j >= 0; j-- {
			if strings.HasPrefix(out[j].text, "goroutine ") && strings.HasSuffix(out[j].text, ":") {
				goroutine = strings.TrimSuffix(out[j].text, ":")
				break
			}
		}
		return s.relative(pkg, m[1]), n, goroutine
	}
	return "", 0, ""
}

// packageDir is the package's directory relative to the tested module: what
// the import path says once the module path is taken off. A package outside
// the module keeps its import path, which is at least not a wrong directory.
func (s *summarizer) packageDir(pkg string) string {
	switch {
	case s.opts.module == "":
		return ""
	case pkg == s.opts.module:
		return "."
	case strings.HasPrefix(pkg, s.opts.module+"/"):
		return strings.TrimPrefix(pkg, s.opts.module+"/")
	}
	return pkg
}

// relative makes a goroutine-dump path relative to the tested module: strip
// the directory the tests ran in, and failing that fall back to the package
// directory plus the file's base name.
func (s *summarizer) relative(pkg, file string) string {
	if s.opts.cwd != "" {
		if rel, ok := strings.CutPrefix(file, s.opts.cwd+"/"); ok {
			return rel
		}
	}
	if !filepath.IsAbs(file) {
		return file
	}
	return path.Join(s.packageDir(pkg), path.Base(file))
}

// annotationPath is the file as the repository sees it: the tested module's
// directory, then the module-relative file.
func (s *summarizer) annotationPath(file string) string {
	if file == "" {
		return ""
	}
	if s.opts.moduleDir == "" || s.opts.moduleDir == "." {
		return path.Clean(file)
	}
	return path.Join(s.opts.moduleDir, file)
}

func sortedPackages(m map[string]*packageState) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// print writes the human summary: one count line, then one block per failure.
func (s *summarizer) print(w io.Writer) {
	fmt.Fprintf(w, "testsum: %d passed, %d skipped, %d failed; %d package(s), %d failed",
		s.passed, s.skipped, s.failed, s.packagesSeen, s.packagesFailed)
	if s.passthrough > 0 {
		fmt.Fprintf(w, "; %d non-JSON line(s) passed through above", s.passthrough)
	}
	fmt.Fprintln(w)

	for _, f := range s.failures {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "%s %s", strings.ToUpper(f.Kind), f.Package)
		if f.Test != "" {
			fmt.Fprintf(w, " %s", f.Test)
		}
		if f.Elapsed > 0 {
			fmt.Fprintf(w, " (%.2fs)", f.Elapsed)
		}
		fmt.Fprintln(w)
		for _, text := range f.Lines {
			fmt.Fprintf(w, "    %s\n", text)
		}
		if f.Test != "" && f.Seed != "" {
			fmt.Fprintf(w, "    rerun: go test -shuffle=%s -run '^%s$' ./%s/\n", f.Seed, regexp.QuoteMeta(f.Test), s.packageDir(f.Package))
		}
	}
}

// annotate writes one workflow command per failure. Properties are escaped
// as GitHub requires; the message carries the failure's lines with newlines
// encoded, which the Checks UI renders as lines again.
func (s *summarizer) annotate(w io.Writer) {
	for _, f := range s.failures {
		title := f.Package
		if f.Test != "" {
			title += "." + f.Test
		}
		title += " " + f.Kind
		props := []string{}
		if file := s.annotationPath(f.File); file != "" {
			props = append(props, "file="+escapeProperty(file))
			if f.Line > 0 {
				props = append(props, "line="+strconv.Itoa(f.Line))
			}
		}
		props = append(props, "title="+escapeProperty(title))
		msg := strings.Join(f.Lines, "\n")
		if f.Seed != "" {
			msg += "\nshuffle seed " + f.Seed
		}
		fmt.Fprintf(w, "::error %s::%s\n", strings.Join(props, ","), escapeData(msg))
	}
}

func escapeData(s string) string {
	r := strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A")
	return r.Replace(s)
}

func escapeProperty(s string) string {
	r := strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C")
	return r.Replace(s)
}

// stepSummary appends the Markdown table the run page shows.
func (s *summarizer) stepSummary(w io.Writer) {
	fmt.Fprintf(w, "### go test: %d passed, %d skipped, %d failed (%d package(s), %d failed)\n\n",
		s.passed, s.skipped, s.failed, s.packagesSeen, s.packagesFailed)
	if len(s.failures) == 0 {
		return
	}
	fmt.Fprintln(w, "| package | test | kind | where | shuffle seed |")
	fmt.Fprintln(w, "|---|---|---|---|---|")
	for _, f := range s.failures {
		where := ""
		if file := s.annotationPath(f.File); file != "" {
			where = "`" + file + "`"
			if f.Line > 0 {
				where = fmt.Sprintf("`%s:%d`", file, f.Line)
			}
		}
		fmt.Fprintf(w, "| `%s` | %s | %s | %s | %s |\n", f.Package, cell(f.Test), f.Kind, where, cell(f.Seed))
	}
	fmt.Fprintln(w)
}

func cell(s string) string {
	if s == "" {
		return ""
	}
	return "`" + s + "`"
}
