// Command modernize reports what Go's `go fix` modernizers would change in
// this repository, and changes nothing. Run it from anywhere in the tree:
//
//	go run ./tools/modernize            # the whole module
//	go run ./tools/modernize ./pkg/...  # one subtree
//	go run ./tools/modernize -sites ./pkg/flowstate/v1/engine/
//
// Go 1.26 moved the modernizers into `go fix`, and this repository's
// toolchain is pinned past that, so the fixers are available today. The
// decision recorded in #521 is that they are *never* applied as a sweep: an
// eleven-thousand-line mechanical diff is the exact shape in which a real
// defect hides from review, and none of what the fixers propose fixes a
// defect. They are applied opportunistically instead — a package at a time,
// inside a diff a reviewer is already reading closely for another reason.
//
// This command is what makes that policy workable rather than aspirational.
// It answers "what is available here" for whatever you are already editing,
// and the weekly deep tier (.github/workflows/deep.yml) runs it over the
// whole tree to keep the number visible without letting a tool commit on
// anyone's behalf. It reports; it has no -fix mode, deliberately. To apply
// what it names, run `go fix` yourself on the one package you are in.
//
// Naming hazard, per #521: `go fix` rewrites Go source, and this
// repository's own `flow fix` rewrites Flowfiles. They are unrelated. Any
// sentence about this command has to say which one it means.
//
// The set of fixers is not hardcoded. `go fix -json` reports diagnostics
// keyed by the analyzer that produced them, so the table below is whatever
// the pinned toolchain registers — a toolchain bump that adds a modernizer
// shows up here without anyone editing a list. (The fifteen analyzers #521
// measured had already become twenty-three by go1.26.6.)
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	sites := flag.Bool("sites", false, "list every site's position rather than only the counts")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/modernize [-sites] [packages...]\n\n")
		fmt.Fprintf(os.Stderr, "Reports what `go fix` would change. Changes nothing. See #521.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	out, err := runGoFix(patterns)
	if err != nil {
		fmt.Fprintf(os.Stderr, "modernize: %v\n", err)
		os.Exit(2)
	}
	rep, err := parseReport(strings.NewReader(out))
	if err != nil {
		fmt.Fprintf(os.Stderr, "modernize: parsing `go fix -json` output: %v\n", err)
		os.Exit(2)
	}
	writeReport(os.Stdout, rep, patterns, *sites)
}

// runGoFix runs `go fix -json` over the given patterns and returns its
// stdout.
//
// The exit status is the whole of the correctness here, so it is worth
// stating what `go fix -json` actually does rather than what the family of
// `go` subcommands suggests. Measured on go1.26.6:
//
//   - Findings only: exit **0**, empty stderr, diagnostics on stdout. The
//     `-diff` mode's "non-zero when the diff is not empty" convention does
//     not apply to `-json`, so a finding never looks like a failure.
//   - A package that does not load or type-check: exit **non-zero**, with
//     `# import/path` headers and the compiler's message on **stderr** —
//     and stdout *still* carries well-formed JSON for every package that did
//     analyse.
//
// That second shape is the trap. Accepting stdout because it is non-empty
// yields a plausible, well-formed, *incomplete* report: the packages that
// failed to load are silently absent, so an advisory whose whole job is to
// say how much is available under-counts and reads exactly like a clean
// tree. That is this repository's "green by not running" failure wearing a
// report's clothes.
//
// So a non-zero exit refuses the report outright and names the packages that
// did not analyse. The report is complete or it is not rendered.
//
// Downloads and build progress also land on stderr, which is why the exit
// code decides this and stderr's emptiness does not.
func runGoFix(patterns []string) (string, error) {
	args := append([]string{"fix", "-json"}, patterns...)
	cmd := exec.Command("go", args...)
	// Tee'd rather than swallowed: the compiler's message is what a person
	// needs to fix the tree, and it is also what CI's log should carry.
	var stderr strings.Builder
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", incompleteAnalysisError(exit.ExitCode(), stderr.String(), len(out))
		}
		return "", fmt.Errorf("running `go %s`: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}

// incompleteAnalysisError explains a non-zero `go fix -json` exit in the
// terms that matter: which packages were not analysed, and why no report is
// being printed even though there may be diagnostics for the rest.
func incompleteAnalysisError(code int, stderr string, stdoutBytes int) error {
	var b strings.Builder
	fmt.Fprintf(&b, "`go fix -json` exited %d: at least one package could not be analysed", code)
	if failed := failedPackages(stderr); len(failed) > 0 {
		fmt.Fprintf(&b, " (%s)", strings.Join(failed, ", "))
	}
	b.WriteString(".\nNo report is printed: with a package missing, a report would be well-formed, plausible and short by an unknown amount")
	if stdoutBytes > 0 {
		fmt.Fprintf(&b, " — %d bytes of diagnostics for the packages that did analyse were discarded for that reason", stdoutBytes)
	}
	b.WriteString(".\nFix the build (see the messages above) and run again.")
	return errors.New(b.String())
}

// failedPackages pulls the import paths out of the `# import/path` headers
// the go command writes above each package's load or type-check errors. The
// JSON stream itself carries none of this — a package that failed to load
// simply does not appear in it, which is precisely why it cannot be detected
// by reading the stream.
func failedPackages(stderr string) []string {
	var paths []string
	seen := map[string]bool{}
	for line := range strings.SplitSeq(stderr, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "# ")
		if !ok {
			continue
		}
		// A header can carry a trailing note, e.g. `# path [path.test]`.
		path := strings.TrimSpace(rest)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		paths = append(paths, path)
	}
	return paths
}

// diagnostic is the part of the analysis driver's JSON this command reads.
// The driver also carries the suggested edits; nothing here wants them,
// because nothing here applies anything.
type diagnostic struct {
	Posn    string `json:"posn"`
	Message string `json:"message"`
}

// report is the aggregate: how many sites each fixer found, where, and how
// many of them sit in files nobody may hand-edit.
type report struct {
	// fixers, keyed by analyzer name.
	fixers map[string]*fixerCount
	// packages, keyed by import path, counting actionable sites only.
	packages map[string]int
	// total actionable sites, and total sites in generated files.
	total     int
	generated int
	// files holding at least one actionable site.
	files map[string]bool
	// listing of every actionable site, in the order the driver reported
	// them, for -sites.
	listing []site
}

type fixerCount struct {
	name      string
	sites     int
	generated int
}

type site struct {
	fixer   string
	posn    string
	message string
}

// parseReport reads the analysis driver's JSON. The driver writes one JSON
// object per analysed package, concatenated rather than wrapped in an array
// — including `{}` for the packages with nothing to say — so this decodes a
// stream of values rather than a single document.
//
// Each object maps import path to analyzer name to that analyzer's
// diagnostics.
func parseReport(r io.Reader) (*report, error) {
	rep := &report{
		fixers:   map[string]*fixerCount{},
		packages: map[string]int{},
		files:    map[string]bool{},
	}
	dec := json.NewDecoder(r)
	for {
		var batch map[string]map[string][]diagnostic
		if err := dec.Decode(&batch); err != nil {
			if errors.Is(err, io.EOF) {
				return rep, nil
			}
			return nil, err
		}
		// Sorted so that a report over the same tree is byte-identical run
		// to run: a weekly job whose output reshuffles is a diff nobody can
		// read.
		for _, pkg := range sortedKeys(batch) {
			byFixer := batch[pkg]
			for _, name := range sortedKeys(byFixer) {
				for _, d := range byFixer[name] {
					count := rep.fixers[name]
					if count == nil {
						count = &fixerCount{name: name}
						rep.fixers[name] = count
					}
					file := fileOf(d.Posn)
					if isGenerated(file) {
						count.generated++
						rep.generated++
						continue
					}
					count.sites++
					rep.total++
					rep.packages[pkg]++
					rep.files[file] = true
					rep.listing = append(rep.listing, site{fixer: name, posn: d.Posn, message: d.Message})
				}
			}
		}
	}
}

// fileOf strips the line:col suffix the driver appends to a position. A
// Windows-style drive letter would defeat splitting on every colon, so this
// takes the two trailing fields only.
func fileOf(posn string) string {
	for range 2 {
		i := strings.LastIndex(posn, ":")
		if i < 0 {
			return posn
		}
		posn = posn[:i]
	}
	return posn
}

// generatedMarker is the line the Go project's convention requires at the
// top of a generated file, and the thing `go fix` itself does not check.
// Sites inside one are not actionable at all: CLAUDE.md's rule is that a
// generated file is never edited directly, so a modernization there could
// only arrive through the generator. Counting them alongside the rest would
// overstate what anyone can act on, which for an advisory report is the
// whole of what it is for.
const generatedMarker = "// Code generated "

func isGenerated(file string) bool {
	if filepath.Ext(file) != ".go" {
		return false
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return false
	}
	// The marker must appear before the package clause; reading the first
	// few lines is enough and keeps this off the whole file.
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "package ") {
			return false
		}
		if strings.HasPrefix(line, generatedMarker) && strings.HasSuffix(line, "DO NOT EDIT.") {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// writeReport renders the report as Markdown: the weekly job files it into
// an issue body verbatim, and a person reading it in a terminal loses
// nothing to the pipes.
func writeReport(w io.Writer, rep *report, patterns []string, sites bool) {
	fmt.Fprintf(w, "`go fix` modernizers available under %s\n\n", strings.Join(patterns, " "))

	if rep.total == 0 && rep.generated == 0 {
		fmt.Fprintf(w, "Nothing to report: the modernizers find no sites here.\n")
		return
	}

	fmt.Fprintf(w, "**%d site(s) across %d file(s) and %d package(s).**", rep.total, len(rep.files), len(rep.packages))
	if rep.generated > 0 {
		fmt.Fprintf(w, " A further %d site(s) sit in generated files and are excluded throughout: a generated file is never hand-edited, so a modernization there can only arrive through its generator.", rep.generated)
	}
	fmt.Fprintf(w, "\n\n")

	counts := make([]*fixerCount, 0, len(rep.fixers))
	for _, name := range sortedKeys(rep.fixers) {
		if rep.fixers[name].sites > 0 {
			counts = append(counts, rep.fixers[name])
		}
	}
	// Descending by site count, ties broken by name so the order is stable.
	sort.SliceStable(counts, func(i, j int) bool {
		if counts[i].sites != counts[j].sites {
			return counts[i].sites > counts[j].sites
		}
		return counts[i].name < counts[j].name
	})

	if len(counts) > 0 {
		fmt.Fprintf(w, "| Fixer | Sites |\n|---|---|\n")
		for _, c := range counts {
			fmt.Fprintf(w, "| `%s` | %d |\n", c.name, c.sites)
		}
		fmt.Fprintf(w, "\n")
	}

	if len(rep.packages) > 0 {
		fmt.Fprintf(w, "| Package | Sites |\n|---|---|\n")
		type pkgCount struct {
			path  string
			sites int
		}
		pkgs := make([]pkgCount, 0, len(rep.packages))
		for _, path := range sortedKeys(rep.packages) {
			pkgs = append(pkgs, pkgCount{path: path, sites: rep.packages[path]})
		}
		sort.SliceStable(pkgs, func(i, j int) bool {
			if pkgs[i].sites != pkgs[j].sites {
				return pkgs[i].sites > pkgs[j].sites
			}
			return pkgs[i].path < pkgs[j].path
		})
		for _, p := range pkgs {
			fmt.Fprintf(w, "| `%s` | %d |\n", p.path, p.sites)
		}
		fmt.Fprintf(w, "\n")
	}

	if sites {
		for _, s := range rep.listing {
			fmt.Fprintf(w, "%s: [%s] %s\n", s.posn, s.fixer, s.message)
		}
		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintf(w, "%s\n", policy)
}

// policy is the decision this command exists to serve, restated wherever the
// numbers are, because the numbers are what tempt someone to sweep them.
const policy = "Per #521 these are applied **opportunistically, never as a sweep**: convert a\n" +
	"package's sites when that package is already open for another reason, so the\n" +
	"conversion rides in a diff a reviewer is reading closely. None of this fixes a\n" +
	"defect, and a mechanical diff thousands of lines long is the shape in which a\n" +
	"real one hides. To apply what is named above, scope `go fix` to the one package\n" +
	"you are in — `go fix ./pkg/flowstate/v1/engine/` — and read the result. Note\n" +
	"that Go's `go fix` rewrites Go source; this repository's own `flow fix` rewrites\n" +
	"Flowfiles, and the two are unrelated.\n"
