package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Command vacuity reports tests that pass without proving anything.
//
//	go run ./tools/vacuity           # counts, and every unasserted site
//	go run ./tools/vacuity -sites    # every site, both checks
//	go run ./tools/vacuity ./pkg/... # one subtree
//
// It changes nothing and has no fix mode, for the reason `tools/modernize` has
// none: what it names is a *claim* somebody has to decide is the right one, and
// a tool cannot write an assertion nobody has thought about. The one-line
// answer to a conditional finding — assert the corpus is non-empty — is only
// correct if the corpus should be non-empty, which is a question about the
// corpus.
//
// It exits non-zero on an unasserted finding and never on a conditional one.
// See [Finding.Fatal] for why the two are enforced differently, which is a
// claim about where the tree stands rather than about which check matters.
func main() {
	sites := flag.Bool("sites", false, "list every site, including the conditional ones")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/vacuity [-sites] [path]\n\n")
		fmt.Fprintf(os.Stderr, "Reports tests that pass without proving anything. Changes nothing.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	root := "."
	if flag.NArg() > 0 {
		root = strings.TrimSuffix(strings.TrimSuffix(flag.Arg(0), "..."), "/")
		if root == "" {
			root = "."
		}
	}

	findings, tests, err := Analyze(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vacuity: %v\n", err)
		os.Exit(2)
	}

	if writeReport(os.Stdout, findings, tests, root, *sites) {
		os.Exit(1)
	}
}

// writeReport prints the findings and reports whether any of them is fatal.
func writeReport(out io.Writer, findings []Finding, tests int, root string, sites bool) (failed bool) {
	byCheck := map[Check][]Finding{}
	for _, finding := range findings {
		byCheck[finding.Check] = append(byCheck[finding.Check], finding)
	}

	fmt.Fprintf(out, "vacuity: %d test function(s) under %s\n\n", tests, root)

	for _, check := range []Check{CheckUnasserted, CheckConditional} {
		found := byCheck[check]

		fmt.Fprintf(out, "%-12s %4d  %s\n", check, len(found), describe(check))

		// Every unasserted site always, because there should be none and a
		// count alone would not say where. Conditional sites only on request,
		// because there are enough of them to bury the first list.
		if len(found) == 0 || (check == CheckConditional && !sites) {
			continue
		}

		for _, finding := range found {
			fmt.Fprintf(out, "    %s: %s", relative(root, finding.Pos), finding.Test)
			if finding.Detail != "" {
				fmt.Fprintf(out, " — every claim is inside a loop over %s, and nothing says it runs", finding.Detail)
			}
			fmt.Fprintln(out)
		}
	}

	for _, finding := range findings {
		if finding.Fatal() {
			failed = true
		}
	}

	fmt.Fprintln(out)
	switch {
	case failed:
		fmt.Fprintf(out, "A test that reaches no assertion is green for a reason unrelated to the code.\n")
		fmt.Fprintf(out, "Assert what it is for, or say why it proves nothing with a comment on the test:\n\n")
		fmt.Fprintf(out, "    %s%s <why this one asserts nothing>\n", marker, CheckUnasserted)
	case !sites && len(byCheck[CheckConditional]) > 0:
		fmt.Fprintf(out, "Run with -sites to see where the conditional ones are.\n")
	default:
		fmt.Fprintf(out, "Nothing here fails a build. See the package comment for what each check is.\n")
	}

	return failed
}

// describe is the one-line account of a check, for the report.
func describe(check Check) string {
	switch check {
	case CheckUnasserted:
		return "reaches no assertion at all (fails this command)"
	case CheckConditional:
		return "every claim inside a loop nothing says will run (reported only)"
	}

	return string(check)
}

// relative shortens a position against the root it was walked from, so the
// report is readable from the directory somebody ran it in.
func relative(root, pos string) string {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return pos
	}
	if shorter, err := filepath.Rel(absolute, pos); err == nil && !strings.HasPrefix(shorter, "..") {
		return shorter
	}

	return pos
}
