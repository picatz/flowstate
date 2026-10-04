package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
)

// junitSuites is the JUnit XML document `flow test --junit` writes: the
// de-facto interchange format CI systems read to annotate failures, show
// durations and track flaky cases over time (#1471).
type junitSuites struct {
	XMLName xml.Name     `xml:"testsuites"`
	Tests   int          `xml:"tests,attr"`
	Failed  int          `xml:"failures,attr"`
	Errors  int          `xml:"errors,attr"`
	Suites  []junitSuite `xml:"testsuite"`
}

type junitSuite struct {
	Name   string      `xml:"name,attr"`
	Tests  int         `xml:"tests,attr"`
	Failed int         `xml:"failures,attr"`
	Errors int         `xml:"errors,attr"`
	Time   string      `xml:"time,attr"`
	Cases  []junitCase `xml:"testcase"`
}

type junitCase struct {
	Class   string        `xml:"classname,attr"`
	Name    string        `xml:"name,attr"`
	Time    string        `xml:"time,attr"`
	Failure *junitProblem `xml:"failure,omitempty"`
	Error   *junitProblem `xml:"error,omitempty"`
}

type junitProblem struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

// junitFromResults renders what the run already decided; it judges nothing.
// A case's failed expectations are `<failure>`; a case that could not be
// judged (its own error, or a file the loader refused) is `<error>`, the
// distinction JUnit consumers use to separate a wrong workflow from a broken
// test. The messages are the report's own, so whatever the report withholds
// (a `sensitive:` input's value) is withheld here too.
func junitFromResults(results []testFileResult) junitSuites {
	var doc junitSuites
	for _, r := range results {
		suite := junitSuite{Name: r.report.GetFile()}
		var total float64
		if refused := r.report.GetRefused(); refused != "" {
			suite.Cases = append(suite.Cases, junitCase{
				Class: suite.Name, Name: "(file)", Time: "0",
				Error: &junitProblem{Message: firstLine(refused), Text: refused},
			})
		}
		for _, c := range r.report.GetCases() {
			seconds := c.GetDuration().AsDuration().Seconds()
			total += seconds
			tc := junitCase{Class: suite.Name, Name: c.GetName(), Time: fmt.Sprintf("%.3f", seconds)}
			switch {
			case c.GetError() != "":
				tc.Error = &junitProblem{Message: firstLine(c.GetError()), Text: c.GetError()}
			case !c.GetPassed():
				var lines []string
				for _, f := range c.GetFailures() {
					lines = append(lines, f.GetMessage())
				}
				text := strings.Join(lines, "\n")
				tc.Failure = &junitProblem{Message: firstLine(text), Text: text}
			}
			suite.Cases = append(suite.Cases, tc)
		}
		for _, c := range suite.Cases {
			suite.Tests++
			if c.Failure != nil {
				suite.Failed++
			}
			if c.Error != nil {
				suite.Errors++
			}
		}
		suite.Time = fmt.Sprintf("%.3f", total)
		doc.Tests += suite.Tests
		doc.Failed += suite.Failed
		doc.Errors += suite.Errors
		doc.Suites = append(doc.Suites, suite)
	}

	return doc
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")

	return line
}

// writeJUnit writes the document to path, truncating a file already there.
func writeJUnit(path string, results []testFileResult) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("--junit: %w", err)
	}
	defer func() {
		if cerr := f.Close(); err == nil && cerr != nil {
			err = fmt.Errorf("--junit: %w", cerr)
		}
	}()

	return encodeJUnit(f, junitFromResults(results))
}

func encodeJUnit(w io.Writer, doc junitSuites) error {
	if _, err := io.WriteString(w, xml.Header); err != nil {
		return fmt.Errorf("--junit: %w", err)
	}
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return fmt.Errorf("--junit: %w", err)
	}

	return nil
}
