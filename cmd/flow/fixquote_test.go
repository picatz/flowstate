package main

import (
	"strings"
	"testing"
)

// quoteTrapFlowfile holds one plain scalar YAML reads as a mapping key: a
// string literal ending in `: ` inside an unquoted expression (#2660).
const quoteTrapFlowfile = `edition: v2026.4
name: trap
inputs:
  who:
    type: string
    default: sam
steps:
  - id: hello
    log:
      message: ${"Hello: " + inputs.who}
`

const quoteTrapRepaired = `edition: v2026.4
name: trap
inputs:
  who:
    type: string
    default: sam
steps:
  - id: hello
    log:
      message: '${"Hello: " + inputs.who}'
`

// TestFixQuotesThePlainScalarThatKeepsAFileFromParsing: `flow fix` writes the
// quoting, nothing else changes, and a second run finds the file current.
func TestFixQuotesThePlainScalarThatKeepsAFileFromParsing(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", quoteTrapFlowfile)

	out, _, err := runFixCommand(t, path)
	if err != nil {
		t.Fatalf("flow fix failed on a repairable file: %v\n%s", err, out)
	}
	if got := string(readFixture(t, path)); got != quoteTrapRepaired {
		t.Errorf("unexpected rewrite:\n--- want\n%s\n--- got\n%s", quoteTrapRepaired, got)
	}

	out, _, err = runFixCommand(t, "--check", path)
	if err != nil {
		t.Fatalf("the repaired file is not current: %v\n%s", err, out)
	}
}

// TestFixCheckNamesTheQuotingWithoutWritingOrComplainingAboutPins: --check
// reports what it would quote, leaves the bytes alone, and does not add a
// second complaint about not being able to read the file's pins.
func TestFixCheckNamesTheQuotingWithoutWritingOrComplainingAboutPins(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", quoteTrapFlowfile)

	out, _, err := runFixCommand(t, "--check", path)
	if err == nil {
		t.Fatalf("--check found work to do and exited zero:\n%s", out)
	}
	if !strings.Contains(out, "would quote the expression") {
		t.Errorf("--check did not say what it would quote:\n%s", out)
	}
	if strings.Contains(out, "digest:") {
		t.Errorf("--check complained about pins it could read once repaired:\n%s", out)
	}
	if got := string(readFixture(t, path)); got != quoteTrapFlowfile {
		t.Errorf("--check wrote to the file:\n%s", got)
	}
}

// TestFixLeavesAnAmbiguousTrapAlone is the negative direction: a file whose
// other YAML error quoting cannot fix is not touched, and says so.
func TestFixLeavesAnAmbiguousTrapAlone(t *testing.T) {
	dir := t.TempDir()
	src := quoteTrapFlowfile + "      other: b: c\n"
	path := writeFixture(t, dir, "workflow.yaml", src)

	out, _, err := runFixCommand(t, path)
	if err == nil {
		t.Fatalf("flow fix accepted a file it could not repair:\n%s", out)
	}
	if got := string(readFixture(t, path)); got != src {
		t.Errorf("a file that still fails to parse was rewritten:\n%s", got)
	}
}
