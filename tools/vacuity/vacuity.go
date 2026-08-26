// Package vacuity finds tests that pass without proving anything.
//
// A vacuous test is not a failing test and not a missing one. It runs, it is
// green, it appears in the coverage report, and it makes no claim — so the
// defect it was written to catch walks straight past it. That is worse than
// having no test, because the absence of a test is visible and a green one
// that asserts nothing is an assurance nobody has any reason to doubt.
//
// The checks here are not general. Each one is a shape this repository has
// actually shipped, which is the only kind worth the false positives:
//
//   - [CheckUnasserted] — a test that reaches no assertion at all.
//   - [CheckConditional] — a test whose every claim is inside a loop, over
//     something nothing says is non-empty.
//
// The second is the one that keeps happening. `for _, c := range Cases() { …
// assert … }` proves exactly nothing when `Cases()` returns an empty slice,
// and an empty slice is what a corpus becomes when somebody moves a case out
// of it. CLAUDE.md records the version of this that ran for months:
// `ZeroValueCases` had one caller where every other corpus function had two,
// "proving half of what it was written for". A count of *cases* would not have
// noticed, because both numbers were the number the code produced.
//
// The clearest evidence that these are oversights rather than a style sits in
// `plugins/codex/ephemeral_test.go`, where two adjacent tests loop over the
// same `childEnv` result. One counts what it found and fails on the wrong
// number; its neighbour, which asserts that the child's environment never
// carries a host variable, is satisfied by an environment with nothing in it.
// Same file, same author, one line apart.
package main

import (
	"go/ast"
	"strings"
)

// Check names one shape of vacuity.
type Check string

const (
	// CheckUnasserted is a test that reaches no assertion.
	//
	// Deliberately conservative: handing the test handle to *anything* counts
	// as an assertion, because whatever received it may fail the test and this
	// analysis cannot see across a package boundary to find out. That trades
	// false negatives for false positives on purpose. A check that cries wolf
	// is one people learn to run with their eyes closed, and this one is meant
	// to be believed — which is what lets it fail a build.
	CheckUnasserted Check = "unasserted"

	// CheckConditional is a test whose every claim is inside a loop over
	// something nothing establishes is non-empty.
	//
	// Reported and never fatal. The repository has 134 of these and most are
	// fine; what the number is for is that a *new* one arrives in a diff
	// somebody is reading, where the one-line answer — assert the corpus is
	// non-empty — is obvious and cheap.
	CheckConditional Check = "conditional"
)

// Finding is one site.
type Finding struct {
	Check Check
	Test  string

	// Pos is where to look: the test for [CheckUnasserted], the loop for
	// [CheckConditional], because the loop is what a reader has to see.
	Pos string

	// Detail names the thing that decides it — the range subject, for a
	// conditional finding — so the report says what to assert rather than only
	// that something is missing.
	Detail string
}

// Fatal reports whether a finding fails the command.
//
// Only [CheckUnasserted] does, and that is a claim about the tree rather than
// about the check: it stands at zero, with the two deliberate sites marked, so
// a finding now is one a diff introduced. [CheckConditional] stands at 134 and
// a number that large can only be a map — enforcing it would mean either a
// sweep this repository has twice paid for, or an allowlist that rots.
func (f Finding) Fatal() bool { return f.Check == CheckUnasserted }

// marker is how a test says that proving nothing is the point.
//
// The spelling mirrors staticcheck's `//lint:ignore <check> <reason>`, which
// this repository already uses and already legislates the shape of: the reason
// is required, because "a check describes a mistake and this test is making
// that mistake on purpose" is a sentence somebody has to be willing to write
// down.
const marker = "//vacuity:ignore "

// suppressed reports whether a comment group excuses a check, and why.
func suppressed(doc *ast.CommentGroup, check Check) (reason string, ok bool) {
	if doc == nil {
		return "", false
	}

	for _, line := range doc.List {
		rest, found := strings.CutPrefix(line.Text, marker)
		if !found {
			continue
		}

		named, reason, _ := strings.Cut(strings.TrimSpace(rest), " ")
		if Check(named) != check {
			continue
		}

		reason = strings.TrimSpace(reason)
		if reason == "" {
			// A marker with no reason is not a decision, it is a way to make
			// the report go quiet. Refused by being ignored, so the finding
			// stands and somebody has to write the sentence.
			continue
		}

		return reason, true
	}

	return "", false
}
