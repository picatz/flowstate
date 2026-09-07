package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/internal/commitcheck"
)

// TestAnAnnotationCannotBeForgedByTheMessageItReports pins the escaping: an
// author's line holding a newline and a `::` reaches the runner as one
// warning about that text, never as a second command.
func TestAnAnnotationCannotBeForgedByTheMessageItReports(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	report(&out, []commitcheck.Finding{{
		Rule:    commitcheck.RuleAbsolute,
		Message: "line one\n::error::forged, with 100% certainty",
		Skill:   "x",
	}}, true)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	assert.Len(t, lines, 1, "the message's newline became a second line:\n%s", out.String())
	assert.True(t, strings.HasPrefix(lines[0], "::warning title=commitcheck/absolute::"), lines[0])
	// A `::` inside the data is literal text: the command's grammar reads
	// the data to the end of the line, so only a line break could start a
	// second command, and that is what the encoding removes.
	assert.Contains(t, lines[0], "%0A", "the newline was not encoded")
	assert.Contains(t, lines[0], "100%25", "the percent sign was not encoded")
}

func TestOutsideActionsAFindingIsAPlainLine(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	report(&out, []commitcheck.Finding{{Rule: commitcheck.RuleIssue, Message: "no issue", Skill: "s"}}, false)
	assert.Equal(t, "commitcheck: issue: no issue (see s)\n", out.String())
}
