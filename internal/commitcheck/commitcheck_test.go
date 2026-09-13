package commitcheck

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rules(findings []Finding) []Rule {
	out := make([]Rule, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.Rule)
	}
	return out
}

func TestAMessageThatFollowsEveryConventionHasNoFinding(t *testing.T) {
	t.Parallel()

	body := "The problem, in a paragraph.\n\n## Verification\n\n- `go test ./tools/gate/`: ok.\n\nRefs #1728\n"
	assert.Empty(t, Check("tools: commitcheck holds a message to the skills", body))
	assert.Empty(t, Check("cli: `flow run --detach` returns once the run has started", body),
		"a subject may open with the name of the thing it changes")
}

func TestASubjectWithoutAScopeOrInTitleCaseIsFound(t *testing.T) {
	t.Parallel()

	body := "Verification: go test ./...\n\nRefs #1\n"
	assert.Equal(t, []Rule{RuleSubject}, rules(Check("Fix bug", body)))
	assert.Equal(t, []Rule{RuleSubject}, rules(Check("tools: Commitcheck holds", body)), "the word after the scope is lowercase")
	assert.Empty(t, Check("tools/gate, ci: one plan", body), "a scope may name two areas")
}

func TestAnIssueIsAReferenceOrADeclaredAbsence(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []Rule{RuleIssue}, rules(Check("a: b", "Verification: ran.\n")))
	assert.Empty(t, Check("a: b", "Verification: ran.\n\nCloses #12\n"))
	assert.Empty(t, Check("a: b", "Verification: ran.\n\nNo-Issue: a typo in a comment\n"))
	assert.Equal(t, []Rule{RuleIssue}, rules(Check("a: b", "Verification: ran.\n\nNo-Issue:\n")), "the trailer needs its reason")
}

func TestVerificationIsALineAHeadingOrADeclaredAbsence(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []Rule{RuleVerification}, rules(Check("a: b", "Refs #1\n")))
	assert.Empty(t, Check("a: b", "Refs #1\n\nVerification: `go test ./...` ok\n"))
	assert.Empty(t, Check("a: b", "Refs #1\n\n## Verification\n\n- ok\n"))
	assert.Empty(t, Check("a: b", "Refs #1\n\nUnverified: docs only, no command applies\n"))
	assert.Equal(t, []Rule{RuleVerification}, rules(Check("a: b", "Refs #1\n\nVerification will be added later.\n")),
		"prose that opens with the word is not the marker")
	assert.Equal(t, []Rule{RuleVerification}, rules(Check("a: b", "Refs #1\n\n## Verification plan\n")),
		"a heading that says more than the word is not the marker")
}

func TestAnAbsoluteNeedsEvidenceOnItsLine(t *testing.T) {
	t.Parallel()

	base := "Refs #1\n\nVerification: ran.\n\n"
	assert.Equal(t, []Rule{RuleAbsolute}, rules(Check("a: b", base+"This change is fully tested.\n")))
	assert.Equal(t, []Rule{RuleAbsolute}, rules(Check("a: b", base+"The migration is backward-compatible and has no impact.\n")),
		"one finding per line, however many absolutes it holds")
	assert.Empty(t, Check("a: b", base+"Retrying is safe because the request carries an idempotency key (#1751).\n"),
		"a clause that says why is evidence")
	assert.Empty(t, Check("a: b", base+"`Safe` is the method's name.\n"), "a code span on the line is evidence")
	assert.Empty(t, Check("a: b", base+"Rolling back is safe as the migration only adds a column.\n"),
		"an \"as\" clause is evidence")
	assert.Equal(t, []Rule{RuleAbsolute}, rules(Check("a: b", base+"This is safe assuming nothing.\n")),
		"\"as\" has to be the word, not a prefix of one")
}

func TestAFindingRepeatsOnlyABoundedExcerptOfTheLine(t *testing.T) {
	t.Parallel()

	long := "This change is fully tested " + strings.Repeat("x", 500)
	findings := Check("a: b", "Refs #1\n\nVerification: ran.\n\n"+long+"\n")
	require.Len(t, findings, 1)
	assert.Less(t, len(findings[0].Message), 300, "an author's long line was repeated whole into the finding")
	assert.True(t, strings.HasSuffix(findings[0].Message, "…"), "a cut excerpt says it was cut")
}

func TestAnEmptyMessageFailsThreeRules(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []Rule{RuleSubject, RuleIssue, RuleVerification}, rules(Check("Fix bug", "")),
		"the issue's acceptance case: a bare title and an empty body name three rules")
}

func TestAbsoluteFindingsAreCappedAndTheRestCounted(t *testing.T) {
	t.Parallel()

	body := "Refs #1\n\nVerification: ran.\n\n" + strings.Repeat("It is safe.\n", 40)
	findings := Check("a: b", body)
	require.Len(t, findings, maxAbsolutes+1, "one finding per line with no bound")
	assert.Contains(t, findings[maxAbsolutes].Message, "30 more line(s)", "the omitted lines are counted rather than listed")

	// Past maxLines nothing is read, and the body is reported as one that
	// was not read whole rather than as one that conforms.
	huge := "Refs #1\n\nVerification: ran.\n\n" + strings.Repeat("x\n", maxLines) + "It is safe.\n"
	assert.Equal(t, []Rule{RuleBounded}, rules(Check("a: b", huge)),
		"a body past the bound was either inspected past it or passed as conforming")

	exact := "Refs #1\n\nVerification: ran.\n" + strings.Repeat("x\n", maxLines-3)
	assert.Empty(t, Check("a: b", exact), "a body at the bound is read whole")
}

// The shapes below are the ones this repository actually shipped: the footer
// and session link squashed into `29ed8f5`, and the pair #1976 carried above
// the forge's own footer, which is what made that body say it three times.
func TestAHandWrittenAttributionFooterIsFound(t *testing.T) {
	t.Parallel()

	conforming := "Refs #1\n\nVerification: `go test ./...` ok\n"

	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\n\U0001F916 Generated with [Claude Code](https://claude.com/claude-code)\n")))
	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\n---\n_Generated by [Claude Code](https://claude.ai/code)_\n")),
		"the forge's own wording, written by hand, is the one that doubles")
	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\nGenerated with Claude Code\n")),
		"the marker is found without its link")
	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\n> _Generated by [Claude Code](https://claude.ai/code)_\n")),
		"a quoted footer is still a footer")
}

func TestASessionLinkIsFoundWhereverItSits(t *testing.T) {
	t.Parallel()

	conforming := "Refs #1\n\nVerification: `go test ./...` ok\n"

	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\nhttps://claude.ai/code/session_01QbFPM866q5cKLh8zyjNFZR\n")))
	assert.Equal(t, []Rule{RuleAttribution}, rules(Check("a: b", conforming+
		"\nPicked up from https://claude.ai/code/session_014fHcHziLtTb1VzzgK32GTv earlier.\n")),
		"a link inside a sentence reaches no more readers than one on its own line")
}

func TestProseAboutTheFooterIsNotAFooter(t *testing.T) {
	t.Parallel()

	conforming := "Refs #1\n\nVerification: `go test ./...` ok\n"

	for _, line := range []string{
		"The `_Generated by [Claude Code](https://claude.ai/code)_` line is appended already, so a second one stacks.",
		"Do not hand-write a \"Generated with Claude Code\" footer in a pull request body.",
		"commitcheck reports a body that ends in Generated with Claude Code above the forge's own.",
	} {
		assert.Empty(t, Check("a: b", conforming+"\n"+line+"\n"),
			"a sentence about the marker carries other words and is a mention, not a claim: %s", line)
	}
}

func TestEachKindOfAttributionIsReportedOnce(t *testing.T) {
	t.Parallel()

	// What #1976 carried: two hand-written footers and a session link, above
	// the one the forge appends.
	body := "Refs #1\n\nVerification: `go test ./...` ok\n" +
		"\n\U0001F916 Generated with [Claude Code](https://claude.com/claude-code)\n" +
		"\nhttps://claude.ai/code/session_014fHcHziLtTb1VzzgK32GTv\n" +
		"\n---\n_Generated by [Claude Code](https://claude.ai/code)_\n"

	found := rules(Check("a: b", body))
	assert.Equal(t, []Rule{RuleAttribution, RuleAttribution}, found,
		"one finding per kind: every footer has the same repair, and so does every link")

	findings := Check("a: b", body)
	require.Len(t, findings, 2)
	assert.Contains(t, findings[0].Message, "twice")
	assert.Equal(t, prSkill, findings[0].Skill)
	assert.Contains(t, findings[1].Message, "session link")
	assert.Equal(t, commitSkill, findings[1].Skill, "the durable harm of a link is in the history")
}
