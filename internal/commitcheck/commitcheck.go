// Package commitcheck holds a pull request's title and body, or a commit's
// subject and body, to the shape the comms-commit and comms-pr skills ask
// for (#1728).
//
// The conventions were followed by about a third of merged commits when
// this was written, which is the gap docs/CI.md names for merges: a
// mechanism made of attention. This is the mechanism. Each rule names the
// skill that explains it, so a finding sends its reader to the reasoning
// rather than to a regular expression.
//
// It reports and never rewrites: what a change is called and what it claims
// to have verified are claims only their author can make.
package commitcheck

import (
	"fmt"
	"regexp"
	"strings"
)

// Rule names one convention.
type Rule string

const (
	// RuleSubject is the subject's shape: `scope: lowercase imperative`, the
	// scope being an area of the tree rather than a type of change, and the
	// first word after it lowercase (or a code span, since a subject may
	// open with the name of the thing it changes).
	RuleSubject Rule = "subject"

	// RuleIssue is the link to the issue, decision, or record the change
	// serves, or the trailer `No-Issue: <reason>` saying there is none.
	RuleIssue Rule = "issue"

	// RuleVerification is the record of what ran: a `Verification:` line or
	// heading, or the trailer `Unverified: <reason>` saying nothing did.
	RuleVerification Rule = "verification"

	// RuleAbsolute is an unearned absolute — "fully tested", "safe",
	// "backward-compatible", "no impact" — on a line that offers no evidence
	// for it.
	RuleAbsolute Rule = "absolute"
)

// Finding is one convention a message does not follow.
type Finding struct {
	Rule Rule

	// Message says what is missing or wrong, in the words a reader can act on.
	Message string

	// Skill is the skill that explains the rule, relative to the repository
	// root.
	Skill string
}

func (f Finding) String() string {
	return string(f.Rule) + ": " + f.Message + " (see " + f.Skill + ")"
}

const (
	commitSkill = ".agents/skills/comms-commit/SKILL.md"
	prSkill     = ".agents/skills/comms-pr/SKILL.md"
)

var (
	// subject is `scope: first-word`, the scope one or more lowercase path-ish
	// tokens and the first word lowercase or a code span.
	subjectShape = regexp.MustCompile("^[a-z0-9/,. -]+: [a-z`]")

	// issueRef is any `#N` reference, or the trailer that says there is none.
	issueRef = regexp.MustCompile(`(?m)(#[0-9]+\b|^No-Issue: \S)`)

	// verification is the line or heading that records what ran, or the
	// trailer that says nothing did.
	verification = regexp.MustCompile(`(?m)^(#+ *[Vv]erification *$|Verification: *\S|Unverified: *\S)`)

	// absolutes are the phrases comms-pr forbids without evidence.
	absolutes = regexp.MustCompile(`(?i)\b(fully tested|backward[- ]compatible|no impact|safe)\b`)

	// evidence is what a line offering an absolute has to carry beside it: a
	// code span, an issue, a parenthetical, or a clause that says why.
	evidence = regexp.MustCompile("`|#[0-9]+|\\(|\\b(because|since|so that|which|as)\\b")
)

// Check holds subject and body to the conventions and returns every finding.
// A message that follows them all returns none.
func Check(subject, body string) []Finding {
	var out []Finding

	if !subjectShape.MatchString(strings.TrimSpace(subject)) {
		out = append(out, Finding{
			Rule:    RuleSubject,
			Message: "the subject is not `scope: lowercase imperative`; name the area of the tree, a colon, and what the change does",
			Skill:   commitSkill,
		})
	}

	if !issueRef.MatchString(body) {
		out = append(out, Finding{
			Rule:    RuleIssue,
			Message: "the body names no issue (`Refs #N`, `Closes #N`, `Part of #N`) and carries no `No-Issue: <reason>` trailer",
			Skill:   prSkill,
		})
	}

	if !verification.MatchString(body) {
		out = append(out, Finding{
			Rule:    RuleVerification,
			Message: "the body records no verification: add a `Verification:` line or heading naming what ran, or an `Unverified: <reason>` trailer",
			Skill:   prSkill,
		})
	}

	// Bounded where the work is spent: an author's body decides how many
	// lines there are, so the walk stops at maxLines and the report at
	// maxAbsolutes, with the rest counted rather than listed.
	lines := strings.Split(body, "\n")
	if len(lines) > maxLines {
		lines = lines[:maxLines]
	}
	omitted := 0
	for _, line := range lines {
		loc := absolutes.FindStringIndex(line)
		if loc == nil {
			continue
		}
		if evidence.MatchString(line[loc[1]:]) || evidence.MatchString(line[:loc[0]]) {
			continue
		}
		if len(out) >= maxAbsolutes {
			omitted++
			continue
		}
		out = append(out, Finding{
			Rule:    RuleAbsolute,
			Message: "\"" + line[loc[0]:loc[1]] + "\" stands without evidence on its line: " + excerpt(strings.TrimSpace(line)),
			Skill:   prSkill,
		})
	}
	if omitted > 0 {
		out = append(out, Finding{
			Rule:    RuleAbsolute,
			Message: fmt.Sprintf("and %d more line(s) carry an absolute without evidence", omitted),
			Skill:   prSkill,
		})
	}

	return out
}

// Bounds on what one message can make this report: findings are one per
// offending line, and a body decides how many lines it has.
const (
	maxLines     = 2000
	maxAbsolutes = 10
)

// maxExcerpt bounds how much of an author's line a finding repeats: enough
// to find it, not enough for one long line to make a warning unreadable.
const maxExcerpt = 120

// excerpt is the line cut to [maxExcerpt] runes, marked when it was cut.
func excerpt(line string) string {
	runes := []rune(line)
	if len(runes) <= maxExcerpt {
		return line
	}
	return string(runes[:maxExcerpt]) + "…"
}
