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

	// RuleBounded is a body longer than this check reads. What was not read
	// cannot be reported as conforming, so a body past the bound is a
	// finding of its own rather than a silent pass.
	RuleBounded Rule = "bounded"

	// RuleAttribution is provenance a message states for itself rather than
	// carries: a hand-written "Generated with Claude Code" footer, which the
	// host or the forge already appends, or a session-specific console link,
	// which resolves for its author and nobody else.
	RuleAttribution Rule = "attribution"
)

// Surface is where a message is headed, which decides whether an attribution
// footer at its end was written by its author or appended for them.
type Surface int

const (
	// SurfaceCommit is a commit or squash message. Nothing appends to one,
	// so every footer it carries was written by whoever wrote the message.
	SurfaceCommit Surface = iota

	// SurfacePullRequest is a pull request title and body. The forge appends
	// one attribution footer after the author's last line and the body is
	// read back with it already there, so that one is not the author's to
	// answer for. A second is.
	SurfacePullRequest
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

	// attributionFooter is a footer standing on its own line, which is the
	// one place the marker is a claim the message makes rather than a phrase
	// it mentions. A sentence about the footer carries other words and is
	// left alone, so a message may say what it is not allowed to append.
	attributionFooter = regexp.MustCompile(`(?im)^[\s>]*(?:[-*_]{3,}[\s>]*)?(?:\x{1F916}\s*)?[_*]{0,2}generated (?:with|by) \[?claude(?: code)?\]?(?:\([^)]*\))?[_*.\s]*$`)

	// horizontalRule is the rule a forge sets above the footer it appends.
	horizontalRule = regexp.MustCompile(`^[\s>]*[-*_]{3,}\s*$`)

	// sessionLink is a console URL naming one agent session. Unlike the
	// footer it is reported wherever it appears: it is unreachable for every
	// reader but its author, so there is no sentence that wants one.
	sessionLink = regexp.MustCompile(`https://claude\.ai/code/session_\w+`)
)

// Check holds subject and body to the conventions and returns every finding.
// A message that follows them all returns none. where says which surface the
// message is headed for, since a pull request body is read back carrying one
// footer its author did not write.
func Check(subject, body string, where Surface) []Finding {
	var out []Finding

	if where == SurfacePullRequest {
		body = withoutAppendedFooter(body)
	}

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
	// lines there are, so the walk stops at maxLines without splitting the
	// rest, and the report stops at maxAbsolutes with the rest counted. A
	// body that runs past the bound is a finding, since what was not read
	// cannot be called conforming (Codex, #1848).
	omitted := 0
	// Each kind of attribution is reported once. Every occurrence of one has
	// the same repair — delete it — so a body that stacks three footers
	// needs three deletions and one sentence saying so.
	var sawFooter, sawSessionLink bool
	rest := body
	for n := 0; rest != ""; n++ {
		if n == maxLines {
			out = append(out, Finding{
				Rule:    RuleBounded,
				Message: fmt.Sprintf("the body runs past %d lines and the rest was not inspected; a message this long is not one a reader can review either", maxLines),
				Skill:   prSkill,
			})
			break
		}
		var line string
		line, rest, _ = strings.Cut(rest, "\n")

		if !sawFooter && attributionFooter.MatchString(line) {
			sawFooter = true
			out = append(out, Finding{
				Rule:    RuleAttribution,
				Message: "the body writes its own attribution footer, which the host or the forge appends already, so the post carries it twice: " + excerpt(strings.TrimSpace(line)),
				Skill:   prSkill,
			})
		}
		if !sawSessionLink && sessionLink.MatchString(line) {
			sawSessionLink = true
			out = append(out, Finding{
				Rule:    RuleAttribution,
				Message: "the body carries a session link, which resolves for its author and for no other reader of this history; delete it",
				Skill:   commitSkill,
			})
		}

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

// withoutAppendedFooter is body without the one attribution footer a forge
// adds after an author's last line: the footer, a horizontal rule set above
// it, and the blank lines around them.
//
// Only that last one is removed. A footer the author wrote as well sits above
// it and is still reported, which is the shape that made this rule necessary:
// a hand-written footer and a session link stacked over the forge's own.
func withoutAppendedFooter(body string) string {
	head, last := cutLastLine(strings.TrimRight(body, " \t\r\n"))
	if !attributionFooter.MatchString(last) {
		return body
	}
	if before, above := cutLastLine(strings.TrimRight(head, " \t\r\n")); horizontalRule.MatchString(above) {
		return before
	}
	return head
}

// cutLastLine is s without its final line, and that line. A string of one
// line is all last and no head.
func cutLastLine(s string) (head, last string) {
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "", s
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
