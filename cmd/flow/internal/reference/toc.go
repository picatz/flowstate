package reference

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"unicode"
)

// tocStart and tocEnd bracket the generated table of contents inside
// docs/DSL.md. Only the lines between them are rewritten — everything before
// tocStart and after tocEnd is a human's prose, untouched.
const (
	tocStart = "<!-- toc:start -->"
	tocEnd   = "<!-- toc:end -->"
)

// SyncTOC returns markdown with the table of contents between [tocStart] and
// [tocEnd] regenerated from the document's own level-2 and level-3 headings.
//
// docs/DSL.md is 4700-odd lines with around ninety `##`/`###` headings and,
// until #702, no way to find one short of scrolling or grepping — and no
// mechanism keeping a hand-written list honest against them either, which is
// exactly the shape of drift this repository's own generated-reference
// pattern (docs/reference/*.md, via cmd/flow/internal/docsgen) exists to
// rule out. So the contents list is derived the same way: this function is
// the derivation, [sync.go]'s go:generate step is what runs it against
// docs/DSL.md (rewriting both the source and the mirror so the two cannot
// disagree), and TestDSLTOCHasNoDrift is what fails when a heading was added,
// renamed, or removed without regenerating.
//
// Level-2 and level-3 only. DSL.md's `####` headings exist mostly as an
// aside inside a round's own subsection (eight of them, against ninety at
// the two levels above), and a fourth indent would make the list itself hard
// to scan — the thing this exists to fix.
func SyncTOC(markdown []byte) ([]byte, error) {
	start := bytes.Index(markdown, []byte(tocStart))
	if start == -1 {
		return nil, fmt.Errorf("reference: %s marker not found", tocStart)
	}

	end := bytes.Index(markdown, []byte(tocEnd))
	if end == -1 {
		return nil, fmt.Errorf("reference: %s marker not found", tocEnd)
	}
	if end < start {
		return nil, fmt.Errorf("reference: %s appears before %s", tocEnd, tocStart)
	}

	headings := tocHeadings(markdown)
	if len(headings) == 0 {
		return nil, fmt.Errorf("reference: found no level-2 or level-3 headings; " +
			"refusing to write an empty table of contents")
	}

	var out bytes.Buffer
	out.Write(markdown[:start+len(tocStart)])
	out.WriteString("\n")
	for _, h := range headings {
		if h.level == 3 {
			out.WriteString("  ")
		}
		fmt.Fprintf(&out, "- [%s](#%s)\n", h.text, h.slug)
	}
	out.Write(markdown[end:])

	return out.Bytes(), nil
}

// tocHeading is one entry: the heading as written (so a reader sees the same
// backticks and emphasis the document itself uses) and the anchor GitHub's
// renderer assigns it.
type tocHeading struct {
	level int
	text  string
	slug  string
}

// tocHeadings walks markdown and returns every level-2 and level-3 ATX
// heading outside a fenced code block, in document order, each carrying the
// anchor slug GitHub's renderer would assign it.
//
// Fence tracking is not optional here. DSL.md's own worked examples open
// with a `# before` / `# after` YAML comment inside a ```yaml fence — a
// heading scanner that does not track fence state finds a phantom "heading"
// on every one of them, none of which are markdown structure.
func tocHeadings(markdown []byte) []tocHeading {
	var (
		headings []tocHeading
		inFence  bool
		fenceCh  byte
		slugSeen = map[string]int{}
	)

	scanner := bufio.NewScanner(bytes.NewReader(markdown))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if ch, ok := fenceChar(trimmed); ok {
			switch {
			case !inFence:
				inFence, fenceCh = true, ch
			case ch == fenceCh:
				inFence, fenceCh = false, 0
			}

			continue
		}
		if inFence {
			continue
		}

		level, text, ok := atxHeading(line)
		if !ok || level < 2 || level > 3 {
			continue
		}

		slug := slugify(stripMarkup(text))
		if n := slugSeen[slug]; n > 0 {
			slugSeen[slug] = n + 1
			slug = fmt.Sprintf("%s-%d", slug, n)
		} else {
			slugSeen[slug] = 1
		}

		headings = append(headings, tocHeading{level: level, text: text, slug: slug})
	}

	return headings
}

// fenceChar reports the character opening or closing a fenced code block, if
// trimmed is a fence line (three or more of the same backtick or tilde).
func fenceChar(trimmed string) (byte, bool) {
	for _, ch := range []byte{'`', '~'} {
		n := 0
		for n < len(trimmed) && trimmed[n] == ch {
			n++
		}
		if n >= 3 {
			return ch, true
		}
	}

	return 0, false
}

// atxHeading parses a CommonMark ATX heading (`## Text`), returning its
// level and text with any optional closing `#`s and surrounding whitespace
// trimmed. DSL.md never indents a heading, so — unlike CommonMark proper —
// this does not tolerate up to three leading spaces; a heading that needed
// that would be a formatting bug worth seeing, not silently accepting.
func atxHeading(line string) (int, string, bool) {
	i := 0
	for i < len(line) && line[i] == '#' {
		i++
	}
	if i == 0 || i > 6 {
		return 0, "", false
	}
	if i == len(line) {
		return 0, "", false // a bare line of "#"s, not a heading
	}
	if line[i] != ' ' && line[i] != '\t' {
		return 0, "", false // "##no-space" is not an ATX heading
	}

	text := strings.TrimSpace(line[i:])
	text = strings.TrimRight(text, "#")
	text = strings.TrimSpace(text)

	return i, text, true
}

// stripMarkup removes the inline markdown DSL.md's headings use — code spans
// and emphasis — so the slug is computed from the same rendered text a
// reader sees, not from the source characters. A heading whose text opens
// with a code span reads, and slugs, with the backticks gone; they are
// formatting, not content.
func stripMarkup(text string) string {
	text = strings.ReplaceAll(text, "`", "")
	text = strings.ReplaceAll(text, "*", "")

	return text
}

// slugify reproduces GitHub's heading-anchor algorithm closely enough that a
// link this package generates lands where GitHub itself would put it:
// lowercase, ASCII punctuation dropped rather than turned into a separator
// (so "retired — not" becomes "retired--not", not "retired-not" — the em
// dash's own two flanking spaces both survive as hyphens), spaces turned
// into hyphens, and an existing hyphen or underscore kept as-is. Validated
// against this document's own pre-existing internal links: the heading
// reading manual: narrows, and the body can read how a run started is
// already linked elsewhere in this file as
// #manual-narrows-and-the-body-can-read-how-a-run-started, which is what
// this produces.
func slugify(text string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(text) {
		switch {
		case r == ' ':
			b.WriteByte('-')
		case r == '-' || r == '_':
			b.WriteRune(r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
		}
	}

	return b.String()
}
