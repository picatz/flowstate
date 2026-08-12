package flowfile

import (
	"fmt"
	"strings"
)

// Interpolation is the rule that lets one scalar hold literal text and more than
// one ${...} expression:
//
//	message: ${run.identity.subject} requests deploying ${inputs.version}
//
// Until #413 that scalar was refused, with a positioned diagnostic saying to
// write it as one expression instead. This file is that refusal's replacement,
// and the shape of the replacement is the point: nothing about what a fence
// *means* changes, and nothing about what a whole-value fence *is* changes. A
// scalar that is exactly one fence keeps the typing it has today — `init: ${0}`
// is still the integer zero, not the string "0" — because the whole-value test
// is still the test, applied first and answered the same way. Only a scalar the
// old rule refused can reach the new path, which is what makes this additive
// rather than a reinterpretation, and so what keeps it out of the editions
// mechanism: no file an older build accepts means anything different here.
//
// # What a fence still is
//
// A fence holds one CEL expression and nothing else. There is no conditional, no
// loop, no filter, no pipeline — the line Helm and Jinja crossed was putting
// *control flow* in text, and this deliberately does not. What an author gains is
// a spelling for concatenation, not a capability: `${'a' + inputs.x + 'b'}` was
// always legal, so anything a multi-fence scalar can say a single fence could
// already say, less readably.
//
// # Where the fence ends
//
// [SplitFence] answers the whole-value question by looking at the two ends of the
// scalar, and leaves "where does this fence close" to CEL, which is correct when
// there is exactly one fence and the value's last byte is its close. A scalar
// with two fences has no such landmark: something has to decide that the `}` in
// `${a} ${b}` closes the first fence rather than sitting inside it.
//
// So [scanInterpolation] decides, by matching braces — but it cannot match them
// naively, because CEL puts braces inside expressions (`${ {'k': 1} }` is one
// fence around a map), inside string literals (`${'}'}`), and inside comments
// (a `//` comment in a block scalar runs to the end of its line and can hold
// anything). Counting braces without knowing about those three would close a
// fence in the middle of a valid expression, and the scalar would then compile —
// as something else. That is the `flow fix` corruption class exactly: a wrong
// answer that still validates. The scanner therefore knows CEL's string literals,
// including raw and triple-quoted forms, and its comments.
//
// The guarantee this owes, and the one [FuzzScanAgreesWithSplitFence] pins, is
// agreement at the boundary: for every scalar that today is a whole-value fence
// whose contents compile, the scanner must answer "one fence, spanning the whole
// value, with exactly these contents". Where the two could differ, the old answer
// wins, because the old answer is what shipped files mean.
//
// # The escape
//
// `$${` is a literal `${`, the spelling Terraform uses. It is recognized only in
// literal text, never inside a fence, so a CEL string containing `$${` needs no
// second escape. It has no migration burden at all: a value holding a literal
// `${` was refused by the old rule too, so no existing file contains one.
//
// # The bound, and the resource it covers
//
// The bound is [maxFencesPerValue], on the number of fences in one scalar.
//
// That is the resource an author multiplies without paying for it in document
// size, and the document's size is the only thing already bounded here. `${a}` is
// four bytes; a scalar inside an otherwise ordinary file can hold thousands of
// them, and each one becomes an operand of the `+` chain this desugars to. The
// chain is *nested* — CEL's `+` is binary — so n fences build a tree n deep, and
// both cel-go's parser and every walk in this repository that recurses over an
// expression descend it. Bounding bytes would not bound that, because the ratio
// of fences to bytes is the author's to choose; bounding fences does.
//
// Nothing else here needs its own bound. The scan is a single left-to-right pass
// with no backtracking, so its time is linear in a scalar whose length the
// document bound already covers, and the desugared source it produces is longer
// than its input by a constant per fence rather than by a factor. There is no
// nesting to bound: a fence's contents are CEL, and CEL has no fences.

// maxFencesPerValue bounds how many ${...} expressions one scalar may hold. See
// this file's doc for why the count, rather than the length, is the bound.
//
// The number is far above any honest use — the corpus's busiest message holds
// four — and far below where a nested `+` chain costs anything to parse or walk.
const maxFencesPerValue = 64

// escapedFence is the spelling of a literal `${` in a value.
const escapedFence = "$" + fenceOpen

// A segment is one piece of a scanned scalar: a run of literal text, or one
// fence's expression source.
type segment struct {
	// fence reports which of the two this is.
	fence bool

	// text is the literal text with escapes resolved, or the expression source
	// between the fence's braces.
	text string

	// start is the byte offset in the scalar where text begins: for a fence,
	// just past the `${`, so that a CEL error's column can be added to it.
	start int

	// open is the byte offset of the `$` of a fence's `${`, and equals start for
	// literal text. end is the offset just past the segment — past the `}` for a
	// fence. Together they cover the segment as written.
	open, end int
}

// scanInterpolation splits a scalar into literal and fence segments.
//
// It reports an error for a fence that is never closed and for a scalar holding
// more than [maxFencesPerValue] fences. It reports no error for a scalar with no
// fences at all: that is literal text, and the segments say so.
func scanInterpolation(s string) ([]segment, error) {
	var (
		segs    []segment
		lit     strings.Builder
		litFrom int
		fences  int
	)

	flush := func(to int) {
		if lit.Len() == 0 {
			return
		}
		segs = append(segs, segment{text: lit.String(), start: litFrom, open: litFrom, end: to})
		lit.Reset()
	}

	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], escapedFence):
			if lit.Len() == 0 {
				litFrom = i
			}
			lit.WriteString(fenceOpen)
			i += len(escapedFence)

		case strings.HasPrefix(s[i:], fenceOpen):
			closeAt, ok := findFenceClose(s, i+len(fenceOpen))
			if !ok {
				// The scanner could not find the close, and the whole-value
				// reading can: prefer that one. This is the guarantee at the
				// boundary, applied to the case where the two disagree — the old
				// answer wins, because the old answer is what shipped files mean
				// and, here, what produces the better sentence. `${'never closed}`
				// has no matching brace to a scanner that knows CEL's string
				// literals, but as a whole-value fence CEL parses it and says the
				// quote is the problem, which is the true and useful thing to say.
				if inner, whole := SplitFence(s); whole && i == 0 {
					return []segment{{
						fence: true,
						text:  inner,
						start: len(fenceOpen),
						open:  0,
						end:   len(s),
					}}, nil
				}
				return nil, fmt.Errorf("unterminated expression in %q: a ${ has to be closed with a }", s)
			}
			flush(i)
			fences++
			if fences > maxFencesPerValue {
				return nil, fmt.Errorf(
					"%q holds more than %d ${...} expressions, which is more than a value is allowed: "+
						"build the text in fewer expressions, or in a `value:` step of its own",
					s, maxFencesPerValue)
			}
			segs = append(segs, segment{
				fence: true,
				text:  s[i+len(fenceOpen) : closeAt],
				start: i + len(fenceOpen),
				open:  i,
				end:   closeAt + len(fenceClose),
			})
			i = closeAt + len(fenceClose)

		default:
			if lit.Len() == 0 {
				litFrom = i
			}
			lit.WriteByte(s[i])
			i++
		}
	}
	flush(len(s))

	return segs, nil
}

// findFenceClose returns the offset of the `}` closing a fence whose contents
// begin at from, and reports whether one was found.
//
// Braces are matched, and the three places CEL puts a brace that does not nest —
// a string literal, a raw or triple-quoted string literal, and a comment — are
// stepped over rather than counted. See this file's doc for why a naive count is
// not merely imprecise but dangerous.
func findFenceClose(s string, from int) (int, bool) {
	depth := 1
	for i := from; i < len(s); {
		switch c := s[i]; {
		case c == '/' && i+1 < len(s) && s[i+1] == '/':
			// A CEL line comment. Everything to the end of the line is text,
			// including any brace in it. Reachable in a block scalar, which is
			// the one value shape that can hold a newline.
			if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
				i += nl + 1
			} else {
				i = len(s)
			}

		case c == '\'' || c == '"':
			end, ok := skipCELString(s, i, false)
			if !ok {
				return 0, false
			}
			i = end

		case isIdentByte(c):
			// An identifier run, consumed whole so that a string's r/b prefix is
			// seen as the prefix it is rather than as an identifier next to a
			// quote. `r'a}b'` holds no closing brace.
			j := i
			for j < len(s) && isIdentByte(s[j]) {
				j++
			}
			if j < len(s) && (s[j] == '\'' || s[j] == '"') && isStringPrefix(s[i:j]) {
				end, ok := skipCELString(s, j, strings.ContainsAny(s[i:j], "rR"))
				if !ok {
					return 0, false
				}
				i = end
				continue
			}
			i = j

		case c == '{':
			depth++
			i++

		case c == '}':
			depth--
			if depth == 0 {
				return i, true
			}
			i++

		default:
			i++
		}
	}
	return 0, false
}

// skipCELString returns the offset just past a string literal beginning at the
// quote at i, and reports whether it is closed at all.
//
// raw says the literal carried an `r` prefix, in which case a backslash is an
// ordinary character rather than an escape.
func skipCELString(s string, i int, raw bool) (int, bool) {
	quote := s[i]
	triple := strings.HasPrefix(s[i:], strings.Repeat(string(quote), 3))

	delim := string(quote)
	if triple {
		delim = strings.Repeat(string(quote), 3)
	}

	for j := i + len(delim); j < len(s); {
		if !raw && s[j] == '\\' {
			// The escaped character cannot close the literal, whatever it is.
			j += 2
			continue
		}
		if strings.HasPrefix(s[j:], delim) {
			return j + len(delim), true
		}
		if !triple && s[j] == '\n' {
			// A single-quoted CEL string cannot span a line. Refusing here keeps
			// an unterminated quote from swallowing the rest of a block scalar.
			return 0, false
		}
		j++
	}
	return 0, false
}

// isStringPrefix reports whether an identifier run is one of CEL's string
// literal prefixes rather than a name.
func isStringPrefix(s string) bool {
	switch strings.ToLower(s) {
	case "r", "b", "rb", "br":
		return true
	}
	return false
}

// A Fence is one ${...} expression found in a value, with the offsets that place
// it back in the text it came from.
type Fence struct {
	// Source is the expression between the braces, exactly as written.
	Source string

	// At is the byte offset of Source within the value's text — past the `${`.
	// Open is the offset of the `$`, and End the offset just past the `}`.
	At, Open, End int
}

// Fences returns every ${...} in a value, in the order they were written.
//
// It is the multi-fence counterpart of [SplitFence] and is exported for the same
// reason that one is: what counts as an expression, and where each one begins and
// ends, has to mean the same thing in the compiler, in validation, and in the
// editor. The language server needs the list to squiggle a CEL error inside the
// fence it belongs to and to answer hover and go-to-definition for the reference
// under the cursor; a second implementation of the rule there is exactly how the
// editor would come to disagree with the engine about what a file says.
//
// A value whose fences cannot be scanned — an unterminated `${` — has none to
// report, and returns nil rather than a guess. The compiler reports that as the
// error it is; an editor has nothing to place and should place nothing.
func Fences(s string) []Fence {
	segs, err := scanInterpolation(s)
	if err != nil {
		return nil
	}
	var out []Fence
	for _, sg := range segs {
		if sg.fence {
			out = append(out, Fence{Source: sg.text, At: sg.start, Open: sg.open, End: sg.end})
		}
	}
	return out
}

// wholeValueFence reports the source of a scan that is exactly one fence
// covering the whole scalar — the shape [SplitFence] recognizes, and the shape
// that keeps its own typing rather than becoming text.
func wholeValueFence(segs []segment, s string) (string, bool) {
	if len(segs) != 1 || !segs[0].fence {
		return "", false
	}
	if segs[0].open != 0 || segs[0].end != len(s) {
		return "", false
	}
	return segs[0].text, true
}

// hasFence reports whether a scan found any expression at all.
func hasFence(segs []segment) bool {
	for _, sg := range segs {
		if sg.fence {
			return true
		}
	}
	return false
}

// literalText returns the scalar's text with `$${` escapes resolved, for a scan
// that found no fences.
func literalText(segs []segment) string {
	var b strings.Builder
	for _, sg := range segs {
		b.WriteString(sg.text)
	}
	return b.String()
}

// interpolationSource renders a scanned scalar as the one CEL expression the
// engine evaluates: literal runs as string literals, fences as their own source
// under `string(...)`, joined by `+`.
//
// One expression rather than a list of them is what makes the two drivers agree
// without either being told about interpolation at all. They evaluate a `Value`
// holding an expression, exactly as they do for a single fence, under the one
// [v1.DefaultCostLimit] budget every value gets — so there is no per-fence
// evaluation order, no per-fence budget, and nothing for a driver to implement
// differently from the other. The desugaring happens here, once, in the compiler
// both drivers read their workflows from.
//
// `string()` is the single named conversion, chosen because it is CEL's own and
// because an author can write it themselves and get the same answer. It is
// defined for string, int, uint, double, bool, duration and timestamp. On a map,
// a list, a null or bytes it has no overload, which is not an oversight: a
// message that wants a structure rendered should say which rendering it wants,
// and [checkExpressionTypes] reports the missing overload with that advice
// whenever the document knows the type.
func interpolationSource(segs []segment) string {
	parts := make([]string, 0, len(segs))
	for _, sg := range segs {
		if sg.fence {
			parts = append(parts, "string("+sg.text+")")
			continue
		}
		parts = append(parts, quoteCELString(sg.text))
	}
	return strings.Join(parts, " + ")
}

// escapeFences renders a literal string so that reading it back produces the
// same string: every `${` becomes `$${`.
//
// Applied left to right in one pass, which is what makes it correct on a string
// that already contains the escape: `$${` becomes `$$${`, and the scanner reads
// that back as `$` followed by an escaped `${`.
func escapeFences(s string) string {
	return strings.ReplaceAll(s, fenceOpen, escapedFence)
}
