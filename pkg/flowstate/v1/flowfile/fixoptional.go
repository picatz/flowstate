package flowfile

import (
	"regexp"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Edition v2026.3 makes optional traversal — `.?` and `orValue()` — a documented
// part of the dialect (issue #412), and this file is the migration's other half:
// the guarded-read idiom the corpus wrote because the shorter spelling did not
// exist yet is rewritten into that spelling as a file is brought forward.
//
// Three shapes, decided on the issue, and nothing else:
//
//	has(x.y) && x.y        →  x.?y.orValue(false)
//	!(has(x.y) && x.y)     →  !x.?y.orValue(false)
//	has(x.y) ? x.y : d     →  x.?y.orValue(d)      (whole expression only)
//
// # Exact match, or nothing
//
// The two paths must be byte-identical, the path must be plain selects — an
// identifier followed by `.name` segments, no call, no index — and anything that
// does not match exactly is left exactly alone. `has()` itself stays: it answers
// presence, which is a different question, and every `filter(r, has(r.x))` in
// the corpus is has() doing the job it keeps. `has(p) && !p` is not the negated
// twin — it asks "answered no", not "not answered yes" — and is untouched.
//
// # Why this one rewrite does not need a scope
//
// The rewriter-safety history in CLAUDE.md is about names: both corruptions came
// from rewriting an identifier the grammar or CEL had bound. This rewrite moves
// no name. Whatever `x` resolves to — a step, a loop binding, the wait's
// `payload`, the http task's response — `x.?y.orValue(false)` reads the same
// binding `has(x.y) && x.y` read, so the scope question that makes rooting hard
// does not arise. What it needs instead is *syntax* honesty: a match inside a
// string literal is prose, so literals are masked before matching; and the
// rewritten expression is re-parsed before it is accepted, so a splice this
// reasoning missed cannot leave a file that no longer parses.
//
// # Why it runs only while a file is being brought forward
//
// The idiom stays legal — `has()` is not retired — so a v2026.3 author who
// writes it has written a valid file, and `flow fix` does not edit valid current
// files to taste. The rewrite runs when the document is on its way into the
// current edition (an older known edition, or no marker at all), which is the
// same run that stamps `edition: v2026.3`. That is the decision recorded on
// issue #412: the edition carries the extension, and the migrator carries the
// rewrite.

// optionalPath matches the plain select path both halves of the idiom must be:
// an identifier and at least one `.name` after it, so the leaf to make optional
// exists.
const optionalPath = `[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+`

var (
	// guardedRead is `has(P) && P`. Group 1 and 2 must be byte-identical, and the
	// neighbours are checked in code — a regexp boundary cannot see that the
	// character before `has` is a `!`, which would make this the wrong half of a
	// negation to rewrite.
	guardedRead = regexp.MustCompile(`has\(\s*(` + optionalPath + `)\s*\)\s*&&\s*(` + optionalPath + `)`)

	// negatedGuardedRead is the hand-negated twin, `!(has(P) && P)`, matched
	// before [guardedRead] so the negation travels with the rewrite instead of
	// being stranded outside it.
	negatedGuardedRead = regexp.MustCompile(`!\(\s*has\(\s*(` + optionalPath + `)\s*\)\s*&&\s*(` + optionalPath + `)\s*\)`)

	// ternaryDefault is `has(P) ? P : D`, anchored to the whole expression
	// because D's extent inside a larger expression is not something a textual
	// match can know. The whole-expression case is the one the corpus writes.
	ternaryDefault = regexp.MustCompile(`^\s*has\(\s*(` + optionalPath + `)\s*\)\s*\?\s*(` + optionalPath + `)\s*:\s*(.+?)\s*$`)
)

// rewriteOptionalReads rewrites the guarded-read idioms in one expression's
// source, reporting whether anything changed.
//
// Anything that keeps it from being sure — source that does not parse, paths
// that differ, a match whose neighbours make it part of something larger, a
// result that does not re-parse — answers with the source unchanged. This
// rewrite has no refusal path on purpose: the idiom is legal in the new edition,
// so a site left alone is a valid file, not a stranded migration.
func rewriteOptionalReads(src string) (string, bool) {
	if !strings.Contains(src, "has(") {
		return src, false
	}
	if !parsesInProfile(src) {
		// Not a valid expression, and not this rewriter's problem to report — the
		// validator says it far better. Left alone rather than half-rewritten.
		return src, false
	}

	masked := maskCELLiterals(src)

	out := src
	// The whole-expression ternary first: it can only match when the entire
	// expression is the idiom, in which case the conjunction patterns cannot.
	if m := ternaryDefault.FindStringSubmatchIndex(masked); m != nil {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path == other {
			out = optionalSpelling(path) + ".orValue(" + strings.TrimSpace(src[m[6]:m[7]]) + ")"
		}
	} else {
		out = rewriteConjunctions(src, masked)
	}

	if out == src {
		return src, false
	}
	if !parsesInProfile(out) {
		// A splice the reasoning above missed. Nothing is written on a guess.
		return src, false
	}
	return out, true
}

// rewriteConjunctions applies the two conjunction shapes, negated twin first,
// splicing from the back so earlier offsets stay true.
func rewriteConjunctions(src, masked string) string {
	type splice struct {
		from, through int // byte offsets into src, half-open
		text          string
	}
	var splices []splice

	consumed := make([]bool, len(src))
	claim := func(from, through int) {
		for i := from; i < through; i++ {
			consumed[i] = true
		}
	}
	free := func(from, through int) bool {
		for i := from; i < through; i++ {
			if consumed[i] {
				return false
			}
		}
		return true
	}

	for _, m := range negatedGuardedRead.FindAllStringSubmatchIndex(masked, -1) {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path != other || !cleanNeighbours(masked, m[0], m[1], false) {
			continue
		}
		splices = append(splices, splice{m[0], m[1], "!" + optionalSpelling(path) + ".orValue(false)"})
		claim(m[0], m[1])
	}
	for _, m := range guardedRead.FindAllStringSubmatchIndex(masked, -1) {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path != other || !free(m[0], m[1]) || !cleanNeighbours(masked, m[0], m[1], true) {
			continue
		}
		splices = append(splices, splice{m[0], m[1], optionalSpelling(path) + ".orValue(false)"})
		claim(m[0], m[1])
	}
	if len(splices) == 0 {
		return src
	}

	slices.SortFunc(splices, func(a, b splice) int { return b.from - a.from })
	out := src
	for _, s := range splices {
		out = out[:s.from] + s.text + out[s.through:]
	}
	return out
}

// cleanNeighbours reports whether the characters around a match leave it whole:
// nothing before that would bind to its start, nothing after that would select
// from, call, or index its end. bareHas says the match starts at `has`, whose
// one dangerous neighbour is a `!` — `!has(P) && P` negates the guard alone, and
// rewriting the conjunction under it would negate the read instead.
func cleanNeighbours(masked string, from, through int, bareHas bool) bool {
	if from > 0 {
		before := masked[from-1]
		if isIdentByte(before) || before == '.' {
			return false
		}
		if bareHas && before == '!' {
			return false
		}
	}
	if through < len(masked) {
		after := masked[through]
		if isIdentByte(after) || after == '.' || after == '(' || after == '[' {
			return false
		}
	}
	return true
}

// isIdentByte reports whether b can appear in a CEL identifier.
func isIdentByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_'
}

// optionalSpelling turns a select path into its optional-leaf spelling:
// `a.b.c` becomes `a.b.?c`. The path always holds a dot — [optionalPath]
// requires one — so the read stays anchored where the guard anchored it:
// `has(a.b.c)` requires `a.b` present exactly as `a.b.?c` does.
func optionalSpelling(path string) string {
	i := strings.LastIndex(path, ".")
	return path[:i] + ".?" + path[i+1:]
}

// parsesInProfile reports whether source parses in the profile's environment —
// the same environment the compiler parses with, so the two agree about what
// the language is (see [rootedUnder] for why that identity matters).
func parsesInProfile(src string) bool {
	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		return false
	}
	env, err := v1.DefaultEvaluator().Env(libs...)
	if err != nil {
		return false
	}
	_, issues := env.Parse(src)
	return issues == nil || issues.Err() == nil
}

// maskCELLiterals blanks the contents of string and bytes literals (and `//`
// comments) so the pattern matcher cannot read prose as code. The result is the
// same length as the input, so every offset found in it indexes the original.
func maskCELLiterals(src string) string {
	out := []byte(src)
	i := 0
	for i < len(out) {
		c := out[i]

		// A raw or bytes prefix sits immediately before the quote.
		if (c == 'r' || c == 'R' || c == 'b' || c == 'B') && i+1 < len(out) && (out[i+1] == '\'' || out[i+1] == '"') {
			i++
			continue
		}

		switch {
		case c == '\'' || c == '"':
			quote := string(c)
			if strings.HasPrefix(src[i:], quote+quote+quote) {
				quote = quote + quote + quote
			}
			end := i + len(quote)
			for end < len(out) {
				if out[end] == '\\' && len(quote) == 1 {
					end += 2
					continue
				}
				if strings.HasPrefix(src[end:], quote) {
					end += len(quote)
					break
				}
				end++
			}
			if end > len(out) {
				end = len(out)
			}
			for j := i + len(quote); j < end-len(quote) && j < len(out); j++ {
				out[j] = ' '
			}
			i = end
		case c == '/' && i+1 < len(out) && out[i+1] == '/':
			for ; i < len(out) && out[i] != '\n'; i++ {
				out[i] = ' '
			}
		default:
			i++
		}
	}
	return string(out)
}

// optionalReads rewrites the guarded-read idiom in every fenced, single-line
// expression under n, in place on the fixer's lines.
//
// Only fenced scalars, because a fence is the one spelling that is an
// expression everywhere it appears; a deferred input written bare is left
// alone, which costs a modernisation and corrupts nothing. Only single lines,
// because a block scalar has no one line to splice — the idiom is legal in the
// new edition, so a multi-line site left alone is a valid file.
func (f *fixer) optionalReads(n ast.Node) {
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			f.optionalReads(v)
		}
	case *ast.MappingValueNode:
		f.optionalReads(node.Value)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			f.optionalReads(v)
		}
	case *ast.StringNode:
		f.optionalReadScalar(node)
	case *ast.LiteralNode:
		f.optionalReadBlockScalar(node)
	}
}

// optionalReadBlockScalar handles the corpus's other common spelling: a folded
// block scalar (`>-`) whose whole content is one fenced expression line. The
// value has no single source line the way a plain scalar does, so the content
// line is located by its text within the literal's span — and any doubt about
// which line that is (none found, or more than one) skips the site, because the
// idiom is legal and a site left alone is a valid file.
func (f *fixer) optionalReadBlockScalar(node *ast.LiteralNode) {
	inner, fenced := SplitFence(strings.TrimSpace(blockText(node)))
	if !fenced || strings.Contains(inner, "\n") {
		return
	}

	rewritten, changed := rewriteOptionalReads(inner)
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() {
		return
	}

	want := fenceOpen + inner + fenceClose
	found := 0
	for n := span.Start.Line; n <= span.End.Line; n++ {
		if strings.Contains(f.line(n), want) {
			if found != 0 {
				return
			}
			found = n
		}
	}
	if found == 0 || strings.Count(f.line(found), want) != 1 {
		return
	}

	f.lines[found-1] = strings.Replace(f.line(found), want, fenceOpen+rewritten+fenceClose, 1)
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    found,
		Message: "guarded read rewritten to optional traversal (`.?` with `orValue`)",
		Pending: "guarded read would be rewritten to optional traversal (`.?` with `orValue`)",
	})
}

// optionalReadScalar rewrites one fenced scalar, splicing the way
// [fixer.rootScalar] does and skipping silently where that splice cannot be
// made — see [fixer.optionalReads] for why silence is correct here.
func (f *fixer) optionalReadScalar(node *ast.StringNode) {
	inner, fenced := SplitFence(node.Value)
	if !fenced {
		return
	}

	rewritten, changed := rewriteOptionalReads(inner)
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() || span.Start.Line != span.End.Line {
		return
	}

	line := f.line(span.Start.Line)
	want, replacement := fenceOpen+inner+fenceClose, fenceOpen+rewritten+fenceClose

	from, located := byteOffsetOfColumn(line, span.Start.Column)
	if !located {
		return
	}
	at := strings.Index(line[from:], want)
	if at < 0 {
		// The line no longer holds the value the parser read — another pass
		// rewrote it first. The fixed-point loop re-parses and this pass sees the
		// updated text next round.
		return
	}
	at += from

	f.lines[span.Start.Line-1] = line[:at] + replacement + line[at+len(want):]
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    span.Start.Line,
		Message: "guarded read rewritten to optional traversal (`.?` with `orValue`)",
		Pending: "guarded read would be rewritten to optional traversal (`.?` with `orValue`)",
	})
}

// modernizesEdition reports whether a document is on its way into the current
// edition — declaring an older known edition, or none at all — which is when
// the optional-read rewrite runs. A current file keeps its author's spelling,
// and a future one is refused elsewhere without this pass adding noise.
//
// An edition arriving only through a merge key answers false: which edition it
// is cannot be read from here (see [fixer.mergedDeclaresEdition]), and this is
// a rewrite the command must only make when it knows the file is moving.
//
// Read from the bytes [Fix] was handed rather than per round, because a later
// round runs over a document an earlier round has already stamped — see the
// call site. Unparseable input answers false; [fixOnce] reports the parse error
// properly, and a pass gated off is a pass that cannot act on a guess.
func modernizesEdition(data []byte) bool {
	file, err := parser.ParseBytes(data, parser.ParseComments)
	if err != nil {
		return false
	}

	probe := &fixer{}
	for _, doc := range file.Docs {
		probe.collectAnchors(doc.Body)
	}

	for _, doc := range file.Docs {
		mapping := asMapping(doc.Body)
		if mapping == nil {
			continue
		}
		for _, v := range mapping.Values {
			if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
				if probe.mergedDeclaresEdition(v) {
					return false
				}
				continue
			}
			name, ok := keyNameOf(v.Key)
			if !ok || name != "edition" {
				continue
			}
			declared, ok := editionText(v.Value)
			if !ok {
				return false
			}
			return declared != CurrentEdition && slices.Contains(knownEditions, declared)
		}
	}
	return true
}
