package lsp

import (
	"fmt"
	"strings"
	"sync"

	lsp "github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Completion offers the profile's functions and hover said nothing about one.
//
// So an author could be shown `sortBy` while typing, accept it, and have no way to
// ask what it is — which is the half of discovery that matters after the first
// time. Hover is where somebody asks about code they are *reading*, including code
// they did not write.

// functionIndex is the profile's functions, keyed for lookup by written name.
var functionIndex = sync.OnceValue(func() (out struct {
	byName     map[string]v1.LibraryFunction
	namespaces map[string]bool
},
) {
	out.byName = map[string]v1.LibraryFunction{}
	out.namespaces = map[string]bool{}

	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		out.byName[fn.Name] = fn
		if qualifier, _, ok := strings.Cut(fn.Name, "."); ok {
			out.namespaces[qualifier] = true
		}
	}

	return out
})

// hoverFunction describes the profile function under the cursor.
//
// The fallback, never the first answer. A resolved reference wins: `value` is a
// function in the optional library and is also a perfectly ordinary step output, so
// `${steps.web.value}` has to describe the output. The caller only reaches this once
// the reference lookups have declined, and never for a `steps.`-rooted reference at
// all — nothing inside one is a call.
func hoverFunction(doc *document, v *value, cursor int) *lsp.Hover {
	fn, span, ok := functionAt(v.expr, cursor)
	if !ok {
		return nil
	}

	rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])

	if fn.Name == "" {
		// A namespace rather than a function: the cursor is on `math` in
		// `math.abs(x)`, or on one written alone.
		return markdownHover(fmt.Sprintf(
			"**`%s`** — a namespace of functions.\n\nWritten `%s.<name>(...)`. "+
				"`flow tasks` lists what is in it, and every other name the profile provides.",
			fn.Library, fn.Library), rng)
	}

	if fn.Macro {
		return markdownHover(fmt.Sprintf(
			"**`%s`** — a macro from the `%s` library.\n\n"+
				"It is written on something — `math.greatest(1, 2)`, `[3,1,2].sortBy(v, v)` — and is "+
				"expanded when the file *compiles*, so what a run carries is the expansion rather "+
				"than this spelling. That is why a macro's meaning is frozen by the spec where a "+
				"function's is resolved by whichever worker evaluates the run.",
			fn.Name, fn.Library), rng)
	}

	return markdownHover(fmt.Sprintf(
		"**`%s`** — from the `%s` library.\n\n"+
			"Available to every expression in the file: an `if:`, a `vars:` value, a task input, "+
			"a loop's `items:`, a `wait_until:`. One profile, one dialect.",
		fn.Name, fn.Library), rng)
}

// functionAt returns the function named at the cursor, and the span of the name as
// the author wrote it.
//
// A zero Name with a Library set means the cursor is on a *namespace* rather than a
// function — `math` in `math.abs(x)`.
//
// Text over the parsed expression, matching [referenceAt], because the same thing
// makes both work: an expression that does not parse is exactly when somebody is
// mid-edit and most wants to know what a name is.
func functionAt(src string, cursor int) (v1.LibraryFunction, [2]int, bool) {
	var none v1.LibraryFunction

	segments, at, ok := segmentAt(src, cursor)
	if !ok {
		return none, [2]int{}, false
	}

	index := functionIndex()

	// A qualified name wins over a bare one, and the difference is real: `replace`
	// is a function in the strings library *and* the tail of `regex.replace` in
	// another. Describing the bare one where the author wrote the qualified one
	// would name the wrong library and the wrong behaviour.
	if at > 0 {
		if fn, found := index.byName[segments[at-1].text+"."+segments[at].text]; found {
			return fn, [2]int{segments[at-1].start, segments[at].end}, true
		}
	}
	if at+1 < len(segments) {
		if fn, found := index.byName[segments[at].text+"."+segments[at+1].text]; found {
			return fn, [2]int{segments[at].start, segments[at+1].end}, true
		}
	}

	if fn, found := index.byName[segments[at].text]; found {
		return fn, [2]int{segments[at].start, segments[at].end}, true
	}

	if index.namespaces[segments[at].text] {
		// Reported as a library with no function name, which is how the caller
		// tells a namespace from a function without a second return value that
		// would be ignored everywhere else.
		return v1.LibraryFunction{Library: segments[at].text},
			[2]int{segments[at].start, segments[at].end}, true
	}

	return none, [2]int{}, false
}

// A segment is one dot-separated part of the word under the cursor, and where it
// sits in the expression.
type segment struct {
	text  string
	start int
	end   int
}

// segmentAt splits the word around the cursor and says which part the cursor is in.
//
// The word is taken the way [referenceAt] takes it, so the two agree about where a
// name begins and ends — a hover that highlighted a different span from the one the
// reference lookup considered would underline something the text beside it is not
// about.
func segmentAt(src string, cursor int) ([]segment, int, bool) {
	if cursor < 0 || cursor > len(src) {
		return nil, 0, false
	}

	isWord := func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}

	start := min(cursor, len(src))
	for start > 0 && isWord(src[start-1]) {
		start--
	}
	end := min(cursor, len(src))
	for end < len(src) && isWord(src[end]) {
		end++
	}
	if start == end {
		return nil, 0, false
	}

	var (
		segments []segment
		at       = -1
		from     = start
	)
	for i := start; i <= end; i++ {
		if i < end && src[i] != '.' {
			continue
		}
		if text := src[from:i]; text != "" {
			segments = append(segments, segment{text: text, start: from, end: i})
			// The cursor sits in this part when it is anywhere from its first
			// character to just past its last — the trailing edge included, since
			// an editor reports a cursor between two characters and hovering the
			// end of a name is hovering the name.
			if cursor >= from && cursor <= i {
				at = len(segments) - 1
			}
		}
		from = i + 1
	}

	if at < 0 || len(segments) == 0 {
		return nil, 0, false
	}

	return segments, at, true
}
