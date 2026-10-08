package flowdebug

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Layout bounds how [RenderValue] writes a value.
//
// Every field is a limit on work and on output, not a preference: a value is
// whatever the run produced, and a debugger that prints all of it is one
// `inspect` away from a screen of ten thousand lines. A zero field takes the
// default beside it.
type Layout struct {
	// Width is the longest a value may be and still be written on one line.
	// Default [DefaultInspectWidth].
	Width int

	// Depth is how many levels of containers are opened. A container at the
	// limit is written as its size. Default [DefaultInspectDepth].
	Depth int

	// MaxChildren is how many entries of one container are written; the rest
	// are counted. Default [DefaultInspectChildren].
	MaxChildren int
}

// The defaults an interactive `inspect` renders under.
const (
	DefaultInspectWidth    = 100
	DefaultInspectDepth    = 3
	DefaultInspectChildren = 48
)

func (l Layout) withDefaults() Layout {
	if l.Width <= 0 {
		l.Width = DefaultInspectWidth
	}
	if l.Depth <= 0 {
		l.Depth = DefaultInspectDepth
	}
	if l.MaxChildren <= 0 {
		l.MaxChildren = DefaultInspectChildren
	}

	return l
}

// RenderValue writes a native value the way a person reads it at a prompt.
//
// A value that fits on one line (see [Layout.Width]) is written as the compact
// JSON every other surface hands an author, byte for byte, so a scalar or a
// small record reads the same here as in a transcript or a file. A larger value
// is written as a tree: a map one sorted key per line, a list one indexed item
// per line, scalars inline and strings quoted and escaped as JSON so a control
// character in data cannot reach a terminal as itself. What a tree leaves out is
// said in place — `… 12 more keys`, `… 4000 more items`, `{… 7 keys}` — because
// a silently short listing is one nobody can tell from a complete one.
//
// # It is given a value that was already redacted
//
// This function formats and never decides what may be shown. The caller has
// redacted the tree, so a withheld leaf arrives as its marker and is written as
// it came, and the line-level backstop still runs over what this returns. What
// the layout leaves out depends only on the tree's shape, never on a leaf's
// content: an elision count that varied with the length of a string beneath it
// would tell a reader something about a value that was cut.
func RenderValue(native any, layout Layout) string {
	layout = layout.withDefaults()

	if compact := nativeText(native); utf8.RuneCountInString(compact) <= layout.Width {
		return compact
	}

	var out strings.Builder
	writeTree(&out, native, "", 1, layout)

	return strings.TrimSuffix(strings.TrimPrefix(out.String(), "\n"), "\n")
}

// writeTree writes value, whose first line continues the caller's, and every
// further line indented by indent.
func writeTree(out *strings.Builder, value any, indent string, depth int, layout Layout) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			out.WriteString("{}\n")

			return
		}
		if depth > layout.Depth {
			fmt.Fprintf(out, " {… %s}\n", plural(len(typed), "key"))

			return
		}
		out.WriteString("\n")
		keys := slices.Sorted(maps.Keys(typed))
		for i, key := range keys {
			if i == layout.MaxChildren {
				fmt.Fprintf(out, "%s… %s\n", indent, plural(len(keys)-i, "more key"))

				break
			}
			fmt.Fprintf(out, "%s%s:", indent, quoteKey(key))
			childIndent := indent + "  "
			if isContainer(typed[key]) {
				writeTree(out, typed[key], childIndent, depth+1, layout)

				continue
			}
			out.WriteString(" ")
			writeTree(out, typed[key], childIndent, depth+1, layout)
		}

	case []any:
		if len(typed) == 0 {
			out.WriteString("[]\n")

			return
		}
		if depth > layout.Depth {
			fmt.Fprintf(out, " [… %s]\n", plural(len(typed), "item"))

			return
		}
		out.WriteString("\n")
		for i, item := range typed {
			if i == layout.MaxChildren {
				fmt.Fprintf(out, "%s… %s\n", indent, plural(len(typed)-i, "more item"))

				break
			}
			fmt.Fprintf(out, "%s[%d]", indent, i)
			childIndent := indent + "  "
			if isContainer(item) {
				writeTree(out, item, childIndent, depth+1, layout)

				continue
			}
			out.WriteString(" ")
			writeTree(out, item, childIndent, depth+1, layout)
		}

	default:
		out.WriteString(nativeText(value))
		out.WriteString("\n")
	}
}

func isContainer(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		return len(typed) > 0
	case []any:
		return len(typed) > 0
	default:
		return false
	}
}

// quoteKey writes a map key as itself when it reads as a name, and as a JSON
// string when anything in it could be mistaken for the layout or reach a
// terminal.
func quoteKey(key string) string {
	if key != "" && strings.IndexFunc(key, func(r rune) bool {
		return !(r == '_' || r == '-' || r == '.' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
	}) < 0 {
		return key
	}

	return strconv.Quote(key)
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}

	return fmt.Sprintf("%d %ss", n, noun)
}
