package flowstatev1

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseDuration reads a duration the way the DSL writes one: Go's syntax, plus
// days.
//
// Go's time.ParseDuration stops at hours, which was fine while the longest thing
// anyone wrote was an activity timeout. It is not fine now that the headline
// capability is waiting a week for someone to approve a deploy: `168h` is the same
// duration as `7d` and communicates none of it, and an author who writes `7d` and
// is told it is not a duration will conclude the feature does not do what it says.
//
// Days are the last unit worth adding. Weeks are ambiguous enough in scheduling
// that spelling them `7d` is clearer, and months are not a duration at all.
//
// # Why this lives here rather than in the compiler
//
// It was `flowfile.parseDuration`, next to the only thing that called it, for as
// long as `sleep: 30s` was the only way to say one. Now an expression can produce
// a duration too — `sleep: ${inputs.grace}` — and a *string* it produces has to
// mean exactly what the same characters mean written literally. Two parsers would
// be the disagreement CLAUDE.md keeps finding: `7d` accepted in the file and
// refused at run time, discovered by whoever wrote the expression. So it sits in
// the package both drivers and the compiler already import, and there is one of
// it.
func ParseDuration(s string) (time.Duration, error) {
	converted, err := expandDays(s)
	if err != nil {
		return 0, err
	}

	return time.ParseDuration(converted)
}

// expandDays rewrites day components as hours, so that Go's parser can read the
// rest.
//
// No unit Go accepts contains the letter d, so a d in a duration is either this
// unit or a typo — which means rewriting it cannot corrupt an otherwise valid
// duration, and anything left over still fails to parse and still gets a
// diagnostic.
func expandDays(s string) (string, error) {
	var out strings.Builder

	for i := 0; i < len(s); {
		if s[i] != 'd' {
			out.WriteByte(s[i])
			i++

			continue
		}

		// Walk back over the number this d belongs to.
		written := out.String()
		start := len(written)
		for start > 0 && (isDigit(written[start-1]) || written[start-1] == '.') {
			start--
		}
		if start == len(written) {
			return "", fmt.Errorf("%q has a d with no number before it", s)
		}

		days, err := strconv.ParseFloat(written[start:], 64)
		if err != nil {
			return "", fmt.Errorf("%q is not a number of days", written[start:])
		}

		out.Reset()
		out.WriteString(written[:start])
		// Written as hours rather than as a scaled duration so that a fractional
		// day keeps its precision through Go's own parser.
		fmt.Fprintf(&out, "%gh", days*24)
		i++
	}

	return out.String(), nil
}

// isDigit reports whether b is an ASCII digit.
func isDigit(b byte) bool { return b >= '0' && b <= '9' }
