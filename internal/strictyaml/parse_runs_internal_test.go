package strictyaml

import (
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml/lexer"
)

// TestNestingCheckReadsALongRunOfPropertiesOnce pins that the check does not
// ask where a run of anchors, tags and comments ends once per member: 200,000
// properties before one value are walked in milliseconds, where asking per
// member is quadratic and takes tens of seconds. The tokens are lexed before
// the clock starts, so what is timed is the check and not the lexer or the
// parser behind it.
func TestNestingCheckReadsALongRunOfPropertiesOnce(t *testing.T) {
	const n = 200_000

	for name, unit := range map[string]string{"anchors": "&a ", "tags": "!t ", "mixed": "&a !t ", "comments": "!t #c\n"} {
		t.Run(name, func(t *testing.T) {
			tokens := lexer.Tokenize(strings.Repeat(unit, n) + "x\n")

			start := time.Now()
			_ = refuseDeepFlow(tokens)

			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("%d %s took %v, want linear in the input", n, name, took)
			}
		})
	}
}
