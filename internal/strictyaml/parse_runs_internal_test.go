package strictyaml

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml/lexer"
	"github.com/goccy/go-yaml/token"
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

// TestPropertyRunsAnswerLikeWalkingTheRun compares the precomputed answer with
// the walk it replaced, at every start index of token streams built from the
// shapes that make a run end oddly: a bare `&`, an anchor whose name is
// another property, tags, comments, line breaks and values.
func TestPropertyRunsAnswerLikeWalkingTheRun(t *testing.T) {
	walk := func(tokens []*token.Token, i, line int) bool {
		for j := i; j < len(tokens); {
			switch tokens[j].Type {
			case token.AnchorType:
				j += 2
			case token.TagType, token.CommentType:
				j++
			default:
				return tokens[j].Position.Line != line
			}
		}

		return true
	}

	parts := []string{"&a ", "& ", "&", "!t ", "!", "#c\n", "\n", "x ", "- ", "? ", "k: ", "&a\n", "!t\n"}
	rng := rand.New(rand.NewPCG(1, 2))

	// The shapes reviewers found: an anchor token inside another anchor's
	// name slot, which a memo of the walk's start and end answers wrongly.
	sources := []string{"&&&&x\n", "- &&&&x\n", "& !t\n", "& & &a x\n"}
	for range 2000 {
		var src strings.Builder
		for range 1 + rng.IntN(12) {
			src.WriteString(parts[rng.IntN(len(parts))])
		}

		sources = append(sources, src.String())
	}

	for _, src := range sources {
		tokens := lexer.Tokenize(src)
		runs := newPropertyRuns(tokens)

		for i := range len(tokens) + 3 {
			for line := range 5 {
				if got, want := runs.lineEndsAfter(i, line), walk(tokens, i, line); got != want {
					t.Fatalf("%q: lineEndsAfter(%d, %d) = %v, the walk says %v", src, i, line, got, want)
				}
			}
		}
	}
}
