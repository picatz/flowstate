package strictyaml

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/lexer"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"
)

// MaxFlowDepth is how deeply collections, flow (`[` and `{`) or block (`- `
// and `key: `), may nest in a document [ParseBytes] reads. It sits well above any document an author
// writes and far below what the parser can be made to pay for: goccy builds
// its tree recursively and its memory grows with the square of the depth, so
// forty thousand levels in an eighty kilobyte file took 2.5 GB (#2338). A
// Flowfile's own bound on the depth of what it reads is lower still and
// reports in the Flowfile's words; this one only has to arrive first.
const MaxFlowDepth = 256

// NestingError is [ParseBytes]'s refusal of a document nested too deeply. It
// carries a position and a fixed sentence and quotes nothing of the document.
type NestingError struct {
	Line, Column int
	Reason       string
}

func (e *NestingError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.Line, e.Column, e.Reason)
}

// ParseBytes parses data into a syntax tree, as [parser.ParseBytes] does, but
// refuses a document whose collections nest more than [MaxFlowDepth]
// levels before the parser builds anything from it. Every caller that parses a
// document it did not write itself goes through here rather than through
// [parser.ParseBytes]. The refusal is a [*NestingError]; the parser's own
// errors are returned as they are.
//
// The depth is read from the lexer's tokens, which are the tokens the parser
// reads, so a bracket in a quoted string or a comment is not counted and one the
// parser would open is. The lexer is linear in the document, which is the bound
// the caller's own size limit already gives it; it is the parser that is not.
func ParseBytes(data []byte, mode parser.Mode) (*ast.File, error) {
	tokens := lexer.Tokenize(string(data))
	if err := refuseDeepFlow(tokens); err != nil {
		return nil, err
	}

	return parser.Parse(tokens, mode)
}

// refuseDeepFlow is the depth check behind [ParseBytes] and [Unmarshal]: the
// lexer's tokens are what the parser reads, so it counts what the parser would
// open and nothing it would not. Flow depth is the brackets still open. Block
// depth is a stack of the columns at which block entries start: a sequence
// entry at its `-`, a mapping entry at the first token of its key (its anchor or
// tag if it has one, never the `:`, whose column an author can move by choosing
// shorter keys). A child must start further right than its parent, so a
// sibling or a dedent pops back and a chain such as `- - - x`, which uses no
// brackets and costs the parser as much, is counted too. A sequence written at
// its parent key's own column pops that key, so the count can fall short by at
// most half; the bound is on cost, not on grammar. Entries inside a flow
// collection belong to it and are not block structure.
//
// The columns are the document's honest reading, and the parser is more
// generous than that: it hands a `-`, a `?`, an anchor or a tag that ends its
// line the next line's content as its value wherever that line starts, even at
// the same column (`- &a` then `k:`), so a column cannot count that level. A
// level opened that way is counted separately, never popped, and bounded by the
// same limit; honest documents put the value of such a token deeper.
func refuseDeepFlow(tokens token.Tokens) error {
	refuse := func(tk *token.Token, what string) error {
		return &NestingError{
			Line:   tk.Position.Line,
			Column: tk.Position.Column,
			Reason: fmt.Sprintf("%s nest more than %d levels deep", what, MaxFlowDepth),
		}
	}

	runs := newPropertyRuns(tokens)

	var (
		flow  int
		block []int

		// entryCol is the column of the first token since the last line break
		// or block indicator: where the entry being read began.
		entryCol int
		fresh    = true
		lastLine int

		// pending is set when a line ended on a token that takes its value from
		// the lines after it, with pendingCol the column its entry started at.
		// shallow counts the times that value began no deeper.
		pending    bool
		pendingCol int
		pendingSeq bool // the token was a `-`, whose null entries may follow one another
		shallow    int
	)

	push := func(tk *token.Token, col int) error {
		for len(block) > 0 && block[len(block)-1] >= col {
			block = block[:len(block)-1]
		}

		block = append(block, col)
		if len(block) > MaxFlowDepth {
			return refuse(tk, "block collections")
		}

		return nil
	}

	for i, tk := range tokens {
		// The parser reads past a comment, wherever it sits, so a comment line
		// between an entry and its value neither ends the entry nor counts as
		// the line its value begins on.
		if tk.Type == token.CommentType {
			continue
		}

		if tk.Position.Line != lastLine {
			if pending && flow == 0 && tk.Position.Column <= pendingCol && !(pendingSeq && tk.Type == token.SequenceEntryType) {
				shallow++
				if shallow > MaxFlowDepth {
					return refuse(tk, "entries whose value starts no deeper than the entry; they")
				}
			}

			pending = false
			lastLine = tk.Position.Line
			fresh = true
		}

		switch tk.Type {
		case token.SequenceStartType, token.MappingStartType:
			flow++
			if flow > MaxFlowDepth {
				return refuse(tk, "flow collections")
			}
		case token.SequenceEndType, token.MappingEndType:
			flow = max(flow-1, 0)
		case token.SequenceEntryType:
			if flow == 0 {
				if err := push(tk, tk.Position.Column); err != nil {
					return err
				}

				fresh = true
				pending, pendingCol, pendingSeq = runs.lineEndsAfter(i+1, tk.Position.Line), tk.Position.Column, true

				continue
			}
		case token.MappingValueType:
			if flow == 0 {
				if err := push(tk, entryCol); err != nil {
					return err
				}

				fresh = true

				continue
			}
		case token.MappingKeyType, token.AnchorType, token.TagType:
			if flow == 0 {
				if fresh {
					entryCol, fresh = tk.Position.Column, false
				}

				after := i + 1
				if tk.Type == token.AnchorType {
					after++ // the anchor's name
				}

				pending, pendingCol, pendingSeq = runs.lineEndsAfter(after, tk.Position.Line), entryCol, false
				if pending || tk.Type == token.MappingKeyType {
					// A property or `?` alone on its line takes its value from
					// the lines below, so it is a level of its own. A `?` with its
					// key on the same line is one too: a comment line after it
					// makes the parser nest the next deeper `?` inside it, two
					// levels per line, which no column otherwise counted (#2372).
					if err := push(tk, entryCol); err != nil {
						return err
					}
				}

				continue
			}
		}

		if fresh {
			entryCol, fresh = tk.Position.Column, false
		}
	}

	return nil
}

// propertyRuns holds, for every token index, where the run of properties
// starting there ends. Every property of a run asks where the run ends, so
// asking by walking would cost a line of n anchors n² token reads before the
// nesting bound refused anything; one right-to-left pass answers any start
// exactly, whatever its alignment with the tokens around it.
type propertyRuns struct {
	tokens token.Tokens
	// end[i] is the index of the first token at or after i that is not an
	// anchor (with its name), a tag or a comment; len(tokens) when the
	// properties run off the stream.
	end []int
}

func newPropertyRuns(tokens token.Tokens) *propertyRuns {
	n := len(tokens)
	end := make([]int, n+1)
	end[n] = n

	for i := n - 1; i >= 0; i-- {
		switch tokens[i].Type {
		case token.AnchorType:
			end[i] = end[min(i+2, n)]
		case token.TagType, token.CommentType:
			end[i] = end[i+1]
		default:
			end[i] = i
		}
	}

	return &propertyRuns{tokens: tokens, end: end}
}

// lineEndsAfter reports whether the tokens from index i on, which follow a
// token on line, leave the line to nothing but further properties: an anchor
// (its `&` and its name), a tag or a comment.
func (r *propertyRuns) lineEndsAfter(i, line int) bool {
	j := r.end[min(i, len(r.tokens))]
	if j >= len(r.tokens) {
		return true
	}

	return r.tokens[j].Position.Line != line
}
