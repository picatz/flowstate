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
func refuseDeepFlow(tokens token.Tokens) error {
	refuse := func(tk *token.Token, what string) error {
		return &NestingError{
			Line:   tk.Position.Line,
			Column: tk.Position.Column,
			Reason: fmt.Sprintf("%s nest more than %d levels deep", what, MaxFlowDepth),
		}
	}

	var (
		flow  int
		block []int

		// entryCol is the column of the first token since the last line break
		// or block indicator: where the entry being read began.
		entryCol int
		fresh    = true
		lastLine int
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

	for _, tk := range tokens {
		if tk.Position.Line != lastLine {
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
		}

		if fresh {
			entryCol, fresh = tk.Position.Column, false
		}
	}

	return nil
}
