package strictyaml

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/lexer"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"
)

// MaxFlowDepth is how deeply flow collections (`[` and `{`) may nest in a
// document [ParseBytes] reads. It sits well above any document an author
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
// refuses a document whose flow collections nest more than [MaxFlowDepth]
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
// open and nothing it would not.
func refuseDeepFlow(tokens token.Tokens) error {
	depth := 0
	for _, tk := range tokens {
		switch tk.Type {
		case token.SequenceStartType, token.MappingStartType:
			depth++
			if depth > MaxFlowDepth {
				return &NestingError{
					Line:   tk.Position.Line,
					Column: tk.Position.Column,
					Reason: fmt.Sprintf("flow collections nest more than %d levels deep", MaxFlowDepth),
				}
			}
		case token.SequenceEndType, token.MappingEndType:
			depth = max(depth-1, 0)
		}
	}

	return nil
}
