package flowfile

import (
	"fmt"
	"regexp"
	"strings"
)

// A CEL expression is parsed by cel-go, which is parsed by ANTLR, and ANTLR
// explains a failure in the vocabulary of the grammar it generated from:
// `mismatched input '<EOF>' expecting {'[', '{', NUM_FLOAT, ..., IDENTIFIER}`.
// Every word of that is true and none of it is the author's. `NUM_FLOAT` is a
// lexer rule name, `<EOF>` is "the input ended", and "mismatched input" is what
// a recursive-descent parser calls a token it did not want.
//
// The house standard for a diagnostic is position, what is wrong, and what to do
// instead (`validate.go`). A parser generator's token names meet none of those,
// so they are translated here, at the boundary where cel-go's answer becomes a
// Flowfile diagnostic (#383).
//
// # What this deliberately is not
//
// It is not a second CEL grammar. Wrapping ANTLR's whole message space would be
// a copy of the parser's rule set kept in sync by hand, which is the thing that
// drifts. It covers the shapes an author actually reaches by mistyping, and
// anything it does not recognise passes through behind a stable prefix rather
// than being swallowed: a message nobody translated is worth strictly more than
// no message, and hiding one would make a real failure look like none.
//
// The position is untouched. cel-go reports a line and column within the
// expression source and [celFailure] already maps that onto the Flowfile's own
// line and column, which is the half of the diagnostic that was never wrong.

// celSyntaxPrefix is what cel-go puts in front of a parse failure, as opposed to
// a type-check or a macro expansion failure. Only these are translated: the
// others are already written in the language of expressions rather than of the
// parser that reads them.
const celSyntaxPrefix = "Syntax error: "

// celUntranslated is the stable prefix an unrecognised parser message keeps, so
// that a reader can tell "this is the parser talking" from a sentence this
// package wrote, and so nothing is ever dropped.
const celUntranslated = "the expression parser reported: "

var (
	// celMismatched matches ANTLR's two ways of saying "not that token here".
	// They differ in whether the parser could resume, which an author cannot act
	// on, so both translate the same way.
	celMismatched = regexp.MustCompile(`^(?:mismatched|extraneous) input '(.*)' expecting (.+)$`)

	// celNoAlternative matches the message for input no rule can begin with.
	celNoAlternative = regexp.MustCompile(`^no viable alternative at input '(.*)'$`)

	// celUnrecognized matches the lexer failing before the parser is reached: a
	// character that is not part of the language, or an unclosed text value.
	celUnrecognized = regexp.MustCompile(`^token recognition error at: '(.*)'$`)

	// celMissing matches ANTLR reporting a token it inserted to recover.
	celMissing = regexp.MustCompile(`^missing '(.*)' at '(.*)'$`)
)

// celEOF is how ANTLR spells "the input ended".
const celEOF = "<EOF>"

// TranslateCELMessage rewrites a cel-go parse failure into the vocabulary of the
// person who wrote the expression.
//
// src is the expression's own source text, used only to name the token an
// author's eye is already on when the parser ran out of input: "the expression
// ends" is true but does not say where to type, and "ends after +" does.
// column is cel-go's 1-based column within src, or zero when it is not known.
func TranslateCELMessage(msg, src string, column int) string {
	rest, ok := strings.CutPrefix(msg, celSyntaxPrefix)
	if !ok {
		return msg
	}

	if m := celMismatched.FindStringSubmatch(rest); m != nil {
		return celMismatchedMessage(m[1], m[2], src, column)
	}
	if m := celNoAlternative.FindStringSubmatch(rest); m != nil {
		return fmt.Sprintf("%q is not valid here", m[1])
	}
	if m := celUnrecognized.FindStringSubmatch(rest); m != nil {
		return celUnrecognizedMessage(m[1])
	}
	if m := celMissing.FindStringSubmatch(rest); m != nil {
		if m[2] == celEOF {
			return fmt.Sprintf("the expression ends with %q missing", m[1])
		}
		return fmt.Sprintf("%q is missing before %q", m[1], m[2])
	}

	return celUntranslated + rest
}

// celMismatchedMessage explains a token the parser did not want, or the input
// ending where it wanted one.
func celMismatchedMessage(got, expecting, src string, column int) string {
	want := celExpectation(expecting)

	// "expecting <EOF>" is the parser saying the expression was already complete,
	// which is a different mistake from writing the wrong thing in an unfinished
	// one: nothing is missing, something is left over. Worth naming, because the
	// fix is to delete rather than to add.
	if strings.TrimSpace(expecting) == celEOF && got != celEOF {
		return fmt.Sprintf("%q is not valid here: the expression is already complete before it", got)
	}

	if got != celEOF {
		if want == "" {
			return fmt.Sprintf("%q is not valid here", got)
		}
		return fmt.Sprintf("%q is not valid here, where %s was expected", got, want)
	}

	// The input ran out. What an author needs is not "it ended" but where it
	// ended, and the last thing they typed is the anchor for that.
	after := celLastToken(src, column)
	switch {
	case after == "" && want == "":
		return "the expression ends before it is complete"
	case after == "":
		return fmt.Sprintf("the expression ends where %s was expected", want)
	case want == "":
		return fmt.Sprintf("the expression ends after %q, before it is complete", after)
	default:
		return fmt.Sprintf("the expression ends after %q, which needs %s to follow", after, want)
	}
}

// celUnrecognizedMessage explains text the lexer could not read at all.
//
// An unclosed text value is worth separating from a stray character, because it
// is the one shape where the character at fault is fine and its missing partner
// is the problem: the lexer reports the whole run from the opening quote to the
// end of the input, so naming that run as "not a character" would be nonsense.
func celUnrecognizedMessage(text string) string {
	if strings.HasPrefix(text, "'") {
		return "a text value opens with ' here and is never closed"
	}
	if strings.HasPrefix(text, `"`) {
		return `a text value opens with " here and is never closed`
	}
	return fmt.Sprintf("%q is not something an expression can contain", text)
}

// celExpectation renders ANTLR's expecting-set as what an author should type.
//
// The set is either a single token or a braced list, and its members are either
// quoted literals ('[', ':') or the names of lexer rules (NUM_FLOAT, STRING,
// IDENTIFIER). A rule name is exactly the parser's way of saying "a value of
// this kind", and every rule name the CEL grammar can expect here names a way of
// starting a value, so a set containing any of them is the set that means "a
// value". That collapses the twelve-member set an author sees today into the one
// word it was saying.
//
// Returns empty when the set says nothing an author can act on, which is what
// `expecting <EOF>` amounts to: "stop typing" is already carried by naming the
// token that should not be there.
func celExpectation(set string) string {
	set = strings.TrimSpace(set)
	if set == celEOF {
		return ""
	}
	set = strings.TrimSuffix(strings.TrimPrefix(set, "{"), "}")

	var literals []string
	for _, member := range celSetMembers(set) {
		if member == "" || member == celEOF {
			continue
		}
		if quoted, ok := strings.CutPrefix(member, "'"); ok {
			literals = append(literals, `"`+strings.TrimSuffix(quoted, "'")+`"`)
			continue
		}
		// A lexer rule name, so the whole set is the value-start set.
		return "a value"
	}

	switch len(literals) {
	case 0:
		return ""
	case 1:
		return literals[0]
	default:
		return strings.Join(literals[:len(literals)-1], ", ") + " or " + literals[len(literals)-1]
	}
}

// celSetMembers splits an expecting-set into its members.
//
// Not `strings.Split(set, ",")`, because a member may *be* a comma: the set for
// an unfinished list is `{']', ','}`, and splitting on the separator cuts that
// member in half and leaves two fragments that are neither literals nor rule
// names. A quoted member is therefore read to its closing quote before the next
// separator is looked for. ANTLR writes these literals without escapes, so the
// closing quote is the next one.
func celSetMembers(set string) []string {
	var members []string
	for i := 0; i < len(set); {
		switch {
		case set[i] == ' ' || set[i] == ',':
			i++
		case set[i] == '\'':
			end := strings.IndexByte(set[i+1:], '\'')
			if end < 0 {
				// Unbalanced, so there is no member boundary left to find.
				members = append(members, set[i:])
				return members
			}
			members = append(members, set[i:i+1+end+1])
			i += 1 + end + 1
		default:
			end := strings.IndexAny(set[i:], ", ")
			if end < 0 {
				members = append(members, set[i:])
				return members
			}
			members = append(members, set[i:i+end])
			i += end
		}
	}
	return members
}

// celLastToken names the last thing written before column, so a diagnostic about
// input that ran out can point at where to keep typing.
//
// Deliberately crude: it takes the trailing run of non-space characters rather
// than lexing, because the answer is being quoted into a sentence for a human to
// recognise and not fed to anything. Lexing here would be a second CEL lexer to
// keep in step with the first, for no gain over the text the author can see.
func celLastToken(src string, column int) string {
	if column > 0 && column-1 <= len(src) {
		src = src[:column-1]
	}
	src = strings.TrimRight(src, " \t\n\r")
	if src == "" {
		return ""
	}
	if i := strings.LastIndexAny(src, " \t\n\r"); i >= 0 {
		return src[i+1:]
	}
	return src
}
