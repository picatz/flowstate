package flowfile

import (
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/token"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Source positions are kept because a diagnostic without one is a diagnostic the
// author has to go looking for. Compiling into the schema throws the source away
// unless something records it on the way past, so [Parse] records a span for
// every field it reads and hands them back alongside the workflow.

// A Position is one point in a Flowfile's source.
//
// Column counts characters rather than bytes, which is what the YAML parser
// reports; an editor that needs UTF-16 offsets converts from the line and column.
type Position struct {
	// Line is the 1-based line, or zero when the position is unknown.
	Line int

	// Column is the 1-based column, or zero when only the line is known.
	Column int
}

// IsValid reports whether the position names a line.
func (p Position) IsValid() bool { return p.Line > 0 }

// String renders the position as line:column, the form editors and terminals
// expect.
func (p Position) String() string {
	switch {
	case !p.IsValid():
		return "?"
	case p.Column > 0:
		return strconv.Itoa(p.Line) + ":" + strconv.Itoa(p.Column)
	default:
		return strconv.Itoa(p.Line)
	}
}

// A Span covers the source text of one thing a Flowfile says.
//
// End is exclusive, so a span over a three-character value on one line has an End
// three columns past its Start. That is what an editor needs to underline exactly
// the offending token and nothing more.
type Span struct {
	Start Position
	End   Position
}

// IsValid reports whether the span names a position.
func (s Span) IsValid() bool { return s.Start.IsValid() }

// String renders the span as start-end, or just the start when the span covers a
// single point.
func (s Span) String() string {
	if s.Start == s.End {
		return s.Start.String()
	}
	return s.Start.String() + "-" + s.End.String()
}

// Positions maps the parts of a compiled workflow to where they were written.
//
// Paths address the source, using the keys a Flowfile actually spells rather than
// the field names of the schema it compiles to, because that is what an author
// reading a diagnostic sees:
//
//	name
//	description
//	steps[0]                        the step itself, starting at its first key
//	steps[0].id
//	steps[0].if
//	steps[0].timeout
//	steps[0].continue_on_error
//	steps[0].retry                  and .attempts, .interval, .backoff, .max_interval
//	steps[0].echo                   the task, keyed by its own name
//	steps[0].echo.message           nested values continue: .headers.A, .args[1]
//	steps[0].for_each               and .items, .iterator, .max_parallel, .steps
//	steps[0].for_each.steps[1].id   body steps are addressed like any other step
//	steps[0].parallel[0].steps[1]   as are the steps of a parallel branch
//
// A key containing a dot or a bracket makes its path ambiguous. Such paths are
// still recorded, and are still exact for [Positions.At], but they cannot be
// constructed reliably by a caller assembling a path from names.
//
// A nil *Positions answers every question with false, so a caller that did not
// ask for positions need not check.
type Positions struct {
	spans map[string]Span
	exprs map[string]Span
	steps map[string]string
}

func newPositions() *Positions {
	return &Positions{
		spans: make(map[string]Span),
		exprs: make(map[string]Span),
		steps: make(map[string]string),
	}
}

// At returns the span of the source written at path.
//
// For a value written as a fenced expression the span covers the whole scalar,
// fence included; [Positions.ExprAt] returns just the expression inside it.
func (p *Positions) At(path string) (Span, bool) {
	if p == nil {
		return Span{}, false
	}
	span, ok := p.spans[path]
	return span, ok
}

// ExprAt returns the span of the expression text at path, exclusive of any
// ${...} fence around it.
//
// This is the span to underline for a problem with an expression itself, such as
// a CEL syntax error or a reference that cannot resolve: the fence is punctuation
// the author did not get wrong.
func (p *Positions) ExprAt(path string) (Span, bool) {
	if p == nil {
		return Span{}, false
	}
	span, ok := p.exprs[path]
	return span, ok
}

// StepPath returns the path of the step with the given id, at whatever depth it
// was declared.
//
// When a file declares the same id twice — which [Validate] reports — the first
// declaration wins, so the position is the one a reader finds first.
//
// That rule is only ambiguous for a file [Validate] refuses. An id may not repeat
// at the top level, and may not shadow one a step is nested inside, so in any file
// that compiles there is exactly one step per id and this is exact rather than a
// choice. The first-wins rule exists for the diagnostics reporting the collision
// itself, which have to land somewhere.
func (p *Positions) StepPath(id string) (string, bool) {
	if p == nil {
		return "", false
	}
	path, ok := p.steps[id]
	return path, ok
}

// Locate returns the tightest span for a problem described the way a
// [Diagnostic] describes one: by the step it is in and the input or property at
// fault, either of which may be empty.
//
// It narrows as far as it can and then gives up gracefully — the named input, else
// the named property, else the step, else nothing — because a diagnostic placed on
// the right step is useful and one placed on line 1 is not.
func (p *Positions) Locate(step, field string) (Span, bool) {
	if p == nil {
		return Span{}, false
	}

	if step == "" {
		return p.At(field)
	}

	base, ok := p.StepPath(step)
	if !ok {
		return Span{}, false
	}
	if field == "" {
		return p.At(base)
	}

	// A field is either the name of a task input or the name of a property of the
	// step, and a Diagnostic does not distinguish them. Both are tried, most
	// specific first.
	//
	// A task input now sits under the task's *name* rather than under a fixed
	// `task.inputs`, and a Diagnostic does not carry which task the step runs — so
	// every registered name is tried. Only one of them can be a key of this step,
	// because a step does exactly one kind of work, so there is no ambiguity to
	// resolve: at most one candidate exists in the map.
	candidates := make([]string, 0, len(v1.TaskNames())+3)
	for _, task := range v1.TaskNames() {
		candidates = append(candidates, base+"."+task+"."+field)
	}
	candidates = append(candidates, base+"."+field, base+".for_each."+field)

	// A `wait_for_signal:` mapping's own fields — its `timeout:` expression and
	// each `outputs.<name>` shaping entry — are recorded under the key the author
	// wrote them in, one level below the step, exactly as a loop's are under
	// `for_each`. Without this candidate a diagnostic about one of them fell back
	// to the whole step, so the squiggle sat on `- id:` while the expression at
	// fault was lines away (#318). A step does one kind of work, so at most one of
	// these candidates can exist in the map and adding this one cannot make the
	// search ambiguous.
	candidates = append(candidates, base+".wait_for_signal."+field)

	for _, candidate := range candidates {
		if span, ok := p.At(candidate); ok {
			return span, true
		}
	}
	return p.At(base)
}

// LocateKind returns the span of a step's kind key — the token naming the work
// the step does, whether that is a task's name or `for_each`.
//
// Addressed exactly rather than by the candidate search [Positions.Locate] does,
// because a kind is a key of the step itself: there is one place it can be, and
// guessing would let a task named like some other task's input resolve to the
// wrong token.
func (p *Positions) LocateKind(step, kind string) (Span, bool) {
	if p == nil {
		return Span{}, false
	}
	base, ok := p.StepPath(step)
	if !ok {
		return Span{}, false
	}
	if kind == "" {
		return p.At(base)
	}
	if span, ok := p.At(fieldPath(base, kind)); ok {
		return span, true
	}

	// No fallback to the step, unlike [Positions.Locate].
	//
	// A missing kind path means the step this resolved to is not the step the
	// diagnostic is about — StepPath keeps the *first* declaration of an id, so a
	// step inside a loop body sharing an id with one at the top level resolves to
	// the wrong one. Falling back would then underline a step that has nothing
	// wrong with it, and a false diagnostic is worse than an unplaced one: it sends
	// an author to correct working code.
	//
	// Declining leaves the diagnostic without a position, which reads as "somewhere
	// in this file" rather than as "here, and here is wrong".
	return Span{}, false
}

// record stores the span of the value at path.
func (p *Positions) record(path string, span Span) {
	if span.IsValid() {
		p.spans[path] = span
	}
}

// recordExpr stores the span of the expression text at path.
func (p *Positions) recordExpr(path string, span Span) {
	if span.IsValid() {
		p.exprs[path] = span
	}
}

// recordStep associates a step id with its path, keeping the first declaration so
// that a duplicated id reports the one a reader reaches first.
func (p *Positions) recordStep(id, path string) {
	if id == "" {
		return
	}
	if _, seen := p.steps[id]; !seen {
		p.steps[id] = path
	}
}

// fieldPath returns the path of a named field of base.
func fieldPath(base, name string) string {
	if base == "" {
		return name
	}
	return base + "." + name
}

// indexPath returns the path of the i'th element of base.
func indexPath(base string, i int) string {
	return base + "[" + strconv.Itoa(i) + "]"
}

// spanOfToken returns the span of a single token's own text.
func spanOfToken(tok *token.Token) Span {
	if tok == nil || tok.Position == nil {
		return Span{}
	}
	start := Position{Line: tok.Position.Line, Column: tok.Position.Column}
	if tok.Type == token.ImplicitNullType {
		// Nothing was written, so there is nothing to underline: the span is the
		// point where a value would have gone.
		return Span{Start: start, End: start}
	}
	return Span{Start: start, End: advance(start, tokenText(tok))}
}

// spanOfNode returns the span covering everything a node was written from,
// including a whole block mapping or sequence rather than just its first token.
func spanOfNode(n ast.Node) Span {
	var out Span
	eachToken(n, func(tok *token.Token) {
		span := spanOfToken(tok)
		if !span.IsValid() {
			return
		}
		if !out.Start.IsValid() || before(span.Start, out.Start) {
			out.Start = span.Start
		}
		if !out.End.IsValid() || before(out.End, span.End) {
			out.End = span.End
		}
	})
	return out
}

// eachToken calls fn for every token in a node's subtree.
//
// Aliases are not followed: an alias is written where it appears, and its span is
// the alias, not the anchor it names.
func eachToken(n ast.Node, fn func(*token.Token)) {
	if n == nil {
		return
	}
	switch x := n.(type) {
	case *ast.MappingNode:
		for _, v := range x.Values {
			eachToken(v, fn)
		}
	case *ast.MappingValueNode:
		eachToken(x.Key, fn)
		eachToken(x.Value, fn)
	case *ast.SequenceNode:
		fn(x.Start)
		for _, v := range x.Values {
			eachToken(v, fn)
		}
	case *ast.AnchorNode:
		fn(x.Start)
		eachToken(x.Name, fn)
		eachToken(x.Value, fn)
	case *ast.TagNode:
		fn(x.Start)
		eachToken(x.Value, fn)
	case *ast.LiteralNode:
		fn(x.Start)
		eachToken(x.Value, fn)
	default:
		fn(n.GetToken())
	}
}

// before reports whether a comes earlier in the source than b.
func before(a, b Position) bool {
	if !a.IsValid() {
		return false
	}
	if !b.IsValid() {
		return true
	}
	if a.Line != b.Line {
		return a.Line < b.Line
	}
	return a.Column < b.Column
}

// advance returns the position just past text, starting from p.
func advance(p Position, text string) Position {
	for _, r := range text {
		if r == '\n' {
			p.Line++
			p.Column = 1
			continue
		}
		p.Column++
	}
	return p
}

// tokenText returns a token's own source text, without the surrounding
// whitespace the parser keeps in Origin so that it can reproduce a document
// byte for byte.
func tokenText(tok *token.Token) string {
	if tok == nil {
		return ""
	}
	if text := strings.TrimSpace(tok.Origin); text != "" {
		return text
	}
	return tok.Value
}

// spanWithin returns the span of inner as it appears inside the source text of
// node, or the node's own span when inner cannot be found there.
//
// It is how the span of an expression is narrowed to the expression: the text of
// ${steps.a.result} is found inside the scalar that carries it, quotes and fence
// and all, so the reported span covers steps.a.result alone.
func spanWithin(n ast.Node, inner string) Span {
	outer := spanOfNode(n)
	if inner == "" || !outer.IsValid() {
		return outer
	}
	text := tokenText(n.GetToken())
	i := strings.Index(text, inner)
	if i < 0 {
		return outer
	}
	start := advance(outer.Start, text[:i])
	return Span{Start: start, End: advance(start, inner)}
}
