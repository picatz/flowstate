package lsp

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/google/cel-go/cel"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Diagnostics are the reason this server exists. Everything else makes authoring
// pleasant; this is what makes it correct.
//
// Four sources feed in, in order of how precisely they can be placed:
//
//  1. YAML syntax errors, which carry a token and so a range.
//  2. CEL syntax errors, placed at the offending character inside the expression
//     rather than on the step, because "something in this step is wrong" is not
//     a useful thing to tell someone with six inputs.
//  3. Schema checks against the task registry's descriptors: an input the task
//     does not declare, or a required one that is missing.
//  4. Everything flowfile.ValidateSource reports, re-placed onto the token at
//     fault using the positional model.
//
// A document with no problems publishes an empty list rather than nothing at all.
// Skipping the notification leaves the editor showing the errors it was told about
// last, which outlives the mistake and trains the author to distrust the squiggles.

// diagnosticSource labels every diagnostic so an editor can attribute it, and so
// a user can tell a Flowfile problem from their YAML plugin's opinion.
const diagnosticSource = "flowstate"

// Stable codes, so an editor can group or filter and a user can search.
const (
	codeYAMLSyntax = "yaml-syntax"
	codeCELSyntax  = "cel-syntax"
	codeFlowfile   = "flowfile"
	codeTooLarge   = "document-too-large"
)

// diagnose returns every problem found in a document.
func diagnose(doc *document) []lsp.Diagnostic {
	set := &diagnosticSet{}

	if doc.tooLarge {
		set.add(lsp.Diagnostic{
			Range:    documentStart,
			Severity: lsp.Warning,
			Source:   diagnosticSource,
			Code:     codeTooLarge,
			Message: fmt.Sprintf(
				"file is %d bytes, larger than the %d byte limit this server analyzes; it is not being checked",
				len(doc.text), maxDocumentBytes),
		})
		return set.sorted()
	}

	if doc.parseErr != nil {
		// Nothing downstream can be trusted once the document does not parse:
		// the model is absent, and every other check would guess. One precise
		// syntax error is the whole report.
		set.add(yamlDiagnostic(doc, doc.parseErr, codeYAMLSyntax))
		return set.sorted()
	}

	// Expressions are checked here rather than left to the validator for one
	// reason: a syntax error gets underlined at the character it occurs on, which
	// a diagnostic positioned at the start of the value cannot do. The fields it
	// flags are recorded so the validator's report of the same problem is not
	// shown a second time.
	flagged := checkExpressions(doc, set)

	// Everything else belongs to flowfile: step structure, durations, ids, unknown
	// tasks, references that cannot resolve. Its diagnostics are used as written
	// and only their positions are improved, because a rule implemented twice is a
	// rule that will eventually differ — and the editor must never disagree with
	// `flow validate` about the same file.
	ds, err := flowfile.ValidateSource([]byte(doc.text))
	if err != nil {
		// A failure to compile now arrives as positioned diagnostics, so it is
		// placed exactly like a validation diagnostic rather than dumped at the
		// top of the file.
		var compiled flowfile.Diagnostics
		if !errors.As(err, &compiled) {
			addCompileFailure(doc, set, err)
			return set.sorted()
		}
		ds = append(ds, compiled...)
	}
	for _, d := range ds {
		rng := rangeOfFlowfileDiagnostic(doc, d)
		if slices.ContainsFunc(flagged, func(f lsp.Range) bool { return overlaps(f, rng) }) {
			// The same expression, already reported with a tighter range. An
			// expression that will not parse cannot also have a second problem, so
			// this is the same mistake described twice.
			continue
		}
		set.add(lsp.Diagnostic{
			Range:    rng,
			Severity: lsp.Error,
			Source:   diagnosticSource,
			Code:     codeFlowfile,
			Message:  d.Message,
		})
	}

	return set.sorted()
}

// A diagnosticSet collects diagnostics, dropping exact duplicates.
//
// Duplicates are possible because two sources can legitimately notice the same
// problem, and an editor renders a doubled message as two overlapping squiggles.
type diagnosticSet struct {
	items []lsp.Diagnostic
	seen  map[string]bool
}

func (s *diagnosticSet) add(d lsp.Diagnostic) {
	key := fmt.Sprintf("%d:%d-%d:%d|%s",
		d.Range.Start.Line, d.Range.Start.Character,
		d.Range.End.Line, d.Range.End.Character, d.Message)
	if s.seen == nil {
		s.seen = make(map[string]bool)
	}
	if s.seen[key] {
		return
	}
	s.seen[key] = true
	s.items = append(s.items, d)
}

func (s *diagnosticSet) empty() bool { return len(s.items) == 0 }

// sorted returns the diagnostics in source order, so the same document always
// produces the same report.
func (s *diagnosticSet) sorted() []lsp.Diagnostic {
	out := s.items
	if out == nil {
		// An empty, non-nil slice: the protocol distinguishes "no problems" from
		// a missing field, and null would leave stale diagnostics in some clients.
		out = []lsp.Diagnostic{}
	}
	slices.SortStableFunc(out, func(a, b lsp.Diagnostic) int {
		if a.Range.Start.Line != b.Range.Start.Line {
			return a.Range.Start.Line - b.Range.Start.Line
		}
		if a.Range.Start.Character != b.Range.Start.Character {
			return a.Range.Start.Character - b.Range.Start.Character
		}
		return strings.Compare(a.Message, b.Message)
	})
	return out
}

// yamlDiagnostic converts a YAML error into a diagnostic at the token it names.
//
// The parser's errors carry the token they failed on, which is what lets a syntax
// error land on the character at fault instead of on line 1 — where an editor
// would show it if the position were simply dropped.
func yamlDiagnostic(doc *document, err error, code string) lsp.Diagnostic {
	d := lsp.Diagnostic{
		Range:    documentStart,
		Severity: lsp.Error,
		Source:   diagnosticSource,
		Code:     code,
		Message:  err.Error(),
	}

	var yamlErr yaml.Error
	if !errors.As(err, &yamlErr) {
		return d
	}

	// The bare message, without the parser's rendered source excerpt: the editor
	// already shows the source.
	if msg := yamlErr.GetMessage(); msg != "" {
		d.Message = msg
	}
	tok := yamlErr.GetToken()
	if tok == nil || tok.Position == nil {
		return d
	}
	start := doc.index.offsetOfYAML(tok.Position.Line, tok.Position.Column)
	width := max(len(tok.Origin), len(tok.Value))
	if trimmed := strings.TrimSpace(tok.Origin); trimmed != "" {
		width = len(trimmed)
	}
	d.Range = doc.index.rangeOfOffsets(start, start+max(width, 1))
	return d
}

// addCompileFailure reports a failure to compile the document to a workflow.
//
// A positioned failure is always reported. An unpositioned one is reported only
// when nothing else was found, because these are almost always the compiler
// refusing an expression that the CEL check above has already flagged precisely —
// and a second copy at line 1 with no range is pure noise.
func addCompileFailure(doc *document, set *diagnosticSet, err error) {
	var yamlErr yaml.Error
	if errors.As(err, &yamlErr) {
		set.add(yamlDiagnostic(doc, err, codeYAMLSyntax))
		return
	}
	if set.empty() {
		set.add(lsp.Diagnostic{
			Range:    documentStart,
			Severity: lsp.Error,
			Source:   diagnosticSource,
			Code:     codeFlowfile,
			Message:  err.Error(),
		})
	}
}

// overlaps reports whether two ranges share any of the document.
//
// It is how a diagnostic this package produced is matched against the validator's
// report of the same problem. Matching on position rather than on the field name
// is what makes it work: the validator names a field for some problems and puts
// the field in the message for others, but both always carry a position.
func overlaps(a, b lsp.Range) bool {
	if a.End.Line < b.Start.Line || b.End.Line < a.Start.Line {
		return false
	}
	if a.End.Line == b.Start.Line && a.End.Character < b.Start.Character {
		return false
	}
	if b.End.Line == a.Start.Line && b.End.Character < a.Start.Character {
		return false
	}
	return true
}

// checkExpressions parses every expression in the document and reports syntax
// errors at the character they occur on.
//
// Only parsing is done, never type checking. A step's outputs are not statically
// known for every task, so a type error reported here could not be trusted, and
// an unjustified squiggle under a working expression is worse than none.
func checkExpressions(doc *document, set *diagnosticSet) []lsp.Range {
	var flagged []lsp.Range
	if doc.parsed == nil {
		return flagged
	}
	ev := v1.DefaultEvaluator()

	// A ${...} expression is resolved by the engine in the base environment, so
	// that is the environment it is checked in. Without one there is nothing to
	// parse with, and silence is the only honest answer.
	baseEnv, err := ev.Env()
	if err != nil {
		return flagged
	}

	for _, s := range doc.parsed.steps {
		def, taskKnown := v1.LookupTask(s.taskName)

		// A step's libraries change how its own expression source parses, because
		// some of them contribute macros.
		stepEnv, libErr := ev.Env(stepLibraries(s, def, taskKnown)...)
		if libErr != nil {
			// Falling back keeps the expression itself checked rather than
			// abandoning the step over a library problem reported elsewhere.
			stepEnv = baseEnv
		}

		for _, in := range s.expressionEntries() {
			// An input the task evaluates itself carries expression source
			// directly, without ${...} — the cel task's expr is the whole point
			// of the task. Which inputs those are is declared on the task
			// definition, so this cannot go stale when a task changes.
			deferred := taskKnown && in != s.conditionEntry &&
				slices.Contains(def.DeferredInputs, in.key)

			walkValues(in.value, func(v *value) {
				var found bool
				switch {
				case v.fenced:
					// Anything fenced is checked, valid or not: a broken
					// expression is the case a precise position matters most for.
					found = reportCELErrors(doc, set, baseEnv, v.expr, v.exprOffset, v.exprRange)

				// A scalar containing a fence but not made of one — `${a} and ${b}`,
				// or an unterminated `${` — is a mistake with no inner span to point
				// at. The validator reports it with a message saying how to fix it,
				// so it is left to that rather than guessed at here.

				case deferred && v.kind == kindScalar && v == in.value && v.text != "":
					// An input the task evaluates itself is expression source
					// directly, without a fence. The validator does not parse
					// these, so this is the only check they get.
					found = reportCELErrors(doc, set, stepEnv, v.text, v.textOffset, v.rng)
				}
				if found {
					flagged = append(flagged, v.rng)
				}
			})
		}
	}
	return flagged
}

// stepLibraries returns the CEL libraries a step enables, skipping any name this
// build does not have.
//
// It reports nothing. An unknown name is a rule, the rule lives in the shared
// validator, and this package's standing constraint is that the editor must never
// disagree with `flow validate` about the same file — so what arrives here is
// re-placed onto the offending element rather than restated. Skipping the name is
// still necessary: handing it to the evaluator would fail environment construction
// and cost the rest of the step its expression checks, over a mistake already
// being reported.
//
// The set of legal names belongs to the evaluator, so an added library becomes
// valid in the editor at the same moment it becomes valid in the engine.
func stepLibraries(s *parsedStep, def v1.TaskDef, taskKnown bool) []string {
	if !taskKnown || def.Inputs == nil {
		return nil
	}
	// A libs input exists only for a task whose schema declares one as a list of
	// strings; nothing here assumes which task that is.
	fd := findField(def.Inputs, "libs")
	if fd == nil || !fd.IsList() || fd.Kind() != protoreflect.StringKind {
		return nil
	}
	in := s.input("libs")
	if in == nil || in.value == nil {
		return nil
	}

	elements := in.value.items
	if in.value.kind == kindScalar {
		// A single library may be written without a list.
		elements = []*value{in.value}
	}

	var libs []string
	for _, el := range elements {
		if el.kind != kindScalar || el.text == "" {
			continue
		}
		if _, ok := lookupCELLibrary(el.text); !ok {
			continue
		}
		libs = append(libs, el.text)
	}
	return libs
}

// reportCELErrors parses src and reports each syntax error at its position in the
// document.
func reportCELErrors(doc *document, set *diagnosticSet, env *cel.Env, src string, srcOffset int, whole lsp.Range) bool {
	if strings.TrimSpace(src) == "" {
		return false
	}
	_, issues := env.Parse(src)
	if issues == nil || issues.Err() == nil {
		return false
	}
	for _, e := range issues.Errors() {
		rng := whole
		if e.Location != nil {
			off := srcOffset + offsetInExpr(src, e.Location.Line(), e.Location.Column())
			if end := srcOffset + len(src); off < end {
				rng = doc.index.rangeOfOffsets(off, off+tokenWidth(src[off-srcOffset:]))
			}
		}
		set.add(lsp.Diagnostic{
			Range:    rng,
			Severity: lsp.Error,
			Source:   diagnosticSource,
			Code:     codeCELSyntax,
			Message:  e.Message,
		})
	}
	return true
}

// tokenWidth returns how many bytes to underline for an error reported at the
// start of s: the identifier or number there, or a single character otherwise.
//
// CEL reports a position but not an extent, and underlining to the end of the
// expression would obscure which part is wrong.
func tokenWidth(s string) int {
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return 1
	}
	return i
}

// acceptsAnyInput reports whether a task takes input names beyond those its
// schema declares.
//
// The cel task does: the compiler flattens a step's `vars` mapping into the input
// map, so every variable an expression uses arrives as an input under its own
// name. That behavior is keyed on a field named `vars` in flowfile's compiler, so
// the same test is used here — any future task declaring a vars mapping gets the
// same treatment the compiler will give it. Guessing wrong in this direction is
// what produces false positives on a perfectly good file, so the test is
// deliberately generous.
func acceptsAnyInput(def v1.TaskDef) bool {
	fd := findField(def.Inputs, "vars")
	return fd != nil && fd.IsMap()
}

// rangeOfFlowfileDiagnostic finds the tightest range for a diagnostic that
// flowfile reported.
//
// The model is preferred because it knows a token's extent, which a point position
// cannot express: underlining exactly `mesage` reads differently from putting a
// caret before it. Where the model cannot resolve the field, the validator's own
// line and column are used, which is still far better than the whole line.
func rangeOfFlowfileDiagnostic(doc *document, d flowfile.Diagnostic) lsp.Range {
	if doc.parsed == nil {
		return reportedRange(doc, d)
	}

	// A problem with the document as a whole, named by the field it concerns.
	if d.Step == "" {
		switch {
		case d.Field == "name" && doc.parsed.nameEntry != nil:
			return doc.parsed.nameEntry.keyRange
		case d.Field == "steps" && doc.parsed.stepsEntry != nil:
			return doc.parsed.stepsEntry.keyRange
		}
		if i, ok := stepIndexField(d.Field); ok && i < len(doc.parsed.steps) {
			return doc.index.rangeOfLineContent(doc.parsed.steps[i].rng.Start.Line)
		}
		return reportedRange(doc, d)
	}

	step := doc.parsed.step(d.Step)
	if step == nil {
		return reportedRange(doc, d)
	}

	// The validator positions some step-level problems precisely without naming a
	// field — a bad timeout, a condition that will not parse — putting the field in
	// the message instead. When its position is on a line of the step other than
	// the one the id is on, it knows something this model cannot infer, so it wins.
	// On the id's own line it does not: for an unknown task the model can point at
	// the task name, which is more useful than the id.
	if d.Field == "" && d.Line > 0 && d.Column > 0 {
		if reported := d.Line - 1; reported != idLine(step) && contains(step.rng, lsp.Position{Line: reported}) {
			return reportedRange(doc, d)
		}
	}

	// A problem with one field: underline its value, narrowed to the expression
	// inside it when there is exactly one, which is where a bad reference is. The
	// field may be a task input or one of the step's own properties — a condition,
	// a timeout, a loop's items — which flowfile distinguishes in its message but
	// not in the field name.
	if d.Field != "" {
		if e := step.entryForField(d.Field); e != nil {
			// An input the task does not declare is a problem with the key, not
			// with what was written under it: `mesage: hello` has a perfectly good
			// value. Which case this is comes from the schema — the field is either
			// declared or it is not — rather than from reading the message.
			if def, known := v1.LookupTask(step.taskName); known && step.input(d.Field) != nil {
				if findField(def.Inputs, d.Field) == nil {
					return e.keyRange
				}
			}
			// One element of a list, when the validator said which. `libs:
			// [json, nope]` is a problem with `nope`, and underlining the whole
			// list makes the reader find it themselves.
			if rng, ok := rangeOfLiteral(e.value, d.Value); ok {
				return rng
			}
			return narrowToExpression(e.valueRange(), e.value)
		}
	}
	return stepProblemRange(doc, step)
}

// stepProblemRange chooses where to underline a problem reported against a whole
// step.
//
// flowfile's step-level diagnostics concern one of two things: the step's id or
// its task. Which one applies is decided from the model and the registry, never
// from the message text, so rewording a diagnostic cannot silently move a
// squiggle. When the id is unusable that is the more fundamental problem and wins.
func stepProblemRange(doc *document, step *parsedStep) lsp.Range {
	idRange := step.rng
	if step.idEntry != nil {
		idRange = step.idEntry.valueRange()
	}

	if idSuspect(doc, step) {
		return idRange
	}
	if _, known := v1.LookupTask(step.taskName); !known {
		switch {
		case step.nameEntry != nil:
			return step.nameEntry.valueRange()
		case step.taskEntry != nil:
			return step.taskEntry.keyRange
		}
	}
	return idRange
}

// idSuspect reports whether a step's id is itself the likely subject of a
// step-level diagnostic.
func idSuspect(doc *document, step *parsedStep) bool {
	if step.id == "" {
		return true
	}
	for i, r := range step.id {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case r >= '0' && r <= '9' && i > 0:
		default:
			return true
		}
	}
	for _, other := range doc.parsed.steps {
		if other != step && other.id == step.id {
			return true
		}
	}
	return false
}

// rangeOfLiteral finds the scalar inside a value whose text is literal.
//
// It is how a diagnostic that names an element — Diagnostic.Value — gets placed on
// that element. The match is on the value the validator reported, never on text
// pulled back out of the message, so rewording a diagnostic cannot move a squiggle.
//
// Only an unambiguous match counts. Two identical entries are two identical
// mistakes, and underlining one of them would be picking arbitrarily; the whole
// value is the honest answer there.
func rangeOfLiteral(v *value, literal string) (lsp.Range, bool) {
	if v == nil || literal == "" {
		return lsp.Range{}, false
	}

	var found []lsp.Range
	walkValues(v, func(c *value) {
		if c.kind == kindScalar && !c.fenced && c.text == literal {
			found = append(found, c.rng)
		}
	})
	if len(found) != 1 {
		return lsp.Range{}, false
	}

	return found[0], true
}

// narrowToExpression returns the range of the single expression inside a value,
// or the value's own range when it holds none or several.
//
// A map input whose one nested value is `${a.b}` should underline that reference,
// not the whole map. With more than one there is no way to tell which is meant, so
// the whole value is the honest answer.
func narrowToExpression(fallback lsp.Range, v *value) lsp.Range {
	if v == nil {
		return fallback
	}
	var found []lsp.Range
	walkValues(v, func(c *value) {
		if c.fenced {
			found = append(found, c.exprRange)
		}
	})
	if len(found) == 1 {
		return found[0]
	}
	return fallback
}

// stepIndexField parses the `steps[N]` field name flowfile uses for a step with
// no id, which cannot be addressed any other way.
func stepIndexField(field string) (int, bool) {
	rest, ok := strings.CutPrefix(field, "steps[")
	if !ok {
		return 0, false
	}
	rest, ok = strings.CutSuffix(rest, "]")
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(rest)
	if err != nil || i < 0 {
		return 0, false
	}
	return i, true
}

// reportedRange converts the position flowfile reported into a range.
//
// The validator gives a 1-based line and a 1-based column counted in code points,
// the same units the YAML parser uses. A column names a point rather than an
// extent, so the range covers the word starting there; with no column at all the
// line's content is the tightest honest answer.
func reportedRange(doc *document, d flowfile.Diagnostic) lsp.Range {
	if d.Line <= 0 {
		return documentStart
	}
	if d.Column <= 0 {
		return doc.index.rangeOfLineContent(d.Line - 1)
	}
	start := doc.index.offsetOfYAML(d.Line, d.Column)
	return doc.index.rangeOfOffsets(start, start+tokenWidth(doc.text[min(start, len(doc.text)):]))
}

// idLine returns the 0-based line a step's id is written on, or the step's first
// line when it has none.
func idLine(step *parsedStep) int {
	if step.idEntry != nil {
		return step.idEntry.keyRange.Start.Line
	}
	return step.rng.Start.Line
}
