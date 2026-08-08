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

// Stable codes for the problems this package finds itself, so an editor can
// group or filter and a user can search.
//
// There is deliberately no code here for a problem the validator found. Those
// carry [v1.DiagnosticCode] already, assigned where the check lives and
// published unchanged by `flow validate --output json` and the Validate RPC, and
// this server used to overwrite all of them with a single constant spelling
// "flowfile". An editor could then filter every Flowfile problem or none, while
// a program reading the same file over JSON could tell an unknown task from an
// unresolved reference. Two surfaces disagreeing about what a problem *is* is
// the drift the schema type exists to prevent, so the code published here is the
// one the message carries.
const (
	codeYAMLSyntax = "yaml-syntax"
	codeCELSyntax  = "cel-syntax"
	codeTooLarge   = "document-too-large"
)

// A carried diagnostic is one problem as the editor sees it, beside the
// validator's own diagnostic that produced it.
//
// The validator's diagnostic is kept rather than projected away because it holds
// what an editor cannot recompute: the suggested edits, whose ranges were
// measured by the check that found the problem. Deriving those from the
// published diagnostic would mean re-deciding, in this package, which text a
// repair covers, and a rewriter that decides that for itself is exactly the
// mistake `flow fix` has twice paid for.
//
// Source is the zero diagnostic for the problems this package finds itself: a
// YAML syntax error, a CEL syntax error, a document too large to analyze. Those
// carry no edits, so nothing downstream has to distinguish them.
type carriedDiagnostic struct {
	published lsp.Diagnostic
	source    flowfile.Diagnostic
}

// diagnose returns every problem found in a document, as an editor is told about
// them.
func diagnose(doc *document) []lsp.Diagnostic {
	carried := diagnoseCarried(doc)
	out := make([]lsp.Diagnostic, 0, len(carried))
	for _, c := range carried {
		out = append(out, c.published)
	}
	return out
}

// diagnoseCarried returns every problem found in a document, each beside the
// validator diagnostic it came from.
//
// The one that does the work; [diagnose] is the projection of it that the
// publishing path wants. Code actions take this one, because an action is
// derived from what the validator said rather than from what was published.
func diagnoseCarried(doc *document) []carriedDiagnostic {
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
	// Path-aware when the document's URI names one, so a `call:` step resolves
	// relative to its directory exactly as `flow validate` resolves it — against
	// doc.text, the live buffer, rather than whatever the file holds on disk,
	// since an editor's unsaved edits are exactly what a diagnostic has to
	// reflect. A document with no filesystem location — untitled, or some other
	// scheme entirely — has no directory to resolve a relative path against, and
	// a `call:` there is refused with a diagnostic saying so.
	var (
		ds  flowfile.Diagnostics
		err error
	)
	if path, ok := doc.filesystemPath(); ok {
		ds, err = flowfile.ValidateSourceAt([]byte(doc.text), path)
	} else {
		ds, err = flowfile.ValidateSource([]byte(doc.text))
	}
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
		set.addFrom(lsp.Diagnostic{
			Range:    rng,
			Severity: lsp.Error,
			Source:   diagnosticSource,
			// The class the validator assigned, published as it stands rather
			// than replaced with a constant naming the checker: see the note on
			// the codes above.
			Code:    d.Proto().GetCode(),
			Message: d.Message,
		}, d)
	}

	return set.sorted()
}

// A diagnosticSet collects diagnostics, dropping exact duplicates.
//
// Duplicates are possible because two sources can legitimately notice the same
// problem, and an editor renders a doubled message as two overlapping squiggles.
type diagnosticSet struct {
	items []carriedDiagnostic
	seen  map[string]bool
}

// add records a problem this package found itself, which has no validator
// diagnostic behind it.
func (s *diagnosticSet) add(d lsp.Diagnostic) {
	s.addFrom(d, flowfile.Diagnostic{})
}

// addFrom records a problem beside the validator diagnostic it was built from.
func (s *diagnosticSet) addFrom(d lsp.Diagnostic, source flowfile.Diagnostic) {
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
	s.items = append(s.items, carriedDiagnostic{published: d, source: source})
}

func (s *diagnosticSet) empty() bool { return len(s.items) == 0 }

// sorted returns the diagnostics in source order, so the same document always
// produces the same report.
func (s *diagnosticSet) sorted() []carriedDiagnostic {
	out := s.items
	if out == nil {
		// An empty, non-nil slice: the protocol distinguishes "no problems" from
		// a missing field, and null would leave stale diagnostics in some clients.
		out = []carriedDiagnostic{}
	}
	slices.SortStableFunc(out, func(a, b carriedDiagnostic) int {
		if a.published.Range.Start.Line != b.published.Range.Start.Line {
			return a.published.Range.Start.Line - b.published.Range.Start.Line
		}
		if a.published.Range.Start.Character != b.published.Range.Start.Character {
			return a.published.Range.Start.Character - b.published.Range.Start.Character
		}
		return strings.Compare(a.published.Message, b.published.Message)
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
			// A compile failure that arrived as a bare error rather than as
			// positioned diagnostics has no class of its own to publish, and
			// "general" is what the same failure is published as everywhere
			// else. Inventing a code here would be this server describing a
			// problem in words no other surface uses.
			Code:    string(v1.DiagnosticCodeGeneral),
			Message: err.Error(),
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

	// One environment, because a workflow speaks one dialect.
	//
	// This used to build two: a base environment for `${...}` values and a
	// per-step one from that step's `libs:`, because a library can contribute
	// macros and so changes how expression source *parses*. With the profile that
	// distinction is gone — every expression in a file is parsed the way the engine
	// will evaluate it, which is the property that keeps a squiggle here from
	// disagreeing with a run.
	//
	// Without an environment there is nothing to parse with, and silence is the
	// only honest answer.
	env, err := ev.ProfileEnv(v1.CurrentProfile)
	if err != nil {
		return flagged
	}

	// The document's own expressions — its `vars:` block — before the steps'.
	//
	// They belong to no step, which is exactly why they were missed: everything here
	// walked `doc.parsed.steps`, and a workflow var is evaluated before the first
	// step runs. Nothing about them is deferred, since there is no task to defer to.
	for _, in := range doc.parsed.expressionEntries() {
		walkValues(in.value, func(v *value) {
			if v.fenced && reportCELErrors(doc, set, env, v.expr, v.exprMapper(doc.index), v.exprRange) {
				flagged = append(flagged, v.rng)
			}
		})
	}

	for _, s := range doc.parsed.steps {
		def, taskKnown := doc.tasks.Lookup(s.taskName)

		for _, in := range s.expressionEntries() {
			// An input the task evaluates itself carries expression source
			// directly, without ${...} — the http task's `expect` is one, since
			// it is checked against a response that does not exist yet. Which
			// inputs those are is declared on the task definition, so this cannot
			// go stale when a task changes.
			//
			// Asked of the *entry* and not only of its key, which is the same rule
			// bindsNow is written to. A key is a word and two things can be spelled
			// the same: since a step's `vars:` bindings joined this list, a var named
			// `expect` on an `http` step would otherwise have its plain text read as
			// CEL and squiggled — a false diagnostic on a perfectly good file, in the
			// position that most needs to be trusted.
			deferred := taskKnown && slices.Contains(s.inputs, in) &&
				slices.Contains(def.DeferredInputs, in.key)

			walkValues(in.value, func(v *value) {
				var found bool
				switch {
				case v.fenced:
					// Anything fenced is checked, valid or not: a broken
					// expression is the case a precise position matters most for.
					// How precise a position is available depends on how the
					// fence was written, which is what the last argument carries.
					found = reportCELErrors(doc, set, env, v.expr, v.exprMapper(doc.index), v.exprRange)

				// A scalar containing a fence but not made of one — `${a} and ${b}`,
				// or an unterminated `${` — is a mistake with no inner span to point
				// at. The validator reports it with a message saying how to fix it,
				// so it is left to that rather than guessed at here.

				case deferred && v.kind == kindScalar && v == in.value && v.text != "" &&
					!strings.Contains(v.text, "${"):
					// An input the task evaluates itself is expression source
					// directly, without a fence. The validator does not parse
					// these, so this is the only check they get.
					//
					// A text carrying `${` is excluded, because a fence is not
					// this case whatever else it is. The shape that first needed
					// the guard — a whole fence written as a block scalar, which
					// reached here unmarked and was squiggled as two syntax
					// errors on a file `flow validate` accepts — now takes the
					// fenced branch above and is checked properly.
					//
					// What still arrives here is a fence this model deliberately
					// does not claim: an unterminated `${`, or text that mixes a
					// fence with prose around it. Neither is CEL, and neither has
					// an inner span worth guessing at; the validator reports both
					// with a message saying how to fix them. So the guard stays,
					// on the same rule as before — a missing diagnostic beats a
					// false one.
					found = reportCELErrors(doc, set, env, v.text, v.textMapper(doc.index), v.rng)
				}
				if found {
					flagged = append(flagged, v.rng)
				}
			})
		}
	}
	return flagged
}

// reportCELErrors parses src and reports each syntax error at its position in the
// document.
//
// span maps a byte span of src to the document, and reports false where it
// cannot — see [value.exprSpan]. Every error it declines is reported against
// whole instead. That is coarse, and it is the point: a squiggle covering the
// value an author wrote is always about the thing they are looking at, whereas a
// line and column resolved against folded text names a position that exists in
// the parser's copy and not in the file.
func reportCELErrors(doc *document, set *diagnosticSet, env *cel.Env, src string, span spanMapper, whole lsp.Range) bool {
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
			// CEL's line and column address the expression source, so they are
			// turned into an offset within it before anything is asked about the
			// document — an expression's line 1 does not start where a document
			// line does, and the shift belongs to the mapping rather than to a
			// column here.
			//
			// An error at the very end of the source is left with the whole
			// range: it is where CEL puts "this expression is unfinished", which
			// names no token to underline.
			if off := offsetInExpr(src, e.Location.Line(), e.Location.Column()); off < len(src) {
				if r, ok := span(off, off+tokenWidth(src[off:])); ok {
					rng = r
				}
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
			if def, known := doc.tasks.Lookup(step.taskName); known && step.input(d.Field) != nil {
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
	if _, known := doc.tasks.Lookup(step.taskName); !known && step.taskEntry != nil {
		// The key *is* the task name now, so the range that used to need finding
		// is simply where the source already is.
		return step.taskEntry.keyRange
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
