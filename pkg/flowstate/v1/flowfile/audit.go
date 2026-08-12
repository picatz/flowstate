package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An author who needs one question answered in four places used to write the
// question four times, because a workflow had no way to name a value and read it
// back. This measurement is what supplied the evidence for `value:` (#411), which
// has landed: a repeated expression here is now one a file can collapse into a
// named step and read as `${steps.<id>.value}`.
//
// The counts stayed after the feature did, and the reason is that they measure
// something the language cannot fix. A file adopting `value:` is one whose count
// went down by an amount somebody can see; a shape `value:` cannot reach, such as a
// structure repeated inside a task's own inputs, keeps its count and is the
// evidence for whatever the next entry turns out to be.
//
// # What this is not
//
// It is not a lint. Nothing here is a diagnostic, nothing here is a defect, and
// nothing here says a file should change. A workflow that states a predicate
// three times is a workflow written in the language as it exists today, and the
// friction belongs to the language rather than to its author. The audience is
// whoever is deciding what the language should grow, which is why the counts are
// the product and there is no severity attached to any of them.
//
// It also reports only what the file itself owns. No deployment is consulted, no
// policy is read, and nothing is resolved over a network: the same rule the
// validator follows about a property of the file versus a decision a deployment
// makes, applied to a command whose whole output is a number somebody will put in
// a trend line.
//
// # What it measures, and why that shape
//
// Structurally identical CEL sub-expressions repeated within one file, counted
// and placed. Exact structural equality by [exprEqual], the same comparison the
// negation lint uses, so two clauses match when they are the same shape over the
// same names and literals. Two expressions that mean the same thing while
// spelling a bound name differently are not matched in this slice: that needs
// alpha-equivalence across the names a `for_each` binds, and counting it wrongly
// would overstate exactly the number the disposition table is weighing.
//
// A repetition where one occurrence is the hand-written negation of the others is
// marked. That pair is the shape #207 already found dangerous (edit one side and
// not the other and the file still validates), and it is the strongest single
// argument for a held entry, so it is counted separately rather than folded into
// the total.
//
// Occurrences are enumerated over every sub-expression, not only over whole
// fields, because the repetition a corpus actually carries is rarely a whole
// `if:`. It is a predicate that appears bare in one condition, as a conjunct of
// another, inside the operand of a `!(...)`, and again inside a ternary in an
// `outputs:` entry. Restricting the walk to top-level conjuncts finds the first
// two and misses the second two.
//
// Three rules keep the enumeration from drowning the signal:
//
//   - Only computations are counted. A repeated literal or a repeated name
//     (`true`, `item`, `steps.approval.outcome`) is what a language is for, not
//     friction it imposes, so constants, identifiers and field selections are
//     never reported on their own. A call, a macro comprehension, a list, or a
//     struct is.
//   - A sub-expression that occurs exactly as often as an expression containing
//     it adds nothing to the count and is dropped, so `has(x) && x` is reported
//     once rather than alongside the `has(x)` inside it.
//   - A bucket needs two occurrences in one file. Across files is a different
//     question, and one this slice deliberately does not ask: two workflows
//     stating the same predicate cannot share a held entry either way.

// maxAuditCandidates bounds how many sub-expression occurrences one file
// contributes.
//
// The walk is over an author's document, and a document is somebody else's, so
// the count of nodes it produces is a number this process does not choose. The
// parser's own bounds cap a single expression; nothing caps the sum over a file
// of arbitrarily many of them, and the map built here holds one entry per
// occurrence. Past the cap the walk stops collecting, which costs an
// undercounted report on a file far larger than any real one and never costs the
// machine. The corpus's largest file contributes under 400.
const maxAuditCandidates = 100_000

// A RepeatedExpr is one expression a file states more than once.
type RepeatedExpr struct {
	// Expr is the expression, rendered from the parsed form rather than quoted
	// from the file, so every occurrence of one bucket prints identically no
	// matter how each was spaced or wrapped.
	Expr string

	// Sites are where the expression is stated, in file order.
	Sites []ExprSite

	// Negated reports that at least one site states the negation of the
	// expression and at least one states it plainly: the hand-negated pair.
	Negated bool
}

// Count is how many times the file states the expression.
func (r RepeatedExpr) Count() int { return len(r.Sites) }

// An ExprSite is one place a file states an expression.
type ExprSite struct {
	// Step is the id of the step the expression was written in, empty for a
	// workflow-level `vars:` or `outputs:` entry.
	Step string

	// Field names the property the expression was written as, using the key an
	// author spells: `if`, `until`, `outputs.decision`, or a task input's name.
	Field string

	// Line is the 1-based line the expression's own source begins on, or zero
	// when the position is not known.
	Line int

	// Negated reports that this site states the expression under a `!`, which is
	// what makes it the other half of a hand-negated pair.
	Negated bool
}

// Audit reports the expressions a workflow states more than once, most repeated
// first.
//
// The positions come from the same [Positions] the parser hands back beside the
// workflow, so a nil one costs the lines and nothing else. See this file's
// leading comment for what is counted, what is deliberately not, and who the
// answer is for.
func Audit(wf *v1.Workflow, pos *Positions) []RepeatedExpr {
	collector := &auditCollector{pos: pos, buckets: map[string]*auditBucket{}}
	collector.workflow(wf)

	return collector.report()
}

// An auditBucket is every occurrence of one expression shape in one file.
type auditBucket struct {
	// key is the rendered expression, which is what occurrences are grouped by.
	key string

	// repr is the first occurrence's parsed form, kept so a later occurrence can
	// be confirmed against it by [exprEqual] rather than trusted to the rendering.
	repr *expr.Expr

	sites []ExprSite

	// containedBy is the set of bucket keys that enclose *every* occurrence so
	// far. It starts as the first occurrence's ancestors and is intersected with
	// each later one's, so what survives is the set of expressions this one never
	// appears outside of.
	containedBy map[string]bool

	// seeded distinguishes an empty containedBy from one not yet populated.
	seeded bool
}

// auditCollector accumulates one file's occurrences.
type auditCollector struct {
	pos     *Positions
	buckets map[string]*auditBucket
	count   int
}

// workflow walks every position the language puts an expression in.
//
// The traversal [checkExpressionTypes] makes, field for field, plus the two
// positions that check deliberately leaves to other validators: a call's
// `with:` arguments, which validate_call.go checks against the callee's own
// declarations, and a signal rule's computed `subject:`, which signals.go
// checks when it routes the expression to subject_from. A type-checker skips a
// position something else already reports on; this walk answers what the
// author wrote, so a position with an expression in it belongs here whichever
// validator owns its diagnostics.
func (c *auditCollector) workflow(wf *v1.Workflow) {
	for _, name := range slices.Sorted(maps.Keys(wf.GetVars())) {
		c.site("", v1.VarsRoot+"."+name, wf.GetVars()[name])
	}

	c.nodes(wf.GetSteps())

	for _, declaration := range wf.GetDeclaredOutputs() {
		// A declared output is a mapping whose expression sits under `value:`, so
		// the field an author reads and the path the position was recorded under
		// are not the same string. The field is the one that gets reported.
		name := "outputs." + declaration.GetName()
		c.siteAt("", name, name+".value", declaration.GetValue())
	}

	for _, policy := range slices.Sorted(maps.Keys(wf.GetSignals())) {
		for i, rule := range wf.GetSignals()[policy].GetAllow() {
			// A computed subject is written under `subject:`; the parser routes
			// it to subject_from when it interpolates, so the field reported is
			// the one the author wrote and the value read is where it landed.
			field := fmt.Sprintf("signals.%s.allow[%d].subject", policy, i)
			c.site("", field, rule.GetSubjectFrom())
		}
	}
}

// nodes walks every expression a list of steps carries, at any depth.
func (c *auditCollector) nodes(nodes []*v1.Node) {
	for _, node := range nodes {
		id := node.GetId()

		c.site(id, "if", node.GetCondition())

		for _, name := range slices.Sorted(maps.Keys(node.GetVars())) {
			c.site(id, v1.VarsRoot+"."+name, node.GetVars()[name])
		}

		if task := node.GetTask(); task != nil {
			for _, name := range slices.Sorted(maps.Keys(task.GetInputs())) {
				c.site(id, name, task.GetInputs()[name])
			}
		}

		if undo := node.GetUndo(); undo != nil {
			inputs := undo.GetTask().GetInputs()
			for _, name := range slices.Sorted(maps.Keys(inputs)) {
				c.site(id, "undo", inputs[name])
			}
		}

		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			c.site(id, "items", kind.ForEach.GetItems())
			c.nodes(kind.ForEach.GetBody())
		case *v1.Node_Loop:
			c.site(id, "until", kind.Loop.GetUntil())
			c.site(id, "init", kind.Loop.GetInitial())
			c.site(id, "update", kind.Loop.GetUpdate())
			c.nodes(kind.Loop.GetBody())
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				c.nodes(branch.GetSteps())
			}
		case *v1.Node_Switch:
			c.site(id, "value", kind.Switch.GetValue())
			for _, body := range v1.SwitchBodies(kind.Switch) {
				c.nodes(body)
			}
		case *v1.Node_Wait:
			c.site(id, "wait_until", kind.Wait.GetUntil())
			c.site(id, "sleep", kind.Wait.GetDurationExpr())
			c.site(id, "timeout", kind.Wait.GetTimeoutExpr())
			c.site(id, "prompt", kind.Wait.GetSignal().GetPrompt())
			shaped := kind.Wait.GetSignal().GetOutputs()
			for _, name := range slices.Sorted(maps.Keys(shaped)) {
				c.site(id, "outputs."+name, shaped[name])
			}
		case *v1.Node_Value:
			// The kind this measurement exists to have produced, which makes it
			// the one an omission here is worst in: a file that adopts `value:`
			// and then writes the same predicate into three of them would read as
			// a file with nothing repeated at all, and the adoption assertions
			// over the corpus would pass without looking at anything.
			c.site(id, "value", kind.Value)
		case *v1.Node_Call:
			// The arguments are this file's writing; the callee under
			// kind.Call.GetWorkflow() is another file's, embedded whole at
			// compile time and audited when the walk reads that file itself.
			// Recursing into it would count a library workflow's expressions
			// against every caller.
			for _, name := range slices.Sorted(maps.Keys(kind.Call.GetArguments())) {
				c.site(id, "with."+name, kind.Call.GetArguments()[name])
			}
		}
	}
}

// site records every countable sub-expression of one written expression.
func (c *auditCollector) site(step, field string, val *v1.Value) {
	c.siteAt(step, field, field, val)
}

// siteAt records one written expression whose position was recorded under a path
// that is not simply its field name.
func (c *auditCollector) siteAt(step, field, path string, val *v1.Value) {
	parsed := val.GetExpr()
	if parsed.GetExpr() == nil {
		return
	}

	at := ExprSite{Step: step, Field: field, Line: c.line(step, path, parsed)}
	written := resolveMacros(parsed.GetExpr(), parsed.GetSourceInfo().GetMacroCalls(), 0)
	c.walk(written, at, nil)
}

// walk records one sub-expression and descends into its children.
//
// ancestors carries the bucket keys of the enclosing sub-expressions, which is
// what lets [auditCollector.report] drop a bucket that never occurs outside a
// larger one of the same size.
func (c *auditCollector) walk(e *expr.Expr, at ExprSite, ancestors []string) {
	if e == nil || c.count >= maxAuditCandidates {
		return
	}

	if key, ok := c.record(e, at, ancestors); ok {
		ancestors = append(slices.Clip(ancestors), key)
	}

	// A `!` marks its operand and nothing under it. `!(A && B)` states the
	// negation of `A && B`, which is the half of a hand-negated pair worth
	// pairing; it does not state the negation of `A`, and marking every
	// descendant would report `steps.x.results.filter(...)` as a negation of
	// itself merely because the `.exists()` wrapped around it sits under a `!`.
	// Flipping rather than setting is what makes `!(!(E))` count as E again.
	if operand, isNot := asLogicalNot(e); isNot {
		flipped := at
		flipped.Negated = !at.Negated
		c.walk(operand, flipped, ancestors)
		return
	}

	plain := at
	plain.Negated = false

	for _, child := range children(e) {
		c.walk(child, plain, ancestors)
	}
}

// record files one occurrence, reporting the bucket key when the expression is
// one this counts at all.
func (c *auditCollector) record(e *expr.Expr, at ExprSite, ancestors []string) (string, bool) {
	if !countable(e) {
		return "", false
	}

	key, ok := renderExpr(e)
	if !ok {
		// An expression the unparser cannot render has no name to report it
		// under, and a report naming nothing is not a measurement. Its children
		// are still walked, so a countable sub-expression inside it is not lost.
		return "", false
	}

	c.count++

	bucket, seen := c.buckets[key]
	if !seen {
		bucket = &auditBucket{key: key, repr: e}
		c.buckets[key] = bucket
	} else if !exprEqual(bucket.repr, e) {
		// Two shapes rendering to one string would be counted as one repetition
		// that the file does not actually contain. The rendering is what groups
		// occurrences quickly; [exprEqual] is what decides, so a disagreement
		// drops the occurrence rather than inflating the number this command
		// exists to report.
		return "", false
	}

	bucket.sites = append(bucket.sites, at)
	bucket.intersect(ancestors)

	return key, true
}

// intersect narrows the set of expressions a bucket never occurs outside of.
func (b *auditBucket) intersect(ancestors []string) {
	if !b.seeded {
		b.containedBy = make(map[string]bool, len(ancestors))
		for _, key := range ancestors {
			b.containedBy[key] = true
		}
		b.seeded = true
		return
	}

	present := make(map[string]bool, len(ancestors))
	for _, key := range ancestors {
		present[key] = true
	}
	for key := range b.containedBy {
		if !present[key] {
			delete(b.containedBy, key)
		}
	}
}

// line reports the line an expression's own source begins on.
//
// The recorded span begins at the scalar holding the expression, which is the
// expression itself when it was written inline (`if: ${...}`) and the `>-` header
// one line above it when it was written as a block scalar. The two are told apart
// by counting: cel-go records a line offset per line of the source it parsed, so
// a span covering more lines than the source does is a span whose first line is
// the header rather than the text.
//
// Counting back from the span's *end* instead would be wrong wherever YAML folds
// a block scalar onto one logical line, which is most of this corpus: the folded
// source has one line while the span still covers all of them.
func (c *auditCollector) line(step, field string, parsed *expr.ParsedExpr) int {
	span, ok := c.pos.locateExpr(step, field)
	if !ok {
		// A few positions are recorded against the key an author wrote rather
		// than against the expression under it, an `undo:` input being the one
		// this corpus has, so the key's line is the answer there and the header
		// adjustment below does not apply to it.
		//
		// `undo` is addressed exactly, the same way [validateParsed] routes a
		// `Kind` through [Positions.LocateKind]: `<step>.undo` is a key of the
		// step itself, and the step's own primary task may separately declare an
		// input literally named `undo` — a plugin task's input names come from
		// its own descriptor, so `undo` is not a reserved word there. The
		// candidate search [Positions.Locate] does tries every registered task's
		// `.undo` input before the step's own `<step>.undo`, so on a step whose
		// task has such an input, Locate would resolve to that unrelated input
		// instead of the compensation. LocateKind has no candidate search to go
		// wrong.
		if field == "undo" {
			if span, ok := c.pos.LocateKind(step, "undo"); ok {
				return span.Start.Line
			}
			return 0
		}
		if span, ok := c.pos.Locate(step, field); ok {
			return span.Start.Line
		}
		return 0
	}

	if !span.Start.IsValid() {
		return 0
	}

	// cel-go's line offsets are the offset just past each newline, with a final
	// sentinel one past the end of the source, so the count of them is the count
	// of lines the source spans.
	sourceLines := len(parsed.GetSourceInfo().GetLineOffsets())
	spanLines := span.End.Line - span.Start.Line + 1

	if span.End.IsValid() && sourceLines > 0 && spanLines > sourceLines {
		return span.Start.Line + 1
	}

	return span.Start.Line
}

// report renders the buckets worth reporting, most repeated first.
func (c *auditCollector) report() []RepeatedExpr {
	var out []RepeatedExpr

	for key, bucket := range c.buckets {
		if len(bucket.sites) < 2 {
			continue
		}
		if c.subsumed(key, bucket) {
			continue
		}

		sites := slices.Clone(bucket.sites)
		slices.SortStableFunc(sites, func(a, b ExprSite) int { return a.Line - b.Line })

		out = append(out, RepeatedExpr{
			Expr:    key,
			Sites:   sites,
			Negated: negatedPair(sites),
		})
	}

	slices.SortFunc(out, func(a, b RepeatedExpr) int {
		switch {
		case len(a.Sites) != len(b.Sites):
			return len(b.Sites) - len(a.Sites)
		case a.Sites[0].Line != b.Sites[0].Line:
			return a.Sites[0].Line - b.Sites[0].Line
		default:
			return strings.Compare(a.Expr, b.Expr)
		}
	})

	return out
}

// subsumed reports whether a bucket occurs only inside a larger one that occurs
// exactly as often.
//
// An expression contained in every occurrence of a larger one occurs at least as
// often as that larger one, so equal counts is the only way containment can be
// total, and when it is, the smaller expression contributes no repetition of its
// own. Reporting both would count the same friction twice, once per level of
// nesting, which is how a corpus of four real predicates turns into forty rows
// nobody can read.
func (c *auditCollector) subsumed(key string, bucket *auditBucket) bool {
	for container := range bucket.containedBy {
		if container == key {
			continue
		}
		if other, ok := c.buckets[container]; ok && len(other.sites) == len(bucket.sites) {
			return true
		}
	}

	return false
}

// negatedPair reports whether the sites hold both halves of a hand-negated pair.
func negatedPair(sites []ExprSite) bool {
	var plain, negated bool
	for _, site := range sites {
		if site.Negated {
			negated = true
		} else {
			plain = true
		}
	}

	return plain && negated
}

// countable reports whether an expression is a computation rather than a name or
// a literal. See this file's leading comment: a repeated name is the language
// working, not friction it imposes.
//
// An empty list or map is excluded along with the names and the literals. It is
// punctuation rather than a computation, it is what a macro's accumulator starts
// from, and a corpus that agrees `[]` means the empty list is a corpus doing
// exactly what a language is for.
func countable(e *expr.Expr) bool {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_CallExpr, *expr.Expr_ComprehensionExpr:
		return true
	case *expr.Expr_ListExpr:
		return len(kind.ListExpr.GetElements()) > 0
	case *expr.Expr_StructExpr:
		return len(kind.StructExpr.GetEntries()) > 0
	default:
		return false
	}
}

// children returns the sub-expressions of one expression, in source order where
// the shape has one.
//
// A comprehension is reached only when [resolveMacros] found no macro call
// recorded for it, and it is walked as the loop it is: something produced it, and
// its parts are the only account of it there is.
func children(e *expr.Expr) []*expr.Expr {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		return []*expr.Expr{kind.SelectExpr.GetOperand()}

	case *expr.Expr_CallExpr:
		return callChildren(kind.CallExpr)

	case *expr.Expr_ListExpr:
		return kind.ListExpr.GetElements()

	case *expr.Expr_StructExpr:
		out := make([]*expr.Expr, 0, len(kind.StructExpr.GetEntries())*2)
		for _, entry := range kind.StructExpr.GetEntries() {
			if key := entry.GetMapKey(); key != nil {
				out = append(out, key)
			}
			out = append(out, entry.GetValue())
		}
		return out

	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		return []*expr.Expr{
			c.GetIterRange(),
			c.GetAccuInit(),
			c.GetLoopCondition(),
			c.GetLoopStep(),
			c.GetResult(),
		}

	default:
		return nil
	}
}

// callChildren returns a call's receiver, if it has one, followed by its
// arguments.
func callChildren(call *expr.Expr_Call) []*expr.Expr {
	out := make([]*expr.Expr, 0, len(call.GetArgs())+1)
	if target := call.GetTarget(); target != nil {
		out = append(out, target)
	}

	return append(out, call.GetArgs()...)
}

// maxAuditResolveDepth bounds how deep [resolveMacros] follows a tree.
//
// The recursion is driven by two things a document supplies: the nesting of the
// expression, and a map from node ids to the calls that produced them. The parser
// bounds the first. Nothing bounds a *map* whose entries could chain, so the
// depth is bounded here rather than trusted, and a tree deeper than any real
// expression is left partly unresolved rather than allowed to run the stack out.
const maxAuditResolveDepth = 200

// resolveMacros rewrites a parsed expression back into the shape its author
// wrote, replacing every macro expansion with the call it came from.
//
// A macro is expanded by the parser, so what a Flowfile stores for
// `results.filter(r, has(r.grantee))` is a comprehension over an accumulator
// named `@result`, initialized to `[]`, stepping through a ternary nobody typed.
// Counting repetition over that tree measures cel-go's expander rather than the
// friction the language imposes: it reports how often a corpus repeats
// `@result + [r]`, which is always, and never reports the `filter` two outputs
// share. Every real repetition in this corpus involving a macro was invisible
// until this ran.
//
// Resolution rather than a lookup at each step, for the reason [record] needs:
// [exprEqual] is what decides that two occurrences match, and it cannot compare
// two expansions of one macro, because the parser leaves each nested macro in a
// recorded call as a bare id standing in for a subtree. Two identical
// expressions carry different ids there, and comparing a node that has no kind
// against another that has no kind either is not a match this can make. Pulling
// the calls in first leaves a tree with nothing standing in for anything, which
// is the tree both the comparison and the rendering want.
//
// The result is a fresh tree; the workflow's own is not touched.
func resolveMacros(e *expr.Expr, calls map[int64]*expr.Expr, depth int) *expr.Expr {
	if e == nil || depth > maxAuditResolveDepth {
		return e
	}

	if call, recorded := calls[e.GetId()]; recorded {
		// Substituted once and then descended into, never re-examined: a recorded
		// call that carries the id it replaces would otherwise resolve to itself
		// forever.
		e = proto.Clone(call).(*expr.Expr)
	} else {
		e = proto.Clone(e).(*expr.Expr)
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		kind.SelectExpr.Operand = resolveMacros(kind.SelectExpr.GetOperand(), calls, depth+1)

	case *expr.Expr_CallExpr:
		if target := kind.CallExpr.GetTarget(); target != nil {
			kind.CallExpr.Target = resolveMacros(target, calls, depth+1)
		}
		for i, arg := range kind.CallExpr.GetArgs() {
			kind.CallExpr.Args[i] = resolveMacros(arg, calls, depth+1)
		}

	case *expr.Expr_ListExpr:
		for i, element := range kind.ListExpr.GetElements() {
			kind.ListExpr.Elements[i] = resolveMacros(element, calls, depth+1)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			if key := entry.GetMapKey(); key != nil {
				entry.KeyKind = &expr.Expr_CreateStruct_Entry_MapKey{
					MapKey: resolveMacros(key, calls, depth+1),
				}
			}
			entry.Value = resolveMacros(entry.GetValue(), calls, depth+1)
		}

	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		c.IterRange = resolveMacros(c.GetIterRange(), calls, depth+1)
		c.AccuInit = resolveMacros(c.GetAccuInit(), calls, depth+1)
		c.LoopCondition = resolveMacros(c.GetLoopCondition(), calls, depth+1)
		c.LoopStep = resolveMacros(c.GetLoopStep(), calls, depth+1)
		c.Result = resolveMacros(c.GetResult(), calls, depth+1)
	}

	return e
}

// renderExpr renders one sub-expression back to CEL text.
//
// The rendering is the bucket key, so it has to be a function of the shape alone:
// cel-go's unparser is, which is what makes two occurrences written with
// different spacing or line breaks group together. It is confirmed by
// [exprEqual] before an occurrence is counted, so the key is a fast index rather
// than the authority on whether two expressions match.
//
// No source info is handed over, and none is needed: [resolveMacros] has already
// put every macro back as the call it was written as, so there is no expansion
// left for the unparser to refuse.
func renderExpr(e *expr.Expr) (string, bool) {
	rendered, err := cel.AstToString(cel.ParsedExprToAst(&expr.ParsedExpr{Expr: e}))
	if err != nil {
		return "", false
	}

	rendered = strings.TrimSpace(rendered)
	if rendered == "" {
		return "", false
	}

	return rendered, true
}
