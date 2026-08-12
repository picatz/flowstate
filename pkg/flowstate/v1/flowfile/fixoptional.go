package flowfile

import (
	"regexp"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Edition v2026.3 makes optional traversal — `.?` and `orValue()` — a documented
// part of the dialect (issue #412), and this file is the migration's other half:
// the guarded-read idiom the corpus wrote because the shorter spelling did not
// exist yet is rewritten into that spelling as a file is brought forward.
//
// Three shapes, decided on the issue, and nothing else:
//
//	has(x.y) && x.y        →  x.?y.orValue(false)
//	!(has(x.y) && x.y)     →  !x.?y.orValue(false)
//	has(x.y) ? x.y : d     →  x.?y.orValue(d)      (whole expression only)
//
// # Exact match, or nothing
//
// The two paths must be byte-identical, the path must be plain selects — an
// identifier followed by `.name` segments, no call, no index — and anything that
// does not match exactly is left exactly alone. `has()` itself stays: it answers
// presence, which is a different question, and every `filter(r, has(r.x))` in
// the corpus is has() doing the job it keeps. `has(p) && !p` is not the negated
// twin — it asks "answered no", not "not answered yes" — and is untouched.
//
// # A textual match proposes; the parse tree decides
//
// The byte patterns below can only see bytes, and an operand boundary is not a
// property of bytes. In `has(p.q) && p.q == false`, `==` binds tighter than
// `&&`, so the substring `has(p.q) && p.q` is not a node of the tree — the tree
// is `has(p.q) && (p.q == false)` — and splicing `p.?q.orValue(false)` over
// that substring reverses the gate: with the field absent, the original
// short-circuits to false and the splice evaluates `false == false`, true.
// That is precisely the corruption class CLAUDE.md's rewriter section records:
// the result still parses, still validates, and computes something else.
//
// So no splice is trusted on the strength of its neighbouring bytes. The
// original expression is parsed with the profile's environment — where the
// grammar's answer about what `&&` binds is a fact rather than a guess — and
// the idiom is rewritten a second time on that tree ([expectedOptionalAST]).
// The spliced text is accepted only when it parses back to exactly the tree the
// structural rewrite produced ([equalParsedExpr]); any disagreement, in either
// direction, leaves the source untouched. Conjunction chains are compared
// flattened, because `&&` is associative and cel-go balances a long chain
// rather than left-nesting it, so the same expression can nest differently on
// the two sides of the comparison.
//
// # Why this one rewrite does not need a scope
//
// The rewriter-safety history in CLAUDE.md is about names: both corruptions came
// from rewriting an identifier the grammar or CEL had bound. This rewrite moves
// no name. Whatever `x` resolves to — a step, a loop binding, the wait's
// `payload`, the http task's response — `x.?y.orValue(false)` reads the same
// binding `has(x.y) && x.y` read, so the scope question that makes rooting hard
// does not arise. What it needs instead is *syntax* honesty: a match inside a
// string literal is prose, so literals are masked before matching; and the
// rewritten expression must parse back to the tree described above, so a splice
// this reasoning missed cannot leave a file that means something else.
//
// # Why it runs only while a file is being brought forward
//
// The idiom stays legal — `has()` is not retired — so a v2026.3 author who
// writes it has written a valid file, and `flow fix` does not edit valid current
// files to taste. The rewrite runs when the document is on its way into the
// current edition (an older known edition, or no marker at all), which is the
// same run that stamps `edition: v2026.3`. That is the decision recorded on
// issue #412: the edition carries the extension, and the migrator carries the
// rewrite.

// optionalPath matches the plain select path both halves of the idiom must be:
// an identifier and at least one `.name` after it, so the leaf to make optional
// exists.
const optionalPath = `[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+`

var (
	// guardedRead is `has(P) && P`. Group 1 and 2 must be byte-identical, and the
	// neighbours are checked in code — a regexp boundary cannot see that the
	// character before `has` is a `!`, which would make this the wrong half of a
	// negation to rewrite.
	guardedRead = regexp.MustCompile(`has\(\s*(` + optionalPath + `)\s*\)\s*&&\s*(` + optionalPath + `)`)

	// negatedGuardedRead is the hand-negated twin, `!(has(P) && P)`, matched
	// before [guardedRead] so the negation travels with the rewrite instead of
	// being stranded outside it.
	negatedGuardedRead = regexp.MustCompile(`!\(\s*has\(\s*(` + optionalPath + `)\s*\)\s*&&\s*(` + optionalPath + `)\s*\)`)

	// ternaryDefault is `has(P) ? P : D`, anchored to the whole expression
	// because D's extent inside a larger expression is not something a textual
	// match can know. The whole-expression case is the one the corpus writes.
	ternaryDefault = regexp.MustCompile(`^\s*has\(\s*(` + optionalPath + `)\s*\)\s*\?\s*(` + optionalPath + `)\s*:\s*(.+?)\s*$`)
)

// rewriteOptionalReads rewrites the guarded-read idioms in one expression's
// source, reporting whether anything changed.
//
// Anything that keeps it from being sure — source that does not parse, paths
// that differ, a match whose neighbours make it part of something larger, a
// result that does not re-parse — answers with the source unchanged. This
// rewrite has no refusal path on purpose: the idiom is legal in the new edition,
// so a site left alone is a valid file, not a stranded migration.
func rewriteOptionalReads(src string) (string, bool) {
	if !strings.Contains(src, "has(") {
		return src, false
	}
	env := profileEnv()
	if env == nil {
		return src, false
	}
	orig, ok := parseInProfile(env, src)
	if !ok {
		// Not a valid expression, and not this rewriter's problem to report — the
		// validator says it far better. Left alone rather than half-rewritten.
		return src, false
	}

	masked := maskCELLiterals(src)

	out := src
	// The whole-expression ternary first: it can only match when the entire
	// expression is the idiom, in which case the conjunction patterns cannot.
	if m := ternaryDefault.FindStringSubmatchIndex(masked); m != nil {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path == other {
			out = optionalSpelling(path) + ".orValue(" + strings.TrimSpace(src[m[6]:m[7]]) + ")"
		}
	} else {
		out = rewriteConjunctions(src, masked)
	}

	if out == src {
		return src, false
	}

	// The textual match proposed; the grammar decides. The idiom is rewritten a
	// second time on the parse tree, where an operand's extent is a fact, and
	// the splice is accepted only when its result parses to exactly that tree —
	// see the package comment's operand-boundary section for the reversal this
	// refuses.
	expected, transformed := expectedOptionalAST(env, orig)
	if !transformed {
		return src, false
	}
	got, ok := parseInProfile(env, out)
	if !ok {
		return src, false
	}
	if !equalParsedExpr(got, expected) {
		return src, false
	}
	return out, true
}

// rewriteConjunctions applies the two conjunction shapes, negated twin first,
// splicing from the back so earlier offsets stay true.
func rewriteConjunctions(src, masked string) string {
	type splice struct {
		from, through int // byte offsets into src, half-open
		text          string
	}
	var splices []splice

	consumed := make([]bool, len(src))
	claim := func(from, through int) {
		for i := from; i < through; i++ {
			consumed[i] = true
		}
	}
	free := func(from, through int) bool {
		for i := from; i < through; i++ {
			if consumed[i] {
				return false
			}
		}
		return true
	}

	for _, m := range negatedGuardedRead.FindAllStringSubmatchIndex(masked, -1) {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path != other || !cleanNeighbours(masked, m[0], m[1], false) {
			continue
		}
		splices = append(splices, splice{m[0], m[1], "!" + optionalSpelling(path) + ".orValue(false)"})
		claim(m[0], m[1])
	}
	for _, m := range guardedRead.FindAllStringSubmatchIndex(masked, -1) {
		path, other := src[m[2]:m[3]], src[m[4]:m[5]]
		if path != other || !free(m[0], m[1]) || !cleanNeighbours(masked, m[0], m[1], true) {
			continue
		}
		splices = append(splices, splice{m[0], m[1], optionalSpelling(path) + ".orValue(false)"})
		claim(m[0], m[1])
	}
	if len(splices) == 0 {
		return src
	}

	slices.SortFunc(splices, func(a, b splice) int { return b.from - a.from })
	out := src
	for _, s := range splices {
		out = out[:s.from] + s.text + out[s.through:]
	}
	return out
}

// cleanNeighbours screens out matches whose adjacent characters visibly extend
// them: something before that would bind to the start, something after that
// would select from, call, or index the end. bareHas says the match starts at
// `has`, whose one dangerous neighbour is a `!` — `!has(P) && P` negates the
// guard alone, and rewriting the conjunction under it would negate the read
// instead.
//
// This is a proposer-side screen, not the decider: bytes cannot see operand
// boundaries (` == false` after a match extends it just as surely as `.` does),
// so whether a match is whole is settled by the parse-tree comparison in
// [rewriteOptionalReads], which refuses every splice this screen lets through
// wrongly.
func cleanNeighbours(masked string, from, through int, bareHas bool) bool {
	if from > 0 {
		before := masked[from-1]
		if isIdentByte(before) || before == '.' {
			return false
		}
		if bareHas && before == '!' {
			return false
		}
	}
	if through < len(masked) {
		after := masked[through]
		if isIdentByte(after) || after == '.' || after == '(' || after == '[' {
			return false
		}
	}
	return true
}

// isIdentByte reports whether b can appear in a CEL identifier.
func isIdentByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_'
}

// optionalSpelling turns a select path into its optional-leaf spelling:
// `a.b.c` becomes `a.b.?c`. The path always holds a dot — [optionalPath]
// requires one — so the read stays anchored where the guard anchored it:
// `has(a.b.c)` requires `a.b` present exactly as `a.b.?c` does.
func optionalSpelling(path string) string {
	i := strings.LastIndex(path, ".")
	return path[:i] + ".?" + path[i+1:]
}

// profileEnv returns the profile's environment — the same environment the
// compiler parses with, so the two agree about what the language is (see
// [rootedUnder] for why that identity matters) — or nil when it cannot be
// built, which refuses the rewrite rather than acting on a guess.
func profileEnv() *cel.Env {
	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		return nil
	}
	env, err := v1.DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil
	}
	return env
}

// parseInProfile parses source in the profile's environment, answering the
// parse tree in the protobuf form [equalParsedExpr] compares.
func parseInProfile(env *cel.Env, src string) (*exprpb.Expr, bool) {
	parsed, issues := env.Parse(src)
	if issues != nil && issues.Err() != nil {
		return nil, false
	}
	checked, err := cel.AstToParsedExpr(parsed)
	if err != nil {
		return nil, false
	}
	return checked.GetExpr(), true
}

// expectedOptionalAST rewrites the guarded-read idiom on the parse tree itself,
// reporting whether anything matched. The result is what a textual splice
// *claims* to mean, built where an operand's extent cannot be misread; the
// caller accepts a splice only when its parse equals this tree.
//
// The whole-expression ternary is handled first and alone, mirroring
// [rewriteOptionalReads]: when the root is `has(P) ? P : D`, the default D is
// grafted into `orValue` untouched — nothing inside D is rewritten in the same
// round, and the fixed-point loop sees D's own idioms on the next parse.
func expectedOptionalAST(env *cel.Env, root *exprpb.Expr) (*exprpb.Expr, bool) {
	if call := root.GetCallExpr(); call.GetFunction() == operators.Conditional &&
		call.GetTarget() == nil && len(call.GetArgs()) == 3 {
		if guard, ok := testOnlySelectPath(call.GetArgs()[0]); ok {
			if read, ok := plainSelectPath(call.GetArgs()[1]); ok && read == guard {
				repl, ok := parseInProfile(env, optionalSpelling(guard)+".orValue(false)")
				if !ok || len(repl.GetCallExpr().GetArgs()) != 1 {
					return root, false
				}
				repl.GetCallExpr().Args[0] = call.GetArgs()[2]
				return repl, true
			}
		}
	}
	return transformOptionalIdioms(env, root)
}

// transformOptionalIdioms walks one parsed expression and rewrites every
// `has(P) && P` conjunction pair into optional traversal, reporting whether
// any pair matched. Unchanged nodes are shared, changed ones rebuilt; the
// input is never mutated.
func transformOptionalIdioms(env *cel.Env, e *exprpb.Expr) (*exprpb.Expr, bool) {
	if e == nil {
		return nil, false
	}
	switch kind := e.GetExprKind().(type) {
	case *exprpb.Expr_CallExpr:
		if op, ok := logicOp(e); ok && op == operators.LogicalAnd {
			return transformConjunctionChain(env, e)
		}
		call := kind.CallExpr
		target, targetChanged := transformOptionalIdioms(env, call.GetTarget())
		args, argsChanged := transformOptionalExprs(env, call.GetArgs())
		if !targetChanged && !argsChanged {
			return e, false
		}
		return &exprpb.Expr{ExprKind: &exprpb.Expr_CallExpr{CallExpr: &exprpb.Expr_Call{
			Target:   target,
			Function: call.GetFunction(),
			Args:     args,
		}}}, true
	case *exprpb.Expr_SelectExpr:
		sel := kind.SelectExpr
		operand, changed := transformOptionalIdioms(env, sel.GetOperand())
		if !changed {
			return e, false
		}
		return &exprpb.Expr{ExprKind: &exprpb.Expr_SelectExpr{SelectExpr: &exprpb.Expr_Select{
			Operand:  operand,
			Field:    sel.GetField(),
			TestOnly: sel.GetTestOnly(),
		}}}, true
	case *exprpb.Expr_ListExpr:
		elements, changed := transformOptionalExprs(env, kind.ListExpr.GetElements())
		if !changed {
			return e, false
		}
		return &exprpb.Expr{ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{
			Elements:        elements,
			OptionalIndices: kind.ListExpr.GetOptionalIndices(),
		}}}, true
	case *exprpb.Expr_StructExpr:
		st := kind.StructExpr
		changed := false
		entries := make([]*exprpb.Expr_CreateStruct_Entry, len(st.GetEntries()))
		for i, entry := range st.GetEntries() {
			key, keyChanged := transformOptionalIdioms(env, entry.GetMapKey())
			value, valueChanged := transformOptionalIdioms(env, entry.GetValue())
			if !keyChanged && !valueChanged {
				entries[i] = entry
				continue
			}
			changed = true
			next := &exprpb.Expr_CreateStruct_Entry{Value: value, OptionalEntry: entry.GetOptionalEntry()}
			if _, isField := entry.GetKeyKind().(*exprpb.Expr_CreateStruct_Entry_FieldKey); isField {
				next.KeyKind = &exprpb.Expr_CreateStruct_Entry_FieldKey{FieldKey: entry.GetFieldKey()}
			} else {
				next.KeyKind = &exprpb.Expr_CreateStruct_Entry_MapKey{MapKey: key}
			}
			entries[i] = next
		}
		if !changed {
			return e, false
		}
		return &exprpb.Expr{ExprKind: &exprpb.Expr_StructExpr{StructExpr: &exprpb.Expr_CreateStruct{
			MessageName: st.GetMessageName(),
			Entries:     entries,
		}}}, true
	case *exprpb.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		iterRange, changedRange := transformOptionalIdioms(env, c.GetIterRange())
		accuInit, changedInit := transformOptionalIdioms(env, c.GetAccuInit())
		loopCondition, changedCond := transformOptionalIdioms(env, c.GetLoopCondition())
		loopStep, changedStep := transformOptionalIdioms(env, c.GetLoopStep())
		result, changedResult := transformOptionalIdioms(env, c.GetResult())
		if !changedRange && !changedInit && !changedCond && !changedStep && !changedResult {
			return e, false
		}
		return &exprpb.Expr{ExprKind: &exprpb.Expr_ComprehensionExpr{ComprehensionExpr: &exprpb.Expr_Comprehension{
			IterVar:       c.GetIterVar(),
			IterVar2:      c.GetIterVar2(),
			IterRange:     iterRange,
			AccuVar:       c.GetAccuVar(),
			AccuInit:      accuInit,
			LoopCondition: loopCondition,
			LoopStep:      loopStep,
			Result:        result,
		}}}, true
	}
	return e, false
}

// transformOptionalExprs maps [transformOptionalIdioms] over a slice, sharing
// it when nothing underneath changed.
func transformOptionalExprs(env *cel.Env, exprs []*exprpb.Expr) ([]*exprpb.Expr, bool) {
	changed := false
	out := make([]*exprpb.Expr, len(exprs))
	for i, e := range exprs {
		next, c := transformOptionalIdioms(env, e)
		out[i] = next
		changed = changed || c
	}
	if !changed {
		return exprs, false
	}
	return out, true
}

// transformConjunctionChain rewrites the idiom inside one `&&` chain. The
// chain is flattened first — `&&` is associative, and cel-go balances a long
// chain rather than left-nesting it, so "the operand after the guard" is only
// readable on the flat list — and an adjacent (has(P), P) pair anywhere on it
// is the idiom: `a && has(x.y) && x.y` guards the same read `has(x.y) && x.y`
// guards, whichever way the parser chose to nest it.
func transformConjunctionChain(env *cel.Env, e *exprpb.Expr) (*exprpb.Expr, bool) {
	ops := flattenLogic(e, operators.LogicalAnd)
	changed := false
	out := make([]*exprpb.Expr, 0, len(ops))
	for i := 0; i < len(ops); i++ {
		if i+1 < len(ops) {
			if guard, ok := testOnlySelectPath(ops[i]); ok {
				if read, ok := plainSelectPath(ops[i+1]); ok && read == guard {
					if repl, ok := parseInProfile(env, optionalSpelling(guard)+".orValue(false)"); ok {
						out = append(out, repl)
						changed = true
						i++
						continue
					}
				}
			}
		}
		op, c := transformOptionalIdioms(env, ops[i])
		out = append(out, op)
		changed = changed || c
	}
	if !changed {
		return e, false
	}
	if len(out) == 1 {
		return out[0], true
	}
	conj := out[0]
	for _, op := range out[1:] {
		conj = &exprpb.Expr{ExprKind: &exprpb.Expr_CallExpr{CallExpr: &exprpb.Expr_Call{
			Function: operators.LogicalAnd,
			Args:     []*exprpb.Expr{conj, op},
		}}}
	}
	return conj, true
}

// logicOp reports the operator when e is a bare `&&` or `||` call.
func logicOp(e *exprpb.Expr) (string, bool) {
	call := e.GetCallExpr()
	if call == nil || call.GetTarget() != nil || len(call.GetArgs()) != 2 {
		return "", false
	}
	switch fn := call.GetFunction(); fn {
	case operators.LogicalAnd, operators.LogicalOr:
		return fn, true
	}
	return "", false
}

// flattenLogic returns e's operand list under one associative operator,
// unfolding however the parser chose to nest the chain.
func flattenLogic(e *exprpb.Expr, op string) []*exprpb.Expr {
	if found, ok := logicOp(e); !ok || found != op {
		return []*exprpb.Expr{e}
	}
	args := e.GetCallExpr().GetArgs()
	return append(flattenLogic(args[0], op), flattenLogic(args[1], op)...)
}

// testOnlySelectPath returns the dotted path a has() guard asks about —
// `has(a.b.c)` expands to a test-only select of `a.b.c` — or refuses anything
// that is not a has() over a plain select path.
func testOnlySelectPath(e *exprpb.Expr) (string, bool) {
	sel := e.GetSelectExpr()
	if sel == nil || !sel.GetTestOnly() {
		return "", false
	}
	prefix, ok := selectChainPath(sel.GetOperand())
	if !ok {
		return "", false
	}
	return prefix + "." + sel.GetField(), true
}

// plainSelectPath returns the dotted path of a plain read — an identifier
// followed by field selects, no call, no index, no has() — or refuses.
func plainSelectPath(e *exprpb.Expr) (string, bool) {
	sel := e.GetSelectExpr()
	if sel == nil || sel.GetTestOnly() {
		return "", false
	}
	prefix, ok := selectChainPath(sel.GetOperand())
	if !ok {
		return "", false
	}
	return prefix + "." + sel.GetField(), true
}

// selectChainPath spells out an ident-rooted select chain, refusing anything
// that is not one.
func selectChainPath(e *exprpb.Expr) (string, bool) {
	switch kind := e.GetExprKind().(type) {
	case *exprpb.Expr_IdentExpr:
		return kind.IdentExpr.GetName(), true
	case *exprpb.Expr_SelectExpr:
		if kind.SelectExpr.GetTestOnly() {
			return "", false
		}
		prefix, ok := selectChainPath(kind.SelectExpr.GetOperand())
		if !ok {
			return "", false
		}
		return prefix + "." + kind.SelectExpr.GetField(), true
	}
	return "", false
}

// equalParsedExpr reports whether two parsed expressions are the same tree,
// ignoring IDs — the one part of a parse that depends on where in a larger
// text the expression sat. `&&` and `||` chains are compared flattened: the
// parser balances a long chain, so the same source can nest differently on
// the two sides, and associativity is what makes the flat comparison the
// true one.
func equalParsedExpr(a, b *exprpb.Expr) bool {
	if a == nil || b == nil {
		return (a == nil) == (b == nil)
	}
	if op, ok := logicOp(a); ok {
		other, okB := logicOp(b)
		if !okB || op != other {
			return false
		}
		return equalParsedExprs(flattenLogic(a, op), flattenLogic(b, op))
	}
	if _, ok := logicOp(b); ok {
		return false
	}
	switch ka := a.GetExprKind().(type) {
	case *exprpb.Expr_ConstExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_ConstExpr)
		return ok && proto.Equal(ka.ConstExpr, kb.ConstExpr)
	case *exprpb.Expr_IdentExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_IdentExpr)
		return ok && ka.IdentExpr.GetName() == kb.IdentExpr.GetName()
	case *exprpb.Expr_SelectExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_SelectExpr)
		return ok && ka.SelectExpr.GetField() == kb.SelectExpr.GetField() &&
			ka.SelectExpr.GetTestOnly() == kb.SelectExpr.GetTestOnly() &&
			equalParsedExpr(ka.SelectExpr.GetOperand(), kb.SelectExpr.GetOperand())
	case *exprpb.Expr_CallExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_CallExpr)
		return ok && ka.CallExpr.GetFunction() == kb.CallExpr.GetFunction() &&
			equalParsedExpr(ka.CallExpr.GetTarget(), kb.CallExpr.GetTarget()) &&
			equalParsedExprs(ka.CallExpr.GetArgs(), kb.CallExpr.GetArgs())
	case *exprpb.Expr_ListExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_ListExpr)
		return ok && slices.Equal(ka.ListExpr.GetOptionalIndices(), kb.ListExpr.GetOptionalIndices()) &&
			equalParsedExprs(ka.ListExpr.GetElements(), kb.ListExpr.GetElements())
	case *exprpb.Expr_StructExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_StructExpr)
		if !ok || ka.StructExpr.GetMessageName() != kb.StructExpr.GetMessageName() ||
			len(ka.StructExpr.GetEntries()) != len(kb.StructExpr.GetEntries()) {
			return false
		}
		for i, entry := range ka.StructExpr.GetEntries() {
			other := kb.StructExpr.GetEntries()[i]
			if entry.GetFieldKey() != other.GetFieldKey() ||
				entry.GetOptionalEntry() != other.GetOptionalEntry() ||
				!equalParsedExpr(entry.GetMapKey(), other.GetMapKey()) ||
				!equalParsedExpr(entry.GetValue(), other.GetValue()) {
				return false
			}
		}
		return true
	case *exprpb.Expr_ComprehensionExpr:
		kb, ok := b.GetExprKind().(*exprpb.Expr_ComprehensionExpr)
		if !ok {
			return false
		}
		ca, cb := ka.ComprehensionExpr, kb.ComprehensionExpr
		return ca.GetIterVar() == cb.GetIterVar() &&
			ca.GetIterVar2() == cb.GetIterVar2() &&
			ca.GetAccuVar() == cb.GetAccuVar() &&
			equalParsedExpr(ca.GetIterRange(), cb.GetIterRange()) &&
			equalParsedExpr(ca.GetAccuInit(), cb.GetAccuInit()) &&
			equalParsedExpr(ca.GetLoopCondition(), cb.GetLoopCondition()) &&
			equalParsedExpr(ca.GetLoopStep(), cb.GetLoopStep()) &&
			equalParsedExpr(ca.GetResult(), cb.GetResult())
	}
	return false
}

// equalParsedExprs compares two expression slices element-wise.
func equalParsedExprs(a, b []*exprpb.Expr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalParsedExpr(a[i], b[i]) {
			return false
		}
	}
	return true
}

// maskCELLiterals blanks the contents of string and bytes literals (and `//`
// comments) so the pattern matcher cannot read prose as code. The result is the
// same length as the input, so every offset found in it indexes the original.
func maskCELLiterals(src string) string {
	out := []byte(src)
	i := 0
	for i < len(out) {
		c := out[i]

		// A raw or bytes prefix sits immediately before the quote.
		if (c == 'r' || c == 'R' || c == 'b' || c == 'B') && i+1 < len(out) && (out[i+1] == '\'' || out[i+1] == '"') {
			i++
			continue
		}

		switch {
		case c == '\'' || c == '"':
			quote := string(c)
			if strings.HasPrefix(src[i:], quote+quote+quote) {
				quote = quote + quote + quote
			}
			end := i + len(quote)
			for end < len(out) {
				if out[end] == '\\' && len(quote) == 1 {
					end += 2
					continue
				}
				if strings.HasPrefix(src[end:], quote) {
					end += len(quote)
					break
				}
				end++
			}
			if end > len(out) {
				end = len(out)
			}
			for j := i + len(quote); j < end-len(quote) && j < len(out); j++ {
				out[j] = ' '
			}
			i = end
		case c == '/' && i+1 < len(out) && out[i+1] == '/':
			for ; i < len(out) && out[i] != '\n'; i++ {
				out[i] = ' '
			}
		default:
			i++
		}
	}
	return string(out)
}

// optionalReads rewrites the guarded-read idiom in every fenced, single-line
// expression under n, in place on the fixer's lines.
//
// Only fenced scalars, because a fence is the one spelling that is an
// expression everywhere it appears; a deferred input written bare is left
// alone, which costs a modernisation and corrupts nothing. Only single lines,
// because a block scalar has no one line to splice — the idiom is legal in the
// new edition, so a multi-line site left alone is a valid file.
func (f *fixer) optionalReads(n ast.Node) {
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			f.optionalReads(v)
		}
	case *ast.MappingValueNode:
		f.optionalReads(node.Value)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			f.optionalReads(v)
		}
	case *ast.StringNode:
		f.optionalReadScalar(node)
	case *ast.LiteralNode:
		f.optionalReadBlockScalar(node)
	}
}

// optionalReadBlockScalar handles the corpus's other common spelling: a folded
// block scalar (`>-`) whose whole content is one fenced expression line. The
// value has no single source line the way a plain scalar does, so the content
// line is located by its text within the literal's span — and any doubt about
// which line that is (none found, or more than one) skips the site, because the
// idiom is legal and a site left alone is a valid file.
func (f *fixer) optionalReadBlockScalar(node *ast.LiteralNode) {
	inner, fenced := SplitFence(strings.TrimSpace(blockText(node)))
	if !fenced || strings.Contains(inner, "\n") {
		return
	}

	rewritten, changed := rewriteOptionalReads(inner)
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() {
		return
	}

	want := fenceOpen + inner + fenceClose
	found := 0
	for n := span.Start.Line; n <= span.End.Line; n++ {
		if strings.Contains(f.line(n), want) {
			if found != 0 {
				return
			}
			found = n
		}
	}
	if found == 0 || strings.Count(f.line(found), want) != 1 {
		return
	}

	f.lines[found-1] = strings.Replace(f.line(found), want, fenceOpen+rewritten+fenceClose, 1)
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    found,
		Message: "guarded read rewritten to optional traversal (`.?` with `orValue`)",
		Pending: "guarded read would be rewritten to optional traversal (`.?` with `orValue`)",
	})
}

// optionalReadScalar rewrites one fenced scalar, splicing the way
// [fixer.rootScalar] does and skipping silently where that splice cannot be
// made — see [fixer.optionalReads] for why silence is correct here.
func (f *fixer) optionalReadScalar(node *ast.StringNode) {
	inner, fenced := SplitFence(node.Value)
	if !fenced {
		return
	}

	rewritten, changed := rewriteOptionalReads(inner)
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() || span.Start.Line != span.End.Line {
		return
	}

	line := f.line(span.Start.Line)
	want, replacement := fenceOpen+inner+fenceClose, fenceOpen+rewritten+fenceClose

	from, located := byteOffsetOfColumn(line, span.Start.Column)
	if !located {
		return
	}
	at := strings.Index(line[from:], want)
	if at < 0 {
		// The line no longer holds the value the parser read — another pass
		// rewrote it first. The fixed-point loop re-parses and this pass sees the
		// updated text next round.
		return
	}
	at += from

	f.lines[span.Start.Line-1] = line[:at] + replacement + line[at+len(want):]
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    span.Start.Line,
		Message: "guarded read rewritten to optional traversal (`.?` with `orValue`)",
		Pending: "guarded read would be rewritten to optional traversal (`.?` with `orValue`)",
	})
}

// modernizesEdition reports whether a document is on its way into the current
// edition — declaring an older known edition, or none at all — which is when
// the optional-read rewrite runs. A current file keeps its author's spelling,
// and a future one is refused elsewhere without this pass adding noise.
//
// An edition arriving only through a merge key answers false: which edition it
// is cannot be read from here (see [fixer.mergedDeclaresEdition]), and this is
// a rewrite the command must only make when it knows the file is moving.
//
// Read from the bytes [Fix] was handed rather than per round, because a later
// round runs over a document an earlier round has already stamped — see the
// call site. Unparseable input answers false; [fixOnce] reports the parse error
// properly, and a pass gated off is a pass that cannot act on a guess.
func modernizesEdition(data []byte) bool {
	file, err := parser.ParseBytes(data, parser.ParseComments)
	if err != nil {
		return false
	}

	probe := &fixer{}
	for _, doc := range file.Docs {
		probe.collectAnchors(doc.Body)
	}

	for _, doc := range file.Docs {
		mapping := asMapping(doc.Body)
		if mapping == nil {
			continue
		}
		for _, v := range mapping.Values {
			if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
				if probe.mergedDeclaresEdition(v) {
					return false
				}
				continue
			}
			name, ok := keyNameOf(v.Key)
			if !ok || name != "edition" {
				continue
			}
			declared, ok := editionText(v.Value)
			if !ok {
				return false
			}
			return declared != CurrentEdition && slices.Contains(knownEditions, declared)
		}
	}
	return true
}
