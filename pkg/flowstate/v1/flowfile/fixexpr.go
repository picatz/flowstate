package flowfile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Rooting a reference — `a.result` becoming `steps.a.result` — is the one rewrite
// in this package that reaches inside an expression rather than moving the lines
// around it.
//
// # Why the AST is used to locate and not to render
//
// cel-go can unparse an expression, and doing so would be a one-liner. It would
// also rewrite every expression in the file: `'a' + b` comes back as `"a" + b`,
// spacing is normalised, and a parenthesis someone added for a reader is gone. A
// migration diff has to be readable, and one that reformats every expression it
// touches is not.
//
// So the AST only *locates* identifiers, and the edit is a splice into the
// original text at those offsets. Everything the author wrote survives, including
// the parts cel-go has an opinion about.
//
// # Why each splice is checked before it is made
//
// A macro — `items.map(i, i + a.result)`, `has(a.b)` — expands into synthetic
// nodes whose recorded positions point at the macro call rather than at anything
// the author typed. Splicing at one of those corrupts the expression *silently*:
// the result still parses and means something else, which is the worst outcome a
// rewriter has.
//
// So every offset is verified to hold the identifier it claims to, on its own
// token boundaries, before anything is written. One that does not is not a
// position to be clever about — the fence is refused and the author is told where.

// rootedExpr rewrites the bare step references in one expression's source.
//
// steps names the step ids in scope. A free identifier matching one is rooted;
// everything else — a loop binding, `now`, a function, a name that resolves to
// nothing — is left exactly as written, because rooting a name that is not a step
// would invent a reference rather than migrate one.
//
// The bool reports whether anything changed. An error means the expression could
// not be rewritten safely and the caller must refuse rather than write a guess.
func rootedExpr(src string, steps map[string]bool) (string, bool, error) {
	return rootedUnder(src, v1.StepsRoot, steps)
}

// responseNames are the names an http task's `expect:` and `outputs:` expressions used
// to bind bare, and now reach through [v1.ResponseRoot].
//
// A fixed set, because it is the set the task injects rather than anything an author
// declares — which is the same property that made rooting them right: a name the system
// chooses and puts in the author's namespace will grow, and a `duration_ms` added later
// would capture a binding somebody already had.
var responseNames = map[string]bool{
	"status_code": true,
	"headers":     true,
	"body":        true,
	"json":        true,
}

// rootedResponseExpr rewrites the bare response references in one expression's source.
//
// Only correct inside the two inputs the http task evaluates itself, because those are
// the only places these four names are bound — anywhere else `body` is an ordinary
// identifier, and rooting it would invent a reference to a response that does not exist
// there. The caller decides; this only knows how.
func rootedResponseExpr(src string) (string, bool, error) {
	return rootedUnder(src, v1.ResponseRoot, responseNames)
}

// rootedUnder is the shared walk: locate the free identifiers naming something in the
// set, and splice the root before each.
//
// Written once for both roots because the hard parts are identical and neither is
// obvious — offsets are verified to hold the identifier they claim to before anything
// is written, and splices are applied from the back so each offset still addresses the
// text it was measured against. A second copy would have one of those subtly wrong.
//
// # Free, and what decides that
//
// "Free" is the load-bearing word, and it is decided entirely by which macros the
// parser knows. A macro's bound variable is an identifier in the source and *not* a
// free one — `[3,1,2].sortBy(name, name)` binds `name` — and the only thing that
// tells this walk so is the expansion, which turns it into a comprehension variable
// the walk does not reach.
//
// So a parser missing a macro makes this rewriter wrong rather than merely limited.
// It parsed against a bare environment, which knows cel-go's standard macros and
// none of the profile's, so `filter` was safe and `sortBy` was not: a file with a
// step called `name` and `sortBy(name, name)` beside it was rewritten to
// `sortBy(steps.name, steps.name)`, which is not a macro invocation at all. `flow
// fix` corrupting a valid file is the worst thing in this package, because the whole
// promise of the command is that it is safe to run on anything.
//
// The profile's environment, therefore — the same one the compiler parses with. The
// two have to agree about what a macro is, or one of them is rewriting a language
// the other does not have.
func rootedUnder(src, root string, names map[string]bool) (string, bool, error) {
	if strings.TrimSpace(src) == "" {
		return src, false, nil
	}

	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		return "", false, err
	}

	env, err := v1.DefaultEvaluator().Env(libs...)
	if err != nil {
		return "", false, err
	}
	ast, issues := env.Parse(src)
	if issues != nil && issues.Err() != nil {
		// Not a valid expression, and not this rewriter's problem to report — the
		// validator says it far better, with a position inside the expression. Left
		// alone rather than half-rewritten.
		return src, false, nil
	}
	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return "", false, err
	}
	positions := parsed.GetSourceInfo().GetPositions()

	// cel-go records a position as a *code-point* offset into the expression source,
	// which is the same number as a byte offset only while the expression is ASCII.
	// Indexing the string with one is therefore wrong twice for a file with a
	// non-ASCII character before a reference: the boundary check reads bytes that are
	// not where the identifier is, so a valid file is refused with a diagnostic
	// blaming a macro; and where the wrong offset happens to *hold* the name — a step
	// called `a` and `${'日本a' + a}`, where the shifted offset lands on the `a`
	// inside the literal — the literal is rewritten, the real reference is left bare,
	// and `flow fix` exits zero on a file `flow validate` rejects.
	//
	// So the whole rewrite works in runes, the way [markerSpan] already does for a
	// diagnostic's column: both the check and the splice index the same units cel-go
	// counted in.
	runes := []rune(src)

	type splice struct {
		offset int
		name   string
	}
	var (
		splices []splice
		seen    = make(map[int64]bool)
		failure error
	)

	collectStepIdents(parsed.GetExpr(), nil, names, func(id int64, name string) error {
		if seen[id] {
			return nil
		}
		seen[id] = true

		offset, ok := positions[id]
		if !ok {
			return fmt.Errorf("the reference to %q has no recorded position, so it cannot be rooted here; write `%s.%s` by hand",
				name, root, name)
		}
		if !identifierAt(runes, int(offset), name) {
			return fmt.Errorf("the reference to %q is recorded at a position that does not hold it, which happens inside a macro; write `%s.%s` by hand",
				name, root, name)
		}
		splices = append(splices, splice{offset: int(offset), name: name})
		return nil
	}, &failure)
	if failure != nil {
		return "", false, failure
	}
	if len(splices) == 0 {
		return src, false, nil
	}

	// Applied from the back, so each offset still addresses the text it was
	// measured against.
	sort.Slice(splices, func(i, j int) bool { return splices[i].offset > splices[j].offset })

	prefix := []rune(root + ".")
	out := runes
	for _, s := range splices {
		next := make([]rune, 0, len(out)+len(prefix))
		next = append(next, out[:s.offset]...)
		next = append(next, prefix...)
		next = append(next, out[s.offset:]...)
		out = next
	}
	return string(out), true, nil
}

// collectStepIdents calls visit for every free identifier naming a step.
//
// A filter over [walkFreeIdents], which is where the scope tracking lives: a
// comprehension's iteration variable is not a step even when it is spelled like
// one, and rooting it would change what the expression means rather than how it
// is written.
func collectStepIdents(e *exprpb.Expr, bound, steps map[string]bool, visit func(int64, string) error, failure *error) {
	walkFreeIdents(e, bound, func(id int64, name string) error {
		if !steps[name] {
			return nil
		}
		return visit(id, name)
	}, failure)
}

// walkFreeIdents calls visit for every identifier an expression reads and does
// not itself bind.
//
// Two callers want different halves of one question and neither can answer it
// alone by inspecting a rendering. The rewriter above asks *which* of these
// names is a step, to root it. [Lint] asks whether any of them is a name the
// grammar binds locally — a loop's item, a step's own `vars:` key, `now` inside
// a wait — because an expression reading one of those cannot be lifted out of
// where it is written, and a suggestion to lift it would be advice that breaks
// the file. Both are the same walk with a different predicate, and writing it
// twice is how one copy comes to know about comprehensions and the other does
// not.
//
// "Free" is decided by which macros the parser knows; see [rootedUnder] for why
// that makes the parsing environment load-bearing rather than incidental.
func walkFreeIdents(e *exprpb.Expr, bound map[string]bool, visit func(int64, string) error, failure *error) {
	if e == nil || *failure != nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *exprpb.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if bound[name] {
			return
		}
		if err := visit(e.GetId(), name); err != nil {
			*failure = err
		}
	case *exprpb.Expr_SelectExpr:
		walkFreeIdents(kind.SelectExpr.GetOperand(), bound, visit, failure)
	case *exprpb.Expr_CallExpr:
		walkFreeIdents(kind.CallExpr.GetTarget(), bound, visit, failure)
		for _, arg := range kind.CallExpr.GetArgs() {
			walkFreeIdents(arg, bound, visit, failure)
		}
	case *exprpb.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			walkFreeIdents(el, bound, visit, failure)
		}
	case *exprpb.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			walkFreeIdents(entry.GetMapKey(), bound, visit, failure)
			walkFreeIdents(entry.GetValue(), bound, visit, failure)
		}
	case *exprpb.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's start are evaluated outside the
		// comprehension's own scope, so a step named there is still a step.
		walkFreeIdents(c.GetIterRange(), bound, visit, failure)
		walkFreeIdents(c.GetAccuInit(), bound, visit, failure)

		inner := make(map[string]bool, len(bound)+3)
		for name := range bound {
			inner[name] = true
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = true
			}
		}
		walkFreeIdents(c.GetLoopCondition(), inner, visit, failure)
		walkFreeIdents(c.GetLoopStep(), inner, visit, failure)
		walkFreeIdents(c.GetResult(), inner, visit, failure)
	}
}

// identifierAt reports whether name is written at offset in src, on its own token
// boundaries.
//
// src is runes rather than bytes because the offset is a code-point offset — see
// [rootedUnder]. A CEL identifier is ASCII, so the name's own length is the same in
// either unit; what is not the same is where in the source that length starts.
//
// The boundary check is the point. `a` appears inside `aardvark` and inside
// `x.a`, and splicing at either produces something that still parses and means
// something else. A non-ASCII neighbour is a boundary too — `é` cannot appear in a
// CEL identifier — so it is only ever read as one rune, never as the bytes it is
// made of.
func identifierAt(src []rune, offset int, name string) bool {
	want := []rune(name)
	if offset < 0 || offset+len(want) > len(src) {
		return false
	}
	if string(src[offset:offset+len(want)]) != name {
		return false
	}
	if offset > 0 && (isIdentifierRune(src[offset-1]) || src[offset-1] == '.') {
		return false
	}
	if end := offset + len(want); end < len(src) && isIdentifierRune(src[end]) {
		return false
	}
	return true
}

// isIdentifierRune reports whether r can appear inside a CEL identifier.
func isIdentifierRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		return true
	default:
		return false
	}
}
