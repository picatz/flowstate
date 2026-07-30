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
func rootedUnder(src, root string, names map[string]bool) (string, bool, error) {
	if strings.TrimSpace(src) == "" {
		return src, false, nil
	}

	env, err := cel.NewEnv()
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
		if !identifierAt(src, int(offset), name) {
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

	out := src
	for _, s := range splices {
		out = out[:s.offset] + root + "." + out[s.offset:]
	}
	return out, true, nil
}

// collectStepIdents calls visit for every free identifier naming a step.
//
// Bound names are tracked the way the validator tracks them, because a
// comprehension's iteration variable is not a step even when it is spelled like
// one — and rooting it would change what the expression means rather than how it
// is written.
func collectStepIdents(e *exprpb.Expr, bound, steps map[string]bool, visit func(int64, string) error, failure *error) {
	if e == nil || *failure != nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *exprpb.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if bound[name] || !steps[name] {
			return
		}
		if err := visit(e.GetId(), name); err != nil {
			*failure = err
		}
	case *exprpb.Expr_SelectExpr:
		collectStepIdents(kind.SelectExpr.GetOperand(), bound, steps, visit, failure)
	case *exprpb.Expr_CallExpr:
		collectStepIdents(kind.CallExpr.GetTarget(), bound, steps, visit, failure)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectStepIdents(arg, bound, steps, visit, failure)
		}
	case *exprpb.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectStepIdents(el, bound, steps, visit, failure)
		}
	case *exprpb.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectStepIdents(entry.GetMapKey(), bound, steps, visit, failure)
			collectStepIdents(entry.GetValue(), bound, steps, visit, failure)
		}
	case *exprpb.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's start are evaluated outside the
		// comprehension's own scope, so a step named there is still a step.
		collectStepIdents(c.GetIterRange(), bound, steps, visit, failure)
		collectStepIdents(c.GetAccuInit(), bound, steps, visit, failure)

		inner := make(map[string]bool, len(bound)+3)
		for name := range bound {
			inner[name] = true
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = true
			}
		}
		collectStepIdents(c.GetLoopCondition(), inner, steps, visit, failure)
		collectStepIdents(c.GetLoopStep(), inner, steps, visit, failure)
		collectStepIdents(c.GetResult(), inner, steps, visit, failure)
	}
}

// identifierAt reports whether name is written at offset in src, on its own token
// boundaries.
//
// The boundary check is the point. `a` appears inside `aardvark` and inside
// `x.a`, and splicing at either produces something that still parses and means
// something else.
func identifierAt(src string, offset int, name string) bool {
	if offset < 0 || offset+len(name) > len(src) {
		return false
	}
	if src[offset:offset+len(name)] != name {
		return false
	}
	if offset > 0 && (isIdentifierByte(src[offset-1]) || src[offset-1] == '.') {
		return false
	}
	if end := offset + len(name); end < len(src) && isIdentifierByte(src[end]) {
		return false
	}
	return true
}

// isIdentifierByte reports whether b can appear inside a CEL identifier.
func isIdentifierByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9', b == '_':
		return true
	default:
		return false
	}
}
