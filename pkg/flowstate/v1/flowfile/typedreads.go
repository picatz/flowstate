package flowfile

import (
	"strings"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// The optional and indexed spellings of a read, typed.
//
// CEL checks `_?._` and `_[_]` as calls, so a variable declared for the path
// `inputs.order.id` types the plain select and leaves the two spellings
// docs/STYLE.md prefers `dyn`. [typeReads] closes that: before the checker runs,
// a chain whose whole path is a declared leaf is written as the select it reads,
// wrapped in `optional.of(...)` when the author asked for an optional read. The
// checker then holds `inputs.order.?id` to `optional(string)` and
// `inputs.order["id"]` to `string`, and a diagnostic still lands on the position
// the author wrote because the outermost node keeps its id.
//
// The rewritten tree is for checking only; it is never evaluated or stored.

// typeReads returns parsed with every optional or literal-index read of a declared
// leaf rewritten as described above, and parsed itself when there is none. A key
// that is computed, a path that is not in leaves, and a root a comprehension binds
// are left as written.
func typeReads(parsed *expr.ParsedExpr, leaves map[string]*cel.Type) *expr.ParsedExpr {
	if parsed == nil || len(leaves) == 0 {
		return parsed
	}

	rewriter := &readTyper{leaves: leaves, next: maxExprID(parsed.GetExpr())}
	root := proto.Clone(parsed.GetExpr()).(*expr.Expr)
	root = rewriter.walk(root, nil)
	if !rewriter.changed {
		return parsed
	}

	return &expr.ParsedExpr{Expr: root, SourceInfo: parsed.GetSourceInfo()}
}

type readTyper struct {
	leaves  map[string]*cel.Type
	next    int64
	changed bool
}

func (r *readTyper) fresh(e *expr.Expr) *expr.Expr {
	r.next++
	e.Id = r.next

	return e
}

func (r *readTyper) walk(e *expr.Expr, bound map[string]struct{}) *expr.Expr {
	if e == nil {
		return nil
	}

	if root, fields, ok := fieldChain(e); ok {
		if _, shadowed := bound[root]; !shadowed {
			if replaced, done := r.replace(e, root, fields); done {
				return replaced
			}
		}
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		kind.SelectExpr.Operand = r.walk(kind.SelectExpr.GetOperand(), bound)
	case *expr.Expr_CallExpr:
		kind.CallExpr.Target = r.walk(kind.CallExpr.GetTarget(), bound)
		for i, arg := range kind.CallExpr.GetArgs() {
			kind.CallExpr.Args[i] = r.walk(arg, bound)
		}
	case *expr.Expr_ListExpr:
		for i, el := range kind.ListExpr.GetElements() {
			kind.ListExpr.Elements[i] = r.walk(el, bound)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			if key := entry.GetMapKey(); key != nil {
				entry.KeyKind = &expr.Expr_CreateStruct_Entry_MapKey{MapKey: r.walk(key, bound)}
			}
			entry.Value = r.walk(entry.GetValue(), bound)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		c.IterRange = r.walk(c.GetIterRange(), bound)
		c.AccuInit = r.walk(c.GetAccuInit(), bound)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		c.LoopCondition = r.walk(c.GetLoopCondition(), inner)
		c.LoopStep = r.walk(c.GetLoopStep(), inner)
		c.Result = r.walk(c.GetResult(), inner)
	}

	return e
}

// replace writes the chain e reads as a select of the declared leaf, when e spells
// at least one link as a call and the whole path is declared.
func (r *readTyper) replace(e *expr.Expr, root string, fields []string) (*expr.Expr, bool) {
	if _, declared := r.leaves[root+"."+strings.Join(fields, ".")]; !declared {
		return nil, false
	}

	optional, indexed := chainSpelling(e)
	if !optional && !indexed {
		return nil, false
	}

	read := r.fresh(&expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: root}}})
	for _, field := range fields {
		read = r.fresh(&expr.Expr{ExprKind: &expr.Expr_SelectExpr{SelectExpr: &expr.Expr_Select{Operand: read, Field: field}}})
	}

	r.changed = true
	id := e.GetId()
	if optional {
		return &expr.Expr{Id: id, ExprKind: &expr.Expr_CallExpr{CallExpr: &expr.Expr_Call{
			Function: "optional.of", Args: []*expr.Expr{read},
		}}}, true
	}
	read.Id = id

	return read, true
}

// chainSpelling reports whether a chain that [fieldChain] accepted asks for an
// optional read anywhere along it, and whether it indexes anywhere along it.
func chainSpelling(e *expr.Expr) (optional, indexed bool) {
	for {
		switch kind := e.GetExprKind().(type) {
		case *expr.Expr_SelectExpr:
			e = kind.SelectExpr.GetOperand()
		case *expr.Expr_CallExpr:
			switch kind.CallExpr.GetFunction() {
			case "_?._", "_[?_]":
				optional = true
			default:
				indexed = true
			}
			e = kind.CallExpr.GetArgs()[0]
		default:
			return optional, indexed
		}
	}
}

func maxExprID(e *expr.Expr) int64 {
	if e == nil {
		return 0
	}

	highest := e.GetId()
	bump := func(child *expr.Expr) { highest = max(highest, maxExprID(child)) }

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		bump(kind.SelectExpr.GetOperand())
	case *expr.Expr_CallExpr:
		bump(kind.CallExpr.GetTarget())
		for _, arg := range kind.CallExpr.GetArgs() {
			bump(arg)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			bump(el)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			highest = max(highest, entry.GetId())
			bump(entry.GetMapKey())
			bump(entry.GetValue())
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		for _, child := range []*expr.Expr{c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult()} {
			bump(child)
		}
	}

	return highest
}
