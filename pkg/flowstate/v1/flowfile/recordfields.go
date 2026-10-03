package flowfile

import (
	"fmt"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// The fields of a record, as an expression reads them.
//
// A record input is a map at run time, so cel-go can only call `inputs.order.id`
// a `dyn`. What the file states is more: `Order` declares `id` a string and
// `total` a `Money`, whose `cents` is an int. That is declared to the checker the
// way every other qualified name is, as one variable per path an expression
// actually writes (`inputs.order.total.cents`), so the work is the size of the
// expression and never the size of the type. A path that leaves the record (into
// a list, a map or a scalar) ends there: what is inside is CEL's, and the loop
// variable of a comprehension over `list(Line)` is not typed yet.
//
// A path naming something the closed record does not declare is refused with the
// fields it does, because at run time it is a missing key: a typo that validates
// and fails hours into a durable run.

// A recordPath is one `inputs.<name>.<field>...` chain an expression writes, with
// each field resolved against the record that holds it.
type recordPath struct {
	// names are the chain from the input's own name (`order`, `total`, `cents`).
	// names[0] is the input and names[i+1] is fields[i].
	names []string

	// fields are the declarations the names after the input resolve to, up to the
	// first one that leaves the record or is not declared. Shorter than names-1
	// when the chain went past a field that is not a record.
	fields []*v1.InputDeclaration

	// missing is the first field name the record does not declare, and in is the
	// record that does not; empty when nothing is missing.
	missing string
	in      *v1.TypeDeclaration
}

// fieldPaths resolves every chain of an expression that starts at a record input.
func (t *typeTable) fieldPaths(parsed *expr.ParsedExpr) []recordPath {
	if t == nil || len(t.records) == 0 || parsed == nil {
		return nil
	}

	var paths []recordPath
	seen := map[string]bool{}

	walkExpr(parsed.GetExpr(), func(root string, fields []string) {
		if root != v1.InputsRoot || len(fields) < 2 {
			return
		}
		key := strings.Join(fields, ".")
		if seen[key] {
			return
		}
		seen[key] = true

		if path, ok := t.resolveFieldPath(fields); ok {
			paths = append(paths, path)
		}
	})

	return paths
}

// resolveFieldPath resolves names (the input, then fields) against the record the
// input is declared as. False for an input that is not a record.
func (t *typeTable) resolveFieldPath(names []string) (recordPath, bool) {
	declared := t.inputTypes[names[0]]
	if declared.GetMessage() == "" {
		return recordPath{}, false
	}

	path := recordPath{names: names}
	record := t.records[declared.GetMessage()]
	for _, name := range names[1:] {
		if record == nil {
			break
		}

		i := slices.IndexFunc(record.GetFields(), func(f *v1.InputDeclaration) bool { return f.GetName() == name })
		if i < 0 {
			path.missing, path.in = name, record
			break
		}

		field := record.GetFields()[i]
		path.fields = append(path.fields, field)
		record = t.records[field.DeclaredType().GetMessage()]
	}

	return path, true
}

// fieldErrors reports each chain in the expression that names a field a record
// does not declare.
func (t *typeTable) fieldErrors(site v1.ValueSite) Diagnostics {
	parsed := site.Value.GetExpr()

	var ds Diagnostics
	for _, path := range t.fieldPaths(parsed) {
		if path.missing == "" {
			continue
		}

		known := make([]string, 0, len(path.in.GetFields()))
		for _, f := range path.in.GetFields() {
			known = append(known, f.GetName())
		}

		message := fmt.Sprintf("%s.%s: the record %s has no field %q; it declares %s",
			v1.InputsRoot, strings.Join(path.names[:len(path.fields)+2], "."), path.in.GetName(), path.missing, quoteAll(known))
		if suggestion, ok := nearest.Name(path.missing, known); ok {
			message += fmt.Sprintf(". Did you mean %q?", suggestion)
		}

		ds = append(ds, Diagnostic{
			Step: site.Step, Field: site.Field(), Message: message,
			Code: v1.DiagnosticCodeUnresolvedReference,
		})
	}

	return ds
}

func quoteAll(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = fmt.Sprintf("%q", n)
	}

	return strings.Join(quoted, ", ")
}

// walkExpr calls visit with the root identifier and the field chain of every
// maximal select chain in e (`inputs.order.id` is root `inputs`, fields `order`,
// `id`), including a `has()` test's final select, and descends into everything
// else. A chain is visited once, whole: its prefixes are the visitor's to take.
func walkExpr(e *expr.Expr, visit func(root string, fields []string)) {
	if e == nil {
		return
	}

	if sel := e.GetSelectExpr(); sel != nil {
		var fields []string
		operand := e
		for operand.GetSelectExpr() != nil {
			fields = append(fields, operand.GetSelectExpr().GetField())
			operand = operand.GetSelectExpr().GetOperand()
		}
		if ident := operand.GetIdentExpr(); ident != nil {
			slices.Reverse(fields)
			visit(ident.GetName(), fields)

			return
		}
		walkExpr(operand, visit)

		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		walkExpr(kind.CallExpr.GetTarget(), visit)
		for _, arg := range kind.CallExpr.GetArgs() {
			walkExpr(arg, visit)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			walkExpr(el, visit)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			walkExpr(entry.GetMapKey(), visit)
			walkExpr(entry.GetValue(), visit)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		walkExpr(c.GetIterRange(), visit)
		walkExpr(c.GetAccuInit(), visit)
		walkExpr(c.GetLoopCondition(), visit)
		walkExpr(c.GetLoopStep(), visit)
		walkExpr(c.GetResult(), visit)
	}
}
