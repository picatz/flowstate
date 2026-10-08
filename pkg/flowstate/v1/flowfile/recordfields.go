package flowfile

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

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
	// record that does not; in is nil when nothing is missing, since a
	// field's name may itself be empty (`inputs.order[""]`).
	missing string
	in      *v1.TypeDeclaration

	// prefix is what precedes names in a sentence about the chain: `inputs.` for a
	// record input, nothing for a function's parameter, which is its own root.
	prefix string
}

// fieldPaths resolves every chain of an expression that starts at a record input,
// or at the iterator of a `for_each` over a list of records, from the step the
// expression is written in.
func (t *typeTable) fieldPaths(parsed *expr.ParsedExpr, step string) []recordPath {
	if t == nil || len(t.records) == 0 || parsed == nil {
		return nil
	}

	var paths []recordPath
	seen := map[string]bool{}

	items := t.recordIterators(step)

	walkExpr(parsed.GetExpr(), func(root string, fields []string) {
		if declared, bound := items[root]; bound {
			names := append([]string{root}, fields...)
			key := strings.Join(names, ".")
			if seen[key] {
				return
			}
			seen[key] = true

			if path, ok := t.resolveFrom(declared, names); ok {
				paths = append(paths, path)
			}

			return
		}

		if root != v1.InputsRoot || len(fields) < 2 {
			return
		}
		key := strings.Join(fields, ".")
		if seen[key] {
			return
		}
		seen[key] = true

		if path, ok := t.resolveFieldPath(fields); ok {
			path.prefix = v1.InputsRoot + "."
			paths = append(paths, path)
		}
	})

	return paths
}

// recordIterators are the iterators visible from step whose item is a record, by
// name, with the record each is declared as. The innermost loop wins a shared name,
// and one that rebinds it to anything else hides the outer record.
func (t *typeTable) recordIterators(step string) map[string]*v1.Type {
	if t == nil || len(t.records) == 0 {
		return nil
	}

	var records map[string]*v1.Type
	for _, binding := range t.scopes[step] {
		if declared := t.elementDeclared(binding); declared.GetMessage() != "" {
			if records == nil {
				records = map[string]*v1.Type{}
			}
			records[binding.name] = declared
		} else {
			delete(records, binding.name)
		}
	}

	return records
}

// parameterPaths resolves every chain of an expression that starts at one of the
// parameters inputTypes names, the way [typeTable.fieldPaths] does for a record
// input. A function body reads its record parameter as `user.id`: the parameter is
// the root, so there is no `inputs` to strip.
func (t *typeTable) parameterPaths(parsed *expr.Expr) []recordPath {
	if t == nil || len(t.records) == 0 || parsed == nil {
		return nil
	}

	var paths []recordPath
	seen := map[string]bool{}

	walkExpr(parsed, func(root string, fields []string) {
		if _, ok := t.inputTypes[root]; !ok {
			return
		}
		names := append([]string{root}, fields...)
		key := strings.Join(names, ".")
		if seen[key] {
			return
		}
		seen[key] = true

		if path, ok := t.resolveFieldPath(names); ok {
			paths = append(paths, path)
		}
	})

	return paths
}

// resolveFieldPath resolves names (the input, then fields) against the record the
// input is declared as. False for an input that is not a record.
func (t *typeTable) resolveFieldPath(names []string) (recordPath, bool) {
	return t.resolveFrom(t.inputTypes[names[0]], names)
}

// resolveFrom resolves names[1:] as fields of the record declared is, names[0]
// being what the chain is rooted at.
func (t *typeTable) resolveFrom(declared *v1.Type, names []string) (recordPath, bool) {
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

// The most a diagnostic about one record field echoes back, and the most it
// reports. A file controls the expression, so it controls how long a missing name
// is, how many there are, and how many fields a record lists; the refusal is worth
// the same to its reader at a fraction of that, and the work of a suggestion grows
// with the product of the lengths. A name past the echo limit is cut and is not
// matched against the record's fields: no declared field is that long to be meant.
const (
	maxEchoedName      = 64
	maxFieldErrors     = 8
	maxEchoedFieldList = 16
)

// fieldErrors reports each chain in the expression that names a field a record
// does not declare, up to [maxFieldErrors] of them: the first is the one to fix and
// the rest follow it.
func (t *typeTable) fieldErrors(site v1.ValueSite) Diagnostics {
	return pathErrors(t.fieldPaths(site.Value.GetExpr(), site.Step), site.Step, site.Field())
}

// pathErrors reports each of paths that names a field its record does not declare,
// up to [maxFieldErrors], against step and field.
func pathErrors(paths []recordPath, step, field string) Diagnostics {
	var ds Diagnostics
	for _, path := range paths {
		if path.in == nil {
			continue
		}
		if len(ds) == maxFieldErrors {
			break
		}

		fields := path.in.GetFields()
		known := make([]string, 0, min(len(fields), maxEchoedFieldList))
		for _, f := range fields[:min(len(fields), maxEchoedFieldList)] {
			known = append(known, f.GetName())
		}
		declares := quoteAll(known)
		if len(fields) > len(known) {
			declares += fmt.Sprintf(", and %d more", len(fields)-len(known))
		}

		chain := make([]string, 0, len(path.fields)+2)
		for _, name := range path.names[:len(path.fields)+2] {
			chain = append(chain, echo(name))
		}

		message := fmt.Sprintf("%s%s: the record %s has no field %q; it declares %s",
			path.prefix, strings.Join(chain, "."), path.in.GetName(), echo(path.missing), declares)
		if len(path.missing) <= maxEchoedName {
			all := make([]string, 0, len(fields))
			for _, f := range fields {
				all = append(all, f.GetName())
			}
			if suggestion, ok := nearest.Name(path.missing, all); ok {
				message += fmt.Sprintf(". Did you mean %q?", suggestion)
			}
		}

		ds = append(ds, Diagnostic{
			Step: step, Field: field, Message: message,
			Code: v1.DiagnosticCodeUnresolvedReference,
		})
	}

	return ds
}

// echo cuts a name a file controls to [maxEchoedName] bytes, on a rune boundary.
func echo(name string) string {
	if len(name) <= maxEchoedName {
		return name
	}

	cut := maxEchoedName
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}

	return name[:cut] + "…"
}

func quoteAll(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = fmt.Sprintf("%q", n)
	}

	return strings.Join(quoted, ", ")
}

// walkExpr calls visit with the root identifier and the field chain of every
// maximal chain of field reads in e (`inputs.order.id` is root `inputs`, fields
// `order`, `id`), and descends into everything else. A `has()` test's final select
// is a chain, and so are an optional selection (`inputs.order.?id`) and an index
// by a literal string (`inputs.order["id"]`): they read the same field, and are
// how an author who follows docs/STYLE.md spells an optional one. A chain is
// visited once, whole: its prefixes are the visitor's to take.
//
// A root a comprehension binds (`xs.exists(inputs, inputs.a)`) is the loop's own
// value and is not visited; the range and the accumulator's start are evaluated
// outside the loop's scope, as [collectReferences] has them.
func walkExpr(e *expr.Expr, visit func(root string, fields []string)) {
	walkExprBound(e, nil, visit)
}

func walkExprBound(e *expr.Expr, bound map[string]struct{}, visit func(root string, fields []string)) {
	if e == nil {
		return
	}

	if root, fields, ok := fieldChain(e); ok {
		if _, shadowed := bound[root]; !shadowed {
			visit(root, fields)
		}

		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		walkExprBound(kind.SelectExpr.GetOperand(), bound, visit)
	case *expr.Expr_CallExpr:
		walkExprBound(kind.CallExpr.GetTarget(), bound, visit)
		for _, arg := range kind.CallExpr.GetArgs() {
			walkExprBound(arg, bound, visit)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			walkExprBound(el, bound, visit)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			walkExprBound(entry.GetMapKey(), bound, visit)
			walkExprBound(entry.GetValue(), bound, visit)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		walkExprBound(c.GetIterRange(), bound, visit)
		walkExprBound(c.GetAccuInit(), bound, visit)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		walkExprBound(c.GetLoopCondition(), inner, visit)
		walkExprBound(c.GetLoopStep(), inner, visit)
		walkExprBound(c.GetResult(), inner, visit)
	}
}

// fieldChain reads e as a chain of field reads that bottoms out in an identifier.
// The three spellings of a read are a select, an optional select (`_?._`) and an
// index (`_[_]`, `_[?_]`) whose key is a string literal; a key that is computed is
// not a field the file names, so it ends the chain, and what is inside it is the
// walk's.
func fieldChain(e *expr.Expr) (root string, fields []string, ok bool) {
	for {
		switch kind := e.GetExprKind().(type) {
		case *expr.Expr_IdentExpr:
			slices.Reverse(fields)

			return kind.IdentExpr.GetName(), fields, len(fields) > 0

		case *expr.Expr_SelectExpr:
			fields = append(fields, kind.SelectExpr.GetField())
			e = kind.SelectExpr.GetOperand()

		case *expr.Expr_CallExpr:
			call := kind.CallExpr
			switch call.GetFunction() {
			case "_?._", "_[_]", "_[?_]":
			default:
				return "", nil, false
			}
			if call.GetTarget() != nil || len(call.GetArgs()) != 2 {
				return "", nil, false
			}
			key, isString := call.GetArgs()[1].GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
			if !isString {
				return "", nil, false
			}
			fields = append(fields, key.StringValue)
			e = call.GetArgs()[0]

		default:
			return "", nil, false
		}
	}
}
