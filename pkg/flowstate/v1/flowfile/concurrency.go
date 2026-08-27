package flowfile

import (
	"fmt"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `concurrency:` — at most one run of this workflow at a time per key, decided by
// the server at submit. See [v1.Concurrency]'s own doc comment for the mechanism
// and for the two things it deliberately does not do; this file is only the
// grammar.
//
// Parsing, writing and validating the block are together here for the reason
// `signals.go` and `triggers.go` keep their three together: [concurrencyToYAML] is
// the inverse of the parser below, and a key one of them knows about and the other
// does not is a `flow fmt` that silently deletes an author's exclusion.
//
// Nothing here binds a name into any expression's scope. `key:` is a value like a
// task input's — a literal string, or an expression over the ambient `inputs.`
// root — so there is no binding for `flow fix`'s rewriter to corrupt, and this
// file carries no scope rules of the kind `as:`, a step's `vars:` and a wait's
// `now` need.

// concurrencyKeys are what the block may say.
var concurrencyKeys = []string{"key", "on_conflict"}

// notInConcurrencyKeyHelp is the refusal for a secret reference in `key:`, read
// the same way [notInVarHelp] is. A key is resolved by the server at submit,
// where there is no activity to resolve a secret in — and the resolved value is
// digested into the run's own workflow id, which is durable and broadly readable
// (invariant 8), so a secret here would be a credential turned into an
// identifier.
const notInConcurrencyKeyHelp = "a secret reference cannot be used as a `concurrency:` key; the key is " +
	"resolved by the server at submit, where there is no activity to resolve one in, and it is " +
	"digested into the run's own workflow id, which is durable and broadly readable. Name the " +
	"resource with an input instead"

// concurrency compiles the top-level `concurrency:` block.
func (c *compiler) concurrency(n ast.Node, path string, r ref) *v1.Concurrency {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, concurrencyKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping saying what at most one run of this workflow may hold at a time: "+
				"`key:`, the resource, and an optional `on_conflict:` (%s)",
			strings.Join(v1.ConcurrencyOnConflictNames(), ", "))
		return nil
	}

	f, found := fields.get("key")
	if !found {
		c.report(spanOfNode(n), r,
			"declares no `key:`, so it names nothing to hold; write the resource one run at a "+
				"time may touch — a literal for a workflow exclusive of itself, or an expression "+
				"over this run's own arguments such as ${inputs.cluster}")
		return nil
	}

	keyPath := fieldPath(path, "key")
	keyRef := ref{path: keyPath, label: "concurrency key"}

	if resolved := c.resolveQuiet(f.value); resolved != nil && c.holdsSecretMarker(resolved) {
		c.report(c.secretMarkerSpan(resolved), keyRef, "%s", notInConcurrencyKeyHelp)
		return nil
	}

	key := c.inputValue(f.value, keyPath, keyRef)
	if key == nil {
		return nil
	}

	concurrency := &v1.Concurrency{Key: key}

	if f, found := fields.get("on_conflict"); found {
		armPath := fieldPath(path, "on_conflict")
		armRef := ref{path: armPath, label: "concurrency on_conflict"}

		if text, ok := c.text(f.value, armPath, armRef); ok {
			arm, known := v1.ParseConcurrencyOnConflict(text)
			switch {
			case known:
				concurrency.OnConflict = arm

			// The three queueing names, refused by name rather than by falling
			// through to "not one of the three". They are real policies with a
			// real spelling in this language — they are just not spellings a
			// *workflow id* can honour, which is a thing an author has no reason
			// to know and every reason to be told. See [v1.Concurrency]'s "what it
			// does not do".
			case queuingOverlapName(text):
				c.report(spanOfNode(f.value), armRef,
					"is %q, which queues a run behind the one already going — and a `concurrency:` "+
						"key cannot queue, because it is enforced by the run's workflow id and an id "+
						"is either free or taken. That policy exists for a schedule's firings, as "+
						"`triggers.schedule.overlap: %s`. Here the answers are %s",
					text, text, strings.Join(v1.ConcurrencyOnConflictNames(), ", "))

			default:
				c.report(spanOfNode(f.value), armRef,
					"is %q, which is not what to do when a run already holds this key; the answers "+
						"are %s",
					text, strings.Join(v1.ConcurrencyOnConflictNames(), ", "))
			}
		}
	}

	return concurrency
}

// queuingOverlapName reports whether a name is one of the schedule overlap
// policies that queues a firing rather than answering immediately.
//
// Read off [v1.ScheduleTrigger_Overlap] rather than written out, so the two
// spellings cannot drift: a name this asks about is one `triggers.schedule.overlap:`
// really accepts, and the diagnostic above sends the author to a key that exists.
func queuingOverlapName(name string) bool {
	overlap, known := v1.ParseOverlap(name)
	if !known {
		return false
	}

	switch overlap {
	case v1.ScheduleTrigger_OVERLAP_BUFFER_ONE,
		v1.ScheduleTrigger_OVERLAP_BUFFER_ALL,
		v1.ScheduleTrigger_OVERLAP_CANCEL_OTHER:
		// cancel_other joins the two that buffer because it is the same shape of
		// promise: the new firing does not start until the old one has stopped,
		// which is a wait, and nothing here waits.
		return true
	default:
		return false
	}
}

// concurrencyToYAML writes the block back out, the inverse of [compiler.concurrency].
func concurrencyToYAML(concurrency *v1.Concurrency) (any, error) {
	key, err := inputValueToYAML(concurrency.GetKey())
	if err != nil {
		return nil, fmt.Errorf("concurrency key: %w", err)
	}

	doc := yaml.MapSlice{{Key: "key", Value: key}}

	// Written only when the author chose an arm, so `on_conflict: reject` and the
	// unwritten default stay distinguishable in the file exactly as they are in
	// the schema — the rule `triggers.schedule.overlap:` already follows.
	if arm := concurrency.GetOnConflict(); arm != v1.Concurrency_ON_CONFLICT_UNSPECIFIED {
		doc = append(doc, yaml.MapItem{Key: "on_conflict", Value: v1.ConcurrencyOnConflictName(arm)})
	}

	return doc, nil
}

// validateConcurrency reports what is wrong with a declared `concurrency:` block
// beyond what the schema's per-field rules already catch: a key reading something
// that does not exist at submit, and the two trigger blocks whose own addressing
// already owns a run's workflow id.
//
// The second half is asked of [v1.CheckConcurrency] rather than restated here,
// because the server refuses the same two shapes before it creates anything and a
// validator that disagreed with it would report a file as fine and then have it
// refused. This adds the line and column that function has no way to know.
func validateConcurrency(wf *v1.Workflow) Diagnostics {
	concurrency := wf.GetConcurrency()
	if concurrency == nil {
		return nil
	}

	var ds Diagnostics

	if err := v1.CheckConcurrency(wf); err != nil {
		ds = append(ds, Diagnostic{Field: "concurrency", Message: err.Error()})
	}

	parsed := concurrency.GetKey().GetExpr()
	if parsed == nil {
		return ds
	}

	field := "concurrency.key"
	rooted, vars, _, run, trigger, bare := referencedIdentifiers(parsed)

	// `inputs` is deliberately absent from what follows: it is the one root this
	// position may read, because binding a run's arguments is the last thing that
	// happens before the key is resolved. Everything else names something that
	// does not exist yet, and each gets its own sentence rather than a shared
	// "unknown name", because the name may be spelled perfectly and the mistake is
	// reading it from here at all.
	for _, ref := range rooted {
		ds = append(ds, Diagnostic{
			Field: field, Value: ref.ID,
			Message: fmt.Sprintf(
				"a concurrency key may not read a step: the key is resolved at submit, before the "+
					"run exists, so %q has produced nothing yet; key on the run's arguments instead, "+
					"such as ${%s.<name>}",
				ref.ID, v1.InputsRoot),
		})
	}

	for _, ref := range vars {
		ds = append(ds, Diagnostic{
			Field: field, Value: ref,
			Message: fmt.Sprintf(
				"a concurrency key may not read a var: `%s:` is evaluated once the run has started "+
					"and the key is resolved before that, at submit; key on the run's arguments "+
					"instead, such as ${%s.<name>}",
				v1.VarsRoot, v1.InputsRoot),
		})
	}

	for range run {
		ds = append(ds, Diagnostic{
			Field: field, Value: v1.RunRoot,
			Message: fmt.Sprintf(
				"a concurrency key may not read `%s`: the key decides what id the run is started "+
					"under, so it is resolved before there is a run to have one",
				v1.RunRoot),
		})
	}

	for range trigger {
		ds = append(ds, Diagnostic{
			Field: field, Value: v1.TriggerRoot,
			Message: fmt.Sprintf(
				"a concurrency key may not read `%s`: the key holds across every start path — "+
					"that is why it is written on the workflow rather than on a trigger — so it is "+
					"resolved against the run's arguments and nothing about how it was started",
				v1.TriggerRoot),
		})
	}

	for _, ref := range bare {
		if isDeclarationRoot(ref) {
			// A root as an operand rather than a selection through it, described
			// by the loops above rather than by the general sentence below.
			ds = append(ds, Diagnostic{
				Field: field, Value: ref,
				Message: fmt.Sprintf(
					"a concurrency key may not read `%s`: it is resolved at submit, against this "+
						"run's arguments (`%s.*`), literals, operators and the profile's functions "+
						"and nothing else",
					ref, v1.InputsRoot),
			})
			continue
		}

		if functionNamespaces[ref] {
			continue
		}

		ds = append(ds, Diagnostic{
			Field: field, Value: ref,
			Message: fmt.Sprintf(
				"references unknown name %q; a concurrency key is resolved at submit, so it may "+
					"use this run's arguments (`%s.*`), literals, operators and the profile's "+
					"functions and nothing else",
				ref, v1.InputsRoot),
		})
	}

	return ds
}
