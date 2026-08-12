package docsgen

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// renderTaskReference documents what this build can execute.
//
// Derived from [v1.Catalog], which is the message the GetCatalog RPC returns
// unchanged — so the document and the RPC cannot describe different tasks. That
// is the point of going through the catalog rather than walking the registry
// here: a second traversal of the registry would be a second answer to "what can
// this build run", and the two would eventually differ in some detail nobody
// compares.
//
// Built-ins only. A plugin's tasks are a property of a deployment, not of this
// binary, and `flow plugins` is the command that asks a deployment.
func (g *Generator) renderTaskReference() string {
	catalog := v1.Catalog()

	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# Task reference\n\n")
	b.WriteString("Every task this build can execute, with the inputs it takes and the outputs it\n")
	b.WriteString("produces. Derived from the task registry (the same `TaskCatalog` the `GetCatalog`\n")
	b.WriteString("RPC and `flow tasks` answer with), so a task cannot behave one way and document\n")
	b.WriteString("another.\n\n")
	b.WriteString("A plugin's tasks are not here: what a plugin adds is a property of a deployment\n")
	b.WriteString("rather than of this binary, and `flow plugins` is what asks a deployment.\n\n")

	b.WriteString("## Tasks\n\n")
	for _, task := range catalog.GetTasks() {
		fmt.Fprintf(&b, "### `%s`\n\n", task.GetName())
		if summary := task.GetSummary(); summary != "" {
			fmt.Fprintf(&b, "%s\n\n", summary)
		}

		writeTaskFields(&b, "Inputs", task.GetInputs(), true)
		writeTaskFields(&b, "Outputs", task.GetOutputs(), false)
	}

	b.WriteString("## Expressions\n\n")
	fmt.Fprintf(&b, "Values are reached through these roots: %s.\n\n", codeList(catalog.GetValueRoots()))
	fmt.Fprintf(&b, "Duration constructors, available to every expression: %s.\n\n", codeList(catalog.GetDurationUnits()))
	fmt.Fprintf(&b, "Inside a wait's own expressions (`sleep:`, `wait_until:`, and a signal's\n"+
		"`timeout:`) and nowhere else, `%s` is bound to the evaluation moment.\n\n",
		catalog.GetNowIdentifier())
	fmt.Fprintf(&b, "CEL libraries every expression reaches: %s.\n\n", codeList(catalog.GetCelLibraries()))

	b.WriteString("### Functions\n\n")
	b.WriteString("What those libraries add. A macro is expanded by the parser rather than called by\n")
	b.WriteString("the evaluator, so its name is not its whole call form; the example is.\n\n")
	b.WriteString("| Function | Library | Kind | Example |\n|---|---|---|---|\n")
	for _, fn := range catalog.GetCelFunctions() {
		kind := "function"
		if fn.GetMacro() {
			kind = "macro"
		}
		example := "—"
		if fn.GetExample() != "" {
			example = "`" + cell(fn.GetExample()) + "`"
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | %s | %s |\n",
			cell(fn.GetName()), cell(fn.GetLibrary()), kind, example)
	}

	return b.String()
}

// writeTaskFields renders one task's inputs or outputs.
func writeTaskFields(b *strings.Builder, heading string, fields []*v1.TaskField, required bool) {
	fmt.Fprintf(b, "**%s**\n\n", heading)

	if len(fields) == 0 {
		b.WriteString("None.\n\n")

		return
	}

	if required {
		b.WriteString("| Name | Type | Required | Deferred | Bounds |\n|---|---|---|---|---|\n")
	} else {
		b.WriteString("| Name | Type | Bounds |\n|---|---|---|\n")
	}

	for _, field := range fields {
		// The bounds the schema and the task already carry, the same phrases `flow
		// tasks <name>` prints, because they come from the same field on the same
		// message. A reference that omitted them would send a reader who wants to
		// know how long a `credential:` may be to the proto, which is the drift this
		// document exists to prevent.
		bounds := "none"
		if constraints := field.GetConstraints(); len(constraints) > 0 {
			bounds = cell(strings.Join(constraints, "; "))
		}

		if !required {
			fmt.Fprintf(b, "| `%s` | `%s` | %s |\n",
				cell(field.GetName()), cell(field.GetType()), bounds)

			continue
		}

		fmt.Fprintf(b, "| `%s` | `%s` | %s | %s | %s |\n",
			cell(field.GetName()), cell(field.GetType()),
			yesNo(field.GetRequired()), yesNo(field.GetDeferred()), bounds)
	}
	b.WriteString("\n")
}
