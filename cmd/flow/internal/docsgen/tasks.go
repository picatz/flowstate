package docsgen

import (
	"fmt"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/taskexample"
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
	b.WriteString("**Required** marks an input a step cannot omit. **Deferred** marks one the\n")
	b.WriteString("task evaluates itself, against a scope the engine does not have when it\n")
	b.WriteString("schedules the step — which is why a deferred input may name something an\n")
	b.WriteString("ordinary one cannot: `http`'s `outputs` may read `status_code` because the\n")
	b.WriteString("task evaluates it after the response arrives, and the engine resolves every\n")
	b.WriteString("other input before the step is even scheduled. **Bounds** is the rest of what\n")
	b.WriteString("the schema and the task already know about what may be written here.\n\n")

	for _, task := range catalog.GetTasks() {
		fmt.Fprintf(&b, "### `%s`\n\n", task.GetName())
		if summary := task.GetSummary(); summary != "" {
			fmt.Fprintf(&b, "%s\n\n", summary)
		}

		writeTaskFields(&b, "Inputs", task.GetInputs(), true)
		writeTaskFields(&b, "Outputs", task.GetOutputs(), false)
		writeTaskExample(&b, task)
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
	b.WriteString("the evaluator, so its name is not its whole call form; the example is. An\n")
	b.WriteString("ordinary function's own overloads carry the same information — argument order,\n")
	b.WriteString("arity, and whether it is written on a namespace or a value — as Signature.\n\n")
	b.WriteString("| Function | Library | Kind | Example | Signature |\n|---|---|---|---|---|\n")
	for _, fn := range catalog.GetCelFunctions() {
		kind := "function"
		if fn.GetMacro() {
			kind = "macro"
		}
		example := "—"
		if fn.GetExample() != "" {
			example = "`" + cell(fn.GetExample()) + "`"
		}
		signature := orDash(signatureCell(fn.GetSignature()))
		fmt.Fprintf(&b, "| `%s` | `%s` | %s | %s | %s |\n",
			cell(fn.GetName()), cell(fn.GetLibrary()), kind, example, signature)
	}

	return b.String()
}

// writeTaskExample renders the smallest Flowfile step that runs task.
//
// Built by [taskexample.Build], the same function `flow tasks <name>` calls,
// so the two cannot show a reader two different "smallest step that works"
// for one task (#702) — a worked example is the one thing #702 found neither
// surface had at all.
//
// task's TaskDef comes from [v1.DefaultRegistry] by name rather than from a
// second walk of it: [v1.Catalog] is built by mapping this exact registry
// through [v1.DescribeTask] one task at a time, so every name this loop sees
// is a name that registry already answered for — this is a lookup into that
// same pass, not a second, possibly-divergent traversal of it.
func writeTaskExample(b *strings.Builder, task *v1.TaskDescription) {
	def, ok := v1.DefaultRegistry().Lookup(task.GetName())
	if !ok {
		// Unreachable: see the doc comment above — task's name came from
		// ranging over this exact registry already.
		panic(fmt.Sprintf("docsgen: task %q is in the catalog but not the registry", task.GetName()))
	}

	example, err := taskexample.Build(def)
	if err != nil {
		// Unreachable in a build that passes cmd/flow/internal/taskexample's own
		// TestBuildValidates, which runs this over every registered task and
		// fails there, by name, before this generator ever runs.
		panic(fmt.Sprintf("docsgen: no worked example for task %q: %v", task.GetName(), err))
	}

	b.WriteString("**A step that uses it:**\n\n```yaml\n")
	for _, line := range strings.Split(example, "\n") {
		// Two spaces of indent for the terminal `flow tasks <name>` prints this
		// beside, which is not what a fenced code block wants: what a reader
		// copies out of a document is the block itself, and it should compile
		// starting at column zero.
		b.WriteString(strings.TrimPrefix(line, "  "))
		b.WriteString("\n")
	}
	b.WriteString("```\n\n")
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
