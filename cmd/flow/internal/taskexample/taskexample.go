// Package taskexample builds the smallest Flowfile step that runs a given
// task.
//
// `flow tasks <name>` and the generated task reference
// (docs/reference/tasks.md, via cmd/flow/internal/docsgen) both need a
// worked example of the same shape — a step somebody can copy that actually
// compiles — so it is built once, here, rather than written by hand in two
// places that would drift the first time a task's required inputs changed.
// See CLAUDE.md's "prefer deriving to duplicating."
package taskexample

import (
	"fmt"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Build is the smallest Flowfile that runs this task, built from the task's
// own required inputs.
//
// Built rather than written down, so it cannot describe a task that has
// changed under it, and checked by compiling it rather than by eye: every
// caller's own mirror test runs the result through [flowfile.ValidateSource],
// the same compiler `flow validate` is. That is the mirror-test discipline
// the rest of this repo applies to a rewriter's output: asserting the bytes
// still mean something, rather than asserting that they were produced.
//
// Only the required inputs. An example carrying every optional input is a
// specification, and the reason to show one at all is that it is the
// shortest thing that works.
func Build(def v1.TaskDef) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "  edition: %s\n  name: example\n  steps:\n    - id: %s\n      %s:\n",
		flowfile.CurrentEdition, stepID(def.Name), def.Name)

	written := 0
	for _, field := range v1.Inputs(def) {
		if !field.Required {
			continue
		}

		val, ok := exampleValue(def, field)
		if !ok {
			// A required input this cannot write a plausible value for. Refused
			// rather than guessed at, because an example that does not compile
			// teaches the wrong thing twice: the reader copies it, and the file
			// they get back a diagnostic on is one this command handed them.
			return "", fmt.Errorf("no example value for the %s task's required input %q (type %s); "+
				"teach exampleValue what to write there", def.Name, field.Name, field.Type)
		}

		fmt.Fprintf(&b, "        %s: %s\n", field.Name, val)
		written++
	}

	if written == 0 {
		// A task with nothing required still needs a body, since a step naming a
		// task with no mapping under it is not a step this grammar has.
		fmt.Fprintf(&b, "        {}\n")
	}

	return strings.TrimRight(b.String(), "\n"), nil
}

// stepID turns a task name into an id the grammar accepts.
//
// A task name may hold a dot, because a plugin's tasks are namespaced
// (`example.greet`), and a step id may not. So the id is the name with
// everything else turned into an underscore, which keeps it recognisably
// about that task rather than a generic `step-1` a reader has to map back.
//
// An underscore and not a hyphen, which is the narrower rule and the one that
// caught this: `Node.id`'s own pattern accepts `example-greet`, and the
// validator then refuses it anyway, because an id is also a name an
// expression reads and `${example-greet.body}` parses as a subtraction. The
// example is written to the rule that decides whether it compiles.
func stepID(name string) string {
	id := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			return r
		default:
			return '_'
		}
	}, name)

	if id == "" || (id[0] >= '0' && id[0] <= '9') {
		// An identifier cannot open with a digit, and a task named for a number is
		// legal in the registry.
		id = "step_" + id
	}

	return id
}

// exampleValue is a plausible literal for one required input.
//
// Plausible rather than merely well-typed, because the task itself gets a
// say: the http task refuses a `url:` that is not something it could fetch,
// so `example` would be a string of the right type that `flow validate`
// rejects. The value is therefore offered to [v1.CheckLiteralInput], which is
// the same check the validator runs, and reported as absent if the task
// refuses it, which fails the mirror test rather than printing something
// that does not compile.
func exampleValue(def v1.TaskDef, field v1.InputField) (string, bool) {
	var candidate string

	switch {
	case slices.Contains(def.ExpressionInputs, field.Name):
		// An input that has to be written as an expression, per the definition's
		// own list. A literal here is a run that fails on its first attempt.
		candidate = "${true}"
	case strings.HasPrefix(field.Type, "list["):
		candidate = "[]"
	case strings.HasPrefix(field.Type, "map["):
		candidate = "{}"
	case field.Type == "bool":
		candidate = "true"
	case field.Type == "int", field.Type == "uint", field.Type == "double":
		candidate = "1"
	case strings.Contains(field.Type, " | "):
		// An enum renders as its choices, so the first of them is a real value.
		candidate = strings.TrimSpace(strings.Split(field.Type, " | ")[0])
	case slices.Contains(field.Constraints, "a URI"):
		candidate = "https://example.com/"
	case field.Type == "string", field.Type == "any":
		candidate = "example"
	default:
		return "", false
	}

	if err := v1.CheckLiteralInput(def.Name, field.Name, &v1.Value{
		Kind: &v1.Value_Literal{Literal: &expr.Value{
			Kind: &expr.Value_StringValue{StringValue: candidate},
		}},
	}); err != nil {
		return "", false
	}

	return candidate, true
}
