package docsgen

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// renderDiagnosticCodeReference documents every stable diagnostic code this
// build assigns.
//
// Derived from [v1.DiagnosticCodes] — the same registry flowfile's
// diagnostic-construction sites assign from — for the reason every generated
// document here exists: a code list kept by hand beside the code that assigns
// codes is a second source of truth, and CLAUDE.md's four proven driftors were
// all exactly that shape. TestDiagnosticCodesAreAssigned, in the flowfile
// package, is the other half — it holds the *registry* to the codes actually
// used, so this file documenting a code nothing assigns (or missing one that
// is assigned) fails there rather than only being wrong here.
func (g *Generator) renderDiagnosticCodeReference() string {
	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# Diagnostic code reference\n\n")
	b.WriteString("Every stable `code` a [`Diagnostic`](../../proto/flowstate/v1/diagnostics.proto)\n")
	b.WriteString("carries, so a program can decide what a validation failure *is* without parsing\n")
	b.WriteString("`message`, which this project reserves the right to reword. Deliberately small:\n")
	b.WriteString("only the classes an agent is actually expected to branch on have their own code,\n")
	b.WriteString("everything else is `general`, and that is documented rather than pretended away.\n\n")

	b.WriteString("| Code | Meaning |\n|---|---|\n")
	for _, info := range v1.DiagnosticCodes() {
		fmt.Fprintf(&b, "| `%s` | %s |\n", cell(string(info.Code)), cell(info.Description))
	}

	b.WriteString("\n")
	b.WriteString(diagnosticShapeSection)

	return b.String()
}

// diagnosticShapeSection worked-example documents [v1.Diagnostic.edits], the
// field an agent needs and the one this page used to say nothing about.
//
// A code table answers "what class of problem is this," which is not the
// question an agent applying a fix asks. `edits` is how a caller repairs a
// file without re-parsing `message`: each [v1.SuggestedEdit] is complete and
// independently applyable, so "apply the first edit" is a correct strategy
// with no judgment about the diagnostic's prose required. Empty is the common
// and honest answer — most diagnostics cannot name an edit safely, see the
// field's own doc comment in diagnostics.proto — so the one case worth
// showing whole is the case where it is populated.
//
// The worked example is a step's own property misspelled (`retryy:` for
// `retry:`), not a task input misspelled (`log:`'s `message:`): the two look
// identical on the page but take different paths through the compiler. A step
// property goes through [compiler.check] and its nearest-match rename offers
// an edit; a task input is checked by validateTaskInputs against the task's
// own schema and does not populate one. Picking the wrong one would document
// a response `flow validate` cannot actually produce — verify any change here
// against real `flow validate --output json` output on the fixture below
// before editing the JSON.
const diagnosticShapeSection = "## Shape\n\n" +
	"The table above narrows *what* is wrong: `code` names the class, `message` says\n" +
	"what a human reads. Neither is what a program acts on to repair a file without a\n" +
	"human reading anything: that is `edits`, a field on every `Diagnostic` (see\n" +
	"[diagnostics.proto](../../proto/flowstate/v1/diagnostics.proto)) with no code of\n" +
	"its own because it is populated per diagnostic, only when the checker that raised\n" +
	"it can name the exact source to replace and the exact text to put there.\n\n" +
	"A step's own property misspelled is one of the checks that can, because the\n" +
	"nearest known property name is an unambiguous rename. Here step `notify` writes\n" +
	"`retryy:` where the grammar has `retry:`, so `flow validate --output json` answers\n" +
	"with a `Diagnostic` carrying an edit that renames it:\n\n" +
	"```yaml\n" +
	"steps:\n" +
	"  - id: notify\n" +
	"    retryy:\n" +
	"      max_attempts: 3\n" +
	"    log:\n" +
	"      message: hello\n" +
	"```\n\n" +
	"```json\n" +
	"{\n" +
	"  \"line\": 5,\n" +
	"  \"column\": 5,\n" +
	"  \"message\": \"unknown key \\\"retryy\\\"; did you mean \\\"retry\\\"?\",\n" +
	"  \"step\": \"notify\",\n" +
	"  \"code\": \"general\",\n" +
	"  \"edits\": [\n" +
	"    {\n" +
	"      \"title\": \"rename to `retry`\",\n" +
	"      \"changes\": [\n" +
	"        {\n" +
	"          \"range\": { \"startLine\": 5, \"startColumn\": 5, \"endLine\": 5, \"endColumn\": 11 },\n" +
	"          \"newText\": \"retry\"\n" +
	"        }\n" +
	"      ]\n" +
	"    }\n" +
	"  ]\n" +
	"}\n" +
	"```\n\n" +
	"Applying every `changes` entry in one `edits[i]` and stopping is a complete\n" +
	"repair for the problem that diagnostic names — not a promise the file has no\n" +
	"other problems, and never a step to compose with a sibling edit. More than one\n" +
	"entry in `edits` means alternatives to choose between, not a sequence to run.\n"
