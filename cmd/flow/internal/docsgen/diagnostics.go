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
	b.WriteString("Every stable `code` a [`Diagnostic`](../../proto/flowstate/v1/flowstate.proto)\n")
	b.WriteString("carries, so a program can decide what a validation failure *is* without parsing\n")
	b.WriteString("`message`, which this project reserves the right to reword. Deliberately small:\n")
	b.WriteString("only the classes an agent is actually expected to branch on have their own code,\n")
	b.WriteString("everything else is `general`, and that is documented rather than pretended away.\n\n")

	b.WriteString("| Code | Meaning |\n|---|---|\n")
	for _, info := range v1.DiagnosticCodes() {
		fmt.Fprintf(&b, "| `%s` | %s |\n", cell(string(info.Code)), cell(info.Description))
	}

	return b.String()
}
