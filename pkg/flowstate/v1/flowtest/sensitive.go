package flowtest

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The redaction set this package builds per case and shares between the stub
// diagnostics, the transcript recorder, the check witnesses and an attached
// debugger. The mechanism itself lives in [v1] — see sensitivevalues.go there
// for why sensitivity has to travel by value rather than by name, and for the
// bounds and the fail-closed rule the walk follows.
//
// It moved out of this package when `flow run local` and `flow run` needed the
// same answer for the run-failure sentence they render, which `flow test` was
// already clearing here and they were printing in the clear: one value with
// one meaning belongs in the package both drivers and the CLI already import
// (CLAUDE.md, "both execution drivers must agree"). The local names below are
// kept for the same reason [literalToGo] is: so the call sites did not have to
// move with it.

// sensitiveInputs is [v1.SensitiveValues] under this package's own name.
type sensitiveInputs = v1.SensitiveValues

// sensitiveMarker is what a redacted value renders as.
const sensitiveMarker = v1.SensitiveMarker

// sensitiveNativeValues builds the redaction set for a run from the scope its
// inputs were bound into. See [v1.SensitiveInputValues], which now carries a
// `%q`-escaped spelling of every string it collects — root or descendant —
// alongside the raw one, so a `sensitive:` input holding a tab, a newline, a
// quote or a backslash prints redacted from a `%q`-rendered witness such as
// [checkWitnesses]'s the same way it already does from a plain one (Codex,
// #2079's issue comment; Copilot, on this fix's own first, root-only pass).
func sensitiveNativeValues(scope *v1.Scope, sensitiveNames map[string]bool) sensitiveInputs {
	return v1.SensitiveInputValues(scope.GetInputs(), sensitiveNames)
}
