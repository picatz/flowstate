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
// inputs were bound into. See [v1.SensitiveInputValues].
//
// A declared input's own root value gets [bothSpellings]'s escaped spelling
// too, which [v1.SensitiveInputValues]'s own substring backstop does not add:
// it holds the value exactly as bound, so a `sensitive:` input holding a tab,
// a newline, a quote or a backslash — concatenated into a step's output, say
// — prints escaped from a `%q`-rendered witness such as [checkWitnesses]'
// (Codex, #2079's issue comment). The validate-time path
// ([File.CheckSignalNames]) has carried both spellings of its own input
// material since #2041; this is the run-time counterpart, the one seam
// [sensitiveNativeValues] itself is, rather than each of its two callers
// adding the escaping on its own.
func sensitiveNativeValues(scope *v1.Scope, sensitiveNames map[string]bool) sensitiveInputs {
	set := v1.SensitiveInputValues(scope.GetInputs(), sensitiveNames)

	return set.WithValues(bothSpellings(sensitiveInputRootStrings(scope.GetInputs(), sensitiveNames))...)
}

// sensitiveInputRootStrings is the string form of each declared `sensitive:`
// input's own root value — not its descendants, which [v1.SensitiveInputValues]
// already walks into its value set and which escaping a second time here
// would gain nothing: a descendant's raw spelling is caught the identical way
// a root's is, and [bothSpellings] exists to add the one spelling a %q
// rendering produces instead. Only a root has that spelling to add through
// this path, and only a string root has one at all — a numeric or boolean
// root's text ([fmt.Sprint]) holds none of the runes %q escapes.
//
// An input this cannot read is silently skipped rather than failing closed on
// its own: [v1.SensitiveInputValues], asked with the same inputs and the same
// names right above, already answers [v1.WithheldSensitiveValues] for that
// case, and [sensitiveInputs.WithValues] carries a withholding set's
// withholdAll through untouched, so the run's actual redaction set still
// withholds everything regardless of what this collects.
func sensitiveInputRootStrings(inputs map[string]*v1.Value, sensitiveNames map[string]bool) []string {
	var roots []string
	for name, value := range inputs {
		if !sensitiveNames[name] {
			continue
		}
		native, err := v1.LiteralToGo(value.GetLiteral())
		if err != nil {
			continue
		}
		if s, ok := native.(string); ok {
			roots = append(roots, s)
		}
	}

	return roots
}
