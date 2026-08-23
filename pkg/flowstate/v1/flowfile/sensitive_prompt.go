package flowfile

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A `wait_for_signal:`'s `prompt:` is the sentence this system puts in front of
// whoever is being asked to approve something. That reader is not the run's
// author: they were handed a run id, they are looking at whatever surface
// renders a parked gate, and they have no way to know what the file behind it
// says. So the question of what a prompt may contain is a different question
// from what a `log:` message may contain, and it gets a different answer.
//
// # Wider than the `log:` lint, on purpose
//
// sensitive_log.go refuses only *direct* surfacing, and its own doc argues
// carefully for that line: `${hash(inputs.token)}` is a digest an author chose
// precisely so the value does not appear, and refusing it would train them to
// distrust the lint. That reasoning holds for a log message, whose reader is the
// operator of the run that produced it.
//
// It does not hold here. A prompt that varies with a private value tells its
// reader something about that value whether or not the value itself appears:
// "approve the payment of ${inputs.salary > 100000 ? 'a large' : 'a small'}
// amount" surfaces nothing verbatim and discloses the thing anyway, to somebody
// the author never decided to disclose it to. So this refuses the *reach*:
// naming a `sensitive:` input in a prompt at all, derived or not.
//
// The false-diagnostic risk that argument usually carries is small here in a way
// it is not for `log:`, and that is the second half of the case. A workflow that
// declares nothing `sensitive:` can never trip this (the check returns before
// looking at a single expression), so the only authors who see it are the ones
// who already told this system that some input of theirs is private.
//
// # And it refuses what it cannot decide
//
// `inputs[whicheverKey]` names no key statically. Where the workflow declares a
// sensitive input, "could not tell" is answered as a refusal rather than as
// silence, which is CLAUDE.md's fail-closed rule applied to a lint: a check that
// allows when it cannot decide will eventually allow the case it exists for.
// Again, only for a workflow that declared one.
//
// # One rule, three places
//
// The rule itself lives in [v1.CheckWaitPromptsAreAskable], which is what the
// submit boundary calls for a specification that never was a Flowfile. This file
// is that same rule with a line and a column attached; it does not re-decide
// anything, it asks the same function and positions the answer. A prompt that
// still holds a secret reference when it is evaluated renders as a refusal
// marker instead of a value; see [v1.PromptWithheldSecret].

// checkSensitivePrompt reports a `wait_for_signal:` whose `prompt:` reaches
// something this system will not put in front of an approver.
//
// Positioned per step by asking [v1.CheckWaitPromptsAreAskable] about one step
// at a time, so the message the author reads and the message a submitted
// specification is refused with are the same sentence; a second wording here
// would be the "same mistake in two voices" this package already avoids
// elsewhere.
func checkSensitivePrompt(wf *v1.Workflow) Diagnostics {
	return sensitivePromptInNodes(wf.GetSteps(), v1.SensitiveInputNames(wf))
}

// sensitivePromptInNodes reports every `wait_for_signal:` in a tree of steps whose
// `prompt:` the shared rule refuses.
//
// The tree comes from [v1.WalkNodes], the one traversal (#508), for the reason
// [sensitiveLogInNodes] gives: `inputs.<name>` means the same workflow input at any
// nesting depth, so one walk from the top covers the whole tree.
//
// A `call:` is not followed, because the traversal does not follow one. A callee is
// a different workflow with its own declared inputs, compiled and validated in its
// own right, and its own author is the one who should be told about its own prompt.
// The submit boundary does descend into an inlined callee, because by then there is
// no separate file left to have been validated.
func sensitivePromptInNodes(nodes []*v1.Node, sensitive map[string]bool) Diagnostics {
	var ds Diagnostics

	v1.WalkNodes(nodes, v1.Walk{
		Node: func(node *v1.Node) {
			if d, ok := sensitivePromptInStep(node, sensitive); ok {
				ds = append(ds, d)
			}
		},
	})

	return ds
}

// sensitivePromptInStep reports the diagnostic for one step when its prompt is
// one the shared rule refuses, and ok=false otherwise.
func sensitivePromptInStep(node *v1.Node, sensitive map[string]bool) (Diagnostic, bool) {
	signal := node.GetWait().GetSignal()
	if signal == nil || signal.GetPrompt() == nil {
		return Diagnostic{}, false
	}

	// The one node, wrapped so the shared walk sees exactly this step's prompt
	// and the sensitive names of the workflow it is written in. Wrapped rather
	// than reimplemented: two spellings of one rule is the divergence this repo
	// keeps paying for, and the rule here is the interesting part.
	err := v1.CheckWaitPromptsAreAskable(&v1.Workflow{
		DeclaredInputs: declarationsFor(sensitive),
		Steps:          []*v1.Node{{Id: node.GetId(), Kind: node.GetKind(), Vars: node.GetVars()}},
	})
	if err == nil {
		return Diagnostic{}, false
	}

	return Diagnostic{
		Step:    node.GetId(),
		Field:   "wait_for_signal.prompt",
		Code:    v1.DiagnosticCodeSensitiveInPrompt,
		Message: fmt.Sprintf("%v", err),
	}, true
}

// declarationsFor rebuilds the minimum declaration set the shared rule needs:
// the names, each marked sensitive.
//
// Only sensitive names, because that is the only property of a declaration the
// rule reads: an input this returns nothing for is one the rule has no opinion
// about, which is exactly its behavior against the real workflow. Handing it the
// whole `inputs:` block would work too and would tie this call to fields the
// rule does not consult.
func declarationsFor(sensitive map[string]bool) []*v1.InputDeclaration {
	if len(sensitive) == 0 {
		return nil
	}

	declarations := make([]*v1.InputDeclaration, 0, len(sensitive))
	for name := range sensitive {
		declarations = append(declarations, &v1.InputDeclaration{Name: name, Sensitive: true})
	}

	return declarations
}
