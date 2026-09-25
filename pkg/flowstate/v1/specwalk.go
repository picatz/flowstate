package flowstatev1

import (
	"errors"
	"iter"
)

// This file is the range-over-func spelling of the two whole-specification
// traversals: every workflow a specification reaches, and every step in them.
//
// # Why an iterator rather than another callback
//
// [walkEmbeddedWorkflows] and [WalkWorkflow] both take a callback, and a
// callback cannot say "stop, I have my answer". Every caller that needed to
// therefore invented its own way to say it, and each paid differently:
//
//   - [DeclaresSensitiveValues] returned a sentinel error from its visit and
//     filtered it back out with errors.Is. That did stop the walk — a failed
//     visit is the one thing walkEmbeddedWorkflows ends on — at the price of
//     spending the error channel on a non-error, so "found one" travelled as a
//     failure through a signature that also reports real failures.
//   - [CheckRequiredSecretInputs] captured a `refusal` variable and opened its
//     callback with `if refusal != nil { return }`. That one really did keep
//     running: the guard skips the remaining work but still visits every
//     remaining step and enters every remaining callee, because the visit it
//     sits in goes on returning nil.
//   - It and [RequiredTaskNames] also wrote the same
//     walkEmbeddedWorkflows-plus-WalkWorkflow pair by hand, two copies of a
//     compound walk that has one meaning.
//
// A `for ... range` loop over an [iter.Seq2] gets all three back: `break` and
// `return` stop the traversal, "found it" is an ordinary value, and the pair is
// written once. That is the whole of what these two functions add — neither
// introduces a traversal, both delegate to the existing ones, so the
// completeness guarantee walk.go describes and the bounds
// walkEmbeddedWorkflows holds are unchanged.
//
// # Which walkers should *not* become a range loop
//
// A visit that fails is not the same as a consumer that stops, and only the
// callback form carries the first one well. walkEmbeddedWorkflowNodes wraps an
// error its visit returns with `step %q calls workflow %q`, so a refusal raised
// inside a callee reaches the author naming the `call:` that reached it. A
// range loop's body returns to its own caller instead, past that edge, and the
// call path is gone. Converting [ResolvePlugins] drops exactly that sentence,
// which is what TestResolvePluginsWalksTheCallTree fails on.
//
// So the split is by what the caller does with a workflow, not by which
// spelling is newer. [CheckDeclarationTypes], [ResolvePlugins] and
// [PinnedPlugins] refuse *in place* and want the call path added on the way
// out; they keep the callback. The three below ask a question and stop when
// they have the answer; they range.
//
// # The error convention
//
// Both yield [iter.Seq2] pairs where a non-nil error is terminal and final: it
// arrives with a nil first element, it is the last pair yielded, and it means
// the specification could not be walked to the end (today, only nesting past
// [maxWorkflowScanDepth], which fails closed). A caller therefore checks the
// error on every iteration and returns.

// errStopWalk carries a consumer's `break` back out through
// [walkEmbeddedWorkflows], whose only way to end a walk early is a visit that
// fails. It is compared with [errors.Is] because the callee edge wraps what it
// is handed, and it never escapes this file: the two iterators below drop it
// rather than yield it, so no caller can mistake stopping for failing.
var errStopWalk = errors.New("specification walk stopped by its consumer")

// specWorkflows yields wf and every workflow embedded in it by a `call:`, in
// the order and under the bound [walkEmbeddedWorkflows] defines.
//
// This is the whole-specification unit: a callee is compiled into its caller
// (see flowfile/call.go), so a check that stopped at the call would inspect
// only the half of a specification spelled at the top.
func specWorkflows(wf *Workflow) iter.Seq2[*Workflow, error] {
	return func(yield func(*Workflow, error) bool) {
		err := walkEmbeddedWorkflows(wf, 0, func(current *Workflow) error {
			if !yield(current, nil) {
				return errStopWalk
			}

			return nil
		})
		if err != nil && !errors.Is(err, errStopWalk) {
			yield(nil, err)
		}
	}
}

// specNodes yields every step of every workflow [specWorkflows] reaches:
// nested control flow and compensations, because [WalkWorkflow] delivers them,
// and inlined callees, because specWorkflows does. A step is yielded exactly
// once — walkEmbeddedWorkflows descends a body only to find the `call:` edges
// under it, and WalkWorkflow is what visits the steps of each workflow it
// finds.
//
// Stopping is honored at two different grains, which is worth stating plainly
// rather than leaving a caller to measure. Breaking out of a range over this
// stops the callee walk immediately, so no further workflow is entered; it
// does not stop [WalkWorkflow] partway through the workflow already in hand,
// because that traversal has no early exit of its own and giving it one would
// change the shape every other caller shares (walk.go, "why there is exactly
// one"). The remaining steps of that one workflow are therefore still visited,
// and discarded here rather than delivered.
func specNodes(wf *Workflow) iter.Seq2[*Node, error] {
	return func(yield func(*Node, error) bool) {
		for current, err := range specWorkflows(wf) {
			if err != nil {
				yield(nil, err)

				return
			}

			// Latched rather than re-tested: calling yield again after it has
			// returned false is a runtime panic, and WalkWorkflow keeps
			// delivering nodes after the consumer has stopped.
			stopped := false
			WalkWorkflow(current, Walk{Node: func(node *Node) {
				if stopped {
					return
				}
				if !yield(node, nil) {
					stopped = true
				}
			}})
			if stopped {
				return
			}
		}
	}
}
