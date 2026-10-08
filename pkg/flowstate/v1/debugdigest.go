package flowstatev1

import "google.golang.org/protobuf/proto"

// WorkflowIRDigest is the content digest of workflow's deterministic encoding,
// as this build encodes it: the identity a debugger's source map is bound to,
// and what a durable run reports so a client can check that binding. It is not
// a canonical cross-language program digest, and is never compared with one.
//
// It leaves out what the control plane writes onto a program when it admits a
// run — [Workflow.ResolvedPlugins] and [Workflow.ResolvedTaskCapabilities],
// both overwritten rather than trusted from the caller — so the durable run
// and the client that compiled the same file name one program. Those fields
// pin which versions and tasks the admitting deployment had; they add, move
// or remove no step, so a source map computed from the file still names every
// site the run can hold at, on the lines it was written on.
//
// Both are cleared on every workflow in the call tree, not only the root:
// [ResolvePlugins] pins each callee against its own requirements. A tree
// nested past what [walkEmbeddedWorkflows] inspects is refused at submission,
// so what it leaves uncleared is never a run's program.
func WorkflowIRDigest(workflow *Workflow) string {
	if workflow != nil {
		workflow = proto.CloneOf(workflow)
		_ = walkEmbeddedWorkflows(workflow, 0, func(wf *Workflow) error {
			wf.ResolvedPlugins = nil
			wf.ResolvedTaskCapabilities = nil
			// Where a step is written is advisory and never part of the program,
			// so a source map bound to this digest still verifies a run whose
			// specification carries locations.
			clearSourceLocations(wf)

			return nil
		})
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(workflow)
	if err != nil {
		return ""
	}

	return ContentDigest(data)
}

// clearSourceLocations drops the advisory [Node.Source] from wf's steps, in
// place.
func clearSourceLocations(wf *Workflow) {
	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) { node.Source = nil }})
}

// WithoutSourceLocations returns a copy of wf with the advisory [Node.Source]
// cleared from every step of it and of any workflow it calls, for a comparison
// of what two specifications do: where a step is written is not part of that, so
// the same program compiled from another file, or after a comment moved a line,
// is the same program.
func WithoutSourceLocations(wf *Workflow) *Workflow {
	if wf == nil {
		return nil
	}
	wf = proto.CloneOf(wf)
	_ = walkEmbeddedWorkflows(wf, 0, func(embedded *Workflow) error {
		clearSourceLocations(embedded)

		return nil
	})

	return wf
}
