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
func WorkflowIRDigest(workflow *Workflow) string {
	if workflow.GetResolvedPlugins() != nil || workflow.GetResolvedTaskCapabilities() != nil {
		workflow = proto.CloneOf(workflow)
		workflow.ResolvedPlugins = nil
		workflow.ResolvedTaskCapabilities = nil
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(workflow)
	if err != nil {
		return ""
	}

	return ContentDigest(data)
}
