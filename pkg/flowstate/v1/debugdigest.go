package flowstatev1

import "google.golang.org/protobuf/proto"

// WorkflowIRDigest is the content digest of workflow's deterministic encoding,
// as this build encodes it: the identity a debugger's source map is bound to,
// and what a durable run reports so a client can check that binding. It is not
// a canonical cross-language program digest, and is never compared with one.
func WorkflowIRDigest(workflow *Workflow) string {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(workflow)
	if err != nil {
		return ""
	}

	return ContentDigest(data)
}
