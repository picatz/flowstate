package flowstatev1

// MergedStepNodes returns the steps whose outputs become visible to the steps
// following nodes: each node itself, plus, for a parallel block or a switch, the
// steps nested inside it whose outputs execution merges out into the enclosing
// namespace.
//
// This is the one spelling of that rule. The validator's scope walk and its
// parallel id-collision check read it, and so do both drivers' parallel joins
// ([MergedStepIDs]), so a file the checker admits cannot name an id the
// runtime then fails to find (#1425).
//
// A loop contributes only itself, because its body's outputs are reported
// through its `results` output rather than merged. A nested parallel block
// contributes its branches' steps and a switch its case and default bodies',
// each walked the same way, so the rule holds at any depth.
//
// The nodes rather than their ids, because a scope that holds only names cannot
// answer what a name's outputs are (#323).
func MergedStepNodes(nodes []*Node) []*Node {
	var out []*Node

	for _, node := range nodes {
		out = append(out, node)

		switch kind := node.GetKind().(type) {
		case *Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				out = append(out, MergedStepNodes(branch.GetSteps())...)
			}
		case *Node_Switch:
			for _, body := range SwitchBodies(kind.Switch) {
				out = append(out, MergedStepNodes(body)...)
			}
		}
	}

	return out
}

// MergedStepIDs is the ids of [MergedStepNodes], for a join that copies outputs
// by name.
func MergedStepIDs(nodes []*Node) []string {
	merged := MergedStepNodes(nodes)
	ids := make([]string, len(merged))
	for i, node := range merged {
		ids[i] = node.GetId()
	}

	return ids
}
