package flowstatev1

import "fmt"

// NodeKind names a step's kind for a person reading a runtime surface: the
// word the file spells, plus the one detail that identifies which one it is.
//
// Kept beside Node because the debugger prompt, its recorded backtrace, DAP,
// MCP, and the terminal panes all need the same spelling. A renderer-specific
// switch would let one front call a step something another front does not.
func NodeKind(node *Node) string {
	switch kind := node.GetKind().(type) {
	case *Node_Task:
		return fmt.Sprintf("task %q", kind.Task.GetName())
	case *Node_Value:
		return "value"
	case *Node_Wait:
		if signal := kind.Wait.GetSignal(); signal != nil {
			return fmt.Sprintf("wait_for_signal %q", signal.GetName())
		}
		if batch := kind.Wait.GetSignalBatch(); batch != nil {
			return fmt.Sprintf("wait_for_signals %q", batch.GetName())
		}

		return "wait"
	case *Node_ForEach:
		return "for_each"
	case *Node_Parallel:
		return "parallel"
	case *Node_Switch:
		return "switch"
	case *Node_Call:
		return fmt.Sprintf("call %q", kind.Call.GetWorkflow())
	default:
		return "step"
	}
}
