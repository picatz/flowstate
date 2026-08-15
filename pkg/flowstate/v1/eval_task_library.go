package flowstatev1

// This file, eval_task_log.go, eval_task_http_def.go, eval_task_http_run.go,
// eval_task_http.go and eval_task_http_check.go are the built-in task
// *implementations* — the one concern in this package that uses the platform
// rather than defining it, and the reason the schema package imports
// pkg/flowstate/v1/netpolicy (see imports_test.go, which pins that set).
//
// They were split out of a single 1,510-line eval_task_library.go so the seam a
// future relocation has to cut is a file boundary rather than a line range
// (#406). What did *not* move with them is in protoliterals.go: the generic
// literal-to-proto plumbing every task shares, which belongs to the schema
// layer and stays.
//
// Relocating them is not the mechanical move it looks like, and the reason is
// worth reading before trying: a package above this one cannot register into
// [DefaultRegistry] unless something imports it, while this package's own
// exported helpers — [TaskNeedsAuthority], [AcceptsNestedSecret],
// [MustBeExpression], [CheckLiteralInput] — answer questions *about* the
// built-in tasks by looking them up there. Each of them answers no, none, or
// nil for a task it cannot find, which is the safe answer for a task this build
// genuinely does not ship and the wrong one for a task whose registration is a
// blank import somebody forgot. [TaskNeedsAuthority] returning false is the
// sharp end: it routes a step's `bearer:` and `credential:` away from the
// identity-aware activity entry point. So the move needs the lookups to leave
// with the tasks, not just the code that runs them.

// builtinTasks returns the definitions of the tasks Flowstate ships with.
//
// Each definition declares how the engine must treat the task's inputs, so no
// part of the engine needs to know a task's name to execute it correctly.
func builtinTasks() []TaskDef {
	return []TaskDef{
		{
			Name:    "log",
			Summary: "Emit a message for a person to read.",
			Inputs:  (&Task_Log_Inputs{}).ProtoReflect().Descriptor(),
			Outputs: (&Task_Log_Outputs{}).ProtoReflect().Descriptor(),
			Fn:      taskFuncLog,
		},
		// `outputs` expressions reference the response (status_code, body,
		// headers), which exists only after the request completes, so the http
		// task evaluates them itself rather than the workflow resolving them.
		HTTPTaskDef(defaultEgressPolicy()),
	}
}
