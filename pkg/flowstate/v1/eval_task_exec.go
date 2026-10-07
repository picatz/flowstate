package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
)

// ExecTaskDef returns the exec task definition enforcing the given policy.
//
// Registering the result replaces the built-in exec task, which is how a
// deployment turns it on: `flow worker --exec-policy` loads a file with
// [ParseExecPolicy] and registers this over the denied default, the way
// [HTTPTaskDef] carries an egress policy. A nil policy is the built-in default
// — every invocation denied, with a message naming [ExecPolicyFlag].
func ExecTaskDef(policy *execpolicy.Policy) TaskDef {
	return TaskDef{
		Name:    "exec",
		Summary: "Run one program from the deployment's allowlist and return its exit code and output.",
		Inputs:  (&Task_Exec_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&Task_Exec_Outputs{}).ProtoReflect().Descriptor(),
		// Declared so the durable driver schedules this task on the activity that
		// carries the run's scope, and with it the attested identity an operator's
		// `identity.*` rules are evaluated against. The plain activity carries the
		// identity only for the task-shape check and the plugin context, not into
		// the task function: without this, an identity-scoped allow rule would
		// match on a laptop (where the rehearsal identity rides the scope) and
		// decline every tenant in production. The task evaluates nothing itself —
		// its inputs are resolved before the activity is scheduled either way.
		NeedsPrevOutputs: true,
		CheckLiteral:     checkExecLiteral,
		Fn:               taskFuncExec(policy),
	}
}

// taskFuncExec is the exec task: check the invocation against the policy, run
// what it admits, and report what happened.
//
// The policy is consulted before the inputs are even parsed when none is
// configured, so an unconfigured worker refuses with the one message that tells
// an operator what to do rather than first complaining about a field.
func taskFuncExec(policy *execpolicy.Policy) TaskFunc {
	return func(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
		if policy == nil {
			return nil, NewTaskError("exec", ErrorKindPolicyDenied, &execpolicy.DeniedError{
				Reason: execpolicy.ReasonNoPolicy,
				Detail: "this worker has no exec policy, so the exec task is denied; an operator enables it by " +
					"starting the worker with " + ExecPolicyFlag + " <file> (or setting " + ExecPolicyEnv +
					") naming the programs it may run",
			})
		}

		taskInputs := &Task_Exec_Inputs{}
		if err := populateProtoMessageFromValueMap(ctx, input, taskInputs, scope); err != nil {
			return nil, NewTaskError("exec", ErrorKindInvalidInput, err)
		}
		if err := Validate(taskInputs); err != nil {
			return nil, NewTaskError("exec", ErrorKindInvalidInput, err)
		}

		request := execpolicy.Request{
			Argv: taskInputs.GetArgv(),
			Dir:  taskInputs.GetDir(),
			Env:  taskInputs.GetEnv(),
		}
		// The run's attested identity, rendered from the one WorkloadIdentity the
		// scope carries: the same source the egress, secret-access and task-shape
		// policies read, so the surfaces agree about who is calling. Absent, the
		// zero identity, which an identity-scoped allow rule declines to match.
		if id := scope.GetIdentity(); id != nil {
			request.Identity = CallerOf(id)
		}

		command, err := policy.Check(ctx, request)
		if err != nil {
			return nil, execFailure(err)
		}

		result, err := command.Run(ctx)
		if err != nil {
			LoggerFrom(ctx).LogAttrs(ctx, slog.LevelWarn, "exec did not complete",
				slog.String("program", request.Argv[0]), slog.String("error", err.Error()))
			return nil, execFailure(err)
		}

		LoggerFrom(ctx).LogAttrs(ctx, slog.LevelInfo, "exec finished",
			slog.String("program", request.Argv[0]),
			slog.Int("exit_code", result.ExitCode),
			slog.Int64("duration_ms", result.Duration.Milliseconds()))

		return nodeOutputsFromProtoMessage(&Task_Exec_Outputs{
			ExitCode:          int32(result.ExitCode),
			Stdout:            result.Stdout,
			Stderr:            result.Stderr,
			StdoutTruncated:   result.StdoutTruncated,
			StderrTruncated:   result.StderrTruncated,
			CaptureIncomplete: result.CaptureIncomplete,
			Signal:            result.Signal,
			DurationMs:        result.Duration.Milliseconds(),
			Outcome:           string(result.Outcome),
		})
	}
}

// execFailure classifies why an invocation did not produce outputs.
//
// A denial is permanent policy; a program that never started cannot have taken
// effect, so retrying it is safe and it is the retryable upstream kind; the
// policy's own time bound is a limit the same invocation would hit again, so it
// is permanent. A caller's deadline or cancellation is returned as the context
// error it carries and no more, so the drivers classify it exactly as they do a
// step cut off by its own `timeout:` — the program may have run, and nothing
// here knows more than the context does.
func execFailure(err error) error {
	if _, ok := errors.AsType[*execpolicy.DeniedError](err); ok {
		return NewTaskError("exec", ErrorKindPolicyDenied, err)
	}
	if runErr, ok := errors.AsType[*execpolicy.RunError](err); ok {
		switch {
		case runErr.Outcome == execpolicy.OutcomeDidNotStart:
			return NewTaskError("exec", ErrorKindUpstream, err)
		case runErr.Outcome == execpolicy.OutcomeTimedOut && runErr.PolicyTimeout:
			return NewTaskError("exec", ErrorKindLimitExceeded, err)
		default:
			return err
		}
	}
	return err
}

// isAbsoluteLiteral reports whether a literal working directory is absolute.
//
// The literal is checked wherever the Flowfile is authored, and the directory
// it names is on the worker, which is usually a different operating system. A
// leading slash is a POSIX absolute path whatever the author's OS says
// (filepath.IsAbs is false for "/srv/work" on Windows), so it is accepted
// here; the host's own notion (native) is accepted too. The worker's exec
// policy applies the worker's native rules to the resolved value.
func isAbsoluteLiteral(dir string, native func(string) bool) bool {
	return strings.HasPrefix(dir, "/") || native(dir)
}

// checkExecLiteral is what the exec task can say about a literal input before
// anything runs, in every deployment alike: a program is a name and never a
// path, and a working directory is an absolute path. Which names exist and which
// directories are allowed are the deployment's policy, so they are not judged
// here (see [checkHTTPLiteral] for why that line sits where it does).
func checkExecLiteral(input string, value *Value) error {
	switch input {
	case "argv":
		list := value.GetLiteral().GetListValue().GetValues()
		if len(list) == 0 {
			return nil
		}
		name := list[0].GetStringValue()
		if strings.ContainsAny(name, `/\`) {
			return fmt.Errorf("argv[0] %q is a path, and the exec task refuses paths: a program is a name the "+
				"deployment's exec policy lists (for example `argv: [go, test]`), never a location a workflow chooses", name)
		}
	case "dir":
		dir := value.GetLiteral().GetStringValue()
		if dir != "" && !isAbsoluteLiteral(dir, filepath.IsAbs) {
			return fmt.Errorf("dir %q is not an absolute path; the exec task never uses the worker's own working "+
				"directory, so name one under a root the deployment's exec policy allows", dir)
		}
	}
	return nil
}
