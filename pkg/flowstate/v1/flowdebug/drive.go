package flowdebug

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Driver runs the debugger's command lines against any [Target]: the prompt's
// vocabulary for a session that has no prompt of its own — a durable run from
// `flow debug attach`, or a retained MCP session.
//
// It is a spelling, not a second session. Every line becomes one call on the
// target, and what comes back is the target's own answer; the driver keeps only
// the breakpoint set it last sent, because a target replaces the whole set at
// once and a line adds or removes one.
type Driver struct {
	target Target

	breakpoints []*v1.DebugBreakpoint
	failureMode v1.DebugFailureMode

	// Wait bounds how long a movement waits for the next stop. Zero waits
	// until ctx ends.
	Wait time.Duration
}

// NewDriver returns a driver over target.
func NewDriver(target Target) *Driver {
	return &Driver{target: target, failureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE}
}

// DriveResult is one line's answer: whichever of the typed answers the line
// produced, and a rendering of them for a person.
type DriveResult struct {
	Receipt     *v1.DebugReceipt
	Snapshot    *v1.DebugSnapshot
	Inspect     *v1.DebugInspectResponse
	Breakpoints []*v1.DebugBreakpointState
	Text        string
}

// DriverHelp lists the lines a [Driver] understands.
const DriverHelp = `status, info                 where the run is, and why
step, s                      run to the next step anywhere, including inside this one
next, n                      run this step whole; stop at the next step at this level or above
finish, out                  run until the enclosing iteration, branch, arm, or call is left
continue, c                  run to the next breakpoint, or the end
until <step>                 run to that step (an id or an address like pages[2]/page)
pause                        hold at the next step boundary
break <step> [hit <n>] [if <expr>]   stop there, when the count and condition allow
log <step> <message>         record {expr} holes at every arrival, without stopping
catch none|uncaught|all      stop where a step fails
delete <step>                remove that breakpoint
breakpoints                  list breakpoints and their hit counts
inspect, p <expr>            evaluate a read-only CEL expression at this stop
expand <expr>                list a map's or list's children
scope                        list what this stop can name
backtrace, bt                the step and every container around it
detach                       clear breakpoints and let the run go on unattended
help                         this list`

// Do runs one line.
func (d *Driver) Do(ctx context.Context, line string) (*DriveResult, error) {
	line = strings.TrimSpace(line)
	if line == "" || IsComment(line) {
		return &DriveResult{}, nil
	}
	verb, rest := split(line)
	rest = strings.TrimSpace(rest)

	switch verb {
	case "status", "info":
		snapshot, err := d.target.Snapshot(ctx)
		if err != nil {
			return nil, err
		}

		return &DriveResult{Snapshot: snapshot, Text: FormatSnapshot(snapshot)}, nil

	case "step", "s":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
	case "next", "n":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER, "")
	case "finish", "fin", "out":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT, "")
	case "continue", "c":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	case "until", "u":
		if rest == "" {
			return nil, errors.New("until needs a step: until <step>")
		}

		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, rest)
	case "detach":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH, "")

	case "pause":
		receipt, err := d.target.Pause(ctx, "")
		if err != nil {
			return nil, err
		}
		result := &DriveResult{Receipt: receipt}
		if !accepted(receipt) {
			result.Text = FormatReceipt(receipt)

			return result, nil
		}
		result.Snapshot, err = d.waitForStop(ctx, receipt.GetRevision())
		if err != nil {
			return nil, err
		}
		result.Text = FormatSnapshot(result.Snapshot)

		return result, nil

	case "break", "b":
		return d.addBreakpoint(ctx, rest)
	case "log":
		target, message := cutWord(rest)
		if target == "" || strings.TrimSpace(message) == "" {
			return nil, errors.New("log needs a step and a message: log <step> <message>")
		}

		return d.replace(ctx, append(d.without(target, true), &v1.DebugBreakpoint{
			Id: "log " + target, Step: target, LogMessage: strings.TrimSpace(message),
		}), d.failureMode)
	case "delete", "d":
		if rest == "" {
			return nil, errors.New("delete needs a step: delete <step>")
		}

		return d.replace(ctx, d.without(rest, false), d.failureMode)
	case "catch":
		modes := map[string]v1.DebugFailureMode{
			"none": v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE, "uncaught": v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
			"all": v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL, "": v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
		}
		mode, ok := modes[rest]
		if !ok {
			return nil, fmt.Errorf("catch takes none, uncaught, or all, not %q", rest)
		}

		return d.replace(ctx, d.breakpoints, mode)
	case "breakpoints":
		snapshot, err := d.target.Snapshot(ctx)
		if err != nil {
			return nil, err
		}

		return &DriveResult{Snapshot: snapshot, Breakpoints: snapshot.GetBreakpoints(), Text: d.formatBreakpoints(snapshot)}, nil

	case "inspect", "p", "print":
		if rest == "" {
			return nil, errors.New("inspect needs an expression: inspect steps.build.artifact")
		}

		return d.inspect(ctx, rest, false)
	case "expand":
		if rest == "" {
			return nil, errors.New("expand needs an expression: expand steps.build")
		}

		return d.inspect(ctx, rest, true)
	case "scope":
		return d.scope(ctx)

	case "backtrace", "bt":
		snapshot, err := d.target.Snapshot(ctx)
		if err != nil {
			return nil, err
		}

		return &DriveResult{Snapshot: snapshot, Text: formatFrames(snapshot)}, nil

	case "help", "h", "?":
		return &DriveResult{Text: DriverHelp + "\n"}, nil

	default:
		return nil, fmt.Errorf("unknown command %q — try `help`", verb)
	}
}

func accepted(receipt *v1.DebugReceipt) bool {
	switch receipt.GetStatus() {
	case v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE,
		v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING:
		return true
	default:
		return false
	}
}

// move resumes and waits for the run's next stop or end.
func (d *Driver) move(ctx context.Context, action v1.DebugResumeAction, until string) (*DriveResult, error) {
	current, err := d.target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	receipt, err := d.target.Resume(ctx, &v1.DebugResumeRequest{
		RequestId:        newRequestID(),
		Action:           action,
		Until:            until,
		ExpectedRevision: current.GetRevision(),
	})
	if err != nil {
		return nil, err
	}

	result := &DriveResult{Receipt: receipt}
	if !accepted(receipt) || action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH {
		result.Text = FormatReceipt(receipt)

		return result, nil
	}

	result.Snapshot, err = d.waitForStop(ctx, receipt.GetRevision())
	if err != nil {
		return nil, err
	}
	result.Text = FormatSnapshot(result.Snapshot)

	return result, nil
}

// waitForStop waits past revision for a hold or the end of the session.
func (d *Driver) waitForStop(ctx context.Context, revision uint64) (*v1.DebugSnapshot, error) {
	if d.Wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Wait)
		defer cancel()
	}

	after := revision
	for {
		snapshot, err := d.target.WaitSnapshot(ctx, after)
		if errors.Is(err, context.DeadlineExceeded) {
			// Still running: say so rather than pretend a stop came.
			return d.target.Snapshot(context.WithoutCancel(ctx))
		}
		if err != nil {
			return nil, err
		}
		if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD || terminal(snapshot.GetState()) {
			return snapshot, nil
		}
		after = snapshot.GetRevision()
	}
}

func (d *Driver) addBreakpoint(ctx context.Context, rest string) (*DriveResult, error) {
	rest, hitText, err := cutHitClause(rest)
	if err != nil {
		return nil, err
	}
	target, condition, _, err := splitCondition(rest, grammarBreak)
	if err != nil {
		return nil, err
	}
	if target == "" {
		return nil, errors.New(usageBreak)
	}

	return d.replace(ctx, append(d.without(target, false), &v1.DebugBreakpoint{
		Id: target, Step: target, Condition: strings.TrimSpace(condition), HitCondition: hitText,
	}), d.failureMode)
}

// without is the current set less the breakpoint on target: the stopping one,
// or the logpoint when logs is set.
func (d *Driver) without(target string, logs bool) []*v1.DebugBreakpoint {
	return slices.DeleteFunc(slices.Clone(d.breakpoints), func(bp *v1.DebugBreakpoint) bool {
		return bp.GetStep() == target && (bp.GetLogMessage() != "") == logs
	})
}

func (d *Driver) replace(ctx context.Context, set []*v1.DebugBreakpoint, mode v1.DebugFailureMode) (*DriveResult, error) {
	response, err := d.target.ReplaceBreakpoints(ctx, &v1.DebugSetBreakpointsRequest{
		RequestId: newRequestID(), Breakpoints: set, FailureMode: mode,
	})
	if err != nil {
		return nil, err
	}
	if !accepted(response.GetReceipt()) && response.GetReceipt().GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSPECIFIED {
		return &DriveResult{Receipt: response.GetReceipt(), Text: FormatReceipt(response.GetReceipt())}, nil
	}
	d.breakpoints, d.failureMode = set, mode

	var b strings.Builder
	for i, state := range response.GetBreakpoints() {
		name := state.GetId()
		if i < len(set) {
			name = set[i].GetStep()
		}
		if state.GetVerified() {
			fmt.Fprintf(&b, "breakpoint at %s\n", name)
		} else {
			fmt.Fprintf(&b, "not armed: %s: %s\n", name, state.GetMessage())
		}
	}
	if b.Len() == 0 {
		b.WriteString("no breakpoints\n")
	}

	return &DriveResult{Receipt: response.GetReceipt(), Breakpoints: response.GetBreakpoints(), Snapshot: response.GetSnapshot(), Text: b.String()}, nil
}

func (d *Driver) formatBreakpoints(snapshot *v1.DebugSnapshot) string {
	states := snapshot.GetBreakpoints()
	if len(states) == 0 && len(d.breakpoints) == 0 {
		return "no breakpoints\n"
	}

	var b strings.Builder
	for _, state := range states {
		name := state.GetId()
		switch {
		case state.GetVerified():
			fmt.Fprintf(&b, "%s  hits %d\n", name, state.GetHits())
		default:
			fmt.Fprintf(&b, "%s  not armed: %s\n", name, state.GetMessage())
		}
		if state.GetLastError() != "" {
			fmt.Fprintf(&b, "  last condition error: %s\n", state.GetLastError())
		}
	}

	return b.String()
}

func (d *Driver) inspect(ctx context.Context, expression string, children bool) (*DriveResult, error) {
	snapshot, err := d.target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	answer, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{
		Revision: snapshot.GetRevision(), Expression: expression, Children: children,
	})
	if err != nil {
		return nil, err
	}

	result := &DriveResult{Inspect: answer}
	switch {
	case answer.GetError() != "":
		result.Text = answer.GetError() + "\n"
	case children:
		var b strings.Builder
		for _, child := range answer.GetChildren() {
			fmt.Fprintf(&b, "%s  %s  %s\n", child.GetName(), child.GetValue().GetType(), child.GetValue().GetRendered())
		}
		if more := int(answer.GetTotal()) - len(answer.GetChildren()); more > 0 {
			fmt.Fprintf(&b, "… and %d more\n", more)
		}
		if b.Len() == 0 {
			fmt.Fprintf(&b, "%s has no children\n", expression)
		}
		result.Text = b.String()
	default:
		result.Text = answer.GetValue().GetRendered() + "\n"
	}

	return result, nil
}

func (d *Driver) scope(ctx context.Context) (*DriveResult, error) {
	snapshot, err := d.target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	roots, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{Revision: snapshot.GetRevision()})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, group := range roots.GetChildren() {
		names, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{
			Revision: snapshot.GetRevision(), Expression: group.GetValue().GetExpression(), Limit: MaxScopeNames,
		})
		if err != nil {
			return nil, err
		}
		listed := make([]string, 0, len(names.GetChildren()))
		for _, name := range names.GetChildren() {
			listed = append(listed, name.GetName())
		}
		line := strings.Join(listed, ", ")
		if more := int(names.GetTotal()) - len(listed); more > 0 {
			line += fmt.Sprintf(" … and %d more", more)
		}
		fmt.Fprintf(&b, "%s: %s\n", group.GetName(), line)
	}
	if b.Len() == 0 {
		b.WriteString("nothing is in scope\n")
	}

	return &DriveResult{Inspect: roots, Text: b.String()}, nil
}

// FormatReceipt renders a command's receipt for a person.
func FormatReceipt(receipt *v1.DebugReceipt) string {
	status := strings.ToLower(strings.TrimPrefix(receipt.GetStatus().String(), "DEBUG_COMMAND_STATUS_"))
	if receipt.GetMessage() == "" {
		return status + "\n"
	}

	return status + ": " + receipt.GetMessage() + "\n"
}

// FormatSnapshot renders a snapshot for a person: the state, where, why, and
// the frames.
func FormatSnapshot(snapshot *v1.DebugSnapshot) string {
	var b strings.Builder
	state := strings.ToLower(strings.TrimPrefix(snapshot.GetState().String(), "DEBUG_RUN_STATE_"))
	state = strings.ReplaceAll(state, "_", " ")

	switch snapshot.GetState() {
	case v1.DebugRunState_DEBUG_RUN_STATE_HELD:
		reason := strings.ToLower(strings.TrimPrefix(snapshot.GetReason().String(), "DEBUG_STOP_REASON_"))
		fmt.Fprintf(&b, "held at %s (%s) — %s", snapshot.GetOccurrence().GetAddress(),
			snapshot.GetOccurrence().GetSite().GetKind(), reason)
		if ids := snapshot.GetBreakpointIds(); len(ids) > 0 {
			fmt.Fprintf(&b, " %s", strings.Join(ids, ", "))
		}
		fmt.Fprintf(&b, ", revision %d\n", snapshot.GetRevision())
		if failure := snapshot.GetFailure(); failure != "" {
			fmt.Fprintf(&b, "  failed: %s\n", failure)
		}
		b.WriteString(formatFrames(snapshot))
	default:
		fmt.Fprintf(&b, "%s", state)
		if address := snapshot.GetOccurrence().GetAddress(); address != "" && !terminal(snapshot.GetState()) {
			fmt.Fprintf(&b, ", last at %s", address)
		}
		fmt.Fprintf(&b, ", revision %d\n", snapshot.GetRevision())
	}
	if message := snapshot.GetMessage(); message != "" {
		fmt.Fprintf(&b, "  %s\n", message)
	}
	if session := snapshot.GetSession(); session.GetSessionId() != "" {
		fmt.Fprintf(&b, "  session %s", session.GetSessionId())
		if expires := session.GetLeaseExpiresAt(); expires != nil {
			fmt.Fprintf(&b, ", lease until %s", expires.AsTime().Format(time.RFC3339))
		}
		b.WriteString("\n")
	}

	return b.String()
}

func formatFrames(snapshot *v1.DebugSnapshot) string {
	var b strings.Builder
	for _, frame := range snapshot.GetFrames() {
		fmt.Fprintf(&b, "  #%d %s", frame.GetId(), frame.GetLabel())
		if source := frame.GetSource(); source.GetRange() != nil {
			fmt.Fprintf(&b, "  line %d", source.GetRange().GetStartLine())
		}
		b.WriteString("\n")
	}

	return b.String()
}

// SourceName is a short name for a source document, for rendering.
func SourceName(uri string) string {
	return filepath.Base(strings.TrimPrefix(uri, "file://"))
}
