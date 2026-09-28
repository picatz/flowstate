package flowdebug

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Driver runs the debugger's command lines against any [Target]: the prompt's
// vocabulary for a session that has no prompt of its own — a durable run from
// `flow debug attach`, or a retained MCP session.
//
// It is a spelling, not a second session. Every line becomes one call on the
// target, and what comes back is the target's own answer; the driver keeps
// only the breakpoint set, because a target replaces the whole set at once and
// a line adds or removes one. Before a line changes the set, the driver adopts
// every breakpoint the target reports that it does not know — one an earlier
// `flow debug do` or an editor set — from the definition the target reports
// with it, so the set it sends keeps them.
type Driver struct {
	target Target

	breakpoints []*v1.DebugBreakpoint
	failureMode v1.DebugFailureMode

	// request and expected are the running line's [DoOptions].
	request  string
	expected uint64

	// sent is the request ids of the lines this driver has sent a target, the
	// latest [maxRememberedRequests] of them, oldest first. A line repeated
	// under one of them is a retry of a line whose answer was lost, so it goes
	// to the target again — which answers from its receipts — rather than
	// being judged stale here for a revision the first sending moved.
	sent []string

	// detached is set once a detach this driver sent was accepted: the
	// session is over, and a line that would change it is refused rather
	// than sent — a durable pause after a detach would attach the run anew.
	detached bool

	// Wait bounds how long a movement waits for the next stop. Zero waits
	// until ctx ends.
	Wait time.Duration
}

// NewDriver returns a driver over target. It leaves the target's failure mode
// as it finds it until a `catch` line names one.
func NewDriver(target Target) *Driver {
	return &Driver{target: target, failureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED}
}

// DoOptions carries what a caller that retries knows about one line.
type DoOptions struct {
	// RequestID is sent as the target request id of the call the line makes,
	// so a retry under the same id is answered from the target's receipts
	// instead of acting twice — a movement whose answer was lost after the
	// target accepted it. Empty mints a fresh id per call.
	RequestID string

	// ExpectedRevision refuses the line as stale unless the session is at
	// this revision. A movement and an inspection carry it to the target,
	// which judges it in the same step as the command — a movement after
	// answering a remembered request id — so neither is ever applied to a
	// stop the run has left. Any other line is checked against a read just
	// before it is sent: the contract carries no revision for a pause or a
	// breakpoint change, neither of which is bound to a stop. Zero skips
	// the check.
	ExpectedRevision uint64
}

// DriveResult is one line's answer: whichever of the typed answers the line
// produced, and a rendering of them for a person.
type DriveResult struct {
	Receipt     *v1.DebugReceipt
	Snapshot    *v1.DebugSnapshot
	Inspect     *v1.DebugInspectResponse
	Breakpoints []*v1.DebugBreakpointState
	Text        string

	// Unarmed is the state of the breakpoint the line itself set — `break`
	// or `log` — when the target took the set but refused that breakpoint,
	// its message saying why: a condition that does not compile, a step the
	// program never declares. Nil when the line set none, the target armed
	// it, or the set is pending and its verdict not yet known.
	Unarmed *v1.DebugBreakpointState
}

// maxRememberedRequests bounds [Driver]'s memory of the request ids it has
// sent: as many as a retained session keeps answers for.
const maxRememberedRequests = 64

// DriverHelp lists the lines a [Driver] understands.
const DriverHelp = `status, info                 where the run is, and why
step, s                      run to the next step anywhere, including inside this one
next, n                      run this step whole; stop at the next step at this level or above
finish, out                  run until the loop, parallel, switch, or call around this step is left
continue, c                  run to the next breakpoint, or the end
until <step>                 run to that step (an id or an address like pages[2]/page)
pause                        hold at the next step boundary
break <step> [hit <n>] [if <expr>]   stop there, when the count and condition allow
log <step> <message>         record {expr} holes at every arrival, without stopping
catch none|uncaught|all      stop where a step fails
delete <step>|log <step>     remove that breakpoint, or that logpoint
clear                        remove every breakpoint, whoever set it
breakpoints                  list breakpoints and their hit counts
inspect, p <expr>            evaluate a read-only CEL expression at this stop
expand <expr>                list a map's or list's children
scope                        list what this stop can name
backtrace, bt                the step and every container around it
detach                       clear breakpoints and let the run go on unattended
help                         this list`

// Do runs one line.
func (d *Driver) Do(ctx context.Context, line string) (*DriveResult, error) {
	return d.DoWith(ctx, line, DoOptions{})
}

// DoWith runs one line with a caller's request id and expected revision.
func (d *Driver) DoWith(ctx context.Context, line string, opts DoOptions) (*DriveResult, error) {
	line = strings.TrimSpace(line)
	if line == "" || IsComment(line) {
		return &DriveResult{}, nil
	}
	verb, rest := split(line)
	rest = strings.TrimSpace(rest)

	if d.detached && changesSession(verb) {
		return nil, errors.New("this session was detached; attach again to debug the run")
	}

	d.request, d.expected = opts.RequestID, opts.ExpectedRevision
	defer func() { d.request, d.expected = "", 0 }()
	retry := d.request != "" && slices.Contains(d.sent, d.request)
	if d.expected != 0 && !movement(verb) && !retry {
		if stale, err := d.staleAt(ctx); stale != nil || err != nil {
			return stale, err
		}
	}

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
		// The prompt's conditional form, refused for what it is: a typed
		// resume names a step and nothing more, so the condition cannot be
		// carried, and dropping it would release the run to the first
		// arrival. Read as a step id it would be refused for holding a
		// space, which names the wrong problem.
		if fields := strings.Fields(rest); len(fields) > 1 && fields[1] == "if" {
			return nil, fmt.Errorf("until %s if ...: a typed resume names a step and no condition; "+
				"set `break %s if <expr>` and `continue` for the same stop", fields[0], fields[0])
		}

		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, rest)
	case "detach":
		return d.move(ctx, v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH, "")

	case "pause":
		d.sending()
		receipt, err := d.target.Pause(ctx, d.request)
		if err != nil {
			return nil, err
		}
		result := &DriveResult{Receipt: receipt}
		if !Accepted(receipt) {
			result.Text = FormatReceipt(receipt)

			return result, nil
		}
		// From the revision before the receipt's: a run already held
		// answers a pause at its current revision without moving, and that
		// stop is the answer, not one to wait past.
		result.Snapshot, err = d.waitForStop(ctx, max(receipt.GetRevision(), 1)-1)
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

		if err := d.adopt(ctx, "log "+target); err != nil {
			return nil, err
		}

		return d.replace(ctx, append(d.withoutID("log "+target), &v1.DebugBreakpoint{
			Id: "log " + target, Step: target, LogMessage: strings.TrimSpace(message),
		}), d.failureMode, "log "+target)
	case "delete", "d":
		if rest == "" {
			return nil, errors.New("delete needs a step: delete <step>")
		}

		if err := d.adopt(ctx, rest); err != nil {
			return nil, err
		}

		return d.replace(ctx, d.removing(rest), d.failureMode, "")
	case "clear":
		return d.replace(ctx, nil, d.failureMode, "")
	case "catch":
		modes := map[string]v1.DebugFailureMode{
			"none": v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE, "uncaught": v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
			"all": v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL, "": v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
		}
		mode, ok := modes[rest]
		if !ok {
			return nil, fmt.Errorf("catch takes none, uncaught, or all, not %q", rest)
		}

		if err := d.adopt(ctx); err != nil {
			return nil, err
		}

		return d.replace(ctx, d.breakpoints, mode, "")
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

// sending records the running line's request id as sent, right before the
// call that carries it to the target: a line that failed before reaching the
// target — a usage error, a read that failed — is not a retry when repeated
// under its id, and is judged stale like any new line.
func (d *Driver) sending() {
	if d.request == "" || slices.Contains(d.sent, d.request) {
		return
	}
	d.sent = append(d.sent, d.request)
	if len(d.sent) > maxRememberedRequests {
		d.sent = d.sent[1:]
	}
}

// changesSession reports whether verb changes the session rather than reads
// it: a movement, a pause, or a change to its breakpoints.
func changesSession(verb string) bool {
	switch verb {
	case "pause", "break", "b", "log", "delete", "d", "clear", "catch":
		return true
	default:
		return movement(verb)
	}
}

// movement reports whether verb resumes the run, and so carries an expected
// revision to the target rather than having the driver check it.
func movement(verb string) bool {
	switch verb {
	case "step", "s", "next", "n", "finish", "fin", "out", "continue", "c", "until", "u", "detach":
		return true
	default:
		return false
	}
}

// staleAt answers a stale receipt when the session has left the revision the
// line was meant for, and nil when it has not.
func (d *Driver) staleAt(ctx context.Context) (*DriveResult, error) {
	current, err := d.target.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if current.GetRevision() == d.expected {
		return nil, nil
	}
	receipt := &v1.DebugReceipt{
		RequestId: d.request,
		Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
		Revision:  current.GetRevision(),
		Message:   fmt.Sprintf("the command was meant for revision %d, and the session is at %d", d.expected, current.GetRevision()),
	}

	return &DriveResult{Receipt: receipt, Snapshot: current, Text: FormatReceipt(receipt)}, nil
}

// revisionAt is the revision an inspection is asked at: the caller's
// expected one when it named one, so the target refuses the inspection if the
// run has left it since the check before the line ran, and otherwise the one
// just read.
func (d *Driver) revisionAt(snapshot *v1.DebugSnapshot) uint64 {
	return cmp.Or(d.expected, snapshot.GetRevision())
}

// staleOr answers an inspection the target refused as stale, against the
// revision the caller expected, as the check before the line would have: a
// STALE receipt with where the session is now. Any other error is returned.
func (d *Driver) staleOr(ctx context.Context, err error) (*DriveResult, error) {
	if d.expected != 0 && errors.Is(err, ErrStaleRevision) {
		if stale, readErr := d.staleAt(ctx); stale != nil || readErr != nil {
			return stale, readErr
		}
	}

	return nil, err
}

// Accepted reports whether a target took the command its receipt answers:
// applied, a duplicate of one it applied, or pending its next step boundary.
func Accepted(receipt *v1.DebugReceipt) bool {
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
	// A detach is never judged stale unless the caller pinned it: it asks
	// to let the run go wherever it has got to, and a stop that lands
	// between reading the revision and sending it must not leave the run
	// held by a session its client has left.
	expected := d.expected
	if expected == 0 && action != v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH {
		current, err := d.target.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		expected = current.GetRevision()
	}
	d.sending()
	receipt, err := d.target.Resume(ctx, &v1.DebugResumeRequest{
		RequestId:        cmp.Or(d.request, newRequestID()),
		Action:           action,
		Until:            until,
		ExpectedRevision: expected,
	})
	if err != nil {
		return nil, err
	}

	result := &DriveResult{Receipt: receipt}
	if action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH && Accepted(receipt) {
		d.detached = true
	}
	if !Accepted(receipt) || action == v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH {
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

// stillRunningRead bounds the read that reports a run still moving when a
// movement's wait ends: a remote target that stops answering must not hold the
// caller past the wait it asked for by more than this.
const stillRunningRead = 5 * time.Second

// waitForStop waits past revision for a hold or the end of the session.
func (d *Driver) waitForStop(ctx context.Context, revision uint64) (*v1.DebugSnapshot, error) {
	wait := ctx
	if d.Wait > 0 {
		var cancel context.CancelFunc
		wait, cancel = context.WithTimeout(ctx, d.Wait)
		defer cancel()
	}

	after := revision
	for {
		snapshot, err := d.target.WaitSnapshot(wait, after)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// Still running: say so rather than pretend a stop came. Only the
			// driver's own wait has ended — a caller's deadline is the caller's
			// error — so the read runs under the caller's context, bounded.
			read, cancel := context.WithTimeout(ctx, stillRunningRead)
			snapshot, err := d.target.Snapshot(read)
			cancel()
			if err != nil {
				return nil, fmt.Errorf("the run had not stopped when the wait ended, and reading where it is failed: %w", err)
			}

			return snapshot, nil
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

	if err := d.adopt(ctx, target); err != nil {
		return nil, err
	}

	return d.replace(ctx, append(d.withoutID(target), &v1.DebugBreakpoint{
		Id: target, Step: target, Condition: strings.TrimSpace(condition), HitCondition: hitText,
	}), d.failureMode, target)
}

// withoutID is the current set less the breakpoint whose id is id. A line
// replaces only the breakpoint it owns — `break build` the one under id
// `build`, `log build` the one under `log build` — so a breakpoint another
// client set on the same step under its own id is kept.
func (d *Driver) withoutID(id string) []*v1.DebugBreakpoint {
	return slices.DeleteFunc(slices.Clone(d.breakpoints), func(bp *v1.DebugBreakpoint) bool {
		return bp.GetId() == id
	})
}

// removing is the current set less what `delete name` names: the breakpoint
// whose id is name — `log build` for the logpoint `log build ...` set — or,
// when no id is name, every stopping breakpoint on the step name, which is
// what a person naming a step asks to remove.
func (d *Driver) removing(name string) []*v1.DebugBreakpoint {
	if slices.ContainsFunc(d.breakpoints, func(bp *v1.DebugBreakpoint) bool { return bp.GetId() == name }) {
		return d.withoutID(name)
	}

	return slices.DeleteFunc(slices.Clone(d.breakpoints), func(bp *v1.DebugBreakpoint) bool {
		return bp.GetStep() == name && bp.GetLogMessage() == ""
	})
}

// adopt adds to the driver's set every breakpoint the target holds that the
// driver does not know, from the definition the target reports with it, so the
// set the line sends keeps what another client installed. A target that
// reports a breakpoint without its definition — a server older than the field
// — cannot have it resent, so a line that would drop one refuses rather than
// drop it in silence, unless the line names it (named: the ids the line itself
// replaces or removes); `clear` discards every breakpoint either way.
//
// A breakpoint another client removes after this driver adopted it is resent
// by this driver's next line: the contract has no conditional replace.
func (d *Driver) adopt(ctx context.Context, named ...string) error {
	current, err := d.target.Snapshot(ctx)
	if err != nil {
		return err
	}
	var undefined []string
	for _, state := range current.GetBreakpoints() {
		id := state.GetId()
		if slices.ContainsFunc(d.breakpoints, func(bp *v1.DebugBreakpoint) bool { return bp.GetId() == id }) {
			continue
		}
		if definition := state.GetDefinition(); definition != nil {
			adopted := proto.CloneOf(definition)
			adopted.Id = id
			d.breakpoints = append(d.breakpoints, adopted)

			continue
		}
		if !slices.Contains(named, id) {
			undefined = append(undefined, id)
		}
	}
	if len(undefined) > 0 {
		return fmt.Errorf("this session holds breakpoints whose definitions the target does not report (%s), so "+
			"replacing the set would drop them; `clear` removes every breakpoint", strings.Join(undefined, ", "))
	}

	return nil
}

// replace sends set as the session's breakpoints. own is the id of the
// breakpoint the line itself sets, empty when it sets none, so the answer can
// say whether that one was armed.
func (d *Driver) replace(ctx context.Context, set []*v1.DebugBreakpoint, mode v1.DebugFailureMode, own string) (*DriveResult, error) {
	d.sending()
	response, err := d.target.ReplaceBreakpoints(ctx, &v1.DebugSetBreakpointsRequest{
		RequestId: cmp.Or(d.request, newRequestID()), Breakpoints: set, FailureMode: mode,
	})
	if err != nil {
		return nil, err
	}
	if !Accepted(response.GetReceipt()) && response.GetReceipt().GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSPECIFIED {
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

	result := &DriveResult{Receipt: response.GetReceipt(), Breakpoints: response.GetBreakpoints(), Snapshot: response.GetSnapshot(), Text: b.String()}
	// A pending set has no verdict yet: its states are the ones before it.
	if own != "" && response.GetReceipt().GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		if i := slices.IndexFunc(result.Breakpoints, func(state *v1.DebugBreakpointState) bool {
			return state.GetId() == own
		}); i >= 0 && !result.Breakpoints[i].GetVerified() {
			result.Unarmed = result.Breakpoints[i]
		}
	}

	return result, nil
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
		Revision: d.revisionAt(snapshot), Expression: expression, Children: children,
	})
	if err != nil {
		return d.staleOr(ctx, err)
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
	roots, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{Revision: d.revisionAt(snapshot)})
	if err != nil {
		return d.staleOr(ctx, err)
	}

	var b strings.Builder
	for _, group := range roots.GetChildren() {
		names, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{
			Revision: d.revisionAt(snapshot), Expression: group.GetValue().GetExpression(), Limit: MaxScopeNames,
		})
		if err != nil {
			return d.staleOr(ctx, err)
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
