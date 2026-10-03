package flowdebug

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Stepping backward by running again.
//
// A stubbed local run is deterministic: the virtual clock, the injected
// scheduler and the stubs make the same commands reach the same stops every
// time (see the package doc, "Commands are a stream"). So "the previous stop"
// does not need history or a journal of state; it needs the run started again
// and the recorded commands replayed up to that stop. [Reversible] does that
// behind the ordinary [Target], so every front that can drive a Target can
// offer it by calling [Reversible.Back].
//
// The wrapper's contract is deliberately narrow, because a rewind is a claim
// about the past that nothing else can check:
//
//   - It never presents a different run as the earlier one. Each stop it
//     showed is fingerprinted (address, state, reason, the observations the
//     stop carried), and the replay must reproduce every fingerprint on the
//     way. A mismatch answers "diverged" and the session stays where it was.
//   - The replay is built and verified before the current run is touched, so
//     a refusal or a divergence loses nothing.
//   - Revisions never go backward. The rewound session counts from one, so
//     its revisions are shifted to continue above everything already shown,
//     and a command fenced to a revision from before the rewind is stale.
//   - Re-execution repeats effects. A host opts in by constructing one, and
//     does so only for a run whose effects it has stubbed; reverse never
//     undoes anything, it re-does it.

// MaxReversibleMoves bounds the movement commands one [Reversible] will
// re-execute to rewind. A session past it answers that the rewind is
// unavailable rather than replaying without limit.
const MaxReversibleMoves = 256

// maxReversibleLog bounds the commands a [Reversible] retains for replay:
// movements and breakpoint replacements together.
const maxReversibleLog = 4 * MaxReversibleMoves

// DefaultReplayTimeout bounds how long one rewind may take, from the
// restart to the last replayed stop. A replay that cannot finish in that time
// is refused as unavailable, and the run being debugged is untouched.
const DefaultReplayTimeout = time.Minute

// ReversibleOption configures [NewReversible].
type ReversibleOption func(*Reversible)

// WithReplayTimeout bounds one rewind. Zero or negative keeps
// [DefaultReplayTimeout].
func WithReplayTimeout(d time.Duration) ReversibleOption {
	return func(r *Reversible) {
		if d > 0 {
			r.timeout = d
		}
	}
}

// Run is one execution of a program under its own controlled [Session].
type Run struct {
	// Session is the run's session, created [Options.Controlled], with the
	// program already running under it.
	Session *Session

	// Stop ends the run and releases everything it holds, and returns when
	// it has. It is called at most once, and never for a run the host still
	// owns. It must cancel the run before releasing its session: closing a
	// session first detaches it, and a detached run carries on to the end,
	// repeating the effects of every step it had not yet reached.
	Stop func()
}

// Launcher starts the program again, from the beginning, under a new session.
//
// The run's lifetime must not be bound to ctx: a rewind launches under the
// context of the request that asked for it, which ends when the request does,
// and the rewound run has to outlive that. A launcher derives the run's own
// context and ends it in [Run.Stop].
//
// It must be deterministic: the same options, inputs, stubs and virtual
// clock every time, because a rewind is only as good as its reproduction. A
// launcher that cannot promise that must not be wrapped; a difference is
// caught and reported, but it is the host's to prevent.
type Launcher func(ctx context.Context) (*Run, error)

// errNoLauncher is what [NewReversible] refuses a nil [Launcher] with.
var errNoLauncher = errors.New("flowdebug: a Reversible needs a Launcher")

// recorded is one state-changing command, in the order the session accepted it.
type recorded struct {
	resume      *v1.DebugResumeRequest
	breakpoints *v1.DebugSetBreakpointsRequest
}

// Reversible is a [Target] over a run it can start again, so that [Reversible.Back]
// can move to the previous stop. Use [NewReversible].
type Reversible struct {
	launch  Launcher
	timeout time.Duration

	// command serializes everything that changes the run: movements,
	// breakpoint replacements, pauses and rewinds. A rewind holds it for the
	// whole replay, so a movement cannot interleave with it.
	command sync.Mutex
	log     []recorded
	stops   []stop
	paused  bool
	rewinds int
	applied map[string]*v1.DebugReceipt
	order   []string

	// mu guards what reads need: the current run, the revision shift and the
	// highest revision shown. A rewind swaps them under it, briefly.
	mu         sync.RWMutex
	run        *Run
	offset     uint64
	highwater  uint64
	generation uint64
	closed     bool
	abort      context.CancelFunc
}

var _ Target = (*Reversible)(nil)

// NewReversible launches the program once and wraps it. The first run is the
// host's to have configured through launch exactly as every later one will be.
func NewReversible(ctx context.Context, launch Launcher, opts ...ReversibleOption) (*Reversible, error) {
	if launch == nil {
		return nil, errNoLauncher
	}
	run, err := launch(ctx)
	if err != nil {
		return nil, fmt.Errorf("flowdebug: launching the run: %w", err)
	}

	r := &Reversible{launch: launch, run: run, timeout: DefaultReplayTimeout, applied: map[string]*v1.DebugReceipt{}}
	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// Stop ends the current run and everything it holds. It is safe to call more
// than once and while a rewind is replaying: the replay is cancelled and the
// rewind discards the run it was building instead of installing it.
func (r *Reversible) Stop() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return
	}
	r.closed = true
	run, abort := r.run, r.abort
	r.mu.Unlock()

	if abort != nil {
		abort()
	}
	run.Stop()
}

// current is the run and the revision shift to read it by.
func (r *Reversible) current() (run *Run, offset, generation uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.run, r.offset, r.generation
}

// shown records that revision has been presented and returns it.
func (r *Reversible) shown(revision uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.highwater = max(r.highwater, revision)

	return revision
}

// toInner is the session's own revision for one a client holds. Zero stays
// zero, "no fence"; a revision from before the shift that cannot name any
// revision of the current run is reported not ok.
func toInner(revision, offset uint64) (uint64, bool) {
	if revision == 0 {
		return 0, true
	}
	if revision <= offset {
		return 0, false
	}

	return revision - offset, true
}

// present is a snapshot as this wrapper shows it: revisions shifted, and the
// capability it adds said. The input is not modified.
func (r *Reversible) present(snapshot *v1.DebugSnapshot, offset uint64) *v1.DebugSnapshot {
	shown := proto.CloneOf(snapshot)
	shown.Revision = r.shown(shown.GetRevision() + offset)
	if shown.GetReceipt() != nil {
		shown.Receipt.Revision += offset
	}
	if shown.GetCapabilities() != nil {
		shown.Capabilities.Reverse = true
	}

	return shown
}

// presentReceipt is a receipt as this wrapper shows it.
func (r *Reversible) presentReceipt(receipt *v1.DebugReceipt, offset uint64) *v1.DebugReceipt {
	shown := proto.CloneOf(receipt)
	shown.Revision = r.shown(shown.GetRevision() + offset)

	return shown
}

// Snapshot implements [Target]. A rewind that lands while it reads is retried
// against the rewound run: the replaced one is being stopped, and what it would
// report then is the end of a session that is, as far as the caller is
// concerned, still alive.
func (r *Reversible) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	for {
		run, offset, generation := r.current()
		snapshot, err := run.Session.Snapshot(ctx)
		if _, _, now := r.current(); now != generation {
			continue
		}
		if err != nil {
			return nil, err
		}

		return r.present(snapshot, offset), nil
	}
}

// WaitSnapshot implements [Target]. A rewind that happens while it waits moves
// the wait to the rewound run rather than answering with the abandoned one.
func (r *Reversible) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	for {
		run, offset, generation := r.current()
		inner, ok := toInner(after, offset)
		if !ok {
			// From before the shift: everything the current run says is newer.
			inner = 0
		}
		snapshot, err := run.Session.WaitSnapshot(ctx, inner)
		if _, _, now := r.current(); now != generation {
			continue
		}
		if err != nil {
			return nil, err
		}

		return r.present(snapshot, offset), nil
	}
}

// answer is a receipt this wrapper issues itself, at the revision the session
// is at.
func (r *Reversible) answer(requestID string, status v1.DebugCommandStatus, message string) *v1.DebugReceipt {
	revision := uint64(0)
	if snapshot, err := r.Snapshot(context.Background()); err == nil {
		revision = snapshot.GetRevision()
	}

	return &v1.DebugReceipt{RequestId: requestID, Status: status, Revision: revision, Message: message}
}

// remember keeps an applied receipt so a retry of the same request id is a
// duplicate and advances nothing, even across a rewind.
func (r *Reversible) remember(receipt *v1.DebugReceipt) {
	id := receipt.GetRequestId()
	if id == "" || receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
		return
	}
	if _, ok := r.applied[id]; !ok {
		r.order = append(r.order, id)
	}
	r.applied[id] = proto.CloneOf(receipt)
	for len(r.order) > maxReceipts {
		delete(r.applied, r.order[0])
		r.order = r.order[1:]
	}
}

// duplicate is the receipt for a request id already applied.
func (r *Reversible) duplicate(requestID string) (*v1.DebugReceipt, bool) {
	original, ok := r.applied[requestID]
	if !ok || requestID == "" {
		return nil, false
	}
	repeat := proto.CloneOf(original)
	repeat.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE

	return repeat, true
}

// Resume implements [Target], and records the movement so a rewind can replay it.
func (r *Reversible) Resume(ctx context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	r.command.Lock()
	defer r.command.Unlock()

	if receipt, ok := r.duplicate(req.GetRequestId()); ok {
		return receipt, nil
	}
	run, offset, _ := r.current()
	forwarded := proto.CloneOf(req)
	inner, ok := toInner(req.GetExpectedRevision(), offset)
	if !ok {
		return r.answer(req.GetRequestId(), v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
			"the command was meant for a revision from before the rewind"), nil
	}
	forwarded.ExpectedRevision = inner

	// The stop being left is fingerprinted before the run leaves it: it is
	// the only moment this wrapper is certain to see it.
	if err := r.noteStop(ctx, run); err != nil {
		return nil, err
	}
	receipt, err := run.Session.Resume(ctx, forwarded)
	if err != nil {
		return nil, err
	}
	if receipt.GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
		if len(r.log) >= maxReversibleLog {
			// Still moved, but nothing past here can be replayed.
			r.paused = true
		} else {
			r.log = append(r.log, recorded{resume: proto.CloneOf(req)})
		}
	}
	shown := r.presentReceipt(receipt, offset)
	r.remember(shown)

	return shown, nil
}

// Pause implements [Target]. A pause lands wherever the run happens to be, so
// a history containing one cannot be replayed to the same stop: after one,
// [Reversible.Back] refuses and says why.
func (r *Reversible) Pause(ctx context.Context, requestID string) (*v1.DebugReceipt, error) {
	r.command.Lock()
	defer r.command.Unlock()

	if receipt, ok := r.duplicate(requestID); ok {
		return receipt, nil
	}
	run, offset, _ := r.current()
	receipt, err := run.Session.Pause(ctx, requestID)
	if err != nil {
		return nil, err
	}
	// Only a pause that is still to land counts: asking a held run to hold
	// is answered applied and changes nothing a replay could miss.
	if receipt.GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING {
		r.paused = true
	}
	shown := r.presentReceipt(receipt, offset)
	r.remember(shown)

	return shown, nil
}

// ReplaceBreakpoints implements [Target], and records the replacement: a
// rewind replays it at the point it was made, so a stop the breakpoints
// decided is decided the same way.
func (r *Reversible) ReplaceBreakpoints(ctx context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	r.command.Lock()
	defer r.command.Unlock()

	if receipt, ok := r.duplicate(req.GetRequestId()); ok {
		snapshot, err := r.Snapshot(ctx)
		if err != nil {
			return nil, err
		}

		return &v1.DebugSetBreakpointsResponse{Receipt: receipt, Breakpoints: snapshot.GetBreakpoints(), Snapshot: snapshot}, nil
	}
	run, offset, _ := r.current()
	response, err := run.Session.ReplaceBreakpoints(ctx, req)
	if err != nil {
		return nil, err
	}
	shown := proto.CloneOf(response)
	if shown.GetReceipt() != nil {
		shown.Receipt = r.presentReceipt(response.GetReceipt(), offset)
		r.remember(shown.Receipt)
	}
	if shown.GetSnapshot() != nil {
		shown.Snapshot = r.present(response.GetSnapshot(), offset)
	}
	if response.GetReceipt().GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
		if len(r.log) >= maxReversibleLog {
			r.paused = true
		} else {
			r.log = append(r.log, recorded{breakpoints: proto.CloneOf(req)})
		}
	}

	return shown, nil
}

// Inspect implements [Target]. Like [Reversible.Snapshot], it is retried
// against the rewound run if a rewind lands while it reads.
func (r *Reversible) Inspect(ctx context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	for {
		run, offset, generation := r.current()
		forwarded := proto.CloneOf(req)
		inner, ok := toInner(req.GetRevision(), offset)
		if !ok {
			return nil, ErrStaleRevision
		}
		forwarded.Revision = inner

		response, err := run.Session.Inspect(ctx, forwarded)
		if _, _, now := r.current(); now != generation {
			continue
		}
		if err != nil {
			return nil, err
		}
		shown := proto.CloneOf(response)
		shown.Revision = r.shown(response.GetRevision() + offset)

		return shown, nil
	}
}

// Close implements [Target]: it detaches the debugger from the current run,
// which then finishes unattended. [Reversible.Stop] ends it instead.
func (r *Reversible) Close() error {
	run, _, _ := r.current()

	return run.Session.Close()
}

// stop is what a person was shown at one stop: a digest of the whole account,
// and the address, which is what a divergence names.
type stop struct {
	sum     [sha256.Size]byte
	address string
}

// fingerprint identifies a stop by what a person was shown at it: where, why,
// the frames, the observations that led there, and scope, the digest of what
// the held run can name. It reads the snapshot and the scope after the
// session's redaction, so it holds no secret and keeps none.
func fingerprint(snapshot *v1.DebugSnapshot, scope string) stop {
	var text strings.Builder
	fmt.Fprintf(&text, "%s\n%s\n%s\n", snapshot.GetState(), snapshot.GetReason(), snapshot.GetOccurrence().GetAddress())
	fmt.Fprintf(&text, "%s\n%s\n", snapshot.GetFailure(), snapshot.GetIrDigest())
	for _, frame := range snapshot.GetFrames() {
		fmt.Fprintf(&text, "frame %s %s\n", frame.GetLabel(), frame.GetOccurrence().GetAddress())
	}
	fmt.Fprintf(&text, "scope %s\n", scope)
	for _, id := range snapshot.GetBreakpointIds() {
		fmt.Fprintf(&text, "bp %s\n", id)
	}
	for _, observation := range snapshot.GetObservations() {
		fmt.Fprintf(&text, "%d %s %s %s\n", observation.GetSequence(), observation.GetKind(), observation.GetAddress(), observation.GetText())
	}

	return stop{sum: sha256.Sum256([]byte(text.String())), address: snapshot.GetOccurrence().GetAddress()}
}

// stopped reports whether the snapshot is a place the run can be held at or
// where it ended: the two kinds of stop a rewind can return to.
func stopped(snapshot *v1.DebugSnapshot) bool {
	switch snapshot.GetState() {
	case v1.DebugRunState_DEBUG_RUN_STATE_HELD, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
		v1.DebugRunState_DEBUG_RUN_STATE_FAILED:
		return true
	default:
		return false
	}
}

// movements is how many movement commands the log holds.
func movements(log []recorded) int {
	n := 0
	for _, c := range log {
		if c.resume != nil {
			n++
		}
	}

	return n
}

// noteStop fingerprints the stop the run is at, if it is at one and has not
// been noted. Callers hold r.command.
//
// The stops list is kept aligned with the movements: after n movements it
// should hold n+1 entries, the entry stop included. A stop noted late is
// appended; one already noted is not repeated.
func (r *Reversible) noteStop(ctx context.Context, run *Run) error {
	snapshot, err := run.Session.Snapshot(ctx)
	if err != nil {
		return err
	}
	if !stopped(snapshot) {
		return nil
	}
	if len(r.stops) > movements(r.log) {
		return nil
	}
	at, err := stopOf(ctx, run.Session, snapshot)
	if err != nil {
		return err
	}
	r.stops = append(r.stops, at)

	return nil
}

// stopOf fingerprints the snapshot together with the scope the session shows at it.
func stopOf(ctx context.Context, target Target, snapshot *v1.DebugSnapshot) (stop, error) {
	scope, err := scopeDigest(ctx, target)
	if err != nil {
		return stop{}, err
	}

	return fingerprint(snapshot, scope), nil
}

// maxScopeGroups bounds how many scope groups a stop's digest walks; the rest
// are not compared.
const maxScopeGroups = 32

// scopeDigest is a hash of every name, type and rendered value a held run can
// reach, read through the session's own redacted inspection, one page per
// group of [MaxInspectLimit] names at most. A run that is not held has no
// scope to name and digests as empty.
func scopeDigest(ctx context.Context, target Target) (string, error) {
	// An inspection whose context ended reports the values it could not read as
	// text, so the context is checked after every read: a digest of "context
	// canceled" would be a fingerprint no later visit could match.
	top, err := target.Inspect(ctx, &v1.DebugInspectRequest{Children: true, Limit: MaxInspectLimit})
	if errors.Is(err, ErrNotPaused) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	h := sha256.New()
	for _, group := range top.GetChildren()[:min(len(top.GetChildren()), maxScopeGroups)] {
		fmt.Fprintf(h, "group %s %s\n", group.GetName(), group.GetValue().GetRendered())
		names, err := target.Inspect(ctx, &v1.DebugInspectRequest{
			Expression: group.GetValue().GetExpression(), Children: true, Limit: MaxInspectLimit,
		})
		if err != nil {
			return "", err
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		for _, variable := range names.GetChildren() {
			fmt.Fprintf(h, "%s %s %s\n", variable.GetName(), variable.GetValue().GetType(), variable.GetValue().GetRendered())
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// Back moves to the previous stop.
//
// It starts the program again under a fresh session, replays every recorded
// command but the last movement, and checks at each stop on the way that the
// run shows what it showed the first time. Only then does the rewound run
// replace the current one, which is stopped; on any refusal or divergence the
// current run is untouched.
//
// expectedRevision, when non-zero, refuses the rewind as stale unless the
// session is still at it, like [v1.DebugResumeRequest.ExpectedRevision]. The
// receipt's status is applied when the rewound run is held at the previous
// stop, and refused otherwise, with the reason: a run with nowhere to go back
// to, a history in which a pause was still to land, a history past [MaxReversibleMoves], a
// replay that failed, or one that diverged from what was shown ("diverged:" is
// the message's first word).
func (r *Reversible) Back(ctx context.Context, requestID string, expectedRevision uint64) (*v1.DebugReceipt, error) {
	r.command.Lock()
	defer r.command.Unlock()

	if receipt, ok := r.duplicate(requestID); ok {
		return receipt, nil
	}
	refuse := func(status v1.DebugCommandStatus, message string) (*v1.DebugReceipt, error) {
		return r.answer(requestID, status, message), nil
	}

	run, offset, _ := r.current()
	if inner, ok := toInner(expectedRevision, offset); !ok {
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
			"the command was meant for a revision from before the rewind")
	} else if inner != 0 {
		if snapshot, err := run.Session.Snapshot(ctx); err != nil {
			return nil, err
		} else if snapshot.GetRevision() != inner {
			return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE,
				fmt.Sprintf("the command was meant for revision %d, and the session is at %d",
					expectedRevision, snapshot.GetRevision()+offset))
		}
	}

	if err := r.noteStop(ctx, run); err != nil {
		return nil, err
	}
	snapshot, err := run.Session.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	moves := movements(r.log)
	switch {
	case snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_DETACHED,
		snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED:
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session was detached, so there is no stop to go back from")
	case !stopped(snapshot):
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "the run is not held; pause it first")
	case r.paused:
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			"a pause or a history too long to keep is in this session, and neither can be replayed to the same stop")
	case moves == 0:
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "this is the first stop; there is nothing to go back to")
	case moves > MaxReversibleMoves:
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED,
			fmt.Sprintf("unavailable: going back replays every movement, and this session has made %d, past the limit of %d", moves, MaxReversibleMoves))
	case len(r.stops) != moves+1:
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, "unavailable: the stops this session showed were not all recorded")
	}

	// The last movement is the one being undone; the breakpoint replacements
	// before and after it stay, so the rewound run holds the same set.
	last := -1
	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].resume != nil {
			last = i

			break
		}
	}
	replay := slices.Concat(r.log[:last], r.log[last+1:])

	r.rewinds++
	replayCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session was stopped")
	}
	r.abort = cancel
	r.mu.Unlock()
	fresh, held, err := r.replay(replayCtx, replay, r.stops[:moves], r.rewinds)
	if err != nil {
		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, err.Error())
	}

	r.mu.Lock()
	if r.closed {
		// Stopped while the replay ran: nothing owns the rewound run, so it
		// goes with the rest.
		r.mu.Unlock()
		fresh.Stop()

		return refuse(v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, "the session was stopped while it was going back")
	}
	old := r.run
	// The rewound session counts from one; shift it above everything shown.
	r.offset = 0
	if r.highwater >= held.GetRevision() {
		r.offset = r.highwater + 1 - held.GetRevision()
	}
	r.run = fresh
	r.generation++
	r.mu.Unlock()
	old.Stop()

	r.log = replay
	r.stops = r.stops[:moves]
	r.paused = false

	receipt := &v1.DebugReceipt{
		RequestId: requestID,
		Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED,
		Revision:  r.shown(held.GetRevision() + r.offset),
		Message:   fmt.Sprintf("back at %s", held.GetOccurrence().GetAddress()),
	}
	r.remember(receipt)

	return receipt, nil
}

// replay starts the program again and re-issues commands, comparing each stop
// it reaches with the one shown the first time. It returns the new run, held
// at the last stop, and that stop's snapshot.
func (r *Reversible) replay(ctx context.Context, commands []recorded, stops []stop, rewind int) (*Run, *v1.DebugSnapshot, error) {
	fresh, err := r.launch(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unavailable: the program could not be started again: %w", err)
	}
	abandon := func(err error) (*Run, *v1.DebugSnapshot, error) {
		fresh.Stop()

		return nil, nil, err
	}

	held, err := waitStop(ctx, fresh.Session, 0)
	if err != nil {
		return abandon(interrupted(ctx, "did not reach its first stop", "unavailable", err))
	}
	got, err := stopOf(ctx, fresh.Session, held)
	if err != nil {
		return abandon(interrupted(ctx, "could not be read at its first stop", "unavailable", err))
	}
	if got != stops[0] {
		return abandon(diverged(0, stops[0], got))
	}

	moved := 0
	for i, c := range commands {
		if c.breakpoints != nil {
			response, err := fresh.Session.ReplaceBreakpoints(ctx, proto.CloneOf(c.breakpoints))
			if err != nil {
				return abandon(fmt.Errorf("unavailable: the replay could not restore the breakpoints: %w", err))
			}
			if status := response.GetReceipt().GetStatus(); status != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
				return abandon(fmt.Errorf("diverged: the replay could not restore the breakpoints (%s)", status))
			}

			continue
		}

		request := proto.CloneOf(c.resume)
		request.RequestId = fmt.Sprintf("rewind-%d-%d", rewind, i)
		request.ExpectedRevision = held.GetRevision()
		receipt, err := fresh.Session.Resume(ctx, request)
		if err != nil {
			return abandon(fmt.Errorf("unavailable: the replay could not move: %w", err))
		}
		if receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
			return abandon(fmt.Errorf("diverged: the replay's movement %d was %s: %s", moved+1, receipt.GetStatus(), receipt.GetMessage()))
		}
		held, err = waitStop(ctx, fresh.Session, held.GetRevision())
		if err != nil {
			return abandon(interrupted(ctx, fmt.Sprintf("did not reach stop %d", moved+1), "diverged", err))
		}
		moved++
		if moved >= len(stops) {
			return abandon(fmt.Errorf("diverged: the replay made more movements than the stops shown"))
		}
		got, err := stopOf(ctx, fresh.Session, held)
		if err != nil {
			return abandon(interrupted(ctx, fmt.Sprintf("could not be read at stop %d", moved), "unavailable", err))
		}
		if got != stops[moved] {
			return abandon(diverged(moved, stops[moved], got))
		}
	}
	if moved != len(stops)-1 {
		return abandon(fmt.Errorf("diverged: the replay made %d movements, and the stops shown call for %d", moved, len(stops)-1))
	}

	return fresh, held, nil
}

// interrupted is the refusal for a replay that stopped waiting. When its own
// deadline, the caller's cancellation or a [Reversible.Stop] ended it, nothing is known
// about the run, so it is unavailable; otherwise the session ended where it
// should have stopped, which is a difference from the first visit.
func interrupted(ctx context.Context, what, kind string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("unavailable: the replay %s (%w)", what, ctx.Err())
	}

	return fmt.Errorf("%s: the replay %s: %w", kind, what, err)
}

// diverged is the refusal for a replay that did not reproduce a stop.
func diverged(index int, want, got stop) error {
	where := fmt.Sprintf("was at %s and is at %s now", want.address, got.address)
	if want.address == got.address {
		where = fmt.Sprintf("was at %s both times, but the account of how the run got there differs", want.address)
	}

	return fmt.Errorf("diverged: stop %d %s; the run is not deterministic, "+
		"so going back would show a different run as the earlier one", index, where)
}

// waitStop blocks until the session is at a stop newer than after, or has
// ended in a way that is no stop.
func waitStop(ctx context.Context, session *Session, after uint64) (*v1.DebugSnapshot, error) {
	for {
		snapshot, err := session.WaitSnapshot(ctx, after)
		if err != nil {
			return nil, err
		}
		if stopped(snapshot) {
			return snapshot, nil
		}
		switch snapshot.GetState() {
		case v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED:
			return nil, fmt.Errorf("the session ended (%s)", snapshot.GetState())
		}
		after = snapshot.GetRevision()
	}
}
