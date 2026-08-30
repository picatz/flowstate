package audit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// MaxResourceKeyBytes bounds the one value in a record a caller influences.
//
// The schema says the same number (AuditRecord.resource_key's max_len), and
// this is the half that has to hold: a decision is audited before the request
// has necessarily been validated, because whether a caller may act does not
// depend on whether their arguments parse. So the emitter bounds what it was
// handed rather than assuming somebody else did.
const MaxResourceKeyBytes = 256

// DefaultWriterQueueSize is the number of audit records the best-effort
// stderr sink can hold while its writer is unavailable. Once full, new records
// are dropped rather than applying an unbounded writer delay to RPC handlers.
const DefaultWriterQueueSize = 256

// Emitter writes one record to one sink.
//
// The error is the reason this interface exists rather than an
// [otellog.Logger]: a required sink's failure has to be able to become the
// caller's failure, and the OpenTelemetry log API's Emit cannot report one.
type Emitter interface {
	Emit(ctx context.Context, record *v1.AuditRecord) error
}

// Subject is what a decision was about, as the seam making it already knows
// it.
//
// The action is deliberately absent: it is derived from RPC through
// [v1.AuthorizationActionForRPC], the deployment's one closed vocabulary, so a
// call site cannot record a decision under an action other than the one that
// authorizes the operation. That derivation is also what makes the audited
// surface a property of the bindings rather than a second list — see
// [AuditedActions].
type Subject struct {
	// RPC is the WorkflowService method by its schema name, e.g. "Signal".
	RPC string

	// Identity is the caller as this deployment attested them. Nil for an
	// unauthenticated caller, which a deployment started with
	// --insecure-no-auth can have.
	Identity *v1.WorkloadIdentity

	// ResourceKind and ResourceKey say what was addressed. Unspecified and
	// empty are a real answer: Validate, Compile and GetCatalog reach no
	// resource at all.
	ResourceKind v1.AuditResourceKind
	ResourceKey  string
}

// Recorder is the process's audit sink, and the policy about it.
//
// A nil *Recorder records nothing and returns no error from every method, so a
// component that has not been given one is not a component that panics. That
// is a library default and not a deployment's answer: a deployment that wants
// records builds one with [NewRecorder], and one that insists on them adds
// [Required].
type Recorder struct {
	emitters []Emitter
	required bool
	noStderr bool
	now      func() time.Time
}

// Option configures a [Recorder].
type Option func(*Recorder)

// WithEmitter adds a sink. Every emitter gets every record, in the order they
// were added.
func WithEmitter(emitter Emitter) Option {
	return func(r *Recorder) {
		if emitter != nil {
			r.emitters = append(r.emitters, emitter)
		}
	}
}

// WithWriter adds a sink that writes one JSON object per line.
//
// [NewRecorder] adds os.Stderr by default, and that default is the floor the
// design insists on: an audit trail has to survive an operator who configured
// no collector, so the cheapest possible sink — a file descriptor the process
// already has — is always present unless [WithoutStderr] takes it away.
func WithWriter(w io.Writer) Option {
	return WithEmitter(NewWriterEmitter(w))
}

// WithoutStderr drops the default stderr sink.
//
// For a test, and for a deployment that has deliberately arranged another
// complete sink. Combined with no other emitter it is a recorder that records
// nothing, which is why [NewRecorder] refuses that combination under
// [Required].
//
// It is a flag rather than an edit to the emitter list, so that it means the
// same thing wherever it appears among the options: an option that cleared
// what earlier options had added would make the result depend on the order
// they were written in.
func WithoutStderr() Option {
	return func(r *Recorder) {
		r.noStderr = true
	}
}

// Required makes an emitter's failure the caller's failure.
//
// This is the fail-closed half: an action that cannot be recorded does not
// happen. It is also why a batch processor cannot back this mode — see the
// package doc and [NewSyncProcessor].
func Required() Option {
	return func(r *Recorder) {
		r.required = true
	}
}

// WithClock replaces the server clock the record is stamped with.
//
// For tests. The clock is the server's own and never a caller's, the rule
// SignalSender.accepted_at already states.
func WithClock(now func() time.Time) Option {
	return func(r *Recorder) {
		if now != nil {
			r.now = now
		}
	}
}

// NewRecorder builds a recorder over stderr, plus whatever else is asked for.
//
// Fails closed on the one combination that would be a lie: [Required] with no
// emitter at all is a deployment insisting that every action be recorded and
// providing nowhere to record it, which would either refuse everything or,
// worse, record nothing while claiming to.
func NewRecorder(opts ...Option) (*Recorder, error) {
	r := &Recorder{now: time.Now}

	for _, opt := range opts {
		opt(r)
	}

	if !r.noStderr {
		r.emitters = append([]Emitter{NewWriterEmitter(os.Stderr)}, r.emitters...)
	}

	if r.required && len(r.emitters) == 0 {
		return nil, errors.New("audit: a required recorder with no sink cannot record anything")
	}

	return r, nil
}

// Allow records that a caller was authorized, before the mutation it permits.
func (r *Recorder) Allow(ctx context.Context, subject Subject) error {
	return r.record(ctx, subject, v1.AuditDecision_AUDIT_DECISION_ALLOW,
		v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED)
}

// Deny records that a caller was refused, and nothing was done.
//
// The code, never the refusal's own words: a reason can quote what was
// refused, and a refusal's words are peer-influenced text. The prose stays on
// the error the caller receives, which is not durable.
func (r *Recorder) Deny(ctx context.Context, subject Subject, code v1.AuditDenyCode) error {
	return r.record(ctx, subject, v1.AuditDecision_AUDIT_DECISION_DENY, code)
}

// Required reports whether a sink failure is the caller's failure.
func (r *Recorder) Required() bool {
	return r != nil && r.required
}

func (r *Recorder) record(ctx context.Context, subject Subject, decision v1.AuditDecision, code v1.AuditDenyCode) error {
	if r == nil {
		return nil
	}

	record, err := r.newRecord(subject, decision, code)
	if err != nil {
		// Not a sink failure, and so not gated on required: a record that
		// cannot be built means this seam cannot say what it just decided, and
		// there is no version of "audit the decision" that survives that. The
		// only way to reach it is an RPC no authorization action names, which
		// TestEveryRPCHasExactlyOneAuthorizationAction already fails on.
		return err
	}

	var failures []error
	for _, emitter := range r.emitters {
		if err := emitter.Emit(ctx, record); err != nil {
			failures = append(failures, err)
		}
	}

	if len(failures) == 0 || !r.required {
		// Best effort when the deployment has not asked for more. A sink that
		// is down is not, by itself, a reason to refuse work an operator never
		// said had to be recorded.
		return nil
	}

	return fmt.Errorf("audit: recording the %s decision for %s: %w",
		decision, subject.RPC, errors.Join(failures...))
}

// newRecord assembles the record, deriving everything derivable.
func (r *Recorder) newRecord(subject Subject, decision v1.AuditDecision, code v1.AuditDenyCode) (*v1.AuditRecord, error) {
	action, err := v1.AuthorizationActionForRPC(subject.RPC)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	return &v1.AuditRecord{
		Action:       action,
		Decision:     decision,
		Rpc:          subject.RPC,
		Identity:     subject.Identity,
		ResourceKind: subject.ResourceKind,
		ResourceKey:  boundResourceKey(subject.ResourceKey),
		DecidedAt:    timestamppb.New(r.now()),
		DenyCode:     code,
	}, nil
}

// boundResourceKey truncates on a rune boundary rather than mid-sequence, so a
// bounded record is still a readable one — #993's one part worth keeping.
func boundResourceKey(key string) string {
	if len(key) <= MaxResourceKeyBytes {
		return key
	}

	return strings.ToValidUTF8(key[:MaxResourceKeyBytes], "")
}

// AuditedActions is the audited surface, derived rather than listed.
//
// It is every action the bindings attach to at least one RPC, in the schema's
// own order. Nothing here is hand-kept, which is the point: an RPC added to
// the service without a binding already fails
// TestEveryRPCHasExactlyOneAuthorizationAction, so an RPC cannot arrive
// unaudited without that failure being the thing a reviewer sees.
//
// The three MCP-only actions — mcp.run_local, mcp.test and mcp.debug — are
// bound to no RPC and therefore fall out of this set. That is v1's written
// exemption rather than an oversight: those tools execute in the process
// serving them and have no RPC seam to decide at.
// TestTheAuditedSurfaceIsTheRPCSurface asserts the exemption is exactly those
// three, so a fourth unaudited action cannot join them quietly.
func AuditedActions() []v1.AuthorizationAction {
	bindings := v1.AuthorizationActionBindings()

	actions := make([]v1.AuthorizationAction, 0, len(bindings))
	for _, binding := range bindings {
		if len(binding.GetRpcs()) > 0 {
			actions = append(actions, binding.GetAction())
		}
	}

	return actions
}

// NewWriterEmitter writes one record per line as JSON.
//
// The unconditional floor. A writer, not a logger: the ordinary logging path
// is sampled and can be configured away, and a record that depends on
// telemetry being configured is not an audit record.
func NewWriterEmitter(w io.Writer) Emitter {
	if w == nil {
		return nil
	}

	return &writerEmitter{w: w}
}

// NewAsyncWriterEmitter writes records on a background goroutine through a
// bounded queue. Emit never waits for the writer: a full queue is reported as
// an error, which a best-effort Recorder deliberately swallows. The returned
// flush waits for records already accepted into the queue, subject to ctx.
//
// This emitter is for best-effort mode only. Required auditing must use
// [NewWriterEmitter], where completion of Emit proves the record was written.
func NewAsyncWriterEmitter(w io.Writer, queueSize int) (Emitter, func(context.Context) error) {
	if w == nil {
		return nil, func(context.Context) error { return nil }
	}
	if queueSize <= 0 {
		queueSize = DefaultWriterQueueSize
	}

	e := &asyncWriterEmitter{
		w:     w,
		queue: make(chan asyncWriterItem, queueSize),
	}
	go e.run()
	return e, e.flush
}

type asyncWriterItem struct {
	line    []byte
	flushed chan error
}

type asyncWriterEmitter struct {
	w     io.Writer
	queue chan asyncWriterItem
}

func (e *asyncWriterEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	line, err := protojson.MarshalOptions{}.Marshal(record)
	if err != nil {
		return fmt.Errorf("audit: marshaling the record: %w", err)
	}

	select {
	case e.queue <- asyncWriterItem{line: append(line, '\n')}:
		return nil
	default:
		return errors.New("audit: writer queue is full")
	}
}

func (e *asyncWriterEmitter) run() {
	for item := range e.queue {
		var err error
		if item.line != nil {
			_, err = e.w.Write(item.line)
			if err != nil {
				err = fmt.Errorf("audit: writing the record: %w", err)
			}
		}
		if item.flushed != nil {
			item.flushed <- err
		}
	}
}

func (e *asyncWriterEmitter) flush(ctx context.Context) error {
	flushed := make(chan error, 1)
	select {
	case e.queue <- asyncWriterItem{flushed: flushed}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-flushed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type writerEmitter struct {
	mu sync.Mutex
	w  io.Writer
}

func (e *writerEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	// protojson, so the field names a reader greps for are the schema's own
	// and a field added to the message reaches the sink without a second
	// marshaller learning about it.
	line, err := protojson.MarshalOptions{}.Marshal(record)
	if err != nil {
		return fmt.Errorf("audit: marshaling the record: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := e.w.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("audit: writing the record: %w", err)
	}

	return nil
}
