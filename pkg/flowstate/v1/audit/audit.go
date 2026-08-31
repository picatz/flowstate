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
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// MaxResourceKeyBytes bounds the one value in a record a caller influences.
//
// The schema says the same number (AuditRecord.resource_key's max_len), and
// this is the half that has to hold: a decision is audited before the request
// has necessarily been validated, because whether a caller may act does not
// depend on whether their arguments parse. So the emitter bounds what it was
// handed rather than assuming somebody else did.
const MaxResourceKeyBytes = 256

// MaxProvenanceBytes bounds each operator-chosen policy label copied from the
// attested Principal. These are not token claims, but configuration is still an
// input to a durable sink and therefore bounded where it is spent.
const MaxProvenanceBytes = auth.MaxPolicyProvenanceBytes

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
// The action is deliberately absent: it is derived from RPC or MCPTool through
// the deployment's one closed vocabulary, so a call site cannot record a
// decision under an action other than the one that authorizes the operation.
// Exactly one operation name must be present.
type Subject struct {
	// RPC is the WorkflowService method by its schema name, e.g. "Signal".
	RPC string

	// MCPTool is the full registered MCP tool name, e.g. "flowstate_test".
	MCPTool string

	// Identity is the caller as this deployment attested them. Nil for an
	// unauthenticated caller, which a deployment started with
	// --insecure-no-auth can have.
	Identity *v1.WorkloadIdentity

	// ResourceKind and ResourceKey say what was addressed. Unspecified and
	// empty are a real answer: Validate, Compile and GetCatalog reach no
	// resource at all.
	ResourceKind v1.AuditResourceKind
	ResourceKey  string

	// IssuerName and Role are policy provenance: operator-chosen values from
	// the TrustedIssuer entry that admitted the caller, never token claims.
	IssuerName string
	Role       string
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

	operation := subject.RPC
	if operation == "" {
		operation = subject.MCPTool
	}
	return fmt.Errorf("audit: recording the %s decision for %s: %w",
		decision, operation, errors.Join(failures...))
}

// newRecord assembles the record, deriving everything derivable.
func (r *Recorder) newRecord(subject Subject, decision v1.AuditDecision, code v1.AuditDenyCode) (*v1.AuditRecord, error) {
	var (
		action v1.AuthorizationAction
		err    error
	)
	switch {
	case subject.RPC != "" && subject.MCPTool != "":
		return nil, errors.New("audit: exactly one of RPC or MCPTool must identify the decision")
	case subject.RPC != "":
		action, err = v1.AuthorizationActionForRPC(subject.RPC)
	case subject.MCPTool != "":
		action, err = v1.AuthorizationActionForMCPTool(subject.MCPTool)
	default:
		return nil, errors.New("audit: no RPC or MCP tool identifies the decision")
	}
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	return &v1.AuditRecord{
		Action:       action,
		Decision:     decision,
		Rpc:          subject.RPC,
		McpTool:      subject.MCPTool,
		Identity:     auditIdentity(subject.Identity),
		ResourceKind: subject.ResourceKind,
		ResourceKey:  boundResourceKey(subject.ResourceKey),
		DecidedAt:    timestamppb.New(r.now()),
		DenyCode:     code,
		IssuerName:   boundString(subject.IssuerName, MaxProvenanceBytes),
		Role:         boundString(subject.Role, MaxProvenanceBytes),
	}, nil
}

// auditIdentity retains the bounded identity coordinates needed to identify
// the caller while structurally excluding claims. Claims may be safe for the
// workload identity carried into a run, but an authorization trail does not
// need their values to say who made which decision.
func auditIdentity(identity *v1.WorkloadIdentity) *v1.WorkloadIdentity {
	if identity == nil {
		return nil
	}

	return &v1.WorkloadIdentity{
		Subject:    identity.GetSubject(),
		Issuer:     identity.GetIssuer(),
		Namespace:  identity.GetNamespace(),
		Deployment: identity.GetDeployment(),
	}
}

// boundResourceKey truncates on a rune boundary rather than mid-sequence, so a
// bounded record is still a readable one — #993's one part worth keeping.
func boundResourceKey(key string) string {
	return boundString(key, MaxResourceKeyBytes)
}

func boundString(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}

	return strings.ToValidUTF8(value[:maxBytes], "")
}

// AuditedActions is the set of actions this recorder can express, derived
// rather than listed. The name predates MCP support.
//
// It is every action the bindings attach to at least one RPC or MCP-only tool,
// in the schema's own order. Actual seam coverage is asserted separately: all
// RPCs reach server audit, and every tool registered by authenticated
// `flow mcp serve` is invoked through its audit wrapper. Local stdio makes no
// bearer authorization decision.
func AuditedActions() []v1.AuthorizationAction {
	bindings := v1.AuthorizationActionBindings()

	actions := make([]v1.AuthorizationAction, 0, len(bindings))
	for _, binding := range bindings {
		if len(binding.GetRpcs()) > 0 || len(binding.GetMcpTools()) > 0 {
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
