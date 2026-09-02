package flowstatev1_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// The worker's four enforcement seams, recorded (picatz/flowstate#1379).
//
// Every test here drives the seam production drives — [v1.CheckTaskPolicy],
// [v1.ResolveSecret], the built-in http task, [v1.AuthorizeCredential] — through
// a real [audit.Recorder] with a recording sink, rather than asserting against
// a rendering. What the sink receives is the record a deployment's trail
// receives.
//
// Where a seam is reached from an activity rather than from workflow code, the
// test says so in its name: the record is written on the worker either way,
// and engine/enforcementaudit_test.go is the durable driver's own half of the
// task-dispatch claim.

// auditSink collects the records a recorder emitted.
type auditSink struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
	fail    error
}

func (s *auditSink) Emit(_ context.Context, record *v1.AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail != nil {
		return s.fail
	}

	s.records = append(s.records, record)
	return nil
}

func (s *auditSink) only(t *testing.T) *v1.AuditRecord {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	require.Len(t, s.records, 1, "expected exactly one record, got %v", s.records)
	return s.records[0]
}

func (s *auditSink) all() []*v1.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*v1.AuditRecord(nil), s.records...)
}

// auditing returns a context whose enforcement seams record to a sink the test
// can read, through the recorder cmd/flow builds.
func auditing(t *testing.T, opts ...audit.Option) (context.Context, *auditSink) {
	t.Helper()

	sink := &auditSink{}
	recorder, err := audit.NewRecorder(append([]audit.Option{
		audit.WithoutStderr(), audit.WithEmitter(sink),
	}, opts...)...)
	require.NoError(t, err)

	return v1.NewContextWithEnforcementAuditor(t.Context(), recorder), sink
}

func testIdentity() *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{
		Subject:   "deploy-bot",
		Issuer:    "https://issuer.example",
		Namespace: "acme",
		Claims:    map[string]string{"team": "payments"},
	}
}

// TestTaskDispatchRecordsBothDirectionsAndTheRuleThatDecided is #353's
// principle 2 at the dispatch seam: the record names the rule and the facts it
// read, on the allow as well as on the deny.
//
// An allow that named nothing would leave the operator asking the question the
// trail exists to answer — which of my rules let this through — of a file
// rather than of the record.
func TestTaskDispatchRecordsBothDirectionsAndTheRuleThatDecided(t *testing.T) {
	t.Parallel()

	policy, err := v1.TaskPolicyConfig{
		Allow: []string{`task == "log"`},
		Deny:  []string{`task == "http" && identity.namespace == "acme"`},
	}.Policy()
	require.NoError(t, err)

	ctx, sink := auditing(t)
	ctx = v1.NewContextWithTaskPolicy(ctx, policy)

	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", testIdentity(), false))

	allowed := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allowed.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH,
		allowed.GetEnforcementPoint())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_TASK, allowed.GetResourceKind())
	require.Equal(t, "log", allowed.GetResourceKey())
	require.Equal(t, `task == "log"`, allowed.GetRule(),
		"an allow must name the rule that permitted it")
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, allowed.GetDenyCode())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED, allowed.GetAction(),
		"an enforcement decision is not named by the caller-facing scope vocabulary")
	require.Equal(t, "deploy-bot", allowed.GetIdentity().GetSubject())
	require.Equal(t, "acme", allowed.GetIdentity().GetNamespace())
	require.Empty(t, allowed.GetIdentity().GetClaims(),
		"claims are removed before emission; their values say nothing about who decided what")
	require.NotNil(t, allowed.GetDecidedAt())

	ctx, sink = auditing(t)
	ctx = v1.NewContextWithTaskPolicy(ctx, policy)

	require.Error(t, v1.CheckTaskPolicy(ctx, "http", testIdentity(), false))

	denied := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, denied.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, denied.GetDenyCode())
	require.Equal(t, `task == "http" && identity.namespace == "acme"`, denied.GetRule())
	require.Equal(t, "http", denied.GetResourceKey())
}

// TestEachDispatchAttemptIsRecordedLocal is one of the two driver callers
// [conformance.AssertADecisionPerDispatchAttempt] asks for: the local driver
// consults the task-shape policy inside its retry loop, so a step attempted
// twice is decided twice and recorded twice, each record naming its attempt.
//
// engine.TestEachDispatchAttemptIsRecordedDurable is the other, over Temporal's
// own retry (Codex, picatz/flowstate#1394).
//
// Registered on a private registry rather than the process-global one, the way
// TestTotalTimeoutEndsTheStepLocal does, so this test needs no coordination
// with anything else registering tasks.
func TestEachDispatchAttemptIsRecordedLocal(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(conformance.DispatchAuditTaskDef(&attempts)))

	ctx, sink := auditing(t)
	ctx = v1.NewContextWithRegistry(ctx, registry)

	_, err := v1.Run(ctx, conformance.DispatchAuditWorkflow())
	require.NoError(t, err, "the fixture succeeds on its second attempt")

	conformance.AssertADecisionPerDispatchAttempt(t, "the local driver", sink.all(), attempts.Load())
}

// TestADenialOnALaterAttemptIsRecordedLocal is the negative direction, shared
// now that both drivers consult the policy per attempt: an operator who
// tightens a policy while a step is retrying has the next attempt refused, and
// the refusal recorded against the attempt it happened on.
//
// Not parallel: the fixture installs the process-wide task policy, which is
// what an operator's change actually is and what both drivers read.
func TestADenialOnALaterAttemptIsRecordedLocal(t *testing.T) {
	var attempts atomic.Int32

	denying, err := v1.TaskPolicyConfig{Deny: []string{conformance.DispatchAuditDenyRule}}.Policy()
	require.NoError(t, err)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(
		conformance.DispatchAuditTighteningTaskDef(&attempts, denying)))

	ctx, sink := auditing(t)
	ctx = v1.NewContextWithRegistry(ctx, registry)

	_, err = v1.Run(ctx, conformance.DispatchAuditWorkflow())
	require.Error(t, err, "the second attempt meets the tightened policy")

	conformance.AssertDispatchAttemptsRecorded(t, "the local driver", sink.all(),
		[]v1.AuditDecision{
			v1.AuditDecision_AUDIT_DECISION_ALLOW,
			v1.AuditDecision_AUDIT_DECISION_DENY,
		})
}

// TestADispatchNoAllowRuleMatchedIsRecordedAsSuchAndNamesNoRule: an allowlist
// nothing matched has no rule to name, and inventing one would be worse than
// the empty field — the deny code is what carries the reason.
func TestADispatchNoAllowRuleMatchedIsRecordedAsSuchAndNamesNoRule(t *testing.T) {
	t.Parallel()

	policy, err := v1.TaskPolicyConfig{Allow: []string{`task == "log"`}}.Policy()
	require.NoError(t, err)

	ctx, sink := auditing(t)
	ctx = v1.NewContextWithTaskPolicy(ctx, policy)

	require.Error(t, v1.CheckTaskPolicy(ctx, "http", testIdentity(), false))

	record := sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_NO_ALLOW_RULE, record.GetDenyCode())
	require.Empty(t, record.GetRule())
}

// TestARuleThatCouldNotBeEvaluatedIsRecordedByItsCodeAndNotItsText is the
// containment direction of the rule field: a rule that fails to evaluate
// reports a CEL error, and a CEL error can quote the data the rule was
// reading. So that denial carries RULE_ERROR and no text at all.
func TestARuleThatCouldNotBeEvaluatedIsRecordedByItsCodeAndNotItsText(t *testing.T) {
	t.Parallel()

	// A rule that compiles and then overruns its evaluation budget, which is
	// how a rule fails at dispatch rather than at load.
	budget := uint64(1)
	policy, err := v1.TaskPolicyConfig{
		Deny:          []string{`identity.subject.split("").map(c, c + task).size() > 0`},
		RuleCostLimit: &budget,
	}.Policy()
	require.NoError(t, err)

	ctx, sink := auditing(t)
	ctx = v1.NewContextWithTaskPolicy(ctx, policy)

	require.Error(t, v1.CheckTaskPolicy(ctx, "log", testIdentity(), false))

	record := sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR, record.GetDenyCode())
	require.Empty(t, record.GetRule(),
		"a rule that failed to evaluate is named by its code; its detail quotes the evaluation error")
}

// fixedProvider resolves every reference to one value, so a test can prove the
// value never reaches the trail.
type fixedProvider struct{ value string }

func (p fixedProvider) Scheme() string { return "env" }

func (p fixedProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, p.value), nil
}

func secretRuntime(t *testing.T, material string, policy auth.SecretAccessPolicy) v1.TaskRuntime {
	t.Helper()

	store, err := secrets.NewStore(fixedProvider{value: material})
	require.NoError(t, err)

	compiled, err := policy.Compile()
	require.NoError(t, err)

	return v1.TaskRuntime{
		Store:  store,
		Policy: compiled,
		Identity: auth.WorkloadIdentity{
			Subject: "deploy-bot", Issuer: "https://issuer.example", Namespace: "acme",
		},
		Step: auth.StepRef{Workflow: "release", Run: "release-42", Step: "fetch"},
	}
}

// TestSecretAccessRecordsTheReferenceAndNeverTheValue is the activity-side
// seam: [v1.ResolveSecret] runs inside a task, on the worker, and there is no
// workflow-side direction of it to assert.
//
// Both decisions are recorded, and the containment claim is structural: the
// record has no field a resolved value could occupy, asserted here through
// every rendering CLAUDE.md's containment matrix names.
func TestSecretAccessRecordsTheReferenceAndNeverTheValue(t *testing.T) {
	t.Parallel()

	const material = "leak-me-not-0451"

	ctx, sink := auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, secretRuntime(t, material,
		auth.SecretAccessPolicy{Allow: []string{`secret.scheme == "env"`}}))

	secret, err := v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
	require.NoError(t, err)
	require.Equal(t, material, secret.Reveal(), "the task still gets the value")

	allowed := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allowed.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_SECRET_ACCESS,
		allowed.GetEnforcementPoint())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_SECRET, allowed.GetResourceKind())
	require.Equal(t, "env:API_TOKEN", allowed.GetResourceKey())
	require.Equal(t, "deploy-bot", allowed.GetIdentity().GetSubject())

	requireRecordContains(t, allowed, material)

	ctx, sink = auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, secretRuntime(t, material,
		auth.SecretAccessPolicy{
			Allow: []string{`secret.scheme == "env"`},
			Deny:  []string{`secret.name == "API_TOKEN"`},
		}))

	_, err = v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
	require.Error(t, err)

	denied := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, denied.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, denied.GetDenyCode())
	require.Equal(t, `secret.name == "API_TOKEN"`, denied.GetRule())
	require.Equal(t, "env:API_TOKEN", denied.GetResourceKey())

	requireRecordContains(t, denied, material)
}

// requireRecordContains asserts the material appears in no rendering of the
// record — on the value, in a struct through an unexported field, and in a
// slice of those — the shapes CLAUDE.md's containment matrix names.
//
// %#v is checked on the record itself and not through the struct or the slice:
// that verb renders a pointer field as an address rather than recursing, which
// is a property of fmt and not of this record.
func requireRecordContains(t *testing.T, record *v1.AuditRecord, material string) {
	t.Helper()

	type holder struct{ record *v1.AuditRecord }
	held := holder{record: record}
	slice := []holder{held}

	renderings := map[string]string{
		"%v on the record":   fmt.Sprintf("%v", record),
		"%+v on the record":  fmt.Sprintf("%+v", record),
		"%#v on the record":  fmt.Sprintf("%#v", record),
		"protojson":          protojsonRecord(t, record),
		"%v on a struct":     fmt.Sprintf("%v", held),
		"%+v on a struct":    fmt.Sprintf("%+v", held),
		"%v on a slice":      fmt.Sprintf("%v", slice),
		"%+v on a slice":     fmt.Sprintf("%+v", slice),
		"String() on record": record.String(),
	}
	//lint:ignore S1025 the %s verb is one of the containment shapes under test, not a roundabout String()
	renderings["%s on the record"] = fmt.Sprintf("%s", record)

	for label, rendered := range renderings {
		require.NotContains(t, rendered, material,
			"%s carried the material into the audit trail", label)
	}
}

// protojsonRecord renders the record the way the writer sink does, which is
// the form that actually reaches an operator's file.
func protojsonRecord(t *testing.T, record *v1.AuditRecord) string {
	t.Helper()

	var buf strings.Builder
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithWriter(&buf))
	require.NoError(t, err)
	require.NoError(t, recorder.EnforcementAllow(t.Context(), v1.EnforcementSubject{
		Point:        record.GetEnforcementPoint(),
		Identity:     record.GetIdentity(),
		ResourceKind: record.GetResourceKind(),
		ResourceKey:  record.GetResourceKey(),
		Rule:         record.GetRule(),
	}))

	return buf.String()
}

// TestEgressRecordsTheDestinationAndNoOtherPartOfTheURL: the endpoint is what
// an egress record exists to name, and the path, query and fragment are
// request content — a webhook URL keeps its credential in the path.
func TestEgressRecordsTheDestinationAndNoOtherPartOfTheURL(t *testing.T) {
	t.Parallel()

	const pathToken = "T000-B000-capability-token"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	ctx, sink := auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue(server.URL + "/services/" + pathToken + "?token=" + pathToken),
	}, &v1.Scope{Identity: testIdentity()})
	require.NoError(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_EGRESS,
		record.GetEnforcementPoint())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_ENDPOINT, record.GetResourceKind())
	require.Equal(t, server.URL, record.GetResourceKey(),
		"the destination, and nothing else of the URL")
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject())

	requireRecordContains(t, record, pathToken)
}

// TestARefusedRequestIsRecordedByTheRuleThatRefusedIt: the deny direction of
// the egress seam, which is the one write-ahead protects — a denied request
// never left, so its record is written before anything happened.
func TestARefusedRequestIsRecordedByTheRuleThatRefusedIt(t *testing.T) {
	t.Parallel()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`host.startsWith("127.")`),
	)
	require.NoError(t, err)

	ctx, sink := auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue("http://127.0.0.1:9/refused"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, record.GetDenyCode())
	require.Equal(t, `host.startsWith("127.")`, record.GetRule())
	require.Equal(t, "http://127.0.0.1:9", record.GetResourceKey())
}

// TestARefusedRedirectIsRecordedAgainstTheHopThatWasRefused: the policy
// re-checks every hop, so the destination a record names has to be the one the
// policy actually refused — not the URL the workflow wrote, which was
// permitted.
func TestARefusedRedirectIsRecordedAgainstTheHopThatWasRefused(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, "http://127.0.0.1:9/elsewhere", http.StatusFound)
	}))
	defer server.Close()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`port == 9`),
	)
	require.NoError(t, err)

	ctx, sink := auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue(server.URL + "/start"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, `port == 9`, record.GetRule())
	require.Equal(t, "http://127.0.0.1:9", record.GetResourceKey(),
		"the record names the hop the policy refused, not the URL the workflow wrote")
}

// TestACancelledRequestRecordsNoEgressDecision: an evaluation the context
// interrupts decides nothing, so it is recorded as nothing — the rule the
// dispatch seam follows for a rule that ran out of time, and the one
// [netpolicy.UndecidedError] carries to this seam.
//
// The rule is one that cannot be evaluated, so evaluation is genuinely
// entered and genuinely fails. What makes the failure *undecided* is the done
// context, which the second half proves by removing it: the identical rule
// under a live context is an ordinary RULE_ERROR denial and is recorded. A
// test that only asserted the empty sink would pass just as well against a
// policy that never ran a rule at all.
func TestACancelledRequestRecordsNoEgressDecision(t *testing.T) {
	t.Parallel()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(),
		netpolicy.WithAllowRules(`int(host) > 0`))
	require.NoError(t, err)

	ctx, sink := auditing(t)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = v1.HTTPTaskDef(policy).Fn(cancelled, map[string]*v1.Value{
		"url": v1.NewValue("http://127.0.0.1:9/never-sent"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled,
		"an interrupted evaluation still answers the context checks every caller makes of it")

	require.Empty(t, sink.all(),
		"a request whose rule never finished recorded an egress decision the policy never made")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue("http://127.0.0.1:9/never-sent"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR, record.GetDenyCode(),
		"the same rule under a live context is a decision, and decisions are recorded")
}

// TestARequestCancelledAfterThePolicyPermittedItIsStillRecorded is the other
// side of the same distinction, and the one a blanket context test got wrong
// (Codex, picatz/flowstate#1394): the policy answered allow, the request left
// this worker, and only then did the context end.
//
// Withholding that record leaves --audit-required silent about precisely the
// request an operator needs it to name — one that may have reached its peer
// and whose outcome nobody knows. The peer here hangs until the context is
// cancelled, which is that request exactly.
func TestARequestCancelledAfterThePolicyPermittedItIsStillRecorded(t *testing.T) {
	t.Parallel()

	reached := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(reached)
		<-r.Context().Done()
	}))
	defer server.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithAllowRules("true"))
	require.NoError(t, err)

	ctx, sink := auditing(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-reached
		cancel()
	}()

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(server.URL + "/hangs"),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_EGRESS,
		record.GetEnforcementPoint())
	require.Equal(t, server.URL, record.GetResourceKey(),
		"the destination the policy permitted, which is where the request went")
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject())
}

// contextHonouringSink refuses to write once the context it is handed is done,
// which is what a real exporter does: [audit.NewSyncProcessor] passes the
// emitter's context straight to Export, and an OTLP exporter checks it.
//
// The in-memory sink every other test here uses ignores its context, which is
// why the cancellation test above passed while the OTel sink could not have
// exported the record it asserts (Codex, picatz/flowstate#1394).
type contextHonouringSink struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (s *contextHonouringSink) Emit(ctx context.Context, record *v1.AuditRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = append(s.records, record)

	return nil
}

func (s *contextHonouringSink) all() []*v1.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*v1.AuditRecord(nil), s.records...)
}

// TestACancelledRequestsRecordStillReachesASinkThatHonoursItsContext: the
// record for a request that left must survive the cancellation that ended the
// request.
//
// The seam writes this one after the request has gone, so the caller's context
// is routinely already done by the time it runs — and a sink that honours its
// context would then refuse the write, leaving the trail silent about a
// request whose outcome nobody knows, and under --audit-required failing the
// step for a collector that was never actually asked.
//
// Mutation-proved: passing the request's own context to the write makes this
// fail with an empty sink and a recorder failure.
func TestACancelledRequestsRecordStillReachesASinkThatHonoursItsContext(t *testing.T) {
	t.Parallel()

	reached := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(reached)
		<-r.Context().Done()
	}))
	defer server.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	sink := &contextHonouringSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink), audit.Required())
	require.NoError(t, err)

	ctx := v1.NewContextWithEnforcementAuditor(t.Context(), recorder)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-reached
		cancel()
	}()

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(server.URL + "/hangs"),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err, "the request itself was cancelled")
	require.False(t, v1.AuditRecorderUnavailable(err),
		"the sink was writable; only the request was cancelled, and the record must not have been refused with it")

	records := sink.all()
	require.Len(t, records, 1)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, records[0].GetDecision())
	require.Equal(t, server.URL, records[0].GetResourceKey())
}

// TestAnUnrecordableRequestThatReachedItsPeerIsNotRetried: under a required
// recorder the egress record is written after the request left, so the failure
// it raises has to carry the classification the sent request earned. An
// unclassified error is Internal, which is retryable, and retrying a POST that
// already reached its peer is the effect this task's own error kinds exist to
// prevent.
func TestAnUnrecordableRequestThatReachedItsPeerIsNotRetried(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	ctx, sink := auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(server.URL),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	kind := v1.ClassifyError(err)
	require.Equal(t, v1.ErrorKindUpstreamUnknown, kind)
	require.False(t, kind.Retryable(),
		"a POST that reached its peer must not be repeated because a sink was down")

	// An idempotent method may be attempted again: nothing can happen twice,
	// and the collector may be back.
	ctx, sink = auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue(server.URL),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.True(t, v1.ClassifyError(err).Retryable())
}

// TestADeniedRedirectHopThatCouldNotBeRecordedIsNotRetried: the denial branch's
// own recorder failure, classified (Codex, picatz/flowstate#1394).
//
// A POST reaches its origin, the peer redirects it somewhere the rules refuse,
// and the required sink cannot write that refusal. The refusal replaces itself
// with the recorder's error, which is unclassified and therefore Internal —
// retryable — so either driver would repeat a POST that already took effect.
// The hop is what makes it so: a first-hop denial sent nothing.
//
// Mutation-proved: removing the [netpolicy.DenyError] arm from
// [requestNeverLeft] makes this retryable, and removing the classification at
// the denial branch makes it Internal.
func TestADeniedRedirectHopThatCouldNotBeRecordedIsNotRetried(t *testing.T) {
	t.Parallel()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:9/elsewhere", http.StatusFound)
	}))
	defer origin.Close()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`port == 9`),
	)
	require.NoError(t, err)

	post := map[string]*v1.Value{
		"url":    v1.NewValue(origin.URL + "/start"),
		"method": v1.NewValue(http.MethodPost),
	}

	ctx, sink := auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, post, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.True(t, v1.AuditRecorderUnavailable(err))
	require.Equal(t, v1.ErrorKindUpstreamUnknown, v1.ClassifyError(err),
		"the original POST reached its peer before the hop was refused, so a repeat repeats it")
	require.False(t, v1.ClassifyError(err).Retryable())

	// A first-hop denial under the same failing sink sent nothing, so it stays
	// retryable: the collector coming back lets the step reach its refusal.
	ctx, sink = auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue("http://127.0.0.1:9/refused"),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.True(t, v1.AuditRecorderUnavailable(err))
	require.True(t, v1.ClassifyError(err).Retryable(),
		"nothing left this worker, so there is nothing a repeat could repeat")
}

// TestARedirectRefusedForItsOwnSakeIsNotRetried: the redirect hook's own
// refusals — redirects disabled, the hop bound reached, an https downgrade —
// are refusals of a hop, which means the origin already reached its peer.
//
// They build their own denial and so carried neither the hop nor the chain,
// which made requestNeverLeft answer "nothing left" for a POST that had: under
// a required recorder, a sink failure while recording the refusal then
// classified retryable and either driver could send the original again
// (Codex, picatz/flowstate#1394).
//
// Mutation-proved: removing the marking in checkRedirect makes this retryable.
func TestARedirectRefusedForItsOwnSakeIsNotRetried(t *testing.T) {
	t.Parallel()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer origin.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithDenyRedirects())
	require.NoError(t, err)

	ctx, sink := auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(origin.URL + "/start"),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.True(t, v1.AuditRecorderUnavailable(err))
	require.Equal(t, v1.ErrorKindUpstreamUnknown, v1.ClassifyError(err),
		"the POST reached its peer before the redirect was refused, so a repeat repeats it")
	require.False(t, v1.ClassifyError(err).Retryable())
}

// TestAResolutionFailureRecordsNoEgressDecision: the address policy and the
// connection-scoped rules decide about an address, and a name that does not
// resolve never produces one — the dialer's control hook is never called, so
// this policy reached no verdict about where the request was going.
//
// Recorded as an allow, that is a sentence the trail cannot support: the
// deployment's address policy might well have refused whatever the name
// resolved to. Under --audit-required it is worse than wrong, because an
// ordinary DNS failure then becomes an audit-sink failure too (Codex,
// picatz/flowstate#1394).
//
// The second half pins the boundary: once the hook has answered, a connection
// the peer refuses is a request this policy permitted, and its allow is
// recorded. "No verdict" and "a verdict the network then defeated" are the two
// sides, and only the first is silent.
//
// Mutation-proved: dropping the Undecided wrap in netpolicy's dial path
// records an allow for the unresolvable host.
func TestAResolutionFailureRecordsNoEgressDecision(t *testing.T) {
	t.Parallel()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	ctx, sink := auditing(t)

	// .invalid is reserved by RFC 2606 precisely so it cannot resolve.
	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue("http://this-name-cannot-exist.invalid/never"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	var dns *net.DNSError
	require.ErrorAs(t, err, &dns,
		"the step still reports the resolver's own failure, unwrapped")

	require.Empty(t, sink.all(),
		"a name that never resolved gave this policy no address to decide about")

	// And the other side of the line: the hook answered, the peer refused.
	ctx, sink = auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue("http://127.0.0.1:9/refused-by-the-peer"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision(),
		"loopback is permitted by this policy, so the request was permitted and only the connection failed")
	require.Equal(t, "http://127.0.0.1:9", record.GetResourceKey())
}

// TestAHopRefusedAtDialTimeIsRecordedAsTheHop: the destination a dial-time
// refusal names.
//
// The address policy decides after DNS, where the only thing in hand is
// "10.0.0.1:9" — not a destination. The record therefore fell back to the URL
// the workflow wrote, which is an endpoint this policy *allowed* and this
// worker reached, so the trail said the origin was refused and never mentioned
// the address that actually was (Codex, picatz/flowstate#1394).
//
// Mutation-proved: dropping the hop from the dialer's mark, or reading Target
// first in refusedEndpoint, records the origin here.
func TestAHopRefusedAtDialTimeIsRecordedAsTheHop(t *testing.T) {
	t.Parallel()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://10.0.0.1:9/internal", http.StatusFound)
	}))
	defer origin.Close()

	// Loopback is permitted, so the origin is reachable; 10.0.0.0/8 is refused
	// by the shipped address policy, at the dial, after the redirect.
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	ctx, sink := auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue(origin.URL + "/start"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	// One record, and it is the refusal: this request's single egress decision
	// is the one that ended it, which is the seam's stated shape (see the
	// comment on recordEgress, and picatz/flowstate#1397 for the per-hop trail
	// that would also name the origin it reached).
	refused := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, refused.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DESTINATION_NOT_PERMITTED, refused.GetDenyCode())
	require.Equal(t, "http://10.0.0.1:9", refused.GetResourceKey(),
		"the record names the address the policy refused, not the origin it allowed")
}

// TestADestinationOutsideThePolicyIsRecordedWithoutARule: netpolicy's six
// non-rule refusals share one code, because the record already names the
// destination and no rule decided.
func TestADestinationOutsideThePolicyIsRecordedWithoutARule(t *testing.T) {
	t.Parallel()

	policy, err := netpolicy.New()
	require.NoError(t, err)

	ctx, sink := auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue("http://127.0.0.1:9/refused"),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DESTINATION_NOT_PERMITTED, record.GetDenyCode())
	require.Empty(t, record.GetRule())
	require.Equal(t, "http://127.0.0.1:9", record.GetResourceKey())
}

type stubExchanger struct{ token string }

func (e stubExchanger) Name() string { return "stub-sts" }

func (e stubExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://resource.example"}
}

func (e stubExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Hour),
		map[string]string{"access_token": e.token})
}

// unavailableExchanger is the relying party being down: the assumption policy
// answers as it always does, and everything after it fails.
type unavailableExchanger struct{ stubExchanger }

func (unavailableExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.Credential{}, fmt.Errorf("%w: the token endpoint returned 503", auth.ErrExchangeUnavailable)
}

// hangingExchanger is the relying party that never answers: the exchange is
// still in flight when the caller's context ends, which is how cancellation
// reaches this seam *after* the policy has already permitted the target.
type hangingExchanger struct {
	stubExchanger

	reached chan struct{}
	once    sync.Once
}

func (e *hangingExchanger) Exchange(ctx context.Context, _ auth.Assertion) (auth.Credential, error) {
	e.once.Do(func() { close(e.reached) })
	<-ctx.Done()

	return auth.Credential{}, fmt.Errorf("%w: the token endpoint never answered: %w",
		auth.ErrExchangeUnavailable, ctx.Err())
}

func assumeBroker(t *testing.T, token string, opts ...auth.BrokerOption) *auth.Broker {
	t.Helper()

	return brokerFor(t, stubExchanger{token: token}, opts...)
}

// brokerFor is [assumeBroker] with the exchanger named, for a test about what
// happens when the exchange itself fails.
func brokerFor(t *testing.T, exchanger auth.Exchanger, opts ...auth.BrokerOption) *auth.Broker {
	t.Helper()

	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("test", private)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer,
		append([]auth.BrokerOption{auth.WithTarget("partner-api", exchanger)}, opts...)...)
	require.NoError(t, err)

	return broker
}

// TestCredentialAssumptionRecordsTheTargetAndNeverTheCredential is the other
// activity-side seam: [v1.AuthorizeCredential] applies material to a request
// inside the task, and the trail records which workload assumed which target.
func TestCredentialAssumptionRecordsTheTargetAndNeverTheCredential(t *testing.T) {
	t.Parallel()

	const material = "jit-token-that-must-not-enter-the-trail"

	runtime := secretRuntime(t, "unused", auth.SecretAccessPolicy{Allow: []string{"true"}})
	runtime.Broker = assumeBroker(t, material, auth.WithAssumeAllowRules(`target == "partner-api"`))

	ctx, sink := auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)
	require.NoError(t, v1.AuthorizeCredential(ctx, req, "partner-api"))
	require.Equal(t, "Bearer "+material, req.Header.Get("Authorization"),
		"the request still carries the credential")

	allowed := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allowed.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION,
		allowed.GetEnforcementPoint())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_CREDENTIAL_TARGET,
		allowed.GetResourceKind())
	require.Equal(t, "partner-api", allowed.GetResourceKey())

	requireRecordContains(t, allowed, material)

	denying := secretRuntime(t, "unused", auth.SecretAccessPolicy{Allow: []string{"true"}})
	denying.Broker = assumeBroker(t, material,
		auth.WithAssumeAllowRules("true"),
		auth.WithAssumeDenyRules(`target == "partner-api"`))

	ctx, sink = auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, denying)

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)
	require.Error(t, v1.AuthorizeCredential(ctx, req, "partner-api"))
	require.Empty(t, req.Header.Get("Authorization"))

	denied := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, denied.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, denied.GetDenyCode())
	require.Equal(t, `target == "partner-api"`, denied.GetRule())

	requireRecordContains(t, denied, material)
}

// TestAPermittedAssumptionIsRecordedEvenWhenTheExchangeFails: the decision, not
// the moment (Codex, picatz/flowstate#1394).
//
// [auth.Broker.Authorize] evaluates the assumption policy and then mints,
// exchanges and applies. When the policy allowed and the exchange then failed,
// this seam saw an error [assumeDenial] cannot classify, treated it as no
// decision, and recorded nothing — so an IdP outage erased exactly the allows
// an operator investigating that outage came to read.
//
// The other two directions stay where they already are, rather than being
// restated here: a refusal is one deny and no allow in
// TestCredentialAssumptionRecordsTheTargetAndNeverTheCredential, which fails on
// its own `only` assertion if this change ever turned a refusal into a
// decision-plus-failure. What is left for this test is the boundary itself —
// a failure *after* an allow, and an evaluation that never got that far.
//
// Mutation-proved: dropping the [auth.AssumptionFailedError] arm from
// AuthorizeCredential leaves the first case with an empty sink.
func TestAPermittedAssumptionIsRecordedEvenWhenTheExchangeFails(t *testing.T) {
	t.Parallel()

	runtime := secretRuntime(t, "unused", auth.SecretAccessPolicy{Allow: []string{"true"}})
	runtime.Broker = brokerFor(t, unavailableExchanger{},
		auth.WithAssumeAllowRules(`target == "partner-api"`))

	ctx, sink := auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)

	err = v1.AuthorizeCredential(ctx, req, "partner-api")
	require.Error(t, err)
	require.True(t, auth.Retryable(err),
		"the exchange failure reaches the caller with the retryability it had: the wrapper is transparent")
	require.Empty(t, req.Header.Get("Authorization"),
		"nothing reached the request, which is why this failure is safe to report as retryable")

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision(),
		"the policy permitted this target; the trail has to say so even though the credential never arrived")
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION,
		record.GetEnforcementPoint())
	require.Equal(t, "partner-api", record.GetResourceKey())
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject())

	// An evaluation the context interrupted is still no decision at all.
	ctx, sink = auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	req, err = http.NewRequestWithContext(cancelled, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)
	require.Error(t, v1.AuthorizeCredential(cancelled, req, "partner-api"))

	require.Empty(t, sink.all(),
		"a request whose context was already done recorded an assumption decision nobody made")
}

// TestACancelledAssumptionsRecordStillReachesASinkThatHonoursItsContext: the
// assumption seam's record is written after its effect too, so it needs the
// same detachment the egress seam's does (Codex, picatz/flowstate#1394).
//
// Cancellation is one of the ordinary ways minting and exchange fail, which
// means the context this seam was handed is routinely already done by the time
// it writes — and the record it is writing is that a policy permitted a target
// an identity provider may already have acted on. A sink that honours its
// context, as an exporter does, would refuse exactly that record.
//
// Mutation-proved: writing this allow on the request's own context leaves the
// sink empty and the step reporting a recorder failure.
func TestACancelledAssumptionsRecordStillReachesASinkThatHonoursItsContext(t *testing.T) {
	t.Parallel()

	exchanger := &hangingExchanger{reached: make(chan struct{})}

	runtime := secretRuntime(t, "unused", auth.SecretAccessPolicy{Allow: []string{"true"}})
	runtime.Broker = brokerFor(t, exchanger,
		auth.WithAssumeAllowRules(`target == "partner-api"`))

	sink := &contextHonouringSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink), audit.Required())
	require.NoError(t, err)

	ctx := v1.NewContextWithEnforcementAuditor(t.Context(), recorder)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-exchanger.reached
		cancel()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)

	err = v1.AuthorizeCredential(ctx, req, "partner-api")
	require.Error(t, err, "the exchange was cancelled")
	require.False(t, v1.AuditRecorderUnavailable(err),
		"the sink was writable; only the exchange was cancelled, and the record must not have been refused with it")
	require.Empty(t, req.Header.Get("Authorization"))

	records := sink.all()
	require.Len(t, records, 1)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, records[0].GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION,
		records[0].GetEnforcementPoint())
	require.Equal(t, "partner-api", records[0].GetResourceKey())
}

// TestAWorkerWithNoAuthorityRecordsWhatItRefused: a worker that was given no
// secret store or no broker refuses, and that refusal is a decision an
// operator should find in the trail rather than only in a step's error —
// "nothing was configured" is the commonest cause of both.
func TestAWorkerWithNoAuthorityRecordsWhatItRefused(t *testing.T) {
	t.Parallel()

	ctx, sink := auditing(t)

	_, err := v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, record.GetDenyCode())
	require.Equal(t, "env:API_TOKEN", record.GetResourceKey())

	ctx, sink = auditing(t)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)
	require.Error(t, v1.AuthorizeCredential(ctx, req, "partner-api"))

	record = sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, record.GetDenyCode())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION,
		record.GetEnforcementPoint())
}

// TestAnUnconfiguredAuthorityRecordsWhoAsked: a worker missing a broker, a
// store or a policy still refuses a *workload*, and the record has to say
// which one (Codex, picatz/flowstate#1394).
//
// This is the denial an operator meets while a deployment is still being
// wired, so it is the one most likely to be read — and a NOT_CONFIGURED line
// that cannot be attributed to a tenant answers none of the questions it is
// read for. Mutation-proved: moving either identity assignment back below its
// configured-or-not check fails this.
func TestAnUnconfiguredAuthorityRecordsWhoAsked(t *testing.T) {
	t.Parallel()

	runtime := secretRuntime(t, "material", auth.SecretAccessPolicy{Allow: []string{"true"}})

	brokerless := runtime
	brokerless.Broker = nil

	ctx, sink := auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, brokerless)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
	require.NoError(t, err)
	require.Error(t, v1.AuthorizeCredential(ctx, req, "partner-api"))

	record := sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, record.GetDenyCode())
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject(),
		"a worker with no broker still refused a workload, and the record must name it")
	require.Equal(t, "acme", record.GetIdentity().GetNamespace())

	storeless := runtime
	storeless.Store, storeless.Policy = nil, nil

	ctx, sink = auditing(t)
	ctx = v1.ContextWithTaskRuntime(ctx, storeless)

	_, err = v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
	require.Error(t, err)

	record = sink.only(t)
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, record.GetDenyCode())
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject(),
		"the same ordering, at the seam that reads secrets")
}

// TestARequiredRecorderStopsTheActionItCouldNotRecord is the fail-closed half
// at the worker seams: an action that cannot be recorded does not happen.
//
// Asserted on the allow direction of each seam that decides before it acts,
// because that is where the claim has teeth — a denial is refused either way.
func TestARequiredRecorderStopsTheActionItCouldNotRecord(t *testing.T) {
	t.Parallel()

	sinkFailure := errors.New("the collector is down")

	ctx, sink := auditing(t, audit.Required())
	sink.fail = sinkFailure

	err := v1.CheckTaskPolicy(ctx, "log", testIdentity(), false)
	require.ErrorIs(t, err, sinkFailure,
		"a dispatch whose allow could not be recorded must not proceed")

	ctx, sink = auditing(t, audit.Required())
	sink.fail = sinkFailure
	ctx = v1.ContextWithTaskRuntime(ctx, secretRuntime(t, "material",
		auth.SecretAccessPolicy{Allow: []string{"true"}}))

	resolved, err := v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
	require.ErrorIs(t, err, sinkFailure)
	require.Empty(t, resolved.Reveal(),
		"the store must not be consulted for a read whose allow could not be recorded")
}

// TestASinkOutageIsRetryableAtTheSeamsThatRecordBeforeTheyAct: the opposite
// case to the egress one above, and the one an operator meets on a bad
// afternoon (Codex, picatz/flowstate#1394).
//
// Secret access and credential assumption record *before* the value is read or
// the credential is used, so a required recorder that could not write has
// stopped the operation with nothing done. That is worth another attempt — and
// a bare recorder error was not: it matched neither [secrets.Retryable] nor
// [auth.Retryable], so it fell through to PolicyDenied, which is permanent, and
// a collector outage failed every secret-backed step for good.
//
// Mutation-proved: dropping [v1.AuditRecorderUnavailable] from either arm of
// the classification returns PolicyDenied here.
func TestASinkOutageIsRetryableAtTheSeamsThatRecordBeforeTheyAct(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	ctx, sink := auditing(t, audit.Required())
	sink.fail = errors.New("the collector is down")
	ctx = v1.ContextWithTaskRuntime(ctx, secretRuntime(t, "material",
		auth.SecretAccessPolicy{Allow: []string{"true"}}))

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(server.URL),
		"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	kind := v1.ClassifyError(err)
	require.Equal(t, v1.ErrorKindUpstream, kind,
		"a collector outage is not a policy denial: the store was never asked, so the step can be attempted again")
	require.True(t, kind.Retryable())
	require.True(t, v1.AuditRecorderUnavailable(err),
		"the recorder's own failure has to stay recognizable through the task error that carries it")

	require.Empty(t, sink.all(), "the failing sink recorded nothing, which is the premise of the test")
}

// TestARequestHeldBackBeforeItLeftIsNotPermanent: the classification a
// never-sent request earns, including when the audit sink is what failed
// (Codex, picatz/flowstate#1394).
//
// This worker's own rate limiter refuses on the initial hop *before* the dial,
// so nothing reached the peer. Reported as UpstreamUnknown — which is what
// happened when the required sink's failure was classified through a question
// that only asked "is this method idempotent" — a POST nobody sent became
// permanent, and a collector coming back could not let the step succeed.
//
// Mutation-proved: removing the rate-limit arm from [requestNeverLeft] makes
// the first case UpstreamUnknown again.
func TestARequestHeldBackBeforeItLeftIsNotPermanent(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	// One token, and a refill measured in hours: the second request through
	// this policy is refused on its initial hop, deterministically.
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxRequestsPerSecondPerProcess("127.0.0.1", 0.001))
	require.NoError(t, err)

	post := map[string]*v1.Value{
		"url":    v1.NewValue(server.URL),
		"method": v1.NewValue(http.MethodPost),
	}

	ctx, sink := auditing(t, audit.Required())

	// Drains the bucket. This one is sent and answered.
	_, err = v1.HTTPTaskDef(policy).Fn(ctx, post, &v1.Scope{Identity: testIdentity()})
	require.NoError(t, err)

	// Held back before the dial, and the sink cannot record the decision.
	sink.fail = errors.New("the collector is down")

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, post, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)
	require.True(t, v1.AuditRecorderUnavailable(err),
		"the sink's failure is what this step is reporting")

	kind := v1.ClassifyError(err)
	require.True(t, kind.Retryable(),
		"a POST the rate limiter refused before the dial never left, so a repeat repeats nothing; "+
			"reporting it permanently means a recovered collector cannot let the step succeed")
	require.NotEqual(t, v1.ErrorKindUpstreamUnknown, kind)

	// And the decision this seam records for such a request is the allow: the
	// rules admitted it, and the bucket is not a policy refusal. A fresh sink,
	// because the successful request above already wrote one.
	ctx, sink = auditing(t)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, post, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_EGRESS,
		record.GetEnforcementPoint())
	require.Equal(t, server.URL, record.GetResourceKey())
}

// TestARedirectHopHeldBackStaysUnknown is the other side of the same
// distinction, and the one #912 phase two already drew: an earlier hop reached
// its peer, so the original request *was* sent and replaying it may repeat an
// effect that already happened.
//
// The bucket is per host, so the hop is addressed as localhost while the
// workflow's own request goes to 127.0.0.1: two keys, one machine.
func TestARedirectHopHeldBackStaysUnknown(t *testing.T) {
	t.Parallel()

	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer destination.Close()

	_, port, err := net.SplitHostPort(strings.TrimPrefix(destination.URL, "http://"))
	require.NoError(t, err)
	hop := "http://localhost:" + port + "/next"

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, hop, http.StatusFound)
	}))
	defer origin.Close()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxRequestsPerSecondPerProcess("localhost", 0.001))
	require.NoError(t, err)

	ctx, _ := auditing(t)

	// Drains the hop host's bucket, so the redirect below is what meets an
	// empty one.
	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url": v1.NewValue(hop),
	}, &v1.Scope{Identity: testIdentity()})
	require.NoError(t, err)

	_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
		"url":    v1.NewValue(origin.URL + "/start"),
		"method": v1.NewValue(http.MethodPost),
	}, &v1.Scope{Identity: testIdentity()})
	require.Error(t, err)

	require.Equal(t, v1.ErrorKindUpstreamUnknown, v1.ClassifyError(err),
		"the original POST reached its peer before the next hop was held back, so whether it took effect is unknown")
	require.False(t, v1.ClassifyError(err).Retryable())
}

// TestALocalRehearsalRecordsNothing: `flow run local` installs no auditor, and
// the seams behave exactly as they did before this existed. The exemption is
// argued in the audit package's doc; this is the assertion that it holds.
func TestALocalRehearsalRecordsNothing(t *testing.T) {
	t.Parallel()

	require.Nil(t, v1.EnforcementAuditorIn(t.Context()),
		"a context with no auditor resolves to none, which records nothing")

	policy, err := v1.TaskPolicyConfig{Deny: []string{`task == "http"`}}.Policy()
	require.NoError(t, err)

	ctx := v1.NewContextWithTaskPolicy(t.Context(), policy)
	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", testIdentity(), true))
	require.Error(t, v1.CheckTaskPolicy(ctx, "http", testIdentity(), true))
}

// TestEveryEnforcementPointIsRecordedBySomeSeam closes the vocabulary end of
// #1018's first question — "what is audited" — for the worker's half: a point
// in the schema that no seam emits is a spelling nobody reads, and a seam
// added without one would be a decision nothing records.
//
// Derived from the enum rather than from a list of it, so a fifth enforcement
// point cannot arrive without a seam that emits it.
func TestEveryEnforcementPointIsRecordedBySomeSeam(t *testing.T) {
	t.Parallel()

	emitters := map[v1.AuditEnforcementPoint]func(t *testing.T, ctx context.Context){
		v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH: func(t *testing.T, ctx context.Context) {
			require.NoError(t, v1.CheckTaskPolicy(ctx, "log", testIdentity(), false))
		},
		v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_SECRET_ACCESS: func(t *testing.T, ctx context.Context) {
			ctx = v1.ContextWithTaskRuntime(ctx, secretRuntime(t, "material",
				auth.SecretAccessPolicy{Allow: []string{"true"}}))
			_, err := v1.ResolveSecret(ctx, secrets.NewRef("env", "API_TOKEN"))
			require.NoError(t, err)
		},
		v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_EGRESS: func(t *testing.T, ctx context.Context) {
			policy, err := netpolicy.New()
			require.NoError(t, err)
			_, err = v1.HTTPTaskDef(policy).Fn(ctx, map[string]*v1.Value{
				"url": v1.NewValue("http://127.0.0.1:9/refused"),
			}, &v1.Scope{})
			require.Error(t, err)
		},
		v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_CREDENTIAL_ASSUMPTION: func(t *testing.T, ctx context.Context) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example", nil)
			require.NoError(t, err)
			require.Error(t, v1.AuthorizeCredential(ctx, req, "partner-api"))
		},
	}

	points := v1.AuditEnforcementPoint(0).Descriptor().Values()
	for i := range points.Len() {
		point := v1.AuditEnforcementPoint(points.Get(i).Number())
		if point == v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_UNSPECIFIED {
			continue
		}

		emit, ok := emitters[point]
		require.True(t, ok,
			"%s is in the schema and no seam in this test emits it; a point nothing writes "+
				"is a spelling nobody reads — see proto/flowstate/v1/audit.proto", point)

		ctx, sink := auditing(t)
		emit(t, ctx)

		records := sink.all()
		require.NotEmpty(t, records, "%s recorded nothing", point)
		for _, record := range records {
			require.Equal(t, point, record.GetEnforcementPoint())
		}
	}
}
