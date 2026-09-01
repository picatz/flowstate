package flowstatev1_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
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

func assumeBroker(t *testing.T, token string, opts ...auth.BrokerOption) *auth.Broker {
	t.Helper()

	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("test", private)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer,
		append([]auth.BrokerOption{auth.WithTarget("partner-api", stubExchanger{token: token})}, opts...)...)
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
