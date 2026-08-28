package flowstatev1_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// identity builds an attested caller for these tests.
func debugIdentity(issuer, subject string, claims map[string]string) *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{
		Issuer:    issuer,
		Subject:   subject,
		Namespace: "team-a",
		Claims:    claims,
	}
}

// debugPolicy is the shape a `debug:` stanza compiles to.
func debugPolicy(rules ...*v1.SignalPolicyRule) *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: rules}
}

// TestADebugLeaseIsHonouredUpToTheCeilingAndNeverPastIt checks all three
// answers [v1.BoundDebugLease] can give, and checks the honoured one first.
//
// The order is the point rather than a habit: a clamp that returned the ceiling
// for *every* request would satisfy an assertion that a large request is capped,
// and only the middle case can tell a ceiling from a constant.
func TestADebugLeaseIsHonouredUpToTheCeilingAndNeverPastIt(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30*time.Second, v1.BoundDebugLease(30*time.Second),
		"a request inside the ceiling is honoured at its value, not replaced by the default")
	assert.Equal(t, v1.MaxDebugLease, v1.BoundDebugLease(v1.MaxDebugLease),
		"a request exactly at the ceiling is honoured")

	assert.Equal(t, v1.MaxDebugLease, v1.BoundDebugLease(v1.MaxDebugLease+time.Nanosecond),
		"a request one tick past the ceiling is cut to it")
	assert.Equal(t, v1.MaxDebugLease, v1.BoundDebugLease(365*24*time.Hour),
		"a holder does not get to widen the ceiling by asking for a year")

	assert.Equal(t, v1.DefaultDebugLease, v1.BoundDebugLease(0),
		"asking for nothing gets the default rather than a lease of no length")
	assert.Equal(t, v1.DefaultDebugLease, v1.BoundDebugLease(-time.Hour),
		"a negative request fails toward the default, which is the shorter hold")

	require.LessOrEqual(t, v1.DefaultDebugLease, v1.MaxDebugLease,
		"the default has to be reachable, or every ask is silently capped")
}

// TestAWorkflowWithNoDebugStanzaIsNotDebuggable is the fail-closed zero case,
// and the one place this policy differs from its neighbour.
//
// The positive direction runs first, so the refusals below are known to be
// about the missing policy rather than about a matcher that refuses everybody.
func TestAWorkflowWithNoDebugStanzaIsNotDebuggable(t *testing.T) {
	t.Parallel()

	caller := debugIdentity("https://idp.example", "sre-1", map[string]string{"role": "sre"})
	policy := debugPolicy(&v1.SignalPolicyRule{Claims: map[string]string{"role": "sre"}})

	require.NoError(t, v1.DebugPolicyCheck(policy, caller, nil, false),
		"a caller matching a declared rule may take a lease")

	absent := v1.DebugPolicyCheck(nil, caller, nil, false)
	require.Error(t, absent,
		"a workflow that declares no `debug:` stanza is not debuggable by anybody")
	require.Error(t, v1.DebugPolicyCheck(debugPolicy(), caller, nil, false),
		"a policy with no rules authorizes nobody rather than everybody")

	// The refusal has to *say* which case this is, and that sentence is the
	// whole of what the zero-case arm adds — [v1.SignalPolicyCheck] already
	// refuses an empty rule list on its own, so deleting the arm would leave
	// the outcome right and the explanation wrong. "The sender does not match
	// any rule this policy declares" told about a workflow with no policy at
	// all sends whoever reads it looking for a rule to fix, and there is none:
	// the file needs a stanza. A diagnostic that misdescribes what happened is
	// worse than no diagnostic (CLAUDE.md).
	assert.Contains(t, absent.Error(), "declares no `debug:` policy",
		"the refusal for an absent stanza reads as though a policy existed and rejected the caller")

	unmatched := v1.DebugPolicyCheck(
		debugPolicy(&v1.SignalPolicyRule{Subject: "https://idp.example#nobody"}), caller, nil, false)
	require.Error(t, unmatched)
	assert.NotContains(t, unmatched.Error(), "declares no `debug:` policy",
		"a caller who simply matched no rule was told the workflow declares nothing")

	// The other direction of the same boundary: an ordinary signal name with no
	// policy is *allowed*, which is what makes the debug zero case a decision
	// rather than a copy.
	require.Error(t, v1.SignalPolicyCheck(debugPolicy(), caller, nil, false),
		"a signal policy with no rules refuses too — the difference is the absent policy, not the empty one")
}

// TestADebugPolicyIsCheckedByTheSignalPolicyMatcher pins the delegation rather
// than the words: if [v1.DebugPolicyCheck] grew a matcher of its own,
// `distinct_from_starter` — which lives on the policy and is ANDed onto every
// rule — is the first thing a copy would drop.
func TestADebugPolicyIsCheckedByTheSignalPolicyMatcher(t *testing.T) {
	t.Parallel()

	starter := debugIdentity("https://idp.example", "starter", nil)
	other := debugIdentity("https://idp.example", "sre-1", nil)

	policy := debugPolicy(&v1.SignalPolicyRule{Namespace: "team-a"})
	policy.DistinctFromStarter = true

	require.NoError(t, v1.DebugPolicyCheck(policy, other, starter, true),
		"somebody who is not the starter may debug under a separation-of-duties policy")

	assert.Error(t, v1.DebugPolicyCheck(policy, starter, starter, true),
		"the run's own starter may not debug it when the policy demands separation")
	assert.Error(t, v1.DebugPolicyCheck(policy, other, nil, false),
		"a run with no recorded starter cannot prove separation, so it does not get it")

	// A rule nobody matches is refused whatever the separation rule says.
	assert.Error(t, v1.DebugPolicyCheck(
		debugPolicy(&v1.SignalPolicyRule{Subject: "https://idp.example#somebody-else"}), other, nil, false),
		"a caller matching no rule is refused")
}

// TestOnlyTheHolderMayBeTheHolder is the negative direction of the lease's
// ownership check: not that a holder recognizes itself, but that nobody else
// does.
func TestOnlyTheHolderMayBeTheHolder(t *testing.T) {
	t.Parallel()

	holder := debugIdentity("https://idp.example", "sre-1", map[string]string{"role": "sre"})
	lease := &v1.DebugSession{AttachedBy: holder}

	require.True(t, v1.DebugLeaseHolder(lease, holder),
		"the identity that took the lease holds it")

	assert.False(t, v1.DebugLeaseHolder(lease, debugIdentity("https://idp.example", "sre-2", nil)),
		"a different subject at the same issuer does not hold it")
	assert.False(t, v1.DebugLeaseHolder(lease, debugIdentity("https://other.example", "sre-1", nil)),
		"the same subject at a different issuer does not hold it — a subject is unique only within its issuer")
	assert.False(t, v1.DebugLeaseHolder(nil, holder),
		"nobody holds a lease that does not exist")

	// Claims and namespace are deliberately not part of the comparison: a token
	// refreshed with one fewer group must not make a lease unreleasable.
	assert.True(t, v1.DebugLeaseHolder(lease, &v1.WorkloadIdentity{
		Issuer: "https://idp.example", Subject: "sre-1", Namespace: "team-b",
	}), "the holder is still the holder after their claims and namespace moved")
}

// TestALeaseThatHasLapsedHoldsNothing pins the boundary condition, including
// the exact instant — which is the one a virtual clock advanced to the expiry
// lands on, and the one a fixture would otherwise be unable to act on.
func TestALeaseThatHasLapsedHoldsNothing(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	lease := &v1.DebugSession{LeaseExpiresAt: timestamppb.New(at)}

	require.True(t, v1.DebugLeaseHeld(lease, at.Add(-time.Nanosecond)),
		"a lease one tick before its expiry is still holding")

	assert.False(t, v1.DebugLeaseHeld(lease, at),
		"a lease at exactly its expiry has lapsed")
	assert.False(t, v1.DebugLeaseHeld(lease, at.Add(time.Hour)),
		"a lease past its expiry has lapsed")
	assert.False(t, v1.DebugLeaseHeld(nil, at),
		"there is no lease")
	assert.False(t, v1.DebugLeaseHeld(&v1.DebugSession{AttachedBy: debugIdentity("i", "s", nil)}, at),
		"a durable lease with no expiry holds nothing rather than holding forever")
}

// TestALeaseRequestThatCannotBeReadAsksForNothing checks the parser fails
// toward the shorter hold, and checks first that it can read a real request —
// a parser that always answered zero would satisfy every refusal below.
func TestALeaseRequestThatCannotBeReadAsksForNothing(t *testing.T) {
	t.Parallel()

	read := v1.DebugLeaseRequested(&v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		v1.DebugLeaseInput: v1.NewLiteral("90s"),
	}})
	require.Equal(t, 90*time.Second, read, "a well-formed request is read at its value")

	for name, payload := range map[string]*v1.Node_Outputs{
		"no payload at all": nil,
		"no lease key":      {NamedValues: map[string]*v1.Value{"something": v1.NewLiteral("90s")}},
		"an empty string":   {NamedValues: map[string]*v1.Value{v1.DebugLeaseInput: v1.NewLiteral("")}},
		"not a duration":    {NamedValues: map[string]*v1.Value{v1.DebugLeaseInput: v1.NewLiteral("soon")}},
		"not a string":      {NamedValues: map[string]*v1.Value{v1.DebugLeaseInput: v1.NewLiteral(90)}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Zero(t, v1.DebugLeaseRequested(payload),
				"an unreadable request asks for nothing, so the default — the shorter hold — applies")
		})
	}

	assert.Equal(t, v1.DefaultDebugLease, v1.BoundDebugLease(v1.DebugLeaseRequested(nil)),
		"and the two compose to the default rather than to a lease of no length")
}

// TestTheEngineOwnsTheReservedSignalPrefix is the collision the reservation
// exists to prevent: a `wait_for_signal:` a pause ask would answer.
func TestTheEngineOwnsTheReservedSignalPrefix(t *testing.T) {
	t.Parallel()

	require.True(t, v1.IsReservedSignalName(v1.DebugPauseSignal))
	require.True(t, v1.IsReservedSignalName(v1.DebugResumeSignal))
	require.True(t, v1.IsDebugSignalName(v1.DebugPauseSignal))
	require.True(t, v1.IsDebugSignalName(v1.DebugResumeSignal))

	assert.False(t, v1.IsReservedSignalName("deploy-approved"),
		"an ordinary signal name is the author's")
	assert.True(t, v1.IsReservedSignalName(v1.ReservedSignalPrefix+"whatever"),
		"the whole prefix is reserved, not only the two names this build reads")
	assert.False(t, v1.IsDebugSignalName(v1.ReservedSignalPrefix+"whatever"),
		"a reserved name this build has no channel for is not a debug ask")

	// Both reserved names must be spellable through the door the ask arrives at,
	// or the reservation would describe names nobody could ever send.
	for _, name := range v1.DebugSignalNames() {
		assert.NoError(t, v1.Validate(&v1.SignalRequest{WorkflowId: "run-1", Name: name}),
			"%s has to satisfy SignalRequest.name's own pattern, or it can never be delivered", name)
	}

	ordinary := &v1.Workflow{
		Name: "ordinary", Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{Id: "gate", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
		}}}},
	}
	require.NoError(t, v1.CheckReservedSignalNames(ordinary),
		"an ordinary workflow is untouched by the reservation")

	colliding := &v1.Workflow{
		Name: "colliding", Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{Id: "gate", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: v1.DebugPauseSignal}},
		}}}},
	}
	assert.Error(t, v1.CheckReservedSignalNames(colliding),
		"a wait on a reserved channel would be answered by a pause ask")

	policied := &v1.Workflow{
		Name: "policied", Profile: v1.CurrentProfile,
		Signals: map[string]*v1.SignalPolicy{
			v1.DebugPauseSignal: debugPolicy(&v1.SignalPolicyRule{Namespace: "team-a"}),
		},
	}
	assert.Error(t, v1.CheckReservedSignalNames(policied),
		"who may debug is `debug:`, not a `signals:` entry under a reserved name")
}

// TestADebugPolicyIsHeldToASignalPolicysShapeRules checks the shared checker is
// shared — and that its diagnostics name the stanza the author wrote, not the
// one it borrowed its grammar from.
func TestADebugPolicyIsHeldToASignalPolicysShapeRules(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckDebugPolicy(nil, false),
		"a workflow with no `debug:` is well formed and simply not debuggable")
	require.NoError(t, v1.CheckDebugPolicy(
		debugPolicy(&v1.SignalPolicyRule{Claims: map[string]string{"role": "sre"}}), false),
		"an ordinary rule is accepted")

	for name, policy := range map[string]*v1.SignalPolicy{
		"no rules at all":        debugPolicy(),
		"a rule matching all":    debugPolicy(&v1.SignalPolicyRule{}),
		"an unqualified subject": debugPolicy(&v1.SignalPolicyRule{Subject: "sre-1"}),
		"an unnarrowed subject_from": debugPolicy(&v1.SignalPolicyRule{
			SubjectFrom: v1.NewExpr("inputs.approver"),
		}),
	} {
		t.Run(name, func(t *testing.T) {
			err := v1.CheckDebugPolicy(policy, false)
			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "debug"),
				"the diagnostic names `debug:`, not `signals:`: %s", err)
			assert.NotContains(t, err.Error(), "signals",
				"an author reading a fault about `debug:` is not told about a stanza they did not write")
		})
	}

	resolved := debugPolicy(&v1.SignalPolicyRule{
		SubjectFrom: v1.NewExpr("inputs.approver"),
		Claims:      map[string]string{"role": "sre"},
	})
	require.NoError(t, v1.CheckDebugPolicy(resolved, false),
		"a narrowed expression is legal in a workflow's own declaration")
	assert.Error(t, v1.CheckDebugPolicy(resolved, true),
		"and is corruption once the policy has been through resolution and frozen")
}

// TestADebugLeaseTakesEveryFactFromWhatWasAttested is the containment claim for
// the lease itself: nothing in it comes from what the ask *said*.
func TestADebugLeaseTakesEveryFactFromWhatWasAttested(t *testing.T) {
	t.Parallel()

	accepted := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	noticed := accepted.Add(7 * time.Minute)

	sender := &v1.SignalSender{
		Identity:   debugIdentity("https://idp.example", "sre-1", map[string]string{"role": "sre"}),
		AcceptedAt: timestamppb.New(accepted),
	}

	lease := v1.NewDebugLease("run-1/debug/0", &v1.RunAddress{RunId: "run-1"}, sender, noticed, 45*time.Second)

	assert.Equal(t, "sre-1", lease.GetAttachedBy().GetSubject(),
		"the holder is the identity the server attested")
	assert.Equal(t, accepted, lease.GetAttachedAt().AsTime(),
		"attached_at is when the server accepted the ask, not when a boundary noticed it")
	assert.Equal(t, noticed.Add(45*time.Second), lease.GetLeaseExpiresAt().AsTime(),
		"the lease runs from the boundary's own deterministic clock")
	assert.False(t, lease.GetLocal(), "a durable lease is never marked local")

	capped := v1.NewDebugLease("run-1/debug/0", nil, sender, noticed, 365*24*time.Hour)
	assert.Equal(t, noticed.Add(v1.MaxDebugLease), capped.GetLeaseExpiresAt().AsTime(),
		"an over-long request is cut where it is built, not where it is read")

	require.NoError(t, v1.Validate(lease), "the lease is a message the schema accepts")
}

// TestTheLeaseCeilingsAreDerivedRatherThanTyped pins the derivation itself.
//
// The numbers are deliberately written as the step-budget constants they come
// from, so this asserts the *identity* rather than a value: a change that
// replaced one with a literal would leave both numbers correct today and let
// them drift the first time anybody moved the other.
func TestTheLeaseCeilingsAreDerivedRatherThanTyped(t *testing.T) {
	t.Parallel()

	assert.Equal(t, v1.DefaultStartToCloseTimeout, v1.DefaultDebugLease,
		"the default hold is one attempt at one step, read from that constant")
	assert.Equal(t, v1.DefaultScheduleToCloseTimeout, v1.MaxDebugLease,
		"the ceiling is the longest one step may legitimately take, read from that constant")
}

// TestAWorkflowMayDeclareBothStanzas is the reachability claim from the
// schema's side: a specification carrying a `debug:` policy beside `signals:`
// is one the validator accepts and the wire can carry.
func TestAWorkflowMayDeclareBothStanzas(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name:    "both",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{Id: "gate", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
			Timeout: durationpb.New(time.Hour),
		}}}},
		Signals: map[string]*v1.SignalPolicy{
			"deploy-approved": debugPolicy(&v1.SignalPolicyRule{Claims: map[string]string{"role": "approver"}}),
		},
		Debug: debugPolicy(&v1.SignalPolicyRule{Claims: map[string]string{"role": "sre"}}),
	}

	require.NoError(t, v1.Validate(wf))
	require.NoError(t, v1.CheckSignalPolicies(wf))
	require.NoError(t, v1.CheckDebugPolicy(wf.GetDebug(), false))
	require.NoError(t, v1.CheckReservedSignalNames(wf))

	assert.NotEqual(t, wf.GetSignals()["deploy-approved"].GetAllow()[0].GetClaims(),
		wf.GetDebug().GetAllow()[0].GetClaims(),
		"who may approve and who may debug are separate answers to separate questions")
}
