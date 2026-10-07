package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestADurableObservationWithholdsSensitiveInputs: a task's error can quote the
// value it was given, and what a durable session reads back is a transcript,
// so the run withholds its declared-sensitive inputs from it as inspection
// does.
func TestADurableObservationWithholdsSensitiveInputs(t *testing.T) {
	const secret = "hunter2-correct-horse"
	e := &executor{
		scope: &v1.Scope{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "team-a"}}, Inputs: map[string]*v1.Value{"token": v1.NewLiteral(secret)}},
		curSpec: &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
			{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
		}},
		debug: &debugControl{carry: &v1.DebugCarry{SessionId: "s"}},
	}

	e.observeForDebug(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, &v1.Node{Id: "call"},
		errors.New("request to https://example.com/?t="+secret+" failed"))

	if len(e.debug.observations) != 1 {
		t.Fatalf("observations = %d, want one", len(e.debug.observations))
	}
	if text := e.debug.observations[0].GetText(); strings.Contains(text, secret) {
		t.Fatalf("a durable observation carried a sensitive input: %q", text)
	}
}

// TestACalleeObservationWithholdsTheRunsSensitiveInputs: sensitivity belongs
// to a value's origin. A callee that received the run's sensitive input under
// a name it does not declare sensitive still does not show it.
func TestACalleeObservationWithholdsTheRunsSensitiveInputs(t *testing.T) {
	const secret = "hunter2-correct-horse"
	root := &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
		{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
	}}
	callee := &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
		{Name: "api_key", Type: v1.InputDeclaration_TYPE_STRING},
	}}
	e := &executor{
		scope:   &v1.Scope{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "team-a"}}, Inputs: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)}},
		curSpec: callee,
		debug: &debugControl{
			carry:         &v1.DebugCarry{SessionId: "s"},
			rootSensitive: v1.SensitiveInputValues(map[string]*v1.Value{"token": v1.NewLiteral(secret)}, v1.SensitiveInputNames(root)),
		},
	}

	e.observeForDebug(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, &v1.Node{Id: "call"},
		errors.New("authorization "+secret+" was refused"))

	if text := e.debug.observations[0].GetText(); strings.Contains(text, secret) {
		t.Fatalf("a callee's observation carried the run's sensitive input: %q", text)
	}
}

// TestADurableMissedUntilIsRedactedAndStillSaid: the notice's `until` is a
// session's text, and a sensitive value can be in it, or be one of the fixed
// words the notice is recognized by. Only the `until` is redacted, so the
// notice still reads as itself, and it is bounded after redaction, so a target
// that redaction lengthens still closes its quote under the durable cap
// (Codex, #2204).
func TestADurableMissedUntilIsRedactedAndStillSaid(t *testing.T) {
	until := strings.Repeat("ab/", 85) + "ab"
	e := &executor{
		scope: &v1.Scope{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "team-a"}}, Inputs: map[string]*v1.Value{
			"word": v1.NewLiteral("run"), "pair": v1.NewLiteral("ab"),
		}},
		curSpec: &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
			{Name: "word", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
			{Name: "pair", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
		}},
		debug: &debugControl{carry: &v1.DebugCarry{
			SessionId: "s", Next: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: until,
		}},
	}

	e.debugRunCompleted()

	if len(e.debug.observations) != 1 {
		t.Fatalf("observations = %d, want one", len(e.debug.observations))
	}
	text := e.debug.observations[0].GetText()
	if !strings.HasPrefix(text, "the run completed without stopping at `until [redacted]/[redacted]") {
		t.Errorf("the notice's own words were redacted, or its `until` was not: %q", text)
	}
	if !strings.HasSuffix(text, "…`") {
		t.Errorf("the redacted `until` was not cut inside the notice, or the notice does not close its quote: %q", text)
	}
	if runes := len([]rune(text)); runes > maxDebugObservationRunes {
		t.Errorf("the notice is %d runes, past the %d the durable driver keeps", runes, maxDebugObservationRunes)
	}
	if strings.Contains(strings.ReplaceAll(text, "[redacted]", ""), "ab") {
		t.Errorf("the notice carried a sensitive input: %q", text)
	}
}

// TestAMissedUntilInheritedFromACalleesHoldIsWithheld: a segment that
// inherited a pending `until` through Continue-As-New from a hold inside a
// callee does not know what that callee withheld, so the notice withholds the
// target whole rather than show a value only the callee declared sensitive
// (Codex, #2204).
func TestAMissedUntilInheritedFromACalleesHoldIsWithheld(t *testing.T) {
	notice := func(depth int32) string {
		e := &executor{
			scope: &v1.Scope{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "team-a"}}},
			debug: &debugControl{carry: &v1.DebugCarry{
				SessionId: "s", Next: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "nested/greet", StepDepth: depth,
			}},
		}
		e.debugRunCompleted()
		if len(e.debug.observations) != 1 {
			t.Fatalf("observations = %d, want one", len(e.debug.observations))
		}

		return e.debug.observations[0].GetText()
	}

	if got, want := notice(1), "the run completed without stopping at `until [redacted]`"; got != want {
		t.Errorf("an inherited `until` from a callee's hold: notice = %q, want %q", got, want)
	}
	if got, want := notice(0), "the run completed without stopping at `until nested/greet`"; got != want {
		t.Errorf("an inherited `until` from the root's hold: notice = %q, want %q", got, want)
	}
}

// TestPendingPausesAreBounded: a run applying asks at boundaries it cannot
// hold at keeps at most [v1.MaxDebugAsksPerBoundary] pauses waiting, and
// refuses the next at once rather than keep it (Codex, #2220).
func TestPendingPausesAreBounded(t *testing.T) {
	d := &debugControl{carry: &v1.DebugCarry{SessionId: "s"}}
	for i := range v1.MaxDebugAsksPerBoundary + 1 {
		d.pausePending(fmt.Sprintf("pause-%d", i))
	}

	if got := len(d.pendingPauses); got != v1.MaxDebugAsksPerBoundary {
		t.Fatalf("pending pauses = %d, want %d", got, v1.MaxDebugAsksPerBoundary)
	}
	receipt := d.receiptFor(fmt.Sprintf("pause-%d", v1.MaxDebugAsksPerBoundary))
	if receipt.GetStatus() != v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED {
		t.Fatalf("the pause past the bound was answered %v, want REFUSED", receipt.GetStatus())
	}
}

// TestARetriedPauseIsTakenOnce: a pause retried under its request id while it
// still waits takes no second place, so it is neither receipted twice nor
// counted twice against the bound.
func TestARetriedPauseIsTakenOnce(t *testing.T) {
	d := &debugControl{carry: &v1.DebugCarry{SessionId: "s"}}
	d.pausePending("pause")
	d.pausePending("pause")

	if got := len(d.pendingPauses); got != 1 {
		t.Fatalf("pending pauses = %d, want 1", got)
	}
}
