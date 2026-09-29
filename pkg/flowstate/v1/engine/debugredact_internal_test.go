package engine

import (
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
		scope: &v1.Scope{Identity: &v1.WorkloadIdentity{Namespace: "team-a"}, Inputs: map[string]*v1.Value{"token": v1.NewLiteral(secret)}},
		curSpec: &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
			{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
		}},
		debug: &debugControl{carry: &v1.DebugCarry{SessionId: "s"}},
	}

	e.observeForDebug(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, &v1.Node{Id: "call"},
		"request to https://example.com/?t="+secret+" failed")

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
		scope:   &v1.Scope{Identity: &v1.WorkloadIdentity{Namespace: "team-a"}, Inputs: map[string]*v1.Value{"api_key": v1.NewLiteral(secret)}},
		curSpec: callee,
		debug: &debugControl{
			carry:         &v1.DebugCarry{SessionId: "s"},
			rootSensitive: v1.SensitiveInputValues(map[string]*v1.Value{"token": v1.NewLiteral(secret)}, v1.SensitiveInputNames(root)),
		},
	}

	e.observeForDebug(v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED, &v1.Node{Id: "call"},
		"authorization "+secret+" was refused")

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
		scope: &v1.Scope{Identity: &v1.WorkloadIdentity{Namespace: "team-a"}, Inputs: map[string]*v1.Value{
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
