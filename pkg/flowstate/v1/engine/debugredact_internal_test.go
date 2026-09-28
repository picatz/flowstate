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
