package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func loadManifest(t *testing.T) manifest {
	t.Helper()
	b, err := os.ReadFile("../../security/operations/registry.json")
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// Mutation-style: deleting the descriptor entry models adding/re-exposing an RPC
// without updating the authorization inventory.
func TestGateDetectsRemovedRegistryEntry(t *testing.T) {
	m := loadManifest(t)
	for i, o := range m.Operations {
		if o.Operation == "flowstate.v1.WorkflowService/Run" {
			m.Operations = append(m.Operations[:i], m.Operations[i+1:]...)
			break
		}
	}
	err := verify(m)
	if err == nil || !strings.Contains(err.Error(), "RPC has no registered authorization action") {
		t.Fatalf("mutation escaped gate: %v", err)
	}
}

// Mutation-style: removing the named enforcement call/path must not degrade to
// documentation-only coverage while the operation remains reachable.
func TestGateDetectsRemovedEnforcementPath(t *testing.T) {
	m := loadManifest(t)
	m.Operations[0].Enforcement = ""
	err := verify(m)
	if err == nil || !strings.Contains(err.Error(), "no enforcement point") {
		t.Fatalf("mutation escaped gate: %v", err)
	}
}

func TestGateDetectsRemovedAuditInstrumentation(t *testing.T) {
	m := loadManifest(t)
	m.Operations[0].Audit = ""
	err := verify(m)
	if err == nil || !strings.Contains(err.Error(), "no decision/audit instrumentation") {
		t.Fatalf("mutation escaped gate: %v", err)
	}
}
