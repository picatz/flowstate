package plugin

import (
	"errors"
	"os/exec"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestProfileSelectionRefusesUnknownProfilesAtStartup(t *testing.T) {
	cfg := Config{PluginProfiles: map[string]ProfileSelection{
		"example": {Admission: "production", Isolation: "production"},
	}}
	err := cfg.validate()
	if !errors.Is(err, ErrAdmissionProfile) {
		t.Fatalf("validate error = %v, want %v", err, ErrAdmissionProfile)
	}
}

func TestPrivilegeDroppingRequiresDedicatedIdentity(t *testing.T) {
	cfg := Config{IsolationProfiles: map[string]*flowstatev1.PluginIsolationProfile{
		"production": {Name: "production", DropPrivileges: true},
	}}
	err := cfg.validate()
	if !errors.Is(err, ErrIsolationProfile) {
		t.Fatalf("validate error = %v, want %v", err, ErrIsolationProfile)
	}
}

func TestUnsupportedIsolationControlIsRefused(t *testing.T) {
	err := configureIsolation(exec.Command("ignored"), &flowstatev1.PluginIsolationProfile{
		Name:            "production",
		ReadOnlyRoot:    true,
		AllowedSyscalls: []string{"read", "write"},
	})
	if !errors.Is(err, ErrIsolationProfile) {
		t.Fatalf("configureIsolation error = %v, want %v", err, ErrIsolationProfile)
	}
}
