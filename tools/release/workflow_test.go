package main

import (
	"os"
	"testing"

	yaml "github.com/goccy/go-yaml"
)

type releaseWorkflow struct {
	On          map[string]any    `yaml:"on"`
	Permissions map[string]string `yaml:"permissions"`
	Jobs        map[string]struct {
		If          string            `yaml:"if"`
		Environment string            `yaml:"environment"`
		Permissions map[string]string `yaml:"permissions"`
	} `yaml:"jobs"`
}

func TestReleaseWorkflowKeepsPublicationBehindBothInterlocks(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse release workflow: %v", err)
	}
	if len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
		t.Fatalf("release triggers = %v, want workflow_dispatch only", workflow.On)
	}
	if len(workflow.Permissions) != 1 || workflow.Permissions["contents"] != "read" {
		t.Fatalf("default permissions = %v, want contents: read only", workflow.Permissions)
	}
	publish, ok := workflow.Jobs["publish"]
	if !ok {
		t.Fatal("release workflow has no publish job")
	}
	if publish.If != "inputs.publish && vars.RELEASES_ENABLED == 'true'" {
		t.Fatalf("publish condition = %q", publish.If)
	}
	if publish.Environment != "release" {
		t.Fatalf("publish environment = %q, want release", publish.Environment)
	}
	for permission, want := range map[string]string{
		"artifact-metadata": "write",
		"attestations":      "write",
		"contents":          "write",
		"id-token":          "write",
	} {
		if publish.Permissions[permission] != want {
			t.Errorf("publish permission %s = %q, want %q", permission, publish.Permissions[permission], want)
		}
	}
	if len(publish.Permissions) != 4 {
		t.Errorf("publish permissions = %v, want exactly the four release grants", publish.Permissions)
	}
	disabled, ok := workflow.Jobs["publication-disabled"]
	if !ok {
		t.Fatal("a disabled publication request would silently succeed")
	}
	if disabled.If != "inputs.publish && vars.RELEASES_ENABLED != 'true'" {
		t.Fatalf("publication-disabled condition = %q", disabled.If)
	}
}
