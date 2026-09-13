package plugin

import (
	"errors"
	"slices"
	"testing"
)

// TestAnEnvironmentFileBecomesTheLaunchEntriesInAStableOrder covers the whole of
// what the file form does: it is shape, not policy, and the order it renders in
// is fixed so that two launches of one file can be compared.
func TestAnEnvironmentFileBecomesTheLaunchEntriesInAStableOrder(t *testing.T) {
	t.Parallel()

	cfg, err := ParseEnvConfig([]byte(`
env:
  oci:
    FLOWSTATE_OCI_REGISTRIES: /etc/flowstate/oci.yaml
    FLOWSTATE_OCI_CACHE: /var/cache/flowstate
  ssh:
    FLOWSTATE_SSH_GRANTS: /etc/flowstate/ssh-grants.yaml
`))
	if err != nil {
		t.Fatalf("parsing a well-formed environment file: %v", err)
	}

	byPlugin := cfg.ByPlugin()
	want := []string{
		"FLOWSTATE_OCI_CACHE=/var/cache/flowstate",
		"FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml",
	}
	if !slices.Equal(byPlugin["oci"], want) {
		t.Errorf("oci entries = %q, want %q sorted by variable name", byPlugin["oci"], want)
	}
	if got := byPlugin["ssh"]; !slices.Equal(got, []string{"FLOWSTATE_SSH_GRANTS=/etc/flowstate/ssh-grants.yaml"}) {
		t.Errorf("ssh entries = %q", got)
	}
}

// TestAMisspelledOrRepeatedEnvironmentKeyIsRefused is why the file is parsed
// strictly: a deployment whose file says something this package does not read is
// a plugin launched without the configuration its operator wrote, discovered
// whenever that plugin next fails.
func TestAMisspelledOrRepeatedEnvironmentKeyIsRefused(t *testing.T) {
	t.Parallel()

	for name, document := range map[string]string{
		"unknown top-level field": "envs:\n  oci:\n    A: b\n",
		"one variable set twice":  "env:\n  oci:\n    A: b\n    A: c\n",
		"a plugin named twice":    "env:\n  oci:\n    A: b\n  oci:\n    C: d\n",
	} {
		if _, err := ParseEnvConfig([]byte(document)); err == nil {
			t.Errorf("%s was accepted", name)
		} else if !errors.Is(err, ErrPluginEnv) {
			t.Errorf("%s: error is %v, want one that is ErrPluginEnv", name, err)
		}
	}
}

// TestAnEmptyEnvironmentFileGrantsNothing keeps "configured nothing" and
// "configured an empty document" the same answer, which is the answer that
// leaves every plugin launched exactly as it was.
func TestAnEmptyEnvironmentFileGrantsNothing(t *testing.T) {
	t.Parallel()

	cfg, err := ParseEnvConfig(nil)
	if err != nil {
		t.Fatalf("parsing an empty environment file: %v", err)
	}
	if got := cfg.ByPlugin(); got != nil {
		t.Errorf("ByPlugin() = %v, want nil", got)
	}
}
