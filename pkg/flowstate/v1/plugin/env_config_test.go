package plugin

import (
	"errors"
	"slices"
	"strings"
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

// TestAnEmptyPluginNameIsRefusedAtValidation covers the root cause behind the
// weekly deep-tier fuzz job's FuzzParseEnvConfig crasher (picatz/flowstate
// #2105): YAML lets a mapping use the empty string as a key just like any
// other, so a well-formed document — a flow mapping's key-only shorthand, or
// an explicit `"":` line — decodes [EnvConfig.Env] to a map keyed by "".
//
// [EnvConfig.ByPlugin] renders that key faithfully, the same as any other: it
// is shape, not policy (its doc comment on [ParseEnvConfig] says so), and the
// crasher was a fuzz property that expected otherwise, not a defect in the
// product. [Config.validate] is the one check every source of EnvByPlugin — a
// file, or `--plugin-env` — runs through before a host is built, and it
// already refuses "" the same way it refuses "GitHub" or a name over
// [MaxNameLen]: this proves that refusal still holds for the shape the
// fuzzer found.
func TestAnEmptyPluginNameIsRefusedAtValidation(t *testing.T) {
	t.Parallel()

	for name, document := range map[string]string{
		"empty key, empty mapping value": "env:\n  \"\": {}\n",
		"empty key, no value at all":     "env:\n  \"\":\n",
	} {
		cfg, err := ParseEnvConfig([]byte(document))
		if err != nil {
			t.Fatalf("%s: parsing: %v", name, err)
		}
		if _, ok := cfg.Env[""]; !ok {
			t.Fatalf("%s: decoded Env has no entry under the empty key; document no longer exercises the shape under test", name)
		}

		out := cfg.ByPlugin()
		if _, ok := out[""]; !ok {
			t.Fatalf("%s: ByPlugin dropped the empty key instead of rendering it faithfully; this test no longer exercises Config.validate's refusal", name)
		}

		err = Config{EnvByPlugin: out}.validate()
		if err == nil {
			t.Fatalf("%s: Config.validate accepted an EnvByPlugin entry under the empty plugin name", name)
		}
		if !strings.Contains(err.Error(), "not a valid plugin name") {
			t.Errorf("%s: validate error = %v, want one naming the empty key as not a valid plugin name", name, err)
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
