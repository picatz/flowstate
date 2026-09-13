package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeGrants writes a grants file and points the environment at it, the way a
// worker's --plugin-env does.
func writeGrants(t *testing.T, document string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "grants.yaml")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing grants: %v", err)
	}
	t.Setenv(grantsEnv, path)
	return path
}

// validDocument is a grants file that should load, for tests that vary one
// thing about it.
const validDocument = `
hosts:
  web-prod:
    address: web1.prod.example.com:22
    user: runbook
    identity_file: /etc/flowstate/ssh/id_ed25519
    host_keys:
      - ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleExampleExampleExampleExampleExam
    commands: [restart-service]
commands:
  restart-service:
    argv: ["/usr/bin/systemctl", "restart", "${service}"]
    parameters:
      service:
        pattern: '[a-z0-9-]{1,64}\.service'
    timeout: 60s
    max_output_bytes: 65536
`

// TestAWellFormedGrantsFileLoads is the premise every refusal below is measured
// against.
func TestAWellFormedGrantsFileLoads(t *testing.T) {
	writeGrants(t, validDocument)

	parsed, err := loadGrants()
	if err != nil {
		t.Fatalf("loadGrants: %v", err)
	}
	if _, ok := parsed.Hosts["web-prod"]; !ok {
		t.Fatal("the host grant did not load")
	}
	command := parsed.Commands["restart-service"]
	if command.Parameters["service"].compiled == nil {
		t.Error("the parameter pattern was not compiled when the file was read")
	}
	if !command.successful(0) || command.successful(1) {
		t.Error("success_exit_codes defaults to exactly zero")
	}
}

// TestNoGrantsFileMeansNoAuthority is the fail-closed direction, and the only
// safe default: the alternative is every host and every command.
func TestNoGrantsFileMeansNoAuthority(t *testing.T) {
	t.Setenv(grantsEnv, "")

	_, err := loadGrants()
	if err == nil {
		t.Fatal("a plugin with no grants file claimed authority")
	}
	if !strings.Contains(err.Error(), "--plugin-env") {
		t.Errorf("the refusal does not tell an operator how to grant anything: %v", err)
	}
}

// TestAGrantsFileIsRefusedForWhatAnOperatorCanGetWrong covers the checks that
// run at startup rather than at three in the morning. Each case is a file an
// operator could plausibly write, and each refusal names what to fix.
func TestAGrantsFileIsRefusedForWhatAnOperatorCanGetWrong(t *testing.T) {
	for name, document := range map[string]string{
		"a command found on the remote PATH": strings.Replace(validDocument, `["/usr/bin/systemctl", "restart", "${service}"]`, `["systemctl", "restart", "${service}"]`, 1),

		"a parameter with no pattern": strings.Replace(validDocument, `        pattern: '[a-z0-9-]{1,64}\.service'`, `        max_bytes: 64`, 1),

		"a placeholder with no parameter": strings.Replace(validDocument, `"${service}"]`, `"${service}", "${unit}"]`, 1),

		"a parameter that is never used": strings.Replace(validDocument, `    parameters:
      service:`, `    parameters:
      unused:
        pattern: 'x'
      service:`, 1),

		"a host permitting a command that does not exist": strings.Replace(validDocument, `    commands: [restart-service]`, `    commands: [restart-service, drop-database]`, 1),

		"a host with no pinned key": strings.Replace(validDocument, `    host_keys:
      - ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleExampleExampleExampleExampleExam`, `    host_keys: []`, 1),

		"a host with no identity": strings.Replace(validDocument, `    identity_file: /etc/flowstate/ssh/id_ed25519`, `    identity_file: ""`, 1),

		"a timeout over the ceiling": strings.Replace(validDocument, `    timeout: 60s`, `    timeout: 30m`, 1),

		"an output limit over the ceiling": strings.Replace(validDocument, `    max_output_bytes: 65536`, `    max_output_bytes: 8388608`, 1),

		"a misspelled key": strings.Replace(validDocument, `    user: runbook`, `    usr: runbook`, 1),

		"no hosts at all": "commands:\n  noop:\n    argv: [\"/bin/true\"]\n",
	} {
		writeGrants(t, document)
		if _, err := loadGrants(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestAPatternIsAnchored is the difference between a pattern that constrains a
// value and one that merely appears somewhere in it.
func TestAPatternIsAnchored(t *testing.T) {
	writeGrants(t, validDocument)

	parsed, err := loadGrants()
	if err != nil {
		t.Fatalf("loadGrants: %v", err)
	}

	pattern := parsed.Commands["restart-service"].Parameters["service"].compiled
	if !pattern.MatchString("nginx.service") {
		t.Error("the pattern does not match the value it was written for")
	}
	if pattern.MatchString("nginx.service; rm -rf /") {
		t.Error("the pattern matched a value with something appended, so it was not anchored")
	}
	if pattern.MatchString("/etc/passwd nginx.service") {
		t.Error("the pattern matched a value with something prepended, so it was not anchored")
	}
}
