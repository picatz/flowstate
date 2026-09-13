package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeGrants writes a grants file and points the environment at it, the way a
// worker's --plugin-env does.
func writeGrants(t *testing.T, document string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "grants.yaml")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing grants: %v", err)
	}
	t.Setenv(grantsEnv, path)
}

// validDocument is a grants file that should load.
const validDocument = `
daemon:
  socket: /var/run/docker.sock
mounts:
  fixtures:
    source: /srv/fixtures
    target: /fixtures
runs:
  smoke:
    image: ghcr.io/acme/tools@sha256:9f6d0e6a3a2c1b5e4d8f7a0c9b2e1d4a7c6f5b8e3d2a1c0b9f8e7d6c5b4a3f21
    argv: ["/usr/bin/check", "--suite", "${suite}"]
    parameters:
      suite:
        pattern: '[a-z0-9-]{1,32}'
    env:
      CI: "true"
    mounts: [fixtures]
    memory_bytes: 268435456
    nano_cpus: 500000000
    pids_limit: 64
    timeout: 2m
    max_output_bytes: 262144
`

// TestAWellFormedGrantsFileLoads is the premise the refusals below are measured
// against.
func TestAWellFormedGrantsFileLoads(t *testing.T) {
	writeGrants(t, validDocument)

	parsed, err := loadGrants()
	if err != nil {
		t.Fatalf("loadGrants: %v", err)
	}
	run, ok := parsed.Runs["smoke"]
	if !ok {
		t.Fatal("the run grant did not load")
	}
	if run.Parameters["suite"].compiled == nil {
		t.Error("the parameter pattern was not compiled when the file was read")
	}
	if run.user() != "65534:65534" {
		t.Errorf("user() = %q, want a non-root default", run.user())
	}
	if run.network() != "none" {
		t.Errorf("network() = %q, want none by default", run.network())
	}
	if parsed.Daemon.apiVersion() == "" {
		t.Error("the daemon grant answers no API version")
	}
}

// TestNoGrantsFileMeansNoAuthority is the fail-closed direction. The permissive
// alternative for this plugin is "any image, any mount, any network", which is
// why there is no default at all.
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

// TestATagIsRefusedWhereADigestIsRequired is the contract's first line: what an
// operator reviewed and what a node executes have to be the same bytes.
func TestATagIsRefusedWhereADigestIsRequired(t *testing.T) {
	for _, image := range []string{
		"ghcr.io/acme/tools:1.4.2",
		"ghcr.io/acme/tools",
		"ghcr.io/acme/tools@sha256:short",
		"ghcr.io/acme/tools@md5:9f6d0e6a3a2c1b5e4d8f7a0c9b2e1d4a",
	} {
		writeGrants(t, strings.Replace(validDocument,
			"    image: ghcr.io/acme/tools@sha256:9f6d0e6a3a2c1b5e4d8f7a0c9b2e1d4a7c6f5b8e3d2a1c0b9f8e7d6c5b4a3f21",
			"    image: "+image, 1))

		_, err := loadGrants()
		if err == nil {
			t.Errorf("the image %q was accepted", image)
			continue
		}
		if !strings.Contains(err.Error(), "digest-pinned") {
			t.Errorf("the image %q was refused for the wrong reason: %v", image, err)
		}
	}
}

// TestAGrantsFileIsRefusedForWhatAnOperatorCanGetWrong covers the checks that
// run at startup. Each case is a file somebody could plausibly write.
func TestAGrantsFileIsRefusedForWhatAnOperatorCanGetWrong(t *testing.T) {
	for name, document := range map[string]string{
		"no memory bound":                  strings.Replace(validDocument, "    memory_bytes: 268435456\n", "", 1),
		"no cpu bound":                     strings.Replace(validDocument, "    nano_cpus: 500000000\n", "", 1),
		"a timeout over the ceiling":       strings.Replace(validDocument, "    timeout: 2m", "    timeout: 45m", 1),
		"an output limit over the ceiling": strings.Replace(validDocument, "    max_output_bytes: 262144", "    max_output_bytes: 8388608", 1),
		"a mount that is not granted":      strings.Replace(validDocument, "    mounts: [fixtures]", "    mounts: [fixtures, secrets]", 1),
		"a relative mount source":          strings.Replace(validDocument, "    source: /srv/fixtures", "    source: srv/fixtures", 1),
		"a placeholder with no parameter":  strings.Replace(validDocument, `"${suite}"]`, `"${suite}", "${extra}"]`, 1),
		"a parameter that is never used": strings.Replace(validDocument, `      suite:
        pattern: '[a-z0-9-]{1,32}'`, `      suite:
        pattern: '[a-z0-9-]{1,32}'
      unused:
        pattern: 'x'`, 1),
		"a pattern that does not compile":          strings.Replace(validDocument, `        pattern: '[a-z0-9-]{1,32}'`, `        pattern: '[unclosed'`, 1),
		"host networking":                          strings.Replace(validDocument, "    memory_bytes:", "    network: host\n    memory_bytes:", 1),
		"a user that is not uid:gid":               strings.Replace(validDocument, "    memory_bytes:", "    user: root\n    memory_bytes:", 1),
		"a relative working directory":             strings.Replace(validDocument, "    memory_bytes:", "    working_dir: work\n    memory_bytes:", 1),
		"a daemon with neither socket nor address": strings.Replace(validDocument, "  socket: /var/run/docker.sock", "  api_version: v1.43", 1),
		"a remote daemon with no TLS":              strings.Replace(validDocument, "  socket: /var/run/docker.sock", "  address: dockerd.internal:2376", 1),
		"a misspelled key":                         strings.Replace(validDocument, "    memory_bytes:", "    memorybytes:", 1),
		"no runs at all":                           "daemon:\n  socket: /var/run/docker.sock\nruns: {}\n",
	} {
		writeGrants(t, document)
		if _, err := loadGrants(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestHostNetworkingIsRefusedAtEveryGrantLevel: a container on the host's
// network reaches everything the worker can, including the loopback services an
// egress policy cannot see. There is no grant that turns it on.
func TestHostNetworkingIsRefusedAtEveryGrantLevel(t *testing.T) {
	writeGrants(t, strings.Replace(validDocument, "    memory_bytes:", "    network: host\n    memory_bytes:", 1))

	_, err := loadGrants()
	if err == nil {
		t.Fatal("host networking was granted")
	}
	if !strings.Contains(err.Error(), "host networking") {
		t.Errorf("the refusal does not say what was refused: %v", err)
	}
}
