package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// testImage is a digest-pinned reference; a tag would not load.
const testImage = "ghcr.io/acme/tools@sha256:9f6d0e6a3a2c1b5e4d8f7a0c9b2e1d4a7c6f5b8e3d2a1c0b9f8e7d6c5b4a3f21"

// testRun is a run grant with the bounds a grants file must state.
func testRun() runGrant {
	return runGrant{
		Image:       testImage,
		Argv:        []string{"/usr/bin/check", "--suite", "${suite}"},
		Parameters:  map[string]parameterGrant{"suite": {Pattern: `[a-z0-9-]{1,32}`}},
		MemoryBytes: 256 << 20,
		NanoCPUs:    500_000_000,
		PidsLimit:   64,
		Timeout:     Duration(30 * time.Second),
	}
}

// withGrants installs an authority for one test.
func withGrants(t *testing.T, authority *grants) {
	t.Helper()

	if err := authority.check(); err != nil {
		t.Fatalf("the test's own grants do not validate: %v", err)
	}

	previous := operatorGrants
	operatorGrants = authority
	t.Cleanup(func() { operatorGrants = previous })
}

// TestTheCreateRequestIsTheContract is the test this plugin exists to pass.
// Every claim doc.go makes about what a container gets is a field in the create
// body, and this reads them off the JSON a real daemon would have parsed.
func TestTheCreateRequestIsTheContract(t *testing.T) {
	fake := newFakeDaemon(t)
	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": testRun()}}
	withGrants(t, authority)

	runtime, err := newDaemon(authority.Daemon)
	if err != nil {
		t.Fatalf("newDaemon: %v", err)
	}

	if _, err := execute(t.Context(), runtime, authority.Runs["check"], "check", []string{"/usr/bin/check", "--suite", "smoke"}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	created := fake.createRequest()
	if created.Image != testImage {
		t.Errorf("Image = %q, want the digest-pinned reference", created.Image)
	}
	if !slices.Equal(created.Cmd, []string{"/usr/bin/check", "--suite", "smoke"}) {
		t.Errorf("Cmd = %q", created.Cmd)
	}
	if created.User != "65534:65534" {
		t.Errorf("User = %q, want a non-root default", created.User)
	}
	if created.Tty || created.AttachStdin || created.OpenStdin {
		t.Errorf("the container was given a TTY or stdin: %+v", created)
	}
	if created.HostConfig.NetworkMode != "none" {
		t.Errorf("NetworkMode = %q, want none by default", created.HostConfig.NetworkMode)
	}
	if !created.HostConfig.ReadonlyRootfs {
		t.Error("ReadonlyRootfs = false, want a read-only root filesystem by default")
	}
	if created.HostConfig.Privileged {
		t.Error("Privileged = true")
	}
	if !slices.Equal(created.HostConfig.CapDrop, []string{"ALL"}) {
		t.Errorf("CapDrop = %q, want every capability dropped", created.HostConfig.CapDrop)
	}
	if !slices.Contains(created.HostConfig.SecurityOpt, "no-new-privileges:true") {
		t.Errorf("SecurityOpt = %q", created.HostConfig.SecurityOpt)
	}
	if created.HostConfig.Memory != 256<<20 || created.HostConfig.NanoCpus != 500_000_000 {
		t.Errorf("resource bounds = %d bytes, %d nanocpus", created.HostConfig.Memory, created.HostConfig.NanoCpus)
	}
	if created.HostConfig.PidsLimit == nil || *created.HostConfig.PidsLimit != 64 {
		t.Error("PidsLimit was not forwarded")
	}
	if created.HostConfig.AutoRemove {
		t.Error("AutoRemove = true, which would race the log read against the daemon deleting the container")
	}
	if len(created.HostConfig.Mounts) != 0 {
		t.Errorf("Mounts = %+v for a grant that names none", created.HostConfig.Mounts)
	}
	if created.Labels["io.flowstate.run"] != "check" {
		t.Errorf("Labels = %v, want the run grant recorded on the container", created.Labels)
	}
}

// TestTheContainerIsRemovedOnEveryPath: a container left behind because a
// workflow failed is the failure mode the contract names.
func TestTheContainerIsRemovedOnEveryPath(t *testing.T) {
	for name, arrange := range map[string]func(*fakeDaemon){
		"a successful run": func(*fakeDaemon) {},
		"a failing exit":   func(f *fakeDaemon) { f.exitCode = 7 },
		"a refused start":  func(f *fakeDaemon) { f.startStatus = 500; f.errorBody = "no such image" },
	} {
		fake := newFakeDaemon(t)
		arrange(fake)

		authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": testRun()}}
		withGrants(t, authority)

		runtime, err := newDaemon(authority.Daemon)
		if err != nil {
			t.Fatalf("%s: newDaemon: %v", name, err)
		}
		_, _ = execute(t.Context(), runtime, authority.Runs["check"], "check", []string{"/usr/bin/check"})

		if fake.removals() == 0 {
			t.Errorf("%s: the container was not removed", name)
		}
	}
}

// TestTheOutputIsDemultiplexedAndBounded covers the daemon's own framing, which
// is why this task cannot be an http step: the streams arrive interleaved with
// eight-byte headers, and reading the body as text would return those too.
func TestTheOutputIsDemultiplexedAndBounded(t *testing.T) {
	fake := newFakeDaemon(t)
	fake.stdout = "all checks passed\n"
	fake.stderr = "one warning\n"

	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": testRun()}}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	out, err := execute(t.Context(), runtime, authority.Runs["check"], "check", []string{"/usr/bin/check"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.stdout != "all checks passed\n" {
		t.Errorf("stdout = %q", out.stdout)
	}
	if out.stderr != "one warning\n" {
		t.Errorf("stderr = %q", out.stderr)
	}
	if out.truncated {
		t.Error("truncated is true for output that fit")
	}
}

// TestOutputOverTheGrantsLimitIsMarkedTruncated keeps a cut-off stream from
// being readable as a complete one.
func TestOutputOverTheGrantsLimitIsMarkedTruncated(t *testing.T) {
	fake := newFakeDaemon(t)
	fake.stdout = strings.Repeat("x", 4096)

	grant := testRun()
	grant.MaxOutputBytes = 128
	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": grant}}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	out, err := execute(t.Context(), runtime, grant, "check", []string{"/usr/bin/check"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(out.stdout) != 128 || !out.truncated {
		t.Errorf("stdout is %d bytes, truncated = %v", len(out.stdout), out.truncated)
	}
}

// TestANonZeroExitFailsTheStepAndKeepsTheOutput.
func TestANonZeroExitFailsTheStepAndKeepsTheOutput(t *testing.T) {
	fake := newFakeDaemon(t)
	fake.exitCode = 2
	fake.stderr = "2 tests failed\n"

	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": testRun()}}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	out, err := execute(t.Context(), runtime, authority.Runs["check"], "check", []string{"/usr/bin/check"})
	if err == nil {
		t.Fatal("a non-zero exit was reported as success")
	}
	if out == nil || out.exitCode != 2 || !strings.Contains(out.stderr, "2 tests failed") {
		t.Errorf("the failure does not carry what the container said: %+v", out)
	}
}

// TestAGrantMayCountOtherExitCodesAsSuccess.
func TestAGrantMayCountOtherExitCodesAsSuccess(t *testing.T) {
	fake := newFakeDaemon(t)
	fake.exitCode = 1

	grant := testRun()
	grant.SuccessExitCodes = []int32{0, 1}
	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": grant}}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	if _, err := execute(t.Context(), runtime, grant, "check", []string{"/usr/bin/check"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

// TestARunThatOutlastsItsTimeoutIsRemovedAndUnknown: the timeout stops waiting
// and removes the container, and what it had already done is not knowable from
// here - so the outcome is unknown rather than a failure to retry.
func TestARunThatOutlastsItsTimeoutIsRemovedAndUnknown(t *testing.T) {
	fake := newFakeDaemon(t)
	fake.waitBlocks = make(chan struct{})
	t.Cleanup(func() { close(fake.waitBlocks) })

	grant := testRun()
	grant.Timeout = Duration(250 * time.Millisecond)
	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": grant}}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	_, err := execute(t.Context(), runtime, grant, "check", []string{"/usr/bin/check"})
	if err == nil {
		t.Fatal("a container that never finished was reported as success")
	}
	if !sdk.IsOutcomeUnknown(err) {
		t.Errorf("error is %v, want the unknown-outcome classification", err)
	}
	if fake.removals() == 0 {
		t.Error("the container was not removed when the run timed out")
	}
}

// TestMountsComeFromGrantsRatherThanFromAWorkflow is the mount-grant contract:
// a workflow names no path, and what a run gets is what the operator resolved.
func TestMountsComeFromGrantsRatherThanFromAWorkflow(t *testing.T) {
	fake := newFakeDaemon(t)

	grant := testRun()
	grant.Mounts = []string{"fixtures", "workspace"}
	authority := &grants{
		Daemon: fake.grant(),
		Mounts: map[string]mountGrant{
			"fixtures":  {Source: "/srv/fixtures", Target: "/fixtures"},
			"workspace": {Source: "/srv/work", Target: "/work", Writable: true},
		},
		Runs: map[string]runGrant{"check": grant},
	}
	withGrants(t, authority)

	runtime, _ := newDaemon(authority.Daemon)
	if _, err := execute(t.Context(), runtime, grant, "check", []string{"/usr/bin/check"}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	mounts := fake.createRequest().HostConfig.Mounts
	if len(mounts) != 2 {
		t.Fatalf("Mounts = %+v", mounts)
	}
	if mounts[0].Source != "/srv/fixtures" || !mounts[0].ReadOnly {
		t.Errorf("a mount grant that did not say writable was not read-only: %+v", mounts[0])
	}
	if mounts[1].Source != "/srv/work" || mounts[1].ReadOnly {
		t.Errorf("a mount grant that said writable was read-only: %+v", mounts[1])
	}
	for _, mount := range mounts {
		if mount.Type != "bind" {
			t.Errorf("mount type = %q", mount.Type)
		}
	}
}

// TestAParameterBecomesOneArgvElement: there is no shell inside the container,
// and this is what makes that true rather than incidental.
func TestAParameterBecomesOneArgvElement(t *testing.T) {
	grant := testRun()
	grant.Parameters = map[string]parameterGrant{"suite": {Pattern: `.*`, MaxBytes: 128}}
	if err := grant.check("check", nil); err != nil {
		t.Fatalf("check: %v", err)
	}

	argv, err := buildArgv(grant, map[string]string{"suite": "smoke; rm -rf /"})
	if err != nil {
		t.Fatalf("buildArgv: %v", err)
	}
	if len(argv) != 3 || argv[2] != "smoke; rm -rf /" {
		t.Errorf("argv = %q, want the value as exactly one element", argv)
	}
}

// TestAValueThatFailsThePatternIsRefusedAndNotEchoed.
func TestAValueThatFailsThePatternIsRefusedAndNotEchoed(t *testing.T) {
	grant := testRun()
	if err := grant.check("check", nil); err != nil {
		t.Fatalf("check: %v", err)
	}

	_, err := buildArgv(grant, map[string]string{"suite": "../../etc/passwd"})
	if err == nil {
		t.Fatal("a value the operator's pattern refuses was used")
	}
	if !sdk.IsInvalidInput(err) {
		t.Errorf("error is %v, want invalid input", err)
	}
	if strings.Contains(err.Error(), "passwd") {
		t.Errorf("the refusal echoes the value that failed the pattern: %v", err)
	}
}

// TestANamespacedRunIsReachableOnlyFromThatNamespace.
func TestANamespacedRunIsReachableOnlyFromThatNamespace(t *testing.T) {
	fake := newFakeDaemon(t)

	grant := testRun()
	grant.Namespaces = []string{"platform"}
	authority := &grants{Daemon: fake.grant(), Runs: map[string]runGrant{"check": grant}}
	withGrants(t, authority)

	if _, err := selectRun("platform", "check"); err != nil {
		t.Fatalf("the namespace the grant names could not spend it: %v", err)
	}
	if _, err := selectRun("other", "check"); !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
	if _, err := selectRun("", "check"); !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied for a caller with no namespace", err)
	}
	if _, err := selectRun("platform", "missing"); !sdk.IsNotFound(err) {
		t.Errorf("error is %v, want not-found", err)
	}
}
