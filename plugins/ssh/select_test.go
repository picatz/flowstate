package main

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sshv1 "github.com/picatz/flowstate/plugins/ssh/gen/ssh/v1"
)

// withGrants installs an authority for the duration of one test.
func withGrants(t *testing.T, authority *grants) {
	t.Helper()

	previous := operatorGrants
	operatorGrants = authority
	t.Cleanup(func() { operatorGrants = previous })
}

// twoHostAuthority is a grants file with a tenant-scoped host and a shared one.
func twoHostAuthority(t *testing.T) *grants {
	t.Helper()

	authority := &grants{
		Hosts: map[string]hostGrant{
			"shared": {
				Address: "shared.example.com:22", User: "runbook",
				IdentityFile: "/dev/null", HostKeys: []string{"ssh-ed25519 AAAA"},
				Commands: []string{"probe"},
			},
			"tenant-a-only": {
				Address: "a.example.com:22", User: "runbook",
				IdentityFile: "/dev/null", HostKeys: []string{"ssh-ed25519 AAAA"},
				Commands: []string{"probe"}, Namespaces: []string{"tenant-a"},
			},
		},
		Commands: map[string]commandGrant{
			"probe":     {Argv: []string{"/bin/true"}},
			"ungranted": {Argv: []string{"/bin/false"}},
		},
	}
	// Deliberately not run through check(): these grants exercise selection,
	// and check() would refuse "ungranted" for being permitted by no host -
	// which is the state this test needs in order to prove selection refuses
	// it too.
	return authority
}

// TestAnUnknownHostIsNotFoundAndNamesWhatExists: an author who mistyped a grant
// name should learn the names this worker has rather than search a
// configuration file they may not be able to read.
func TestAnUnknownHostIsNotFoundAndNamesWhatExists(t *testing.T) {
	withGrants(t, twoHostAuthority(t))

	_, _, err := selectGrants("", &sshv1.RunInputs{Host: "typo", Command: "probe"})
	if !sdk.IsNotFound(err) {
		t.Fatalf("error is %v, want not-found", err)
	}
	if !strings.Contains(err.Error(), "shared") {
		t.Errorf("the refusal does not name the grants that exist: %v", err)
	}
}

// TestACommandTheHostDoesNotPermitIsRefused is the second half of the pair: a
// command may exist and still not be granted here.
func TestACommandTheHostDoesNotPermitIsRefused(t *testing.T) {
	withGrants(t, twoHostAuthority(t))

	_, _, err := selectGrants("", &sshv1.RunInputs{Host: "shared", Command: "ungranted"})
	if !sdk.IsPermissionDenied(err) {
		t.Fatalf("error is %v, want permission denied", err)
	}
	if !strings.Contains(err.Error(), "probe") {
		t.Errorf("the refusal does not name what this host does permit: %v", err)
	}
}

// TestANamespacedGrantIsReachableOnlyFromThatNamespace is the multi-tenant
// boundary, and it turns on the namespace the host established for the caller
// rather than anything the workload said about itself.
func TestANamespacedGrantIsReachableOnlyFromThatNamespace(t *testing.T) {
	withGrants(t, twoHostAuthority(t))

	if _, _, err := selectGrants("tenant-a", &sshv1.RunInputs{Host: "tenant-a-only", Command: "probe"}); err != nil {
		t.Fatalf("the tenant the grant names could not spend it: %v", err)
	}

	_, _, err := selectGrants("tenant-b", &sshv1.RunInputs{Host: "tenant-a-only", Command: "probe"})
	if !sdk.IsPermissionDenied(err) {
		t.Fatalf("error is %v, want permission denied: another tenant spent a namespaced grant", err)
	}

	// A workload whose namespace the host did not establish is the empty
	// namespace, which a grant naming namespaces does not match either.
	if _, _, err := selectGrants("", &sshv1.RunInputs{Host: "tenant-a-only", Command: "probe"}); !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied for a caller with no namespace", err)
	}

	// A grant naming no namespaces is every namespace, which is what a
	// single-tenant deployment has.
	if _, _, err := selectGrants("tenant-b", &sshv1.RunInputs{Host: "shared", Command: "probe"}); err != nil {
		t.Errorf("a grant naming no namespaces refused a caller: %v", err)
	}
}

// TestACallWithNoGrantsAtAllIsRefusedBeforeAnythingElse keeps an unconfigured
// plugin from doing anything, and tells the operator what to configure.
func TestACallWithNoGrantsAtAllIsRefusedBeforeAnythingElse(t *testing.T) {
	previousGrants, previousRefusal := operatorGrants, grantsRefusal
	operatorGrants, grantsRefusal = nil, errNoGrantsForTest
	t.Cleanup(func() { operatorGrants, grantsRefusal = previousGrants, previousRefusal })

	_, err := sshRun(t.Context(), map[string]*flowstatev1.Value{
		"host":    flowstatev1.NewValue("shared"),
		"command": flowstatev1.NewValue("probe"),
	}, nil)
	if err == nil {
		t.Fatal("a plugin with no grants ran something")
	}
	if !strings.Contains(err.Error(), "no grants") {
		t.Errorf("the refusal does not say what is missing: %v", err)
	}
}

// errNoGrantsForTest stands in for the reason loadGrants would have recorded.
var errNoGrantsForTest = errTest{}

type errTest struct{}

func (errTest) Error() string { return "no grants file is configured" }
