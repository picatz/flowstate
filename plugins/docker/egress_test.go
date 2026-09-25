package main

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// withEgress installs an egress posture for one test.
func withEgress(t *testing.T, policy *netpolicy.Policy, isDefault bool, refusal error) {
	t.Helper()

	previousPolicy, previousDefault, previousRefusal := egressPolicy, egressIsDeploymentDefault, egressRefusal
	egressPolicy, egressIsDeploymentDefault, egressRefusal = policy, isDefault, refusal
	t.Cleanup(func() {
		egressPolicy, egressIsDeploymentDefault, egressRefusal = previousPolicy, previousDefault, previousRefusal
	})
}

// remoteDaemon is a grant naming a daemon on another host. The TLS material is
// never loaded here: every one of these tests refuses before it would be.
func remoteDaemon() daemonGrant {
	return daemonGrant{
		Address:     "daemon.internal:2376",
		TLSCAFile:   "/etc/flowstate/docker-ca.pem",
		TLSCertFile: "/etc/flowstate/docker-client.pem",
		TLSKeyFile:  "/etc/flowstate/docker-client-key.pem",
	}
}

// TestARemoteDaemonNeedsTheDeploymentsEgressPolicy is the second operator
// statement, and the one a grants file cannot make for itself.
//
// A remote daemon is ambient authority on another host: whoever answers there
// decides what runs. The worker's built-in default policy is what a deployment
// runs under when nobody has decided anything about destinations, which is not
// a decision to hand a container runtime to a workflow.
func TestARemoteDaemonNeedsTheDeploymentsEgressPolicy(t *testing.T) {
	permissive, err := netpolicy.New(netpolicy.WithSchemes("https"))
	if err != nil {
		t.Fatalf("building a policy: %v", err)
	}

	for name, arrange := range map[string]func(*testing.T){
		"no policy at all":     func(t *testing.T) { withEgress(t, nil, false, nil) },
		"a refused grant":      func(t *testing.T) { withEgress(t, nil, false, errNoOperatorEgressPolicy) },
		"the worker's default": func(t *testing.T) { withEgress(t, permissive, true, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			arrange(t)

			if _, err := newDaemon(remoteDaemon()); err == nil {
				t.Fatal("a remote daemon was reachable without an operator's egress policy")
			} else if !sdk.IsPermissionDenied(err) {
				t.Errorf("error is %v, want permission denied", err)
			}
		})
	}

	t.Run("an operator's policy", func(t *testing.T) {
		withEgress(t, permissive, false, nil)

		// The TLS material the grant names does not exist, which is as far as
		// this gets: the point is that the egress check is no longer what
		// refuses it.
		_, err := newDaemon(remoteDaemon())
		if err == nil {
			t.Fatal("a grant naming TLS material that does not exist was accepted")
		}
		if sdk.IsPermissionDenied(err) {
			t.Errorf("error is %v, want the refusal to have moved past the egress check", err)
		}
	})
}

// TestALocalSocketNeedsNoEgressPolicy is the other half: a socket is a file the
// operator already named, not a destination, and requiring a policy document to
// use one would be ceremony rather than a boundary.
func TestALocalSocketNeedsNoEgressPolicy(t *testing.T) {
	fake := newFakeDaemon(t)
	withEgress(t, nil, false, errNoOperatorEgressPolicy)

	if _, err := newDaemon(fake.grant()); err != nil {
		t.Fatalf("a local daemon socket was refused for want of an egress policy: %v", err)
	}
}

// TestHealthReportsAnUngrantedRemoteDaemon proves an operator who granted the
// address but not the destination learns it from the plugin's health rather
// than from the first workflow that fails.
func TestHealthReportsAnUngrantedRemoteDaemon(t *testing.T) {
	withEgress(t, nil, false, nil)
	withGrants(t, &grants{Daemon: remoteDaemon(), Runs: map[string]runGrant{"check": testRun()}})

	if err := checkHealth(t.Context()); err == nil {
		t.Fatal("health is good for a remote daemon no egress policy permits")
	}
}
