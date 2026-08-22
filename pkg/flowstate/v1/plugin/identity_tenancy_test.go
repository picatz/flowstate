package plugin

import (
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The negative direction for what a plugin is told about who is calling it.
//
// CLAUDE.md's rule about tenancy tests is that asserting each party reaches its
// own resource is a functionality test wearing a security test's clothes; the
// question worth asking is whether A can reach B. Applied here, "reach" means
// *be described as*: a plugin that receives one tenant's identity alongside
// another tenant's namespace will authorize against whichever of the two its own
// policy happens to read, and the host is the only party in a position to make
// sure it never sees such a pair.
//
// Two seams carry that pair, and until now only one of them was tested from this
// direction. Secret resolution drops a mismatched identity ([identityForNamespace],
// workload.go); task execution copies the pair from a unary [pluginv1.ExecuteRequest]
// into a streaming one ([Plugin.executeTask], task.go), a copy nothing asserted
// on the streaming side at all.

// TestIdentityFromAnotherNamespaceIsNeverForwarded is the drop path stated as a
// unit, on the function that decides it.
//
// The end-to-end case in TestSecretIdentityCrossesBoundary proves the fake
// plugin's *value* changes; this proves the thing that must be true whatever a
// plugin then does with it — team-b's identity does not travel beside team-a's
// namespace, at all, ever.
func TestIdentityFromAnotherNamespaceIsNeverForwarded(t *testing.T) {
	t.Parallel()

	teamA := &flowstatev1.WorkloadIdentity{Subject: "ci", Issuer: "https://issuer.example", Namespace: "team-a"}
	teamB := &flowstatev1.WorkloadIdentity{Subject: "ci", Issuer: "https://issuer.example", Namespace: "team-b"}
	unscoped := &flowstatev1.WorkloadIdentity{Subject: "ci", Issuer: "https://issuer.example"}

	tests := []struct {
		name      string
		identity  *flowstatev1.WorkloadIdentity
		namespace string
		want      *flowstatev1.WorkloadIdentity
	}{
		{
			name:      "another tenant's identity is dropped",
			identity:  teamB,
			namespace: "team-a",
			want:      nil,
		},
		{
			name:      "and in the other direction too",
			identity:  teamA,
			namespace: "team-b",
			want:      nil,
		},
		{
			// The empty namespace is a tenant like any other rather than a
			// wildcard, which is the rule the secrets package applies and the
			// one an "unauthenticated means anything" reading would break.
			name:      "an identity from a named tenant is dropped in the default one",
			identity:  teamA,
			namespace: "",
			want:      nil,
		},
		{
			name:      "its own tenant is forwarded",
			identity:  teamA,
			namespace: "team-a",
			want:      teamA,
		},
		{
			// An identity that claims no namespace claims no *other* namespace
			// either, so it travels: a deployment whose identity provider does
			// not issue namespaces still gets to tell a plugin who is asking.
			name:      "an identity with no namespace of its own is forwarded",
			identity:  unscoped,
			namespace: "team-a",
			want:      unscoped,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx := NewContextWithIdentity(t.Context(), test.identity)

			got := identityForNamespace(ctx, test.namespace)

			if test.want == nil {
				if got != nil {
					t.Fatalf("identity %q would have been sent to a plugin resolving in namespace %q; "+
						"a plugin told one tenant's identity beside another's namespace authorizes against the wrong one",
						got.GetNamespace(), test.namespace)
				}
				return
			}
			if got == nil {
				t.Fatalf("the caller's own identity was dropped in namespace %q", test.namespace)
			}
			if got.GetSubject() != test.want.GetSubject() || got.GetNamespace() != test.want.GetNamespace() {
				t.Errorf("forwarded identity = %q/%q, want %q/%q",
					got.GetNamespace(), got.GetSubject(), test.want.GetNamespace(), test.want.GetSubject())
			}
		})
	}
}

// TestNoIdentityIsSentWhenThereIsNone checks the shape a single-tenant
// deployment has: no identity in the context is no identity on the wire, rather
// than a zero-valued one a plugin might read as an authenticated caller from the
// default tenant.
func TestNoIdentityIsSentWhenThereIsNone(t *testing.T) {
	t.Parallel()

	if got := identityForNamespace(t.Context(), "team-a"); got != nil {
		t.Errorf("an identity was invented for a context that carried none: %v", got)
	}
}

// TestExecuteStreamCarriesTheCallersOwnIdentity is the seam with no test today:
// a plugin advertising CAPABILITY_TASK_PROGRESS is dispatched through
// ExecuteStream rather than unary Execute, and the identity and namespace on
// that streaming request are copied by hand in [Plugin.executeTask]. A copy that
// dropped a field, or that reused a previous call's, would be a plugin told the
// wrong tenant for its whole streaming life while every unary test stayed green.
//
// Two tenants run the same task through the same plugin process, one after the
// other, and each must see its own pair and nothing of the other's.
func TestExecuteStreamCarriesTheCallersOwnIdentity(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "identity-stream")))

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}
	def := defs[0]

	execute := func(t *testing.T, namespace, subject string) map[string]*flowstatev1.Value {
		t.Helper()

		ctx := NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
			Subject:   subject,
			Issuer:    "https://issuer.example",
			Namespace: namespace,
		})

		outputs, err := def.Fn(ctx, map[string]*flowstatev1.Value{
			"message": flowstatev1.NewLiteral("hello"),
		}, nil)
		if err != nil {
			t.Fatalf("executing as %s/%s: %v", namespace, subject, err)
		}

		return outputs.GetNamedValues()
	}

	for _, tenant := range []struct{ namespace, subject string }{
		{"team-a", "a-ci"},
		{"team-b", "b-ci"},
		// team-a again, after team-b: a copy that held onto the previous
		// call's pair would pass a two-tenant test that never went back.
		{"team-a", "a-ci"},
	} {
		got := execute(t, tenant.namespace, tenant.subject)

		if ns := got["namespace"].GetLiteral().GetStringValue(); ns != tenant.namespace {
			t.Errorf("ExecuteStream carried namespace %q, want %q", ns, tenant.namespace)
		}
		if subject := got["subject"].GetLiteral().GetStringValue(); subject != tenant.subject {
			t.Errorf("ExecuteStream carried subject %q, want %q", subject, tenant.subject)
		}
		// The identity's own namespace and the request's namespace are one
		// fact sent twice, and a plugin may read either. They must agree, or
		// the plugin's choice of which to read decides which tenant it
		// authorizes against.
		if ns := got["identity_namespace"].GetLiteral().GetStringValue(); ns != tenant.namespace {
			t.Errorf("the identity on the stream claimed namespace %q while the request said %q",
				ns, tenant.namespace)
		}
	}
}
