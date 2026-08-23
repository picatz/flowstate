package netpolicy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// runIdentity stands in for the identity recorded on a run. The real one is the
// generated message, which satisfies [RunIdentity] through the same accessors.
type runIdentity struct {
	namespace string
	subject   string
}

func (i *runIdentity) GetNamespace() string {
	if i != nil {
		return i.namespace
	}
	return ""
}

func (i *runIdentity) GetSubject() string {
	if i != nil {
		return i.subject
	}
	return ""
}

// controlPlaneServer starts a stand-in control plane on loopback and returns it
// with its address and port.
func controlPlaneServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "control plane")
	}))
	t.Cleanup(server.Close)

	return server, server.Listener.Addr().String()
}

func Test_WithControlPlane_declaration(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		// Negative cases first.
		{
			name:    "a hostname cannot be reserved",
			opts:    []Option{WithControlPlane("flowstate.internal:8080")},
			wantErr: "must be a literal address and port",
		},
		{
			name:    "an address without a port cannot be reserved",
			opts:    []Option{WithControlPlane("127.0.0.1")},
			wantErr: "must be a literal address and port",
		},
		{
			name:    "port zero is not a destination",
			opts:    []Option{WithControlPlane("127.0.0.1:0")},
			wantErr: "must be a literal address and port",
		},
		{
			// Permitting something that was never declared permits nothing, which is
			// more likely a misconfiguration than an intent.
			name:    "self-administration without a declared address is refused",
			opts:    []Option{WithSelfAdministration()},
			wantErr: "no control-plane address was declared",
		},

		{
			name: "an address and port is accepted",
			opts: []Option{WithControlPlane("127.0.0.1:8080")},
		},
		{
			name: "an IPv6 address is accepted",
			opts: []Option{WithControlPlane("[::1]:8080")},
		},
		{
			name: "several addresses are accepted",
			opts: []Option{WithControlPlane("127.0.0.1:8080", "10.0.0.1:8080")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)

			if test.wantErr != "" {
				require.Nil(t, policy)
				require.ErrorIs(t, err, ErrInvalidPolicy)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, policy)
		})
	}
}

// Test_controlPlane_isReservedNotMerelyDenied covers the property that makes the
// capability deliberate: the control plane stays denied even when its address
// category is allowed, so a worker opened up for local development does not quietly
// gain administrative reach.
func Test_controlPlane_isReservedNotMerelyDenied(t *testing.T) {
	server, addr := controlPlaneServer(t)

	tests := []struct {
		name  string
		opts  []Option
		check func(t *testing.T, resp *http.Response, err error)
	}{
		{
			name: "loopback allowed, control plane undeclared: reachable",
			opts: []Option{WithAllowLoopback()},
			check: func(t *testing.T, resp *http.Response, err error) {
				// Nothing declared it, so it is an ordinary loopback address. This is
				// the state the reservation exists to change.
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "loopback allowed, control plane declared: refused",
			opts: []Option{WithAllowLoopback(), WithControlPlane(addr)},
			check: func(t *testing.T, _ *http.Response, err error) {
				// The whole point: permitting loopback for development must not also
				// permit administering the server sitting on it.
				requireDenied(t, err, ReasonControlPlane, "reserved")
				require.ErrorContains(t, err, "WithSelfAdministration")
			},
		},
		{
			name: "everything allowed, control plane declared: still refused",
			opts: []Option{
				WithAllowLoopback(),
				WithAllowPrivateNetworks(),
				WithAllowLinkLocal(),
				WithControlPlane(addr),
			},
			check: func(t *testing.T, _ *http.Response, err error) {
				requireDenied(t, err, ReasonControlPlane, "reserved")
			},
		},
		{
			name: "declared and permitted, but no run identity: refused",
			opts: []Option{WithAllowLoopback(), WithControlPlane(addr), WithSelfAdministration()},
			check: func(t *testing.T, _ *http.Response, err error) {
				// Reachability without authority. A request that does not say on whose
				// behalf it acts is the ambient-authority failure.
				requireDenied(t, err, ReasonControlPlane, "must carry the run's identity")
				require.ErrorContains(t, err, "authorizes the run rather than the worker")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			resp, err := get(t, policy, server.URL)
			test.check(t, resp, err)
		})
	}
}

func Test_controlPlane_withRunIdentity(t *testing.T) {
	server, addr := controlPlaneServer(t)

	policy, err := New(WithAllowLoopback(), WithControlPlane(addr), WithSelfAdministration())
	require.NoError(t, err)

	newRequest := func(t *testing.T, ctx context.Context) *http.Request {
		t.Helper()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)

		return req
	}

	t.Run("a run identity permits the request", func(t *testing.T) {
		ctx := WithRunIdentity(t.Context(), &runIdentity{namespace: "team-a", subject: "workflow/deploy"})

		resp, err := policy.Client().Do(newRequest(t, ctx))
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("an identity with only a namespace is enough", func(t *testing.T) {
		// A single-tenant deployment may establish a tenant without a subject.
		ctx := WithRunIdentity(t.Context(), &runIdentity{namespace: "team-a"})

		resp, err := policy.Client().Do(newRequest(t, ctx))
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("an identity that established nothing is not an identity", func(t *testing.T) {
		// This is the placeholder a run gets when authentication produced nothing.
		// Treating it as authority is how an unauthenticated run would act as one.
		ctx := WithRunIdentity(t.Context(), &runIdentity{})

		_, err := policy.Client().Do(newRequest(t, ctx))
		requireDenied(t, err, ReasonControlPlane, "must carry the run's identity")
	})

	t.Run("a typed-nil identity is refused rather than panicking", func(t *testing.T) {
		ctx := WithRunIdentity(t.Context(), (*runIdentity)(nil))

		_, err := policy.Client().Do(newRequest(t, ctx))
		requireDenied(t, err, ReasonControlPlane, "must carry the run's identity")
	})

	t.Run("a nil identity is refused rather than panicking", func(t *testing.T) {
		ctx := WithRunIdentity(t.Context(), nil)

		_, err := policy.Client().Do(newRequest(t, ctx))
		requireDenied(t, err, ReasonControlPlane, "must carry the run's identity")
	})
}

// Test_controlPlane_grantsOnlyTheDeclaredAddress covers the other half of "not a
// side effect of debugging against localhost": the capability permits the control
// plane and nothing else.
func Test_controlPlane_grantsOnlyTheDeclaredAddress(t *testing.T) {
	controlPlane, addr := controlPlaneServer(t)
	other, _ := testServer(t, "some other service")

	policy, err := New(WithControlPlane(addr), WithSelfAdministration())
	require.NoError(t, err)

	ctx := WithRunIdentity(t.Context(), &runIdentity{namespace: "team-a"})

	t.Run("the control plane is reachable", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, controlPlane.URL, nil)
		require.NoError(t, err)

		resp, err := policy.Client().Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("another loopback service is not", func(t *testing.T) {
		// Loopback was never allowed. The capability is one address, not a category.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, other.URL, nil)
		require.NoError(t, err)

		_, err = policy.Client().Do(req)
		requireDenied(t, err, ReasonAddress, "loopback")
	})

	t.Run("a denied network still wins over the capability", func(t *testing.T) {
		// So an operator can carve one address back out without withdrawing the
		// capability entirely.
		host, _, err := net.SplitHostPort(addr)
		require.NoError(t, err)

		hostAddr, err := netip.ParseAddr(host)
		require.NoError(t, err)

		carved, err := New(
			WithControlPlane(addr),
			WithSelfAdministration(),
			WithDenyNetworks(netip.PrefixFrom(hostAddr, hostAddr.BitLen())),
		)
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, controlPlane.URL, nil)
		require.NoError(t, err)

		_, err = carved.Client().Do(req)
		requireDenied(t, err, ReasonAddress, "denied network")
	})
}

func Test_RunIdentityFrom(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		want bool
	}{
		{
			name: "no identity attached",
			ctx:  func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "nil identity",
			ctx:  func(t *testing.T) context.Context { return WithRunIdentity(t.Context(), nil) },
		},
		{
			name: "typed-nil identity",
			ctx: func(t *testing.T) context.Context {
				return WithRunIdentity(t.Context(), (*runIdentity)(nil))
			},
		},
		{
			name: "identity that established nothing",
			ctx: func(t *testing.T) context.Context {
				return WithRunIdentity(t.Context(), &runIdentity{})
			},
		},
		{
			name: "namespace only",
			ctx: func(t *testing.T) context.Context {
				return WithRunIdentity(t.Context(), &runIdentity{namespace: "team-a"})
			},
			want: true,
		},
		{
			name: "subject only",
			ctx: func(t *testing.T) context.Context {
				return WithRunIdentity(t.Context(), &runIdentity{subject: "workflow/deploy"})
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity, ok := RunIdentityFrom(test.ctx(t))
			require.Equal(t, test.want, ok)

			if !test.want {
				require.Nil(t, identity)
			}
		})
	}
}

func Test_controlPlane_addressForms(t *testing.T) {
	// A control plane declared one way and dialed another must be recognized as the
	// same reservation, or the reservation is bypassed by spelling.
	server, addr := controlPlaneServer(t)

	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	policy, err := New(WithAllowLoopback(), WithControlPlane("[::ffff:127.0.0.1]:"+port))
	require.NoError(t, err)

	_, err = get(t, policy, server.URL)
	requireDenied(t, err, ReasonControlPlane, "reserved")
}

// Test_controlPlane_identityIsRecheckedAcrossRequests is the connection-reuse
// direction for self-administration, and it is the same defect the CEL
// connection rules had: [Policy.checkControlPlane] reads the run identity off
// the *request's* context, but it runs in the dialer, which a reused connection
// never enters.
//
// So a request carrying an identity opens the connection, and a request
// carrying none rides it straight to the control plane — acting with the
// worker's authority instead of the run's, which is the exact substitution the
// missing-identity denial exists to prevent. This policy declares no CEL rules,
// which is what made it slip past a no-reuse condition keyed only on those.
//
// Asserted in the negative direction, and by connection count as well as by the
// error: a denial that arrives after the request already reached the control
// plane is not a denial.
func Test_controlPlane_identityIsRecheckedAcrossRequests(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "control plane")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	addr := server.Listener.Addr().String()

	policy, err := New(WithAllowLoopback(), WithControlPlane(addr), WithSelfAdministration())
	require.NoError(t, err)

	ctx := WithRunIdentity(t.Context(), &runIdentity{namespace: "team-a", subject: "workflow/deploy"})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	resp, err := policy.Client().Do(req)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int64(1), connections.Load())

	// No run identity on this one. It must be refused rather than carried by the
	// connection the identity-carrying request established.
	anonymous, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	_, err = policy.Client().Do(anonymous)
	requireDenied(t, err, ReasonControlPlane, "must carry the run's identity")
	require.Equal(t, int64(1), connections.Load(), "the denied request must not reach the control plane")
}
