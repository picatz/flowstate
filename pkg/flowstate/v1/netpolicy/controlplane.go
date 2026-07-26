package netpolicy

import (
	"context"
	"fmt"
	"net/netip"
)

// Administering Flowstate with Flowstate is a capability worth having: a workflow
// that cancels a stuck run, or fans work out by starting child runs, is a workflow
// like any other. It is also the one target where reaching it by accident is worst,
// because the control plane is what decides who may do what.
//
// So it is deliberate in both directions. [WithControlPlane] declares the address
// and reserves it — denied even when its category would otherwise be allowed, the
// same treatment cloud metadata gets, so a worker opened up for local development
// does not quietly gain administrative reach. [WithSelfAdministration] then permits
// it, and permits it only for a request carrying the run's own identity.
//
// # Ambient authority is the failure mode
//
// The danger is not that a workflow reaches the control plane. It is that it
// reaches the control plane *as the worker*. A worker holds credentials, sits
// inside the trust boundary, and often talks to a server on loopback that trusts
// loopback. A workflow borrowing any of that would be able to do whatever the
// worker can — cancel another tenant's runs, read another tenant's history —
// without any of it appearing in a policy, because the authority was ambient
// rather than granted.
//
// The answer is that authority comes from the run, not from where the request was
// made. A request to the control plane is refused unless the run's identity is
// attached to it with [WithRunIdentity], so there is no path on which the control
// plane is asked to act without being told on whose behalf. What that identity is
// permitted to do is the control plane's decision, not this package's; what this
// package guarantees is that the question is always asked with an answer available.

// controlPlaneIdentity is the identity a request to the control plane acts as.
//
// Its methods are the accessors protoc generates, so the identity recorded on a run
// satisfies it directly and this package needs no dependency on the schema to
// accept one.
type controlPlaneIdentity interface {
	// GetNamespace returns the tenant the run belongs to.
	GetNamespace() string

	// GetSubject returns the workload the run acts as.
	GetSubject() string
}

// RunIdentity is the identity a request acts as when reaching Flowstate's own
// control plane.
//
// It is satisfied by the identity message recorded on a run, which is where it
// should come from: an identity assembled by the caller of a task would be the
// workflow choosing its own authority, which is the thing this exists to prevent.
type RunIdentity = controlPlaneIdentity

// runIdentityKey is the context key for the run identity. It is an unexported
// empty struct type so no other package can collide with it or forge a value.
type runIdentityKey struct{}

// WithRunIdentity returns a context carrying the identity a request to the control
// plane acts as.
//
// A task that may reach the control plane attaches the identity of the run it is
// executing, and nothing else: not the worker's identity, and not an identity built
// from a task input. Without this, a request to a declared control-plane address is
// refused.
//
// It is a context value rather than a parameter because it has to survive the
// request path down into the dialer, where the check happens, without every
// intermediate signature carrying it. That is the same reason the request
// attributes travel that way.
func WithRunIdentity(ctx context.Context, identity RunIdentity) context.Context {
	return context.WithValue(ctx, runIdentityKey{}, identity)
}

// RunIdentityFrom returns the run identity carried by ctx, reporting whether one
// was attached and is usable.
//
// An identity with no namespace and no subject is reported as absent. A typed-nil
// message reads as empty through its generated accessors rather than panicking, and
// an identity that established neither tenant nor workload is not an identity — it
// is the placeholder a run gets when authentication produced nothing, and treating
// it as authority is how an unauthenticated run would act as one.
func RunIdentityFrom(ctx context.Context) (RunIdentity, bool) {
	identity, ok := ctx.Value(runIdentityKey{}).(RunIdentity)
	if !ok || identity == nil {
		return nil, false
	}

	if identity.GetNamespace() == "" && identity.GetSubject() == "" {
		return nil, false
	}

	return identity, true
}

// WithControlPlane declares an address as Flowstate's own control plane, reserving
// it.
//
// A reserved address is denied even when its category is allowed, which is what
// makes the capability deliberate: a worker running with [WithAllowLoopback] for
// local development does not thereby gain administrative reach over the server
// beside it. [WithSelfAdministration] is what permits it.
//
// The address must be a literal address and port, such as "127.0.0.1:8080" or
// "[::1]:8080". A hostname is refused, because a reservation written as a name is
// not enforceable: the check runs against the address actually dialed, and a
// workflow naming the same server by its IP would sail past a name-based
// reservation. An operator whose control plane is reached by name lists the
// addresses it resolves to.
//
// Declaring several addresses is supported, for a control plane reachable on more
// than one, and each is reserved.
func WithControlPlane(addrPorts ...string) Option {
	return func(c *config) error {
		for _, s := range addrPorts {
			addrPort, err := netip.ParseAddrPort(s)
			if err != nil {
				return errInvalidControlPlane(s)
			}
			if addrPort.Port() == 0 {
				return errInvalidControlPlane(s)
			}

			if c.controlPlane == nil {
				c.controlPlane = make(map[netip.AddrPort]struct{})
			}
			c.controlPlane[normalizeAddrPort(addrPort)] = struct{}{}
		}

		return nil
	}
}

// WithSelfAdministration permits requests to the addresses declared by
// [WithControlPlane], so that a workflow can administer Flowstate.
//
// A request is still refused unless the run's identity is attached with
// [WithRunIdentity]. That is the whole point: the capability grants reachability,
// never authority. The control plane authorizes the identity exactly as it would
// authorize any other caller, so a run can do what its own namespace and
// permissions allow and no more — which is what keeps a workflow from escalating
// by virtue of running inside the thing it is calling.
//
// It has no effect without [WithControlPlane], since there is nothing to permit;
// that combination is refused rather than silently doing nothing.
func WithSelfAdministration() Option {
	return func(c *config) error {
		c.selfAdministration = true
		return nil
	}
}

// errInvalidControlPlane reports a control-plane declaration that cannot be
// enforced.
func errInvalidControlPlane(s string) error {
	return fmt.Errorf(
		"control plane %q must be a literal address and port such as \"127.0.0.1:8080\"; "+
			"a hostname cannot be reserved, because the check runs against the address actually dialed "+
			"and a request naming the same server by its address would bypass it",
		s,
	)
}

// normalizeAddrPort puts a declared address into the form the dial-time check
// compares against, so that a control plane declared as ::ffff:127.0.0.1 and dialed
// as 127.0.0.1 is recognized as the same reservation.
func normalizeAddrPort(addrPort netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(normalize(addrPort.Addr()), addrPort.Port())
}

// isControlPlane reports whether an address is a declared control plane.
func (p *Policy) isControlPlane(addrPort netip.AddrPort) bool {
	if len(p.cfg.controlPlane) == 0 {
		return false
	}

	_, ok := p.cfg.controlPlane[normalizeAddrPort(addrPort)]

	return ok
}

// checkControlPlane decides a request to a declared control-plane address.
//
// It returns nil when the address is not a control plane, so the ordinary address
// policy decides it.
func (p *Policy) checkControlPlane(ctx context.Context, addrPort netip.AddrPort) (handled bool, err error) {
	if !p.isControlPlane(addrPort) {
		return false, nil
	}

	target := addrPort.String()

	if !p.cfg.selfAdministration {
		return true, &DenyError{
			Reason: ReasonControlPlane,
			Target: target,
			Detail: "Flowstate's own control plane is reserved; permit it with WithSelfAdministration " +
				"if administering Flowstate from a workflow is intended",
		}
	}

	if _, ok := RunIdentityFrom(ctx); !ok {
		// Reaching the control plane without saying on whose behalf is how a
		// workflow would act with the worker's authority instead of its own.
		return true, &DenyError{
			Reason: ReasonControlPlane,
			Target: target,
			Detail: "a request to the control plane must carry the run's identity, and this one carries none; " +
				"attach it with WithRunIdentity so the control plane authorizes the run rather than the worker",
		}
	}

	// The address is permitted, and deliberately bypasses the category checks: the
	// operator named this address, so whether it happens to be loopback or private
	// is not the question. The denied-network list still applies, and is checked
	// before this.
	return true, nil
}
