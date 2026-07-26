package temporalclient

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/client"
)

// NamespaceMapper reports which Temporal namespace a Flowstate namespace's runs
// execute in.
//
// It is an interface rather than a concrete type so this package does not depend on
// the one that parses trust policies, and so a test can describe a mapping without
// building a policy. [auth.Tenancy] satisfies it.
type NamespaceMapper interface {
	// TemporalNamespace returns the Temporal namespace for a Flowstate namespace.
	//
	// False with no error means this deployment maps nothing, so the caller should
	// use the namespace it was configured with. An error means the deployment does
	// map namespaces but has neither an entry for this one nor a default, which is
	// a configuration gap rather than a reason to place a tenant's runs somewhere
	// arbitrary.
	TemporalNamespace(namespace string) (string, bool, error)

	// TemporalNamespaces returns every Temporal namespace the mapping can select.
	TemporalNamespaces() []string
}

// Pool holds one Temporal client per namespace a deployment can route to.
//
// # Why a pool rather than dialing per run
//
// Selecting where a tenant's runs go has to be a lookup, not a connection attempt.
// Dialing on the request path would put a network round trip — and a possible
// failure — between a caller and a run being accepted, and would do it again for
// every run. Every namespace a deployment can reach is therefore dialed once at
// startup, which also means a namespace that is unreachable or misconfigured is
// discovered when the server starts rather than by whichever tenant happens to
// submit first.
//
// # The zero-configuration path
//
// A deployment that maps nothing is the common case, and it still works: the pool
// holds exactly the client the process was configured with, and every run uses it.
// That is what keeps a single-tenant deployment from having to describe a tenancy it
// does not have.
type Pool struct {
	// mapper decides which Temporal namespace a Flowstate namespace routes to. Nil
	// means nothing is mapped.
	mapper NamespaceMapper

	// fallback is the client built from the process's own configuration, used when
	// the deployment maps nothing.
	fallback client.Client

	// byNamespace holds one client per mapped Temporal namespace.
	byNamespace map[string]client.Client

	closeOnce sync.Once
}

// NewPool dials a client for every namespace the mapper can select, plus the one the
// process was configured with.
//
// A nil mapper, or one that maps nothing, yields a pool holding only the configured
// client — the zero-configuration path.
func NewPool(ctx context.Context, cfg Config, mapper NamespaceMapper) (*Pool, error) {
	fallback, err := Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pool := &Pool{
		mapper:      mapper,
		fallback:    fallback,
		byNamespace: make(map[string]client.Client),
	}

	if mapper == nil {
		return pool, nil
	}

	for _, namespace := range mapper.TemporalNamespaces() {
		// Same configuration, different namespace: address and credentials come
		// from the environment exactly as they do for the fallback, so a mapping
		// cannot quietly point a tenant at a different cluster.
		scoped := cfg
		scoped.Namespace = namespace

		cl, err := Dial(ctx, scoped)
		if err != nil {
			// Close what was already dialed rather than leaking connections on a
			// startup that is about to fail.
			pool.Close()
			return nil, fmt.Errorf("dialing Temporal namespace %q for tenancy mapping: %w", namespace, err)
		}
		pool.byNamespace[namespace] = cl
	}

	return pool, nil
}

// For returns the client a run belonging to the given Flowstate namespace should use.
//
// It fails closed: a deployment that maps namespaces but has no entry for this one
// and no default gets an error, rather than having a tenant's runs placed wherever
// the process happened to be pointed.
func (p *Pool) For(namespace string) (client.Client, error) {
	if p.mapper == nil {
		return p.fallback, nil
	}

	mapped, ok, err := p.mapper.TemporalNamespace(namespace)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Nothing is mapped, so the configured namespace is the answer.
		return p.fallback, nil
	}

	cl, ok := p.byNamespace[mapped]
	if !ok {
		// Unreachable unless the mapping changed after startup, and deliberately
		// not repaired by dialing here: a lazily dialed client on the request path
		// is the cost this type exists to avoid, and a mapping that grew since
		// startup means the process is running against configuration it never
		// validated.
		return nil, fmt.Errorf("no Temporal client for namespace %q (mapped to %q); "+
			"the tenancy mapping changed after startup, so restart to pick it up",
			namespace, mapped)
	}
	return cl, nil
}

// Namespaces returns every Temporal namespace this pool holds a client for, for
// logging at startup.
func (p *Pool) Namespaces() []string {
	if p.mapper == nil {
		return nil
	}
	return p.mapper.TemporalNamespaces()
}

// Close closes every client in the pool.
//
// Safe to call more than once, so a deferred Close and an error path that already
// cleaned up do not double-close.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		if p.fallback != nil {
			p.fallback.Close()
		}
		for _, cl := range p.byNamespace {
			cl.Close()
		}
	})
}
