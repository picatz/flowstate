package temporalclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// describeNamespaceTimeout bounds NewPool's per-namespace existence check.
//
// The mapping names namespaces, not machines this process controls, so a check
// against one of them shares the fate of any other network call to a peer it does
// not run: it has to be bounded, or a slow or wedged cluster hangs `flow server`
// startup indefinitely waiting on an answer that may never come. Ten seconds is
// generous for a single unary RPC and small next to the minutes a dev server
// itself can take to boot in these tests.
const describeNamespaceTimeout = 10 * time.Second

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

	// FlowstateNamespaces returns the Flowstate namespaces this mapping routes to
	// the given Temporal namespace, so a diagnostic about a broken Temporal
	// namespace can also name who it affects. The empty string denotes the
	// default tenant. Order is unspecified and may be empty even when the
	// Temporal namespace is one [TemporalNamespaces] returned, if the mapper
	// cannot answer the reverse question.
	FlowstateNamespaces(temporalNamespace string) []string
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
// submit first. "Misconfigured" includes a namespace nobody registered: dialing
// only proves the cluster answers — the SDK's eager check is a cluster-scoped RPC
// with no namespace argument — so NewPool additionally asks the cluster to
// describe each mapped namespace and fails construction on one it does not have,
// rather than leaving that discovery for a tenant's first submit.
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

	// fallbackNamespace is the Temporal namespace fallback is dialed for.
	//
	// A [client.Client] is dialed for a namespace and can never be asked which,
	// so the answer has to be kept at the moment the client is made. It is the
	// value [Config.Options] *resolved*, not Config.Namespace — see [dial] for
	// why those are different values, and why recording the wrong one would be
	// worse than recording nothing at all.
	//
	// [NewPool] is the only constructor and Options always resolves a non-empty
	// namespace, so this is never empty on a Pool anything outside this package
	// can hold. TestNewPoolRecordsTheResolvedNamespaceNotTheOverride is what
	// keeps that true.
	fallbackNamespace string

	// byNamespace holds one client per mapped Temporal namespace.
	byNamespace map[string]client.Client

	closeOnce sync.Once
}

// NewPool dials a client for every namespace the mapper can select, plus the one the
// process was configured with, and verifies each mapped namespace actually exists.
//
// A nil mapper, or one that maps nothing, yields a pool holding only the configured
// client — the zero-configuration path, which issues no existence check either.
//
// logger receives one warning per mapped namespace NewPool could dial but not
// describe (a locked-down client identity, a transient error) — see the existence
// check below for why that warns rather than fails. A nil logger uses
// [slog.Default].
func NewPool(ctx context.Context, cfg Config, mapper NamespaceMapper, logger *slog.Logger) (*Pool, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// dial rather than Dial: the namespace this client is dialed for is read back
	// from the options this very call resolved, rather than resolved a second
	// time from cfg. See [dial].
	fallback, opts, err := dial(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pool := &Pool{
		mapper:            mapper,
		fallback:          fallback,
		fallbackNamespace: opts.Namespace,
		byNamespace:       make(map[string]client.Client),
	}

	if mapper == nil {
		return pool, nil
	}

	// TemporalNamespaces is already the deduplicated set a connection layer dials
	// (see its doc), so this loop already checks each distinct namespace once
	// however many tenants share it.
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

		if err := verifyNamespaceExists(ctx, cl, namespace); err != nil {
			var notFound *serviceerror.NamespaceNotFound
			if errors.As(err, &notFound) {
				pool.Close()
				return nil, fmt.Errorf(
					"the tenancy mapping routes tenant(s) %s to Temporal namespace %q, which this cluster "+
						"does not have; register the namespace or fix the mapping: %w",
					formatTenants(mapper.FlowstateNamespaces(namespace)), namespace, err)
			}

			// Reachable but not describable — a client identity locked down below
			// operator privileges, or a transient error. The
			// EnsureSearchAttributesRegistered posture: degrade to a loud warning
			// and keep the client, rather than bricking a deployment whose identity
			// may legitimately describe nothing. A typo would still be caught here
			// on any deployment where the identity can read namespace metadata,
			// which is the common case; this is the fallback for the one that can't.
			logger.Warn("could not verify a mapped Temporal namespace exists; continuing, but a "+
				"nonexistent namespace here will only be discovered at the first tenant to submit",
				"namespace", namespace, "error", err)
		}
	}

	return pool, nil
}

// verifyNamespaceExists asks the cluster cl is dialed to whether namespace is
// registered.
//
// A cluster-scoped RPC succeeding (which is all [Dial] proves) says nothing about
// any particular namespace: [client.Client.WorkflowService]'s DescribeNamespace is
// the namespace-scoped ask, bounded by [describeNamespaceTimeout] so a slow or
// wedged cluster cannot hang pool construction.
func verifyNamespaceExists(ctx context.Context, cl client.Client, namespace string) error {
	ctx, cancel := context.WithTimeout(ctx, describeNamespaceTimeout)
	defer cancel()

	_, err := cl.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	return err
}

// formatTenants renders the Flowstate namespaces a broken Temporal namespace
// affects, for an error message an operator reads once and has to act on.
//
// Empty means the mapper could not answer which tenants route there — reported
// rather than silently omitted, because a diagnostic that goes quiet on one input
// is a diagnostic nobody can trust on the others.
func formatTenants(tenants []string) string {
	if len(tenants) == 0 {
		return "(unknown; the mapper could not report which tenant this namespace belongs to)"
	}

	parts := make([]string, len(tenants))
	for i, tenant := range tenants {
		if tenant == "" {
			parts[i] = "(default)"
			continue
		}
		parts[i] = strconv.Quote(tenant)
	}
	return strings.Join(parts, ", ")
}

// resolve reports which Temporal namespace a run belonging to the given Flowstate
// namespace executes in, and the client dialed for it.
//
// It fails closed: a deployment that maps namespaces but has no entry for this one
// and no default gets an error, rather than having a tenant's runs placed wherever
// the process happened to be pointed.
//
// Both halves come from one lookup, and a caller that needs both must take them
// from one call — which is why [Pool.ForWithNamespace] hands back the pair rather
// than there being an accessor for the namespace alone. A client and a namespace
// drawn from two lookups are a pair only if nothing changed in between, and
// [NamespaceMapper] is an interface: whether an implementation answers the same
// way twice is not this package's to assume. A mismatched pair is not a visible
// failure either, because a raw Temporal request carries its namespace as a
// request field — so client A addressed at namespace B is a perfectly legal
// request that succeeds against the wrong tenant's history, with nothing anywhere
// saying so (Codex, #1139).
func (p *Pool) resolve(namespace string) (string, client.Client, error) {
	if p.mapper == nil {
		return p.fallbackNamespace, p.fallback, nil
	}

	mapped, ok, err := p.mapper.TemporalNamespace(namespace)
	if err != nil {
		return "", nil, err
	}
	if !ok {
		// Nothing is mapped, so the configured namespace is the answer.
		return p.fallbackNamespace, p.fallback, nil
	}

	cl, ok := p.byNamespace[mapped]
	if !ok {
		// Unreachable unless the mapping changed after startup, and deliberately
		// not repaired by dialing here: a lazily dialed client on the request path
		// is the cost this type exists to avoid, and a mapping that grew since
		// startup means the process is running against configuration it never
		// validated.
		return "", nil, fmt.Errorf("no Temporal client for namespace %q (mapped to %q); "+
			"the tenancy mapping changed after startup, so restart to pick it up",
			namespace, mapped)
	}
	return mapped, cl, nil
}

// For returns the client a run belonging to the given Flowstate namespace should use.
//
// It fails closed: a deployment that maps namespaces but has no entry for this one
// and no default gets an error, rather than having a tenant's runs placed wherever
// the process happened to be pointed.
func (p *Pool) For(namespace string) (client.Client, error) {
	_, cl, err := p.resolve(namespace)
	return cl, err
}

// ForWithNamespace is [Pool.For], also reporting the Temporal namespace the
// returned client is dialed for.
//
// It exists because a [client.Client] cannot be asked. Every request the SDK sends
// carries the namespace its client was dialed for, so a caller going through the
// SDK never has to know it — but Temporal's raw APIs take the namespace as a
// request field, and a caller reaching for one has nowhere else to get it. Giving
// `GetTimeline` a request budget independent of the events it scans needs exactly
// that (see maxTimelineScan in server/timeline.go, which names this as the
// follow-up it was blocked on). Nothing in this repository calls it yet, and that
// is deliberate: plumbing a namespace to where it can be read is a change that
// misroutes a tenant's reads when it is wrong, so it lands and is reviewed on its
// own rather than inside the change that wants it.
//
// # Why there is no accessor for the namespace alone
//
// Because the pair is the answer, and half of it is not a smaller answer but a
// different one. A caller holding a client from one call and a namespace from
// another holds a pair only on the assumption that nothing changed in between,
// and [NamespaceMapper] is an interface this package hands to whoever implements
// it — a mapper that reloads a policy, or reads a tenant's routing from a store,
// owes nobody the same answer twice. The pairing therefore has to survive a
// mapper that answers differently on the second call, which it can only do by
// there being no second call. That is the difference between a guarantee and a
// habit, and it was a review finding on this very method, which previously
// returned the namespace alone and claimed a structural guarantee two wrappers
// could not provide (Codex, #1139).
//
// [Pool.For] stays, for the many callers that need only a client — one lookup,
// nothing to pair. What is gone is the way to get a namespace without the client
// it belongs to.
//
// It fails closed identically to [Pool.For], and by construction rather than by
// resemblance: both are [Pool.resolve]. An accessor that answered where For
// refuses would hand a caller another tenant's namespace to address — the breach
// the pool exists to prevent, arriving through the door left open for reading
// rather than for writing.
func (p *Pool) ForWithNamespace(namespace string) (client.Client, string, error) {
	name, cl, err := p.resolve(namespace)
	if err != nil {
		return nil, "", err
	}
	return cl, name, nil
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
