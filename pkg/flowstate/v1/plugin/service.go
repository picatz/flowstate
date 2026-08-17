package plugin

import (
	"context"
	"fmt"
	"runtime"

	"connectrpc.com/connect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	pluginv1connect "github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
)

// The generated service is the extension contract, and these hand back an
// implementation of it.
//
// Connect generates client and handler interfaces with identical method
// signatures, so one value satisfies both: an in-process provider is a plain Go
// type implementing the service with no server and no socket, a plugin-backed
// one is a client to the same service, and nothing consuming it can tell them
// apart. Nothing serializes when the call is a Go method call, so an in-process
// implementation pays nothing for sharing the contract.
//
// That is why there is no second Go interface here for a plugin to be adapted
// onto. A hand-written interface mirroring the service would be a second
// definition of one contract, and two definitions drift — which is what the
// proto-first invariant exists to prevent.
//
// What these return is better than the raw client for the same reason a caller
// would otherwise have to build it themselves: it is bounded by the host's
// per-call timeout, it refuses a capability the plugin did not advertise, and it
// resolves the current process on every call, so it keeps working across a
// restart instead of holding a connection to a process that has gone.

// SecretService returns this plugin's secret resolution as the generated service.
//
// It reports an error if the plugin did not advertise CAPABILITY_SECRETS: a
// plugin serves what its manifest claims and nothing else, and asking it for
// something it never offered is a mistake to surface rather than a request to
// send.
func (p *Plugin) SecretService() (pluginv1connect.SecretServiceClient, error) {
	if !p.HasCapability(pluginv1.Capability_CAPABILITY_SECRETS) {
		return nil, pluginError(p.name, p.path, fmt.Errorf(
			"%w: does not advertise CAPABILITY_SECRETS", ErrCapability))
	}
	return secretService{plugin: p}, nil
}

// TaskService returns this plugin's task execution as the generated service.
//
// It reports an error if the plugin did not advertise CAPABILITY_TASKS.
func (p *Plugin) TaskService() (pluginv1connect.TaskServiceClient, error) {
	if !p.HasCapability(pluginv1.Capability_CAPABILITY_TASKS) {
		return nil, pluginError(p.name, p.path, fmt.Errorf(
			"%w: does not advertise CAPABILITY_TASKS", ErrCapability))
	}
	return taskService{plugin: p}, nil
}

// secretService is a plugin's SecretService, bound to the plugin rather than to
// one of its processes.
type secretService struct{ plugin *Plugin }

// Resolve implements [pluginv1connect.SecretServiceClient], and by having the
// same signature also implements [pluginv1connect.SecretServiceHandler].
func (s secretService) Resolve(
	ctx context.Context,
	req *connect.Request[pluginv1.ResolveRequest],
) (*connect.Response[pluginv1.ResolveResponse], error) {
	inst, err := s.plugin.ready()
	if err != nil {
		// Unavailable rather than a bare error, because this is the wire
		// contract's own vocabulary for "try again later" and the caller may be
		// several hops from anything that knows what a plugin is.
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}

	ctx, cancel := s.plugin.callContext(ctx)
	defer cancel()

	return inst.clients.secret.Resolve(ctx, req)
}

// taskService is a plugin's TaskService, bound to the plugin rather than to one
// of its processes.
type taskService struct{ plugin *Plugin }

// Execute implements [pluginv1connect.TaskServiceClient], and by having the
// same signature also implements [pluginv1connect.TaskServiceHandler].
func (s taskService) Execute(
	ctx context.Context,
	req *connect.Request[pluginv1.ExecuteRequest],
) (*connect.Response[pluginv1.ExecuteResponse], error) {
	inst, err := s.plugin.ready()
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}

	ctx, cancel := s.plugin.callContext(ctx)
	defer cancel()

	return inst.clients.task.Execute(ctx, req)
}

// ExecuteStream implements [pluginv1connect.TaskServiceClient] only.
//
// It is the one place the symmetry the package doc comment describes does not
// hold, and it cannot: a streaming client's method returns a stream for its
// caller to read from, and a streaming handler's method is instead *handed*
// one to write into and returns only an error, so the client and handler
// shapes for one streaming RPC are never the same Go signature — no
// implementation, however written, satisfies both. taskService is used only
// as a client (see [Plugin.TaskService]'s own doc comment; nothing in this
// package ever serves it as a handler), so that is a gap in the pattern
// rather than in what this type needs to do.
//
// This still applies [Plugin.callContext]'s own per-call timeout, but not with
// Execute's `defer cancel()` — that scopes the cancel to when the *wrapping*
// call returns, which for Execute is right (it does not return until the
// whole call is done) and wrong here, because ExecuteStream returns as soon
// as the stream exists, before its caller has read anything from it. A
// deferred cancel at that point would tear the stream down before its first
// Receive. The package's own promise is the other direction: every service
// this file hands back — including this one — is bounded by the host's
// per-call timeout, so a plugin that opens a stream and then never sends a
// terminal ExecuteStreamResponse cannot hold the call open past
// Config.CallTimeout; only a plugin that finishes within it, or a reader that
// keeps calling Receive, gets to run longer than that under its own context's
// deadline.
//
// The bounded context is kept alive for exactly that reason: it is not
// canceled here, so it lives for the stream's whole read, and it is released
// once the stream is no longer reachable rather than left to leak — this
// method has no hook into [connect.ServerStreamForClient.Close] to run cancel
// eagerly on a normal close (the interface this method fills in returns that
// concrete type, not a wrapper this package controls), so a
// [runtime.AddCleanup] backstop releases it once the stream is garbage
// collected. That is a cleanliness guarantee, not the security one: the
// timeout itself is what stops an indefinite hold, whether or not anything
// ever reads to completion or closes explicitly.
func (s taskService) ExecuteStream(
	ctx context.Context,
	req *connect.Request[pluginv1.ExecuteStreamRequest],
) (*connect.ServerStreamForClient[pluginv1.ExecuteStreamResponse], error) {
	inst, err := s.plugin.ready()
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}

	ctx, cancel := s.plugin.callContext(ctx)

	stream, err := inst.clients.task.ExecuteStream(ctx, req)
	if err != nil {
		cancel()
		return nil, err
	}

	runtime.AddCleanup(stream, func(cancel context.CancelFunc) { cancel() }, cancel)

	return stream, nil
}

// Compile-time proof that one implementation satisfies both sides of the
// contract for every method where that is possible, which is the whole basis
// for the generated service being the extension point. secretService still
// proves the full symmetry; taskService proves only the client half, because
// ExecuteStream's client and handler shapes differ by construction — see that
// method's own doc comment.
var (
	_ pluginv1connect.SecretServiceClient  = secretService{}
	_ pluginv1connect.SecretServiceHandler = secretService{}
	_ pluginv1connect.TaskServiceClient    = taskService{}
)

// SecretServiceForScheme returns the service resolving a secret scheme, and
// which plugin answers for it.
//
// It is how the engine dispatches a reference without knowing that another
// process is involved: the scheme selects the service, and the service is the
// same shape whether it is backed by a plugin or by something in this process.
func (h *Host) SecretServiceForScheme(scheme string) (pluginv1connect.SecretServiceClient, *Plugin, bool) {
	h.mu.RLock()
	p, ok := h.schemes[scheme]
	h.mu.RUnlock()

	if !ok {
		return nil, nil, false
	}

	service, err := p.SecretService()
	if err != nil {
		// A scheme is only ever recorded for a plugin that advertised the
		// capability, so this cannot happen; refusing rather than returning a
		// nil service keeps it from becoming a panic if that ever changes.
		return nil, nil, false
	}

	return service, p, true
}

// TaskServiceForTask returns the service executing a task, and which plugin
// provides it.
func (h *Host) TaskServiceForTask(name string) (pluginv1connect.TaskServiceClient, *Plugin, bool) {
	h.mu.RLock()
	binding, ok := h.taskDefs[name]
	h.mu.RUnlock()

	if !ok {
		return nil, nil, false
	}

	service, err := binding.plugin.TaskService()
	if err != nil {
		return nil, nil, false
	}

	return service, binding.plugin, true
}
