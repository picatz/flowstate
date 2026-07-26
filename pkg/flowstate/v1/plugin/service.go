package plugin

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
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
func (p *Plugin) SecretService() (flowstatev1connect.SecretServiceClient, error) {
	if !p.HasCapability(flowstatev1.Capability_CAPABILITY_SECRETS) {
		return nil, pluginError(p.name, p.path, fmt.Errorf(
			"%w: does not advertise CAPABILITY_SECRETS", ErrCapability))
	}
	return secretService{plugin: p}, nil
}

// TaskService returns this plugin's task execution as the generated service.
//
// It reports an error if the plugin did not advertise CAPABILITY_TASKS.
func (p *Plugin) TaskService() (flowstatev1connect.TaskServiceClient, error) {
	if !p.HasCapability(flowstatev1.Capability_CAPABILITY_TASKS) {
		return nil, pluginError(p.name, p.path, fmt.Errorf(
			"%w: does not advertise CAPABILITY_TASKS", ErrCapability))
	}
	return taskService{plugin: p}, nil
}

// secretService is a plugin's SecretService, bound to the plugin rather than to
// one of its processes.
type secretService struct{ plugin *Plugin }

// Resolve implements [flowstatev1connect.SecretServiceClient], and by having the
// same signature also implements [flowstatev1connect.SecretServiceHandler].
func (s secretService) Resolve(
	ctx context.Context,
	req *connect.Request[flowstatev1.ResolveSecretRequest],
) (*connect.Response[flowstatev1.ResolveSecretResponse], error) {
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

// Execute implements [flowstatev1connect.TaskServiceClient], and by having the
// same signature also implements [flowstatev1connect.TaskServiceHandler].
func (s taskService) Execute(
	ctx context.Context,
	req *connect.Request[flowstatev1.ExecuteTaskRequest],
) (*connect.Response[flowstatev1.ExecuteTaskResponse], error) {
	inst, err := s.plugin.ready()
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}

	ctx, cancel := s.plugin.callContext(ctx)
	defer cancel()

	return inst.clients.task.Execute(ctx, req)
}

// Compile-time proof that one implementation satisfies both sides of the
// contract, which is the whole basis for the generated service being the
// extension point. If a future change to the schema broke the symmetry — a
// streaming method, say, whose client and handler shapes differ — it would fail
// here rather than wherever someone first tried to substitute one for the other.
var (
	_ flowstatev1connect.SecretServiceClient  = secretService{}
	_ flowstatev1connect.SecretServiceHandler = secretService{}
	_ flowstatev1connect.TaskServiceClient    = taskService{}
	_ flowstatev1connect.TaskServiceHandler   = taskService{}
)

// SecretServiceForScheme returns the service resolving a secret scheme, and
// which plugin answers for it.
//
// It is how the engine dispatches a reference without knowing that another
// process is involved: the scheme selects the service, and the service is the
// same shape whether it is backed by a plugin or by something in this process.
func (h *Host) SecretServiceForScheme(scheme string) (flowstatev1connect.SecretServiceClient, *Plugin, bool) {
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
func (h *Host) TaskServiceForTask(name string) (flowstatev1connect.TaskServiceClient, *Plugin, bool) {
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
