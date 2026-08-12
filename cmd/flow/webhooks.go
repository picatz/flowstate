package main

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// Turning `--webhook <file>` into a receiver, which is the deployment's half of a
// webhook trigger.
//
// A file declares a webhook; a deployment decides whether *this* installation
// serves it. That split is deliberate and is the reason there is a flag at all
// rather than a receiver that serves whatever it is handed: staging must not fire
// the production webhook, and which endpoints exist is an operational fact rather
// than something a repository decides by containing a file (#490).

// addWebhookFlags declares the receiver's surface on the server command.
func addWebhookFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("webhook", nil,
		"serve deliveries for the webhooks declared in this Flowfile, at "+
			"/webhooks/<workflow>/<trigger>. Repeatable. The file is compiled, its `verify:` keys "+
			"are resolved, and this deployment's own checks are run against it at startup, so a "+
			"workflow this deployment cannot serve stops the server rather than refusing deliveries "+
			"later. Needs the --secret-* flags that reach the signing keys")

	cmd.Flags().String("webhook-namespace", "",
		"the Flowstate tenant a delivery's run belongs to, and the tenant its `verify:` keys are "+
			"read in. A sender presents a signature rather than an identity, so there is no caller "+
			"to take a tenant from and an operator names it here. Required on a deployment whose "+
			"trust policy maps tenants onto Temporal namespaces, which has nowhere to route the "+
			"unnamed tenant; a single-tenant deployment leaves it empty")
}

// webhookReceiver builds the receiver for whatever --webhook names, or nil when
// nothing does.
//
// Every failure here stops the server. That is the point: the alternative is a
// deployment that starts, serves, and refuses every genuine delivery for a reason
// visible only in a log line nobody is reading — which is precisely the failure
// mode "decide when configuration loads" exists to prevent.
func webhookReceiver(cmd *cobra.Command, flowServer *server.FlowstateServer, logger *slog.Logger) (*server.WebhookReceiver, error) {
	paths, _ := cmd.Flags().GetStringArray("webhook")
	if len(paths) == 0 {
		return nil, nil
	}

	workflows := make([]*v1.Workflow, 0, len(paths))
	for _, path := range paths {
		// Compiled through the same parser `flow validate` uses, so a file this
		// serves is a file an author could have checked, with the same
		// diagnostics naming the same positions.
		workflow, _, err := flowfile.ParseFile(path)
		if err != nil {
			return nil, fmt.Errorf("--webhook %s: %w", path, err)
		}
		workflows = append(workflows, workflow)
	}

	registry, _, closeProviders, err := secretRegistry(cmd)
	if err != nil {
		return nil, fmt.Errorf("configuring the secret providers a webhook's `verify:` keys resolve through: %w", err)
	}
	// Kept open for the process's life: nothing is resolved after startup, but a
	// provider holding a file handle is closed when the process ends rather than
	// here, and the server runs until it ends.
	cmd.PostRun = func(*cobra.Command, []string) { closeProviders() }

	if len(registry.Schemes()) == 0 {
		return nil, fmt.Errorf("--webhook was given but no secret provider is configured, so no `verify:` " +
			"key can be resolved and every delivery would be refused; configure one with the " +
			"--secret-* flags")
	}

	store, err := newSecretStore(cmd, registry)
	if err != nil {
		return nil, err
	}

	// The tenant an operator established, handed over *with* the store rather than
	// used to scope it here: the receiver scopes the keys itself, from the same
	// value it records a delivery's run under, so the tenant a key is read in and
	// the tenant the run belongs to cannot be given different answers. See
	// [server.FlowstateServer.NewWebhookReceiver].
	//
	// A deployment running with --secret-require-namespace and no
	// --webhook-namespace is refused there, out loud, rather than resolving a key
	// in a tenant nobody chose — as is one whose trust policy maps tenants onto
	// Temporal namespaces and has no entry for the tenant named here.
	namespace, _ := cmd.Flags().GetString("webhook-namespace")

	receiver, err := flowServer.NewWebhookReceiver(cmd.Context(), namespace, workflows, store,
		server.WithWebhookLogger(logger))
	if err != nil {
		return nil, err
	}

	// What is served, at startup, sorted: an operator pointing a provider at this
	// deployment needs the exact path, and finding it by reading source is the
	// sort of friction that gets solved by guessing.
	routes := receiver.Routes()
	slices.Sort(routes)
	logger.Info("serving webhook deliveries", "prefix", server.WebhookPathPrefix, "webhooks", routes)

	return receiver, nil
}
