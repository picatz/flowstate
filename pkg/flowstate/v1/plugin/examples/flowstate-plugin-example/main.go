// Command flowstate-plugin-example is a Flowstate plugin that both resolves
// secrets and provides a task.
//
// It exists to demonstrate the multi-capability case, which is the one worth
// demonstrating: the useful integrations are rarely single-purpose. A plugin for
// a cloud provider naturally offers both its secrets manager and tasks that call
// its API, and splitting those would mean two processes, two handshakes, and two
// copies of the same credentials. One binary advertises both, and the engine
// uses whichever it needs.
//
// # Running it
//
// Build it, put it somewhere a worker is configured to look, and the worker does
// the rest:
//
//	go build -o /usr/local/lib/flowstate/plugins/flowstate-plugin-example ./...
//
// Running it from a shell prints an explanation and exits, because it is a
// plugin rather than a command.
//
// # What it does
//
// It resolves the "example" scheme from the process environment, scoped by
// namespace so that the same reference means different things to different
// tenants — which is what a real backend must do, and the thing that is easiest
// to get wrong. The variable for ${secret('example:api-key')} in namespace
// "team-a" is EXAMPLE_SECRET_TEAM_A_API_KEY, and in the empty namespace it is
// EXAMPLE_SECRET_API_KEY.
//
// It provides one task, written `example.greet:` in a Flowfile, whose input and output messages are
// defined in this plugin's own schema rather than the engine's. That is the
// realistic case: the engine has never seen these messages, and learns their
// shape from the descriptors this plugin ships in its manifest.
//
// # The other direction: a host secret, in a plugin task
//
// `greet` also takes `token`, named in its manifest as a SecretInput. That is
// the reverse of resolveSecret below: not this plugin answering for its own
// "example:" scheme, but a task of this plugin *consuming* a secret the host
// manages under whatever scheme the deployment configured — `env:`, `vault:`,
// anything else a worker's secret providers answer for. A Flowfile writes
// `token: ${secret('env:GREET_TOKEN')}` and greet receives a value, never a
// reference: the host resolves it before this process ever sees the request,
// under the caller's authenticated identity, through its own providers and
// policy — the same activity-side moment a built-in task's own secret input
// resolves at. This plugin never gains a scheme to fish with for it.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	examplev1 "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/gen/example/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

func main() {
	sdk.Main(sdk.Plugin{
		Name:        "example",
		Version:     "0.1.0",
		Description: "Resolves example: secrets and greets people.",

		Secrets: &sdk.Secrets{
			Schemes: []string{"example"},
			Resolve: resolveSecret,
		},

		Tasks: []sdk.Task{{
			Name:    "greet",
			Summary: "Greet someone by name.",
			Input:   &examplev1.GreetInputs{},
			Output:  &examplev1.GreetOutputs{},

			// `token` is a credential the *host* manages, not this plugin's own
			// "example:" scheme above. Naming it here is what lets a Flowfile
			// write `token: ${secret('env:...')}` and have the value arrive
			// resolved: the host refuses to resolve a reference into any input
			// not named here, and refuses to forward one, resolved or not, into
			// this plugin process at all.
			SecretInputs: []string{"token"},

			Fn: greet,
		}},

		Health: checkHealth,
	})
}

// secretEnvPrefix is what the environment variables holding this plugin's
// secrets are named with.
const secretEnvPrefix = "EXAMPLE_SECRET_"

// resolveSecret answers a reference from the environment, scoped by namespace.
//
// The namespace is part of the variable's name rather than something checked
// afterwards, which is what makes the tenant boundary structural: there is no
// path through this function that reads one tenant's variable while another's
// namespace was asked for. A backend that took the namespace as a filter applied
// after the lookup would have such a path, and it would be one line from being a
// tenancy breach.
func resolveSecret(_ context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
	name := envSegment(req.Name)
	if name == "" {
		return sdk.SecretResponse{}, sdk.InvalidInput("secret name is empty")
	}

	variable := secretEnvPrefix
	if namespace := envSegment(req.Namespace); namespace != "" {
		variable += namespace + "_"
	}
	variable += name

	value, ok := os.LookupEnv(variable)
	if !ok {
		// The error names the reference and the variable, both of which are safe
		// to log, and nothing else. A resolution failure that does not say what
		// was looked for is not diagnosable; one that says what was found would
		// be a leak.
		return sdk.SecretResponse{}, sdk.NotFound(
			"no secret %q in namespace %q (looked for %s)", req.Name, req.Namespace, variable)
	}

	if value == "" {
		return sdk.SecretResponse{}, sdk.NotFound("%s is set but empty", variable)
	}

	return sdk.SecretResponse{Value: []byte(value)}, nil
}

// envSegment renders part of a reference as part of an environment variable
// name, so that a name from a workflow cannot construct a variable name of its
// choosing.
func envSegment(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// greet assembles a greeting.
//
// It is the shape every plugin task has: decode the inputs into the message the
// task declared, do the work, encode the outputs. The engine has already
// resolved the input expressions, so what arrives here are values — including
// `token`, which the host resolves from whichever secret provider a Flowfile
// named before this process ever saw the request. This function never sees a
// reference and never has a scheme to fish with; it either receives a token or
// it does not.
func greet(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	// The SDK extracted the host's W3C parent before invoking us. Using its
	// configured tracer therefore makes this a child of the plugin RPC without
	// selecting an exporter or telemetry backend here. Nothing below makes an
	// outbound call, so the child context the tracer returns is discarded; a
	// task that calls anything would thread it through instead.
	if tracer := sdk.Tracer(ctx); tracer != nil {
		_, span := tracer.Start(ctx, "example.greet")
		defer span.End()
	}
	var in examplev1.GreetInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	if in.GetName() == "" {
		return nil, sdk.InvalidInput("name is required")
	}

	greeting := in.GetGreeting()
	if greeting == "" {
		greeting = "Hello"
	}

	message := fmt.Sprintf("%s, %s!", greeting, in.GetName())

	// Proof of receipt, deliberately never the value itself: reporting
	// in.GetToken() back as an output would turn a step output — durable
	// workflow history — into the very leak routing it through SecretInputs
	// exists to prevent. A length or a boolean says the value arrived without
	// saying what it is.
	outputs, err := sdk.EncodeOutputs(&examplev1.GreetOutputs{
		Message:       message,
		Length:        int64(len(message)),
		Authenticated: in.GetToken() != "",
	})
	if err != nil {
		return nil, sdk.Failed("%v", err)
	}

	return outputs, nil
}

// checkHealth reports whether this plugin can serve.
//
// A plugin with a real backend checks that it can reach it here, and reports the
// failure rather than failing every request: the engine restarts a plugin that
// stops answering and reports one that answers "cannot serve", because
// restarting does not make an unreachable backend reachable. This one has no
// backend, so the check is a placeholder that shows where the real one goes.
func checkHealth(context.Context) error {
	if os.Getenv("EXAMPLE_UNHEALTHY") != "" {
		return fmt.Errorf("EXAMPLE_UNHEALTHY is set, so this plugin is pretending its backend is unreachable")
	}
	return nil
}
