package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	anthropicv1 "github.com/picatz/flowstate/plugins/anthropic/gen/anthropic/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "anthropic",
		Version:     "0.1.0",
		Description: "Puts typed questions to a Claude model and returns validated, provider-neutral answers; outbound only.",
		Credentials: []*flowstatev1.CredentialDeclaration{{
			Name:        "api_key",
			Description: "Anthropic API key, held as a secret reference and resolved by the host; sent only as the x-api-key header.",
		}},
		Tasks: []sdk.Task{{
			Name:                 "decide",
			Summary:              "Answer a set of predicate, choice and score questions about evidence, with a self-reported confidence only when the model gives one.",
			Input:                &anthropicv1.DecideInputs{},
			Output:               &anthropicv1.DecideOutputs{},
			SecretInputs:         []string{"api_key"},
			RequiredSecretInputs: []string{"api_key"},
			Fn:                   anthropicDecide,
		}},
		Health: checkHealth,
	})
}

func checkHealth(_ context.Context) error {
	// There is no long-lived connection to probe. Keep discovery and validation
	// available without granting network authority; anthropicDecide checks for
	// the operator snapshot at the task boundary before it decodes inputs or
	// sends a request.
	return nil
}
