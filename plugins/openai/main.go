package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	openaiv1 "github.com/picatz/flowstate/plugins/openai/gen/openai/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "openai",
		Version:     "0.1.0",
		Description: "Puts typed questions to an OpenAI model through the Decisions API and returns validated, provider-neutral answers with model-derived probabilities; outbound only.",
		Credentials: []*flowstatev1.CredentialDeclaration{{
			Name:        "api_key",
			Description: "OpenAI API key, held as a secret reference and resolved by the host; sent only as the Authorization bearer token.",
		}},
		Tasks: []sdk.Task{{
			Name:                 "decide",
			Summary:              "Answer a set of predicate, choice and score questions about evidence, each with the probabilities the Decisions API returns.",
			Input:                &openaiv1.DecideInputs{},
			Output:               &openaiv1.DecideOutputs{},
			SecretInputs:         []string{"api_key"},
			RequiredSecretInputs: []string{"api_key"},
			Fn:                   openaiDecide,
		}},
		Health: checkHealth,
	})
}

func checkHealth(_ context.Context) error {
	// There is no long-lived connection to probe. Keep discovery and validation
	// available without granting network authority; openaiDecide checks for
	// the operator snapshot at the task boundary before it decodes inputs or
	// sends a request.
	return nil
}
