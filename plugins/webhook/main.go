package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	webhookv1 "github.com/picatz/flowstate/plugins/webhook/gen/webhook/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "webhook",
		Version:     "0.1.0",
		Description: "Sends one signed outbound webhook delivery, signing with the schemes a webhook trigger's verify: block speaks, so a delivery from one Flowstate verifies at another.",
		Tasks: []sdk.Task{{
			Name:                 "send",
			Summary:              "Sign a body with a secret key and POST it once to a receiver, with an optional idempotency key.",
			Input:                &webhookv1.SendInputs{},
			Output:               &webhookv1.SendOutputs{},
			SecretInputs:         []string{"signing_key"},
			RequiredSecretInputs: []string{"signing_key"},
			Fn:                   webhookSend,
		}},
		Health: checkHealth,
	})
}

func checkHealth(_ context.Context) error {
	// There is no connection to probe. Discovery and validation stay available
	// without network authority; webhookSend checks for the operator snapshot at
	// the task boundary.
	return nil
}
