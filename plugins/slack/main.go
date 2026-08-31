package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

func main() {
	if err := installEgressPolicy(); err != nil {
		fmt.Fprintf(os.Stderr, "slack: %v\n", err)
		os.Exit(1)
	}

	sdk.Main(sdk.Plugin{
		Name:        "slack",
		Version:     "0.1.0",
		Description: "Posts bounded accessible text to Slack for approval and human-in-the-loop notifications; outbound only.",
		Tasks: []sdk.Task{{
			Name:                 "post",
			Summary:              "Post one bounded text message with a stable client message key; production runs only.",
			Input:                &slackv1.PostInputs{},
			Output:               &slackv1.PostOutputs{},
			SecretInputs:         []string{"token"},
			RequiredSecretInputs: []string{"token"},
			Fn:                   slackPost,
		}},
		Health: checkHealth,
	})
}

func checkHealth(_ context.Context) error {
	// There is no long-lived Slack connection to probe. Keep discovery and
	// validation available without granting network authority; slackPost checks
	// for the operator snapshot at the task boundary before decoding inputs or
	// attempting a write.
	return nil
}
