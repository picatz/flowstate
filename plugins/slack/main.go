// Command flowstate-plugin-slack posts and updates Slack messages: plain text,
// vendor-neutral cards, and native Block Kit. Outbound only; see README.md.
package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "slack",
		Version:     "0.2.0",
		Description: "Posts and updates Slack messages for approval and human-in-the-loop notifications: text, cards, and typed Block Kit; outbound only.",
		Tasks: []sdk.Task{
			{
				Name:                 "post",
				Summary:              "Post one message (text, card, or Block Kit blocks), optionally in a thread or ephemeral, with a stable client message key; production runs only.",
				Input:                &slackv1.PostInputs{},
				Output:               &slackv1.PostOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   slackPost,
			},
			{
				Name:                 "update",
				Summary:              "Replace the body of a message this app posted, such as an approval card with its outcome; idempotent on its target, so safe to retry; production runs only.",
				Input:                &slackv1.UpdateInputs{},
				Output:               &slackv1.UpdateOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   slackUpdate,
			},
			{
				Name:    "respond",
				Summary: "Answer a Slack interaction through its response_url: replace or delete the clicked message, or show a new one to the clicker or the channel; takes no token; production runs only.",
				Input:   &slackv1.RespondInputs{},
				Output:  &slackv1.RespondOutputs{},
				Fn:      slackRespond,
			},
		},
		Health: checkHealth,
	})
}

func checkHealth(_ context.Context) error {
	// There is no long-lived Slack connection to probe. Keep discovery and
	// validation available without granting network authority; each task checks
	// for the operator snapshot at the task boundary before decoding inputs or
	// attempting a write.
	return nil
}
