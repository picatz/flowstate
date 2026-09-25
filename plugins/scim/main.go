package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	scimv1 "github.com/picatz/flowstate/plugins/scim/gen/scim/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "scim",
		Version:     "0.1.0",
		Description: "Reads and deactivates identity-provider users over SCIM 2.0 (RFC 7643, RFC 7644), for access reviews and joiner-mover-leaver workloads.",
		Tasks: []sdk.Task{
			{
				Name:                 "user_get",
				Summary:              "Read one user by provider id or by exact user name, with the attributes an access review acts on.",
				Input:                &scimv1.UserGetInputs{},
				Output:               &scimv1.UserGetOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   scimUserGet,
			},
			{
				Name:                 "user_list",
				Summary:              "Read a bounded page of users matching a SCIM filter, resumable through next_start_index.",
				Input:                &scimv1.UserListInputs{},
				Output:               &scimv1.UserListOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   scimUserList,
			},
			{
				Name:                 "user_deactivate",
				Summary:              "Turn one account off, optionally only if it has not changed since the reviewer read it.",
				Input:                &scimv1.UserDeactivateInputs{},
				Output:               &scimv1.UserDeactivateOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   scimUserDeactivate,
			},
		},
		Health: checkHealth,
	})
}

// checkHealth reports serving. There is no single provider to probe: which one
// a call means is that call's own base_url, so no endpoint's reachability would
// say anything about the next call. Each task checks for the operator's egress
// grant at its own boundary before it decodes inputs.
func checkHealth(_ context.Context) error {
	return nil
}
