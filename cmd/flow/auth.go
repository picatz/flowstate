package main

import (
	"encoding/json"
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/spf13/cobra"
)

func newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "auth", Short: "Inspect authentication protocol capabilities"}
	capabilities := &cobra.Command{
		Use: "capabilities", Short: "Report implemented authentication profiles",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			rows := make([]map[string]any, 0, len(auth.AuthProfiles()))
			for _, p := range auth.AuthProfiles() {
				rows = append(rows, map[string]any{
					"id": p.GetId(), "revision": p.GetRevision(), "maturity": p.GetMaturity().String(),
					"implemented": true, "advertised": true, "policy_enabled": false,
					"experimental": p.GetMaturity().String() == "AUTH_PROFILE_MATURITY_EXPERIMENTAL", "deprecated": p.GetDeprecated(),
				})
			}
			b, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), string(b))
			return err
		},
	}
	cmd.AddCommand(capabilities)
	return cmd
}
