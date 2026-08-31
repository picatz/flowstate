package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

func main() {
	if err := installEgressPolicy(); err != nil {
		fmt.Fprintf(os.Stderr, "sql: %v\n", err)
		os.Exit(1)
	}

	sdk.Main(sdk.Plugin{
		Name:    "sql",
		Version: "0.1.0",
		Description: "Policy-governed PostgreSQL: bounded, typed reads (sql.query) and one " +
			"transaction per activity (sql.exec); DSNs must be host-resolved secrets. See doc.go.",

		// No Secrets field: like plugins/codex, this plugin declares dsn in
		// each task's SecretInputs below and lets the host resolve it
		// against whatever provider the deployment already configured, so
		// this plugin process never holds a provider credential or
		// reference of its own - see doc.go, "Secrets."
		Tasks: []sdk.Task{
			{
				Name:    "query",
				Summary: "One bounded, parameterized, read-only SQL query: typed rows out, refused rather than truncated past max_rows.",
				Input:   &sqlv1.QueryInputs{},
				Output:  &sqlv1.QueryOutputs{},
				// dsn is the only input either task accepts a host secret
				// reference through. Nothing else here is a credential.
				SecretInputs:         []string{"dsn"},
				RequiredSecretInputs: []string{"dsn"},
				Fn:                   sqlQuery,
			},
			{
				Name:                 "exec",
				Summary:              "One or more parameterized SQL statements, run as one transaction that begins and ends inside this call.",
				Input:                &sqlv1.ExecInputs{},
				Output:               &sqlv1.ExecOutputs{},
				SecretInputs:         []string{"dsn"},
				RequiredSecretInputs: []string{"dsn"},
				Fn:                   sqlExec,
			},
		},

		Health: checkHealth,
	})
}

// checkHealth reports whether this plugin can serve.
//
// There is no long-lived backend connection to check - see doc.go,
// "Transactions end where the activity ends": every call opens and closes
// its own connection, so there is nothing durable here to probe. What is
// worth reporting is only that the process itself came up cleanly, the same
// minimal-but-honest health check plugins/vcs and plugins/git report for
// the identical reason (no long-lived backend connection either).
func checkHealth(_ context.Context) error {
	return nil
}
