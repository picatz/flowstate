package main

import "github.com/spf13/cobra"

// Turning `--gates-ui` into the browser page for approval gates (#1748).
//
// A page that lets a person answer a gate is a surface a deployment opts into,
// the way it opts into --webhook: where it is not asked for, the routes do not
// exist, so there is nothing to probe and nothing to harden on a deployment whose
// approvers use the CLI. See pkg/flowstate/v1/gates for what the page is and
// what authenticates it.

// addGatesUIFlags declares the gate page's surface on the server command.
func addGatesUIFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("gates-ui", false,
		"serve a page at /gates/<workflow-id>/<signal> for each pending `wait_for_signal:` gate, "+
			"showing the question it asks and Approve and Deny buttons that deliver the signal. "+
			"The page holds no credential: it forwards the visitor's Authorization header to this "+
			"server's own API, so who may see or answer a gate is decided by the same authenticator, "+
			"tenancy check and `signals:` policy `flow signal` meets, and every answer is audited the "+
			"same way. Reach it through an identity-aware proxy that sets the header. Without this "+
			"flag the routes do not exist")
}

// gatesUIOptions is the handler options --gates-ui asks for: none when it is
// not given, so a deployment that never asked builds the handler it always did.
func gatesUIOptions(cmd *cobra.Command) []handlerOption {
	if on, _ := cmd.Flags().GetBool("gates-ui"); on {
		return []handlerOption{withGatesUI()}
	}

	return nil
}
