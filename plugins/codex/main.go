package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

func main() {
	sdk.Main(sdk.Plugin{
		Name:    "codex",
		Version: "0.1.0",
		Description: "Runs a single bounded OpenAI Codex agentic turn (over the codex CLI) and returns its final " +
			"message, any patch it produced, and usage - the demonstration that flowstate orchestrates AI agents " +
			"as durable workloads.",

		// No Secrets field: unlike plugins/vcs and plugins/github, which
		// each stand up their own secret scheme because they predate
		// secret_inputs, this plugin declares api_key in Task.SecretInputs
		// below and lets the host resolve it against whatever provider the
		// deployment already configured (env, file, vault, ...) - see
		// doc.go, "Secrets," for what that does and does not guarantee.
		Tasks: []sdk.Task{
			{
				Name:    "exec",
				Summary: "One bounded agentic run of OpenAI Codex: a prompt in, a final message, an optional patch, and usage out.",
				Input:   &codexv1.ExecInputs{},
				Output:  &codexv1.ExecOutputs{},
				// api_key is the only input this task accepts a host secret
				// reference through. Nothing else here is a credential.
				SecretInputs: []string{"api_key"},
				Fn:           codexExec,
			},
		},

		Health: checkHealth,
	})
}

// checkHealth reports whether this plugin's own configuration is coherent.
//
// There is no long-lived backend connection to check - every codex.exec
// call launches its own subprocess - so what is worth reporting is whether
// the one thing every call needs (a configured, valid codex binary) is
// actually in place, the same reasoning plugins/github's checkHealth gives
// for its own auth configuration: surfaced as "not serving" here, rather
// than as a mysterious failure on the first task call.
func checkHealth(_ context.Context) error {
	if _, err := resolveCodexBinary(); err != nil {
		return err
	}
	if _, err := loadOperatorPolicy(); err != nil {
		return err
	}
	return nil
}
