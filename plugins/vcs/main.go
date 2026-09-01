package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	vcsv1 "github.com/picatz/flowstate/plugins/vcs/gen/vcs/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "vcs",
		Version:     "0.1.0",
		Description: "Reads a repository's commit history and diffs two revisions of it, over git (go-git). No shared workspace, no subprocesses.",

		Secrets: &sdk.Secrets{
			Schemes: []string{secretScheme},
			Resolve: resolveSecret,
		},

		Tasks: []sdk.Task{
			{
				Name:                 "log",
				Summary:              "A bounded slice of a repository's commit history.",
				Input:                &vcsv1.LogInputs{},
				Output:               &vcsv1.LogOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   vcsLog,
			},
			{
				Name:                 "diff",
				Summary:              "The changes between two revisions of a repository, as a unified diff and a per-file summary.",
				Input:                &vcsv1.DiffInputs{},
				Output:               &vcsv1.DiffOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   vcsDiff,
			},
		},

		Health: checkHealth,
	})
}

// checkHealth reports whether this plugin can serve.
//
// There is no long-lived backend connection to check - every clone is a
// fresh, self-contained request - so the only thing worth reporting here is
// whether the deployment's egress policy reached this process, which is what
// every task is governed by. A real "can I reach GitHub/GitLab/wherever"
// check would be a check on the *remote* this run happens to name, which
// is not this plugin's to assume - a health check answers "is this plugin
// able to serve," not "is the internet up."
//
// Describe and the catalog keep working when this is unhealthy, deliberately:
// knowing what a plugin offers is not the same as being able to reach a remote.
func checkHealth(_ context.Context) error {
	return requireEgressPolicy()
}
