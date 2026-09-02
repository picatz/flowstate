package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "git",
		Version:     "0.1.0",
		Description: "Reads a remote's refs, history, and file content, and writes a commit to a branch, over git (go-git). One activity, one write - see doc.go.",

		Secrets: &sdk.Secrets{
			Schemes: []string{secretScheme},
			Resolve: resolveSecret,
		},

		Tasks: []sdk.Task{
			{
				Name:                 "ls_remote",
				Summary:              "The refs a remote currently advertises, without cloning.",
				Input:                &gitv1.LsRemoteInputs{},
				Output:               &gitv1.LsRemoteOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   gitLsRemote,
			},
			{
				Name:                 "log",
				Summary:              "A bounded slice of a repository's commit history reachable from ref, including each commit's author, committer, message, and parents.",
				Input:                &gitv1.LogInputs{},
				Output:               &gitv1.LogOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   gitLog,
			},
			{
				Name:                 "read_file",
				Summary:              "One file's content, size, mode, and whether it is binary, at one ref.",
				Input:                &gitv1.ReadFileInputs{},
				Output:               &gitv1.ReadFileOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   gitReadFile,
			},
			{
				Name:                 "commit_push",
				Summary:              "Materialize base_ref, apply files and/or a patch, commit, and push to branch - one activity, compare-and-swapped against base_ref, never forced.",
				Input:                &gitv1.CommitPushInputs{},
				Output:               &gitv1.CommitPushOutputs{},
				SecretInputs:         []string{"token"},
				RequiredSecretInputs: []string{"token"},
				Fn:                   gitCommitPush,
			},
		},

		Health: checkHealth,
	})
}

// checkHealth reports whether this plugin can serve - see plugins/vcs's
// identical function for why there is no long-lived backend connection to
// check beyond the egress policy having installed successfully.
//
// The deployment's grant is that policy now, so an unhealthy answer here is a
// worker whose grant this plugin could not use, and it says which. Describe and
// the catalog still work in that state, deliberately: knowing what a plugin
// offers is not the same as being able to reach a remote.
func checkHealth(_ context.Context) error {
	return requireEgressPolicy()
}
