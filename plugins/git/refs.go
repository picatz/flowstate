package main

import (
	"context"
	"net/url"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	gitv1 "github.com/picatz/flowstate/plugins/git/gen/git/v1"
)

// remoteRef pairs a ref name with its content hash - the in-process
// equivalent of gitv1.RemoteRef, used by both git.ls_remote and
// commit_push's own probe, so the two share exactly one implementation of
// "ask a remote what it currently advertises."
type remoteRef struct {
	name string
	sha  string
}

// listRemoteRefs asks u what it currently advertises, with no clone and no
// object fetched - a bare ls-remote, the same network round trip a clone's
// first step makes, stopped there. This is the machinery git.ls_remote
// exposes directly, and commit_push's own compare-and-swap probe (see
// commit_push.go) depends on it too: both need a ref's current hash without
// paying for a clone to find out.
//
// username is expected already resolved (see [resolveUsername]) - never
// empty by the time it reaches here, the same contract cloneOptions.username
// keeps. An empty value still falls back to [defaultBasicAuthUsername]
// rather than sending an empty username, the same fail-safe direction
// cloneBounded takes.
func listRemoteRefs(ctx context.Context, u *url.URL, token func() string, username string) ([]remoteRef, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{u.String()},
	})

	opts := &git.ListOptions{}
	if tok := tokenValueOf(token); tok != "" {
		user := username
		if user == "" {
			user = defaultBasicAuthUsername
		}
		opts.Auth = &githttp.BasicAuth{Username: user, Password: tok}
	}

	refs, err := remote.ListContext(ctx, opts)
	if err != nil {
		return nil, classifyGitError(err)
	}

	out := make([]remoteRef, 0, len(refs))
	for _, r := range refs {
		// HEAD (a symbolic ref) and every peeled tag object
		// ("refs/tags/x^{}") are advertised alongside the refs a workflow
		// actually names; only a ref with a direct, resolved hash is one of
		// this task's own RemoteRef entries.
		if r.Hash().IsZero() {
			continue
		}
		out = append(out, remoteRef{name: r.Name().String(), sha: r.Hash().String()})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })

	return out, nil
}

// tokenValueOf reads a token closure, treating nil as "no credential" - the
// same shape cloneOptions.tokenValue gives clone.go.
func tokenValueOf(token func() string) string {
	if token == nil {
		return ""
	}
	return token()
}

// findRemoteRef returns the ref named exactly name, if listRemoteRefs found
// one.
func findRemoteRef(refs []remoteRef, name string) (remoteRef, bool) {
	for _, r := range refs {
		if r.name == name {
			return r, true
		}
	}
	return remoteRef{}, false
}

// gitLsRemote implements git.ls_remote: the refs a remote currently
// advertises, without cloning.
func gitLsRemote(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in gitv1.LsRemoteInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	repoURL, err := validateRepositoryURL(in.GetUrl())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	token, err := tokenFromValue(ctx, in.GetToken())
	if err != nil {
		return nil, err
	}

	username, err := resolveUsername(in.GetUsername())
	if err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	refs, err := listRemoteRefs(ctx, repoURL, func() string { return token }, username)
	if err != nil {
		return nil, err
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	prefix := in.GetPrefix()
	out := make([]*gitv1.RemoteRef, 0, len(refs))
	truncated := false
	for _, r := range refs {
		if prefix != "" && !strings.HasPrefix(r.name, prefix) {
			continue
		}
		if len(out) >= maxRemoteRefs {
			truncated = true
			break
		}
		out = append(out, &gitv1.RemoteRef{Name: r.name, Sha: r.sha})
	}

	return sdk.EncodeOutputs(&gitv1.LsRemoteOutputs{
		Refs:      out,
		Truncated: truncated,
	})
}
