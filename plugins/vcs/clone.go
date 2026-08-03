package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// egressPolicy is the one netpolicy.Policy this process builds, installed as
// go-git's http(s) transport at startup (see main.go). Every byte this
// plugin ever reads from a remote crosses it, which is what makes
// maxResponseBytes an enforced bound rather than a comment: netpolicy caps a
// response body in the RoundTripper itself, on every response regardless of
// status code, which is the layer CLAUDE.md's own lesson about connect-go's
// non-200 unmarshaler names as the only place a cap cannot be bypassed by an
// error path the caller forgot to check.
//
// It is built once, at process start, rather than per clone: netpolicy.New
// compiles CEL rules and is meant to be reused, and every clone in this
// process shares the same egress rules for the same reason every http task
// invocation in the core engine shares its worker's policy.
var egressPolicy *netpolicy.Policy

// installEgressPolicy builds the policy and registers it as the client every
// go-git https operation in this process uses.
//
// go-git's own client registry (transport.Protocols) is a package-level map,
// which is why this happens once at startup and not per request: go-git has
// no per-CloneOptions way to say "use this http.Client," only a process-wide
// default per scheme. That is a real constraint worth naming rather than
// working around by, say, spinning up a second process per clone - this
// plugin has exactly one egress policy, and go-git's global registration
// model is a fine fit for that.
func installEgressPolicy() error {
	policy, err := netpolicy.New(
		netpolicy.WithMaxResponseBytes(maxResponseBytes),
		netpolicy.WithTimeout(requestTimeout),
	)
	if err != nil {
		return fmt.Errorf("building the egress policy: %w", err)
	}
	egressPolicy = policy

	client.InstallProtocol("https", githttp.NewClient(policy.Client()))

	return nil
}

// cloneOptions is what every task in this plugin asks of a clone: a URL
// already checked by validateRepositoryURL, a bounded depth, and an already
// -resolved (never a literal) authentication token.
//
// token is a closure rather than a plain string field, on purpose: fmt
// reaches an ordinary field through reflection regardless of whether
// anything actually calls Println on this struct today, and a value type
// that *could* print a resolved credential if someone later logged it for
// debugging is a hazard this codebase's own CLAUDE.md calls out specifically
// - "hold material in a closure; reflection cannot reach a captured
// variable." See clone_test.go for the containment-shape tests this pattern
// exists to pass: %v, %+v, %#v, and %s, on this struct directly and on a
// slice of them.
type cloneOptions struct {
	url   *url.URL
	depth int
	token func() string // nil or returns "" when the repository is public
}

// tokenValue reads opts.token, treating a nil closure as "no credential."
func (opts cloneOptions) tokenValue() string {
	if opts.token == nil {
		return ""
	}
	return opts.token()
}

// cloneBounded opens a shallow, in-memory, single-invocation clone.
//
// Three things about this function are the point of the whole plugin, not
// incidental to it:
//
//   - In-memory ([memory.NewStorage]) and no worktree (passing a nil
//     billy.Filesystem, which go-git treats as "bare, objects only"). There
//     is no path on disk this ever writes, so there is nothing to clean up,
//     nothing an activity retry could see left over from a previous attempt,
//     and nothing a later, differently-privileged step could read even by
//     mistake - see doc.go's "why no vcs.clone" for why that property is
//     load-bearing rather than a convenience.
//   - Scoped to the ctx it is given, which is the activity's own context:
//     when the workflow step is cancelled or its timeout lapses, the
//     underlying HTTP request this makes is cancelled with it, and this
//     function returns rather than continuing to fetch into memory nobody
//     will read.
//   - Bounded twice over on size: [cloneOptions.depth] bounds the commit
//     graph go-git asks the remote for, and the egress policy installed in
//     installEgressPolicy bounds every individual HTTP response along the
//     way to maxResponseBytes. Depth does not bound the size of any single
//     blob within that depth - a shallow clone of a repository whose latest
//     commit adds one enormous file is still one enormous file - and this
//     plugin does not claim otherwise: the response-byte cap is the actual
//     backstop for that case, not a true "maximum repository size," and
//     that gap is recorded in the README rather than left for someone to
//     discover.
func cloneBounded(ctx context.Context, opts cloneOptions) (*git.Repository, error) {
	if opts.depth <= 0 || opts.depth > maxCloneDepth {
		return nil, fmt.Errorf("clone depth %d is out of bounds (1-%d)", opts.depth, maxCloneDepth)
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	cloneOpts := &git.CloneOptions{
		URL:          opts.url.String(),
		Depth:        opts.depth,
		SingleBranch: false, // vcs.diff needs to resolve two revisions, which may sit on different branches
		Tags:         git.NoTags,
		NoCheckout:   true, // objects only; nothing here ever needs a working tree
	}

	if token := opts.tokenValue(); token != "" {
		// GitHub, GitLab, and Gitea all accept the same shape over HTTPS: a
		// bearer-style token as the password of HTTP Basic auth, with the
		// username effectively ignored by the server (each forge does read
		// it for logging/attribution in places, so "x-access-token" is
		// written as a convention some forges recognize, not a requirement
		// this plugin depends on). That is the one thing about this
		// authentication shape that is not forge-specific, and it is why
		// this plugin's own scheme is named "vcs" rather than after any one
		// forge - see the plugin's README, "why this plugin resolves its own
		// secrets."
		cloneOpts.Auth = &githttp.BasicAuth{
			Username: "x-access-token",
			Password: token,
		}
	}

	repo, err := git.CloneContext(ctx, memory.NewStorage(), nil, cloneOpts)
	if err != nil {
		return nil, classifyGitError(err)
	}

	return repo, nil
}

// resolve turns a revision string into a commit hash within a cloned repo,
// treating an empty string as "the ref the clone already checked out"
// (HEAD).
func resolve(repo *git.Repository, revision string) (plumbing.Hash, error) {
	if revision == "" {
		head, err := repo.Head()
		if err != nil {
			return plumbing.ZeroHash, classifyGitError(err)
		}
		return head.Hash(), nil
	}

	hash, err := repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return plumbing.ZeroHash, classifyGitError(err)
	}
	return *hash, nil
}
