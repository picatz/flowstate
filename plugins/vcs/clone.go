package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// egressClient is the one client this process builds from the deployment's
// grant, installed as go-git's http(s) transport at startup (see main.go).
// Every byte this plugin ever reads from a remote crosses it, which is what
// makes maxResponseBytes an enforced bound rather than a comment: netpolicy caps
// a response body in the RoundTripper itself, on every response regardless of
// status code, which is the layer CLAUDE.md's own lesson about connect-go's
// non-200 unmarshaler names as the only place a cap cannot be bypassed by an
// error path the caller forgot to check.
//
// It is taken once, at process start, rather than per clone: the grant is a
// launch-time snapshot, and every clone in this process shares the same egress
// rules for the same reason every http task invocation in the core engine
// shares its worker's policy.
//
// The rules are the deployment's, not this plugin's. Until #1322 this process
// built its own safe default with netpolicy.New, which meant an operator who
// wrote a deny rule in --egress-policy governed the built-in http task and not
// a single vcs.* clone. What stays this plugin's own are the two transport
// bounds below: a packfile is not the shape of response an operator sizes
// max_response_bytes for - see [sdk.HTTPClientWithBounds].
//
// Nil means the grant could not be used, and [egressRefusal] says why.
var egressClient *http.Client

// egressRefusal is why there is no policy, kept so the task boundary can refuse
// with the SDK's message - which names the environment variable and the worker
// that sets it - rather than with a denial of its own invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant and registers it as the
// client every go-git https operation in this process uses.
//
// go-git's own client registry (transport.Protocols) is a package-level map,
// which is why this happens once at startup and not per request: go-git has
// no per-CloneOptions way to say "use this http.Client," only a process-wide
// default per scheme. That is a real constraint worth naming rather than
// working around by, say, spinning up a second process per clone - this
// plugin has exactly one egress policy, and go-git's global registration
// model is a fine fit for that.
//
// A grant this process cannot use does not stop it, and under `flow` no such
// grant arrives: a policy that cannot be parsed or built is refused when the CLI
// reads the operator's file and again by plugin.NewHost before any plugin is
// launched. What is left is a launch by something that is not a Flowstate worker
// - a shell, a third-party host - which grants nothing, and which is still worth
// answering "here is what I can do" to. It must not leave go-git's own default
// client in that slot either, since that client is governed by nothing -
// [refusingTransport] takes it instead, so the fail-closed answer holds even on
// a path that forgot to ask.
func installEgressPolicy() {
	// One build of the grant, not two. The client is what every byte crosses,
	// and it is the SDK's rather than policy.Client() so that a clone sending a
	// token is marked as carrying a credential: go-git sets Authorization for
	// BasicAuth, and an operator rule naming `credentials` has to decide this
	// request the way it decides the built-in http task's. Nothing here needs
	// the policy object itself, and building it a second time would give this
	// process two rate-limit buckets for one deployment's policy - the bound is
	// per process, and two of them is not the number the operator wrote.
	governed, err := sdk.HTTPClientWithBounds(maxResponseBytes, requestTimeout)
	if err != nil {
		egressRefusal = err
		client.InstallProtocol("https", githttp.NewClient(&http.Client{Transport: refusingTransport{err}}))
		return
	}

	egressClient = governed
	client.InstallProtocol("https", githttp.NewClient(governed))
}

// requireEgressPolicy is what every task calls before it reaches a remote.
//
// The transport below would refuse anyway; this is here for the answer's shape
// rather than its existence. A refusal arriving as a transport error reads as a
// network failure a retry might fix, when nothing about this worker will change
// until it is relaunched with a grant.
func requireEgressPolicy() error {
	if egressClient != nil {
		return nil
	}

	return sdk.PermissionDenied("this plugin was launched without a usable egress policy: %v", egressRefusal)
}

// refusingTransport answers every request with why the grant could not be used,
// and dials nothing.
type refusingTransport struct{ err error }

func (t refusingTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, t.err }

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
//   - Bounded three times over on size: [cloneOptions.depth] bounds the
//     commit graph go-git asks the remote for, the egress policy installed
//     in installEgressPolicy bounds every individual HTTP response along
//     the way to maxResponseBytes (the compressed bytes a remote sends),
//     and the [packBoundedStorer] this function stores into bounds the
//     decompressed object bytes go-git's packfile parser materializes from
//     those responses to maxInflatedBytes (see packbound.go for what that
//     third bound does and does not close - a real residual, documented
//     rather than hidden). Depth does not bound the size of any single blob
//     within that depth - a shallow clone of a repository whose latest
//     commit adds one enormous file is still one enormous file - and this
//     plugin does not claim otherwise: the byte caps are the actual
//     backstop for that case, not a true "maximum repository size," and
//     that gap is recorded in the README rather than left for someone to
//     discover.
func cloneBounded(ctx context.Context, opts cloneOptions) (*git.Repository, error) {
	return cloneBoundedWithInflationCap(ctx, opts, maxInflatedBytes)
}

// cloneBoundedWithInflationCap is cloneBounded with the packfile-inflation
// bound taken as a parameter rather than the package's own maxInflatedBytes
// constant. cloneBounded is the only production caller, always with the real
// constant; this exists so packbound_test.go can drive a real clone -
// through the real transport, the real packfile parser, a real local git
// repository - against a small, fast-to-exceed cap without needing a fixture
// repository whose decompressed content actually reaches 512 MiB.
func cloneBoundedWithInflationCap(ctx context.Context, opts cloneOptions, maxInflated int64) (*git.Repository, error) {
	if opts.depth <= 0 || opts.depth > maxCloneDepth {
		return nil, fmt.Errorf("clone depth %d is out of bounds (1-%d)", opts.depth, maxCloneDepth)
	}

	if err := requireEgressPolicy(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	cloneOpts := &git.CloneOptions{
		URL:          opts.url.String(),
		Depth:        opts.depth,
		SingleBranch: false, // vcs.diff needs to resolve two revisions, which may sit on different branches
		// AllTags, not NoTags or the git.TagFollowing default: the task
		// schema and README both advertise `ref: v1.2.3` as a supported
		// lookup, and TagFollowing only fetches a tag whose target commit
		// is already within the shallow window this clone fetched - which a
		// release tag, named specifically because it is *not* one of the
		// last few commits, usually is not. AllTags fetches every tag ref
		// (and, for one that points outside the shallow window, the single
		// commit object it points to) regardless of depth, which is what
		// actually resolves a tag named by an author rather than only one
		// that happens to sit near a branch tip. The cost is bounded the
		// same way everything else here is: tag refs are lightweight, and
		// any object AllTags pulls in still crosses the egress policy's
		// maxResponseBytes cap like every other byte this clone reads.
		Tags:       git.AllTags,
		NoCheckout: true, // objects only; nothing here ever needs a working tree
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

	repo, err := git.CloneContext(ctx, newPackBoundedStorer(maxInflated), nil, cloneOpts)
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
