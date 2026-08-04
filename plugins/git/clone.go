package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// egressPolicy is the one netpolicy.Policy this process builds, installed as
// go-git's http(s) transport at startup - see plugins/vcs/clone.go for the
// full argument, which applies here unchanged: every byte this plugin reads
// from or writes to a remote crosses it, and it is built once because
// netpolicy.New compiles CEL rules and go-git's client registry is a
// process-wide default per scheme, not a per-request option.
var egressPolicy *netpolicy.Policy

// installEgressPolicy builds the policy and registers it as the client every
// go-git https operation in this process uses - reads and writes alike, since
// [githttp.NewClient] backs both upload-pack (clone/fetch) and receive-pack
// (push) sessions.
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

// cloneOptions is what every task in this plugin asks of a clone. token is a
// closure, never a plain string field, for the containment reason
// plugins/vcs/clone.go documents at length: fmt's reflection reaches an
// ordinary field regardless of what calls Println on it, and a closure is
// the one shape reflection cannot follow. See clone_test.go for the
// containment-shape tests this exists to pass.
//
// username is a plain field, deliberately: it is never secret material (see
// resolveUsername's own doc comment - it is a forge-facing identity string,
// not a credential), so it carries none of token's containment concern.
// Callers are expected to have already run it through [resolveUsername], so
// it is always the value this invocation should actually send - never
// empty, since resolveUsername turns an unset input into
// [defaultBasicAuthUsername] before cloneOptions is ever built. cloneBounded
// still falls back to that default itself if it somehow is empty, the same
// fail-safe direction as every other default in this plugin.
type cloneOptions struct {
	url      *url.URL
	depth    int
	token    func() string // nil or returns "" when the repository is public
	username string        // resolved; see resolveUsername
}

func (opts cloneOptions) tokenValue() string {
	if opts.token == nil {
		return ""
	}
	return opts.token()
}

// cloneBounded opens a shallow, in-memory, single-invocation clone - see
// plugins/vcs/clone.go's cloneBounded for the full argument on why in-memory,
// why NoCheckout, why AllTags, the depth-vs-blob-size gap the egress
// policy's response-byte cap backstops rather than closes, and the storer
// this now clones into ([packBoundedStorer], packbound.go) that additionally
// bounds packfile inflation past what the response-byte cap alone reaches.
// This plugin's own
// use is narrower than vcs's: it only ever needs to resolve base_ref and
// build a tree from it, never a branch tip a workflow author named directly,
// so SingleBranch is left false for the same reason vcs leaves it false -
// base_ref and, for the retry probe, the target branch, may not be the same
// ref.
func cloneBounded(ctx context.Context, opts cloneOptions) (*git.Repository, error) {
	return cloneBoundedWithInflationCap(ctx, opts, maxInflatedBytes)
}

// cloneBoundedWithInflationCap is cloneBounded with the packfile-inflation
// bound taken as a parameter rather than the package's own maxInflatedBytes
// constant - see plugins/vcs's identical seam for why: it lets
// packbound_test.go drive a real clone against a small, fast-to-exceed cap
// without a fixture repository whose decompressed content actually reaches
// 512 MiB. cloneBounded is the only production caller, always with the real
// constant.
func cloneBoundedWithInflationCap(ctx context.Context, opts cloneOptions, maxInflated int64) (*git.Repository, error) {
	if opts.depth <= 0 || opts.depth > maxCloneDepth {
		return nil, fmt.Errorf("clone depth %d is out of bounds (1-%d)", opts.depth, maxCloneDepth)
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	cloneOpts := &git.CloneOptions{
		URL:          opts.url.String(),
		Depth:        opts.depth,
		SingleBranch: false,
		Tags:         git.AllTags,
		NoCheckout:   true, // objects only; this plugin never uses a working tree
	}

	if token := opts.tokenValue(); token != "" {
		username := opts.username
		if username == "" {
			username = defaultBasicAuthUsername
		}
		cloneOpts.Auth = &githttp.BasicAuth{
			Username: username,
			Password: token,
		}
	}

	packStorer := newPackBoundedStorer(maxInflated)
	repo, err := git.CloneContext(ctx, packStorer, nil, cloneOpts)
	if err != nil {
		return nil, classifyGitError(err)
	}

	// The accounting above exists only to bound bytes go-git's packfile
	// parser decompresses from *this remote*, during *this* CloneContext
	// call. That call is synchronous - every object it was ever going to
	// write already has been by the time it returns - so there is nothing
	// left for packBoundedStorer to usefully count from here on. What comes
	// next in this plugin's only write path (doCommitPush's rebuildTree and
	// writeCommit, commit_push.go) writes this task's own new tree and
	// commit objects - built from files/patch content already bounded
	// separately, by maxFiles/maxFileBytes/maxTotalFileBytes/maxPatchBytes -
	// through this same repo.Storer. Left wrapped, those local writes would
	// keep incrementing total against a budget that is supposed to describe
	// what a remote sent, which is wrong in both directions: a clone that
	// landed comfortably under the cap, followed by an ordinary commit,
	// could be refused as a "remote decompression bomb" it never was, and
	// the number would stop meaning what its own name says.
	//
	// Unwrapping the field here - back to the plain *memory.Storage
	// packStorer was built around, never packStorer itself - is what keeps
	// that true: every go-git operation this plugin runs against the
	// returned *git.Repository after this line reads repo.Storer fresh
	// (Repository.Remote, for instance, builds a new *Remote from the
	// current field value on every call rather than caching one from clone
	// time - checked in go-git's own source, not assumed), so nothing here
	// depends on go-git holding a stale reference to the wrapped storer.
	repo.Storer = packStorer.Storage

	return repo, nil
}

// resolve turns a revision string into a commit hash within a cloned repo.
func resolve(repo *git.Repository, revision string) (plumbing.Hash, error) {
	hash, err := repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return plumbing.ZeroHash, classifyGitError(err)
	}
	return *hash, nil
}
