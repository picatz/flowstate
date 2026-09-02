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

// egressClient is the client the deployment's own egress policy governs,
// granted to this process at launch and installed as go-git's http(s) transport
// at startup - see plugins/vcs/clone.go for the full argument, which applies
// here unchanged: every byte this plugin reads from or writes to a remote
// crosses it, and it is installed once because go-git's client registry is a
// process-wide default per scheme, not a per-request option.
//
// The policy behind it used to be one this plugin built for itself, which meant
// a deny rule an operator wrote in --egress-policy did not reach a git.* task at
// all (#1321). The rules are now the deployment's; the two bounds this plugin
// states for its own transport are not, because a packfile is not the shape of
// response the operator's file is sized for - see
// [sdk.HTTPClientWithBounds].
//
// Nil means the grant could not be used, and [egressRefusal] says why.
var egressClient *http.Client

// egressRefusal is why there is no policy, kept so the task boundary can
// refuse with the SDK's message - which names the environment variable and the
// worker that sets it - rather than with a denial of its own invention.
var egressRefusal error

// installEgressPolicy takes the deployment's grant and registers it as the
// client every go-git https operation in this process uses - reads and writes
// alike, since [githttp.NewClient] backs both upload-pack (clone/fetch) and
// receive-pack (push) sessions.
//
// A grant this process cannot use does not stop it, and under `flow` no such
// grant arrives: a policy that cannot be parsed or built is refused when the CLI
// reads the operator's file and again by plugin.NewHost before any plugin is
// launched. What is left for this to handle is a launch by something that is not
// a Flowstate worker - a shell, a third-party host - which grants nothing, and
// where refusing to start would take away the one thing such a launch is good
// for: being asked what this plugin can do. The refusal belongs to whoever asks
// for a policy, which is the task boundary.
//
// What it must not do is leave go-git's own default client installed, which is
// ungoverned: [refusingTransport] takes that slot instead, so the fail-closed
// answer holds even for a path that forgot to ask.
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
// The transport below would refuse anyway; this is here for the answer's
// shape rather than its existence. A refusal arriving as a transport error
// reads as a network failure a retry might fix, when in fact nothing about
// this worker will change until it is relaunched with a grant.
func requireEgressPolicy() error {
	if egressClient != nil {
		return nil
	}

	return sdk.PermissionDenied("this plugin was launched without a usable egress policy: %v", egressRefusal)
}

// refusingTransport answers every request with why the grant could not be
// used, and dials nothing.
//
// go-git resolves its http client from a process-wide registry, so the choice
// at startup is between a governed client and *some* client - leaving the slot
// alone leaves go-git's own, which is bound by nothing the deployment said.
type refusingTransport struct{ err error }

func (t refusingTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, t.err }

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
	// The ceiling here is maxResumeCloneDepth, not maxCloneDepth: every
	// caller except a cursor-driven resume (log.go's own
	// resumeCloneDepthSteps) asks for at most maxCloneDepth and is
	// unaffected by this being larger; a resume is the one path that
	// deliberately widens its own request past that as pagination goes
	// deeper into history, and this is the bound that keeps that widening
	// itself bounded rather than unbounded - see maxResumeCloneDepth's own
	// doc comment.
	if opts.depth <= 0 || opts.depth > maxResumeCloneDepth {
		return nil, fmt.Errorf("clone depth %d is out of bounds (1-%d)", opts.depth, maxResumeCloneDepth)
	}

	if err := requireEgressPolicy(); err != nil {
		return nil, err
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
