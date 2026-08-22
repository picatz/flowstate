package credentialsource

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors this package returns. Callers distinguish failures with
// [errors.Is].
var (
	// ErrUnknownSource is returned by [Resolve] for a name it does not
	// recognize. A typo fails closed rather than building a Source that
	// silently does nothing.
	ErrUnknownSource = errors.New("credentialsource: unknown source")

	// ErrSourceUnusable is returned by a [Source] that was asked for a token
	// and could not presently produce one: an ambient identity that is not
	// there (no ACTIONS_ID_TOKEN_REQUEST_URL, an unset environment variable,
	// a token file that does not exist), or a token the source received that
	// it cannot parse enough of to use. A Source that has one wraps this
	// rather than returning it bare, so a caller can tell which source failed.
	ErrSourceUnusable = errors.New("credentialsource: source could not produce a token")
)

// Source acquires a bearer token to present to a Flowstate server.
//
// [Source.Token] is meant to be called on every request an owning transport
// makes, the same pattern the CLI already used for a rotating token file:
// re-read (or re-mint) fresh each time, rather than cache once at startup and
// go stale. A Source that can cache internally — [SourceGitHubActions] does —
// still honors this by returning quickly from cache until its margin says
// otherwise.
//
// A Source obtained by name never returns a zero [Token] with a nil error. A
// caller that got no error and no token could not tell "anonymous is fine
// here" from "this source is broken", and a Source built through [Resolve] is
// always one the caller explicitly asked for — so "could not produce a token"
// is always [ErrSourceUnusable], never silence.
type Source interface {
	// Name identifies the source in errors and diagnostics.
	Name() string

	// Token returns a bearer token usable right now.
	Token(ctx context.Context) (Token, error)
}

// Names of the sources [Resolve] knows how to build.
const (
	SourceGitHubActions  = "github-actions"
	SourceGitLab         = "gitlab"
	SourceTerraformCloud = "terraform-cloud"
	SourceFile           = "file"
	SourceEnv            = "env"
)

// knownSources lists every buildable name, for the error a typo gets. Derived
// from the constants above rather than written out again, so a source added
// without being listed here is not a thing that can happen.
var knownSources = []string{
	SourceGitHubActions,
	SourceGitLab,
	SourceTerraformCloud,
	SourceFile,
	SourceEnv,
}

// Config gathers the values a named [Source] may need. Which fields a given
// name reads is documented on that name's constant; a field a name does not
// use is ignored.
type Config struct {
	// Audience is the value the token's "aud" claim must carry.
	//
	// Required by [SourceGitHubActions], which mints a token addressed to it.
	// Optional for [SourceGitLab] and [SourceTerraformCloud], where the
	// platform bound the audience when it minted the token and the only thing
	// left to do is *check* it: given, a token addressed elsewhere is refused
	// with a diagnostic naming the job or workspace setting to change; empty,
	// the token is presented with whatever audience it carries and the server
	// decides. Ignored by [SourceFile] and [SourceEnv].
	Audience string

	// TokenFile is the path [SourceFile] reads.
	TokenFile string

	// EnvVar is the environment variable to read.
	//
	// [SourceEnv] defaults to FLOWSTATE_TOKEN. [SourceGitLab] defaults to
	// [DefaultGitLabIDTokenEnvVar] — GitLab lets the job author name the
	// `id_tokens:` key, so a job that already uses another name says so here.
	// [SourceTerraformCloud] defaults to
	// [DefaultTerraformCloudTokenEnvVar], and is how a run using a tagged
	// audience names the tagged variable (see
	// [TerraformCloudTaggedEnvVar]). Ignored by the rest.
	EnvVar string
}

// Resolve builds the named [Source].
//
// Fails closed: a name this package does not recognize is a construction-time
// error rather than a Source that quietly never produces a token. A caller
// naming a source explicitly is a caller for whom "did not work" and "worked
// anonymously" must never be confused.
func Resolve(name string, cfg Config) (Source, error) {
	switch {
	case name == SourceGitHubActions:
		if cfg.Audience == "" {
			return nil, fmt.Errorf("%w: %s needs an audience naming the Flowstate server "+
				"this token is for (--audience or FLOWSTATE_AUDIENCE)", ErrSourceUnusable, SourceGitHubActions)
		}
		return NewGitHubActionsSource(cfg.Audience)

	case name == SourceGitLab:
		return NewGitLabSource(
			WithGitLabEnvVar(cfg.EnvVar),
			WithGitLabAudience(cfg.Audience),
		)

	case name == SourceTerraformCloud:
		return NewTerraformCloudSource(
			WithTerraformCloudEnvVar(cfg.EnvVar),
			WithTerraformCloudAudience(cfg.Audience),
		)

	case name == SourceFile:
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("%w: %s needs a path (--token-file or FLOWSTATE_TOKEN_FILE)",
				ErrSourceUnusable, SourceFile)
		}
		return NewFileSource(cfg.TokenFile), nil

	case name == SourceEnv:
		v := cfg.EnvVar
		if v == "" {
			v = "FLOWSTATE_TOKEN"
		}
		return NewEnvSource(v), nil

	default:
		return nil, fmt.Errorf("%w: %q (known sources: %s)",
			ErrUnknownSource, name, strings.Join(knownSources, ", "))
	}
}
