package main

import (
	"regexp"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// maxReferenceBytes bounds a reference before any of it is parsed. A real one
// is a few dozen bytes; the bound is what keeps a workflow's input from being
// the size of the work this plugin does on it.
const maxReferenceBytes = 1024

// dockerHubRegistry and dockerHubEndpoint are the one host rewrite this plugin
// performs, and it is written here rather than hidden in a dialer so that the
// egress policy an operator writes can name what will actually be dialed.
//
// Docker Hub is spelled docker.io everywhere a person writes an image, and its
// distribution API is served from registry-1.docker.io. Refusing to know that
// would make the single most common registry unusable; inferring a *registry*
// from a bare name ("alpine" meaning Docker Hub's library/alpine) is a
// different thing, and this plugin does not do it.
const (
	dockerHubRegistry = "docker.io"
	dockerHubEndpoint = "registry-1.docker.io"
)

var (
	// repositoryPattern is the distribution specification's own grammar for a
	// repository name: lower-case alphanumeric components, separated by one
	// period, one or two underscores, or one or more hyphens, in path segments.
	repositoryPattern = regexp.MustCompile(`^[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*$`)

	// tagPattern is likewise the specification's: at most 128 characters, and
	// never beginning with a period or a hyphen.
	tagPattern = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)

	// registryPattern bounds what may be dialed as a host: a DNS name or an
	// IPv4 literal, with an optional port. It is deliberately not a general URI
	// authority - no userinfo, no path, no scheme - because everything else
	// about a reference is parsed by position, and a permissive host would make
	// "registry/repo" ambiguous.
	registryPattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*(:[0-9]{1,5})?$`)
)

// reference is an artifact's address: where it lives, what it is called, and
// which version of it - by tag, by digest, or both.
type reference struct {
	// Registry is the host as the reference wrote it, and Endpoint is what this
	// plugin dials. They differ only for Docker Hub.
	Registry string
	Endpoint string

	// Repository is the path within the registry, without a leading slash.
	Repository string

	// Tag and Digest are the two ways to name a version. A reference may carry
	// both, in which case the digest decides and the tag is commentary - which
	// is what `repo:v1.2@sha256:...` means everywhere else it is written, and
	// what makes a pinned reference readable.
	Tag    string
	Digest string
}

// String renders the canonical, digest-pinned form when the digest is known,
// and the tagged form otherwise. It is what a workflow carries forward.
func (r reference) String() string {
	if r.Digest != "" {
		return r.Registry + "/" + r.Repository + "@" + r.Digest
	}
	if r.Tag != "" {
		return r.Registry + "/" + r.Repository + ":" + r.Tag
	}
	return r.Registry + "/" + r.Repository
}

// target is the tag or digest a manifest request addresses, which the
// distribution specification calls a reference and this type has two of.
func (r reference) target() string {
	if r.Digest != "" {
		return r.Digest
	}
	return r.Tag
}

// parseReference reads registry/repository[:tag][@digest].
//
// The registry is required and never inferred. Tooling that defaults it decides
// on a workflow's behalf which company's servers a deployment talks to, and the
// decision is invisible in the file being reviewed; a reference in a Flowfile
// says where its bytes come from or it does not parse.
func parseReference(raw string) (reference, error) {
	if raw == "" {
		return reference{}, sdk.InvalidInput("reference is required")
	}
	if len(raw) > maxReferenceBytes {
		return reference{}, sdk.InvalidInput("reference is %d bytes, over the %d-byte limit", len(raw), maxReferenceBytes)
	}

	rest := raw
	var digest string
	if at := strings.Index(rest, "@"); at >= 0 {
		digest = rest[at+1:]
		rest = rest[:at]
		if err := flowstatev1.ValidateContentDigest(digest); err != nil {
			return reference{}, sdk.InvalidInput(
				"reference names the digest %q, which is not one this plugin can compare bytes against: %v",
				truncate(digest, 96), err)
		}
	}

	registry, remainder, found := strings.Cut(rest, "/")
	if !found {
		return reference{}, sdk.InvalidInput(
			"reference %q names no registry; write registry/repository[:tag][@digest], such as "+
				"ghcr.io/owner/image:1.2.3 — this plugin never infers a registry, so a Flowfile says where its bytes come from",
			truncate(raw, 96))
	}
	if !registryPattern.MatchString(registry) || !looksLikeHost(registry) {
		return reference{}, sdk.InvalidInput(
			"reference %q begins with %q, which is not a registry host; the first segment must be a host such as "+
				"ghcr.io, docker.io or registry.example.com:5000",
			truncate(raw, 96), truncate(registry, 64))
	}

	repository := remainder
	var tag string
	if colon := strings.LastIndex(remainder, ":"); colon >= 0 {
		repository, tag = remainder[:colon], remainder[colon+1:]
		if !tagPattern.MatchString(tag) {
			return reference{}, sdk.InvalidInput(
				"reference %q names the tag %q, which is not a tag: at most 128 characters of letters, digits, "+
					"period, underscore and hyphen, not beginning with a period or hyphen",
				truncate(raw, 96), truncate(tag, 64))
		}
	}
	if !repositoryPattern.MatchString(repository) {
		return reference{}, sdk.InvalidInput(
			"reference %q names the repository %q, which is not one: lower-case alphanumeric path segments, "+
				"separated by a period, underscores or hyphens",
			truncate(raw, 96), truncate(repository, 96))
	}

	// A reference with neither is the repository itself, which none of these
	// tasks can act on: there is no default tag here, because "latest" is a
	// convention this plugin has no business asserting on a workflow's behalf.
	if tag == "" && digest == "" {
		return reference{}, sdk.InvalidInput(
			"reference %q names neither a tag nor a digest; this plugin has no default tag, because a workflow that "+
				"meant :latest should say so",
			truncate(raw, 96))
	}

	endpoint := registry
	if registry == dockerHubRegistry {
		endpoint = dockerHubEndpoint
	}

	return reference{
		Registry:   registry,
		Endpoint:   endpoint,
		Repository: repository,
		Tag:        tag,
		Digest:     digest,
	}, nil
}

// parsePinnedReference is [parseReference] for the tasks whose answer is about
// specific bytes rather than about a name.
func parsePinnedReference(raw, task string) (reference, error) {
	ref, err := parseReference(raw)
	if err != nil {
		return reference{}, err
	}
	if ref.Digest == "" {
		return reference{}, sdk.InvalidInput(
			"%s needs a digest-pinned reference, registry/repository@sha256:…, because its answer is about specific "+
				"bytes: a tag would answer about whatever it pointed at when this call landed. Resolve the tag first "+
				"with oci.resolve and pass its reference output",
			task)
	}
	return ref, nil
}

// looksLikeHost distinguishes a registry from the first path segment of a
// repository. The rule is the one every other implementation uses, written
// down: a host has a dot or a port, or it is localhost.
func looksLikeHost(candidate string) bool {
	return strings.ContainsAny(candidate, ".:") || candidate == "localhost"
}

// truncate bounds a value before it is interpolated into a refusal, so that a
// workflow's own oversized input cannot become this plugin's oversized error.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
