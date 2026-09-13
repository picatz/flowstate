package main

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

// indexMediaTypes are the two spellings of "this manifest is a list of
// manifests" - the OCI one and the Docker one a registry still serves for
// images pushed by older tooling.
var indexMediaTypes = map[string]bool{
	"application/vnd.oci.image.index.v1+json":                   true,
	"application/vnd.docker.distribution.manifest.list.v2+json": true,
}

// manifestDocument is the part of a manifest or index this plugin reads. Every
// other field is left alone: this task answers what a reference is, not what an
// image contains.
type manifestDocument struct {
	MediaType string `json:"mediaType"`
	Manifests []struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int64  `json:"size"`
		Platform  struct {
			OS      string `json:"os"`
			Arch    string `json:"architecture"`
			Variant string `json:"variant"`
		} `json:"platform"`
	} `json:"manifests"`
}

func ociResolve(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("oci.resolve has no usable egress policy, so no registry is authorized: %v", egressRefusal)
	}

	var in ociv1.ResolveInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}

	ref, err := parseReference(in.GetReference())
	if err != nil {
		return nil, err
	}
	wanted, err := parsePlatform(in.GetPlatform())
	if err != nil {
		return nil, err
	}
	creds, err := credentialsFrom(in.GetUsername(), in.GetPassword())
	if err != nil {
		return nil, err
	}

	client, err := newRegistryClient(creds)
	if err != nil {
		return nil, err
	}

	out, err := resolve(ctx, client, ref, wanted)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// resolve fetches the manifest a reference names and says what it is.
func resolve(ctx context.Context, client *registryClient, ref reference, wanted platform) (*ociv1.ResolveOutputs, error) {
	body, mediaType, digest, err := client.manifest(ctx, ref, ref.target())
	if err != nil {
		return nil, err
	}

	out := &ociv1.ResolveOutputs{
		Reference:  ref.Registry + "/" + ref.Repository + "@" + digest,
		Digest:     digest,
		MediaType:  mediaType,
		Size:       int64(len(body)),
		Registry:   ref.Registry,
		Repository: ref.Repository,
	}
	if !wanted.requested() {
		return out, nil
	}

	if !indexMediaTypes[mediaType] {
		return nil, sdk.InvalidInput(
			"platform %s was requested and %s resolves to a single %s manifest, which names no platform to match; "+
				"omit platform to pin this manifest as it is",
			wanted, truncate(ref.String(), 96), truncate(mediaType, 96))
	}

	var document manifestDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, sdk.Failed("%s returned an index this plugin cannot read", ref.Registry)
	}

	for _, entry := range document.Manifests {
		// Attestation and signature manifests ride in the same index with a
		// platform of unknown/unknown. Matching one would return, as "the
		// linux/amd64 image", something that is not an image at all.
		if entry.Platform.OS == "unknown" || entry.Platform.Arch == "unknown" {
			continue
		}
		if !wanted.matches(entry.Platform.OS, entry.Platform.Arch, entry.Platform.Variant) {
			continue
		}
		if err := flowstatev1.ValidateContentDigest(entry.Digest); err != nil {
			return nil, sdk.Failed("%s named a child digest this plugin cannot use: %v", ref.Registry, err)
		}

		matched := wanted.String()
		return &ociv1.ResolveOutputs{
			Reference:       ref.Registry + "/" + ref.Repository + "@" + entry.Digest,
			Digest:          entry.Digest,
			MediaType:       entry.MediaType,
			Size:            entry.Size,
			Registry:        ref.Registry,
			Repository:      ref.Repository,
			Platform:        matched,
			PlatformMatched: true,
		}, nil
	}

	// A refusal rather than an output saying false: a workflow that asked for a
	// platform and got an unmatched answer would have to check a boolean it did
	// not know to check, and would deploy the index's digest believing it had
	// the platform's.
	return nil, sdk.NotFound(
		"%s holds no %s image; the index lists %s",
		truncate(ref.String(), 96), wanted, platformsIn(document))
}

// manifest fetches one manifest or index, bounded and verified.
//
// target is the tag or digest to request, which is the reference's own for a
// resolve and a child's digest for a platform selection.
func (c *registryClient) manifest(ctx context.Context, ref reference, target string) (body []byte, mediaType, digest string, err error) {
	response, err := c.get(ctx, ref, "/v2/"+ref.Repository+"/manifests/"+target, acceptedManifestTypes)
	if err != nil {
		return nil, "", "", err
	}
	defer drainAndClose(response)

	if response.StatusCode != http.StatusOK {
		return nil, "", "", classifyStatus(ref, response, "reading a manifest")
	}

	body, readErr := readBounded(response.Body, maxManifestBytes)
	if readErr != nil {
		return nil, "", "", sdk.Failed("reading the manifest from %s: %v", ref.Registry, readErr)
	}

	digest = digestOf(body)

	// A pinned reference is a claim about bytes, and this is where the claim is
	// checked - against what was actually received, not against the
	// Docker-Content-Digest header, which is the same party's assertion as the
	// body it describes.
	if ref.Digest != "" && target == ref.Digest {
		if err := verifyDigest(ref.Digest, body); err != nil {
			return nil, "", "", err
		}
	}

	mediaType = strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	if mediaType == "" {
		// Some registries answer without one; the document names itself.
		var document manifestDocument
		if err := json.Unmarshal(body, &document); err == nil {
			mediaType = document.MediaType
		}
	}
	return body, mediaType, digest, nil
}

// platformsIn renders what an index does hold, so a refusal tells the author
// what to ask for instead of only what was missing.
func platformsIn(document manifestDocument) string {
	seen := make([]string, 0, len(document.Manifests))
	for _, entry := range document.Manifests {
		if entry.Platform.OS == "" || entry.Platform.OS == "unknown" {
			continue
		}
		rendered := entry.Platform.OS + "/" + entry.Platform.Arch
		if entry.Platform.Variant != "" {
			rendered += "/" + entry.Platform.Variant
		}
		if !slices.Contains(seen, rendered) {
			seen = append(seen, rendered)
		}
	}
	if len(seen) == 0 {
		return "no platforms"
	}
	return truncate(strings.Join(seen, ", "), 256)
}
