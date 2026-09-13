package main

import (
	"context"
	"encoding/json"
	"net/http"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

const (
	// defaultReferrerLimit is what a call that names no limit gets. An image
	// with more attachments than this has something worth looking at directly.
	defaultReferrerLimit = 50

	// maxReferrerLimit is the ceiling a workflow cannot raise past. Each
	// referrer becomes a map in durable history, so this bounds what one step
	// can write there as much as what it reads.
	maxReferrerLimit = 200

	// maxReferrersBytes bounds the index a registry answers with, which holds
	// one descriptor per attachment.
	maxReferrersBytes = 4 << 20

	// maxAnnotations bounds the annotations carried out of one descriptor.
	// Annotations are arbitrary key-value pairs chosen by whoever pushed the
	// artifact, which makes them exactly the kind of other-party-controlled
	// collection that needs a limit before it reaches workflow history.
	maxAnnotations = 32
)

// referrersDocument is the image index a registry answers the referrers API
// with.
type referrersDocument struct {
	Manifests []struct {
		MediaType    string            `json:"mediaType"`
		ArtifactType string            `json:"artifactType"`
		Digest       string            `json:"digest"`
		Size         int64             `json:"size"`
		Annotations  map[string]string `json:"annotations"`
	} `json:"manifests"`
}

func ociReferrers(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("oci.referrers has no usable egress policy, so no registry is authorized: %v", egressRefusal)
	}

	var in ociv1.ReferrersInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}

	ref, err := parsePinnedReference(in.GetReference(), "oci.referrers")
	if err != nil {
		return nil, err
	}
	limit, err := boundedLimit(in.GetLimit())
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

	out, err := listReferrers(ctx, client, ref, in.GetArtifactType(), limit)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// boundedLimit applies the task's ceiling, refusing rather than lowering.
//
// Silently clamping would answer a workflow that asked for three hundred
// referrers with two hundred and no way to tell that from an image that has
// two hundred.
func boundedLimit(requested int32) (int, error) {
	switch {
	case requested < 0:
		return 0, sdk.InvalidInput("limit is negative")
	case requested == 0:
		return defaultReferrerLimit, nil
	case requested > maxReferrerLimit:
		return 0, sdk.InvalidInput("limit is %d, over this task's ceiling of %d", requested, maxReferrerLimit)
	default:
		return int(requested), nil
	}
}

// listReferrers asks the registry what is attached to a digest.
func listReferrers(ctx context.Context, client *registryClient, ref reference, artifactType string, limit int) (*ociv1.ReferrersOutputs, error) {
	path := "/v2/" + ref.Repository + "/referrers/" + ref.Digest
	if artifactType != "" {
		path += "?artifactType=" + urlQueryEscape(artifactType)
	}

	response, err := client.get(ctx, ref, path, []string{"application/vnd.oci.image.index.v1+json"})
	if err != nil {
		return nil, err
	}
	defer drainAndClose(response)

	if response.StatusCode == http.StatusNotFound {
		// The specification's own answer for a registry that does not implement
		// the referrers API, and indistinguishable at this layer from a
		// repository that does not exist. Both are reported rather than turned
		// into an empty list: a workflow gating on "no attestation is attached"
		// must not read "this registry cannot tell me" as "there is none".
		return nil, sdk.NotFound(
			"%s answered 404 for the referrers of %s; either the repository does not exist or this registry does not "+
				"implement the referrers API, and neither is the same as no attachments",
			ref.Registry, truncate(ref.Digest, 96))
	}
	if response.StatusCode != http.StatusOK {
		return nil, classifyStatus(ref, response, "listing referrers")
	}

	body, readErr := readBounded(response.Body, maxReferrersBytes)
	if readErr != nil {
		return nil, sdk.Failed("reading the referrers index from %s: %v", ref.Registry, readErr)
	}

	var document referrersDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, sdk.Failed("%s returned a referrers index this plugin cannot read", ref.Registry)
	}

	// The registry is asked to filter and may decline to; it says which by
	// setting OCI-Filters-Applied. Filtering here as well makes the result mean
	// the same thing either way, which is what a workflow's condition depends
	// on.
	values := make([]*expr.Value, 0, min(len(document.Manifests), limit))
	truncated := response.Header.Get("Link") != ""
	for _, entry := range document.Manifests {
		if artifactType != "" && entry.ArtifactType != artifactType {
			continue
		}
		if len(values) == limit {
			truncated = true
			break
		}
		values = append(values, sdk.Literal(map[string]any{
			"digest":        entry.Digest,
			"media_type":    entry.MediaType,
			"artifact_type": entry.ArtifactType,
			"size":          entry.Size,
			"annotations":   boundedAnnotations(entry.Annotations),
		}))
	}

	return &ociv1.ReferrersOutputs{
		Referrers: values,
		Count:     int64(len(values)),
		Truncated: truncated,
	}, nil
}

// boundedAnnotations keeps a descriptor's annotations to a size a step's output
// can carry, sorted so that two runs of one call write the same history.
func boundedAnnotations(annotations map[string]string) map[string]any {
	if len(annotations) == 0 {
		return map[string]any{}
	}

	bounded := make(map[string]any, min(len(annotations), maxAnnotations))
	for _, key := range sortedKeys(annotations) {
		if len(bounded) == maxAnnotations {
			break
		}
		bounded[truncate(key, 256)] = truncate(annotations[key], 1024)
	}
	return bounded
}
