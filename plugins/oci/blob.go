package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

const (
	// defaultBlobBytes is what a call that names no limit may download. An
	// in-toto attestation or a small SBOM fits; a layer does not, on purpose.
	defaultBlobBytes = 1 << 20

	// maxBlobBytes is the ceiling a workflow cannot raise past. What this task
	// reads becomes a step output, so the ceiling bounds durable history as
	// much as it bounds this process's memory.
	maxBlobBytes = 8 << 20
)

func ociBlob(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("oci.blob has no usable egress policy, so no registry is authorized: %v", egressRefusal)
	}

	var in ociv1.BlobInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}

	ref, err := parsePinnedReference(in.GetReference(), "oci.blob")
	if err != nil {
		return nil, err
	}
	limit, err := boundedBlobBytes(in.GetMaxBytes())
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

	out, err := fetchBlob(ctx, client, ref, limit, in.GetParseJson())
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// boundedBlobBytes applies the task's ceiling, refusing rather than lowering,
// so a workflow never reads a truncated document as a complete one.
func boundedBlobBytes(requested int64) (int64, error) {
	switch {
	case requested < 0:
		return 0, sdk.InvalidInput("max_bytes is negative")
	case requested == 0:
		return defaultBlobBytes, nil
	case requested > maxBlobBytes:
		return 0, sdk.InvalidInput("max_bytes is %d, over this task's ceiling of %d", requested, maxBlobBytes)
	default:
		return requested, nil
	}
}

// fetchBlob downloads one content-addressed blob and proves it is the one asked
// for.
func fetchBlob(ctx context.Context, client *registryClient, ref reference, limit int64, parseJSON bool) (*ociv1.BlobOutputs, error) {
	response, err := client.get(ctx, ref, "/v2/"+ref.Repository+"/blobs/"+ref.Digest, nil)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(response)

	if response.StatusCode != http.StatusOK {
		return nil, classifyStatus(ref, response, "reading a blob")
	}

	body, readErr := readBounded(response.Body, limit)
	if readErr != nil {
		return nil, sdk.InvalidInput(
			"the blob at %s is longer than the %d bytes this call allowed; raise max_bytes, up to this task's ceiling of %d",
			truncate(ref.Digest, 96), limit, maxBlobBytes)
	}

	// The reason this task exists rather than an http step: the caller named
	// the bytes by their hash, so bytes that hash to anything else are not a
	// smaller answer or a different version, they are the wrong bytes.
	if err := verifyDigest(ref.Digest, body); err != nil {
		return nil, err
	}

	out := &ociv1.BlobOutputs{
		Digest:    ref.Digest,
		Size:      int64(len(body)),
		Content:   boundedText(body),
		MediaType: strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]),
	}

	if parseJSON {
		var document any
		if err := json.Unmarshal(body, &document); err != nil {
			return nil, sdk.Failed(
				"parse_json was set and the blob at %s is not valid JSON; it is returned as content or not at all",
				truncate(ref.Digest, 96))
		}
		out.Json = sdk.Literal(document)
	}

	return out, nil
}
