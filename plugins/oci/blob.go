package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

const (
	// defaultBlobBytes is what a call that names no limit may download. An
	// in-toto attestation or a small SBOM fits; a layer does not, on purpose.
	defaultBlobBytes = 1 << 20

	// blobEnvelopeReserve is what a result costs beside the blob's own bytes:
	// the digest, the media type, the size, the parsed form when one is asked
	// for, and the framing around them. Sized for parse_json, which is the
	// output that can hold the payload a second time.
	blobEnvelopeReserve = 128 << 10

	// maxBlobBytes is the ceiling a workflow cannot raise past, derived from
	// the host's own rather than chosen. What this task reads becomes a step
	// output, and an output over flowstatev1.MaxTaskOutputBytes is refused by
	// the engine - so a higher ceiling here would let a registry read succeed
	// and its answer be thrown away, which spends the bytes and returns
	// nothing.
	maxBlobBytes = (flowstatev1.MaxTaskOutputBytes - blobEnvelopeReserve) / 2
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
		document, err := decodeJSON(body)
		if err != nil {
			return nil, sdk.Failed(
				"parse_json was set and the blob at %s could not be decoded (%v); it is returned as content or not at all",
				truncate(ref.Digest, 96), err)
		}
		out.Json = sdk.Literal(document)
	}

	return out, nil
}

// decodeJSON decodes a blob into the shapes a CEL value carries, without
// rounding an integer on the way.
//
// encoding/json decodes every number into a float64 when the destination is
// `any`, and a float64 holds integers exactly only up to 2^53. An attestation
// or SBOM carrying a larger one - a build number, an epoch in nanoseconds, an
// identifier - would reach a workflow as a different value from the one in the
// bytes this task just verified against their digest, and a policy decision
// made on it would be made on evidence the registry did not serve. Numbers are
// decoded as text and converted, so an integer stays an integer and anything
// that is not representable is refused rather than rounded.
func decodeJSON(body []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, errors.New("the blob is not valid JSON")
	}
	if decoder.More() {
		return nil, errors.New("the blob carries more than one JSON document")
	}
	return exactNumbers(document)
}

// exactNumbers replaces every json.Number with the Go value a CEL literal can
// carry without loss.
func exactNumbers(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer, nil
		}
		asFloat, err := typed.Float64()
		if err != nil {
			return nil, fmt.Errorf("the blob carries the number %s, which is neither an integer nor a float this plugin can represent",
				truncate(typed.String(), 64))
		}
		// A non-integer that survives the round trip is exact as a float; one
		// that does not is a value this task would be changing.
		if strconv.FormatFloat(asFloat, 'g', -1, 64) != typed.String() && big.NewFloat(asFloat).Text('g', -1) != typed.String() {
			if _, ok := new(big.Float).SetString(typed.String()); ok {
				return nil, fmt.Errorf("the blob carries the number %s, which cannot be represented exactly; a policy decision on a rounded value is a decision on evidence this registry did not serve",
					truncate(typed.String(), 64))
			}
		}
		return asFloat, nil
	case map[string]any:
		for key, entry := range typed {
			converted, err := exactNumbers(entry)
			if err != nil {
				return nil, err
			}
			typed[key] = converted
		}
		return typed, nil
	case []any:
		for i, entry := range typed {
			converted, err := exactNumbers(entry)
			if err != nil {
				return nil, err
			}
			typed[i] = converted
		}
		return typed, nil
	default:
		return value, nil
	}
}
