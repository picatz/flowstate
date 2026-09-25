package main

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

func main() {
	installEgressPolicy()

	sdk.Main(sdk.Plugin{
		Name:        "oci",
		Version:     "0.1.0",
		Description: "Reads an OCI registry: resolves a reference to its digest, lists what is attached to it, and fetches one verified blob.",
		Tasks: []sdk.Task{
			{
				Name:                 "resolve",
				Summary:              "Resolve a registry reference to the digest it names, optionally selecting one platform out of an index.",
				Input:                &ociv1.ResolveInputs{},
				Output:               &ociv1.ResolveOutputs{},
				SecretInputs:         []string{"password"},
				RequiredSecretInputs: []string{"password"},
				Fn:                   ociResolve,
			},
			{
				Name:                 "referrers",
				Summary:              "List the artifacts attached to a digest - signatures, SBOMs, in-toto attestations - bounded and filterable by artifact type.",
				Input:                &ociv1.ReferrersInputs{},
				Output:               &ociv1.ReferrersOutputs{},
				SecretInputs:         []string{"password"},
				RequiredSecretInputs: []string{"password"},
				Fn:                   ociReferrers,
			},
			{
				Name:                 "blob",
				Summary:              "Fetch one content-addressed blob, bounded, and refuse it unless the bytes hash to the digest that was asked for.",
				Input:                &ociv1.BlobInputs{},
				Output:               &ociv1.BlobOutputs{},
				SecretInputs:         []string{"password"},
				RequiredSecretInputs: []string{"password"},
				Fn:                   ociBlob,
			},
		},
		Health: checkHealth,
	})
}

// checkHealth reports serving. There is no registry to probe: which registries
// this plugin may reach is the deployment's egress policy and which one a call
// means is that call's own input, so there is no single endpoint whose
// reachability would say anything about the next call. Discovery and validation
// stay available without network authority; each task checks for the operator
// snapshot at its own boundary, before it decodes inputs.
func checkHealth(_ context.Context) error {
	return nil
}
