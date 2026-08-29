package plugin

import (
	"fmt"

	"github.com/goccy/go-yaml"
)

// PinsConfig is the file form of [Config.PinnedDigests]: what an operator
// writes and hands to `flow worker --plugin-pins`, mirroring how
// [github.com/picatz/flowstate/pkg/flowstate/v1.TaskPolicyConfig] is the file
// form of a task-shape policy — "the file is the point" (#1010): it is the
// artifact an operator diffs in review, and its values are exactly what
// `sha256sum` prints for the pinned binary, prefixed the way
// [github.com/picatz/flowstate/pkg/flowstate/v1.ContentDigest] always is.
type PinsConfig struct {
	// Pins maps a plugin name to the digest the binary answering to it must
	// have. Merged into [Config.PinnedDigests] verbatim; see that field for
	// what a pin does and does not enforce.
	Pins map[string]string `json:"pins,omitempty" yaml:"pins,omitempty"`
}

// ParsePinsConfig decodes a pins file from YAML or JSON, a subset of YAML.
// Unknown and duplicate fields are errors, so a misspelled key, or a name
// pinned twice in the same file, fails loudly at startup rather than
// silently pinning fewer plugins than the file's author wrote — the same
// rule [github.com/picatz/flowstate/pkg/flowstate/v1.ParseTaskPolicyConfig]
// and `netpolicy.ParseConfig` apply to their own files.
//
// This checks the document's shape only. Whether each entry is a spelling
// [Config] can compare against — a valid plugin name, and a digest of the
// form "sha256:" plus sixty-four lower-case hex characters — is
// [Config.validate]'s job, via [validateDigestPin], run once at host
// construction regardless of whether a pin arrived through this file, a
// --plugin-pin flag, or a [Config] a caller built directly — one check, one
// error message, for every source of a pin.
func ParsePinsConfig(data []byte) (PinsConfig, error) {
	var cfg PinsConfig

	if err := yaml.UnmarshalWithOptions(data, &cfg, yaml.Strict()); err != nil {
		return PinsConfig{}, fmt.Errorf("%w: %w", ErrDigestPin, err)
	}

	return cfg, nil
}
