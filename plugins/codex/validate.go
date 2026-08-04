package main

import (
	"unicode"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// validatePrompt bounds and lightly sanity-checks codex.exec's required
// prompt input before it is written to the subprocess's stdin.
//
// This is not a content filter - the prompt is exactly what the codex CLI
// receives, and this task has no opinion about what an agent should or
// should not be asked to do. What it bounds is size (maxPromptBytes, so a
// workflow cannot make this task hold an arbitrarily large string before
// the subprocess is even started) and the presence of a NUL byte, which
// os/exec's argument and stdin handling cannot represent in a C string and
// which no legitimate prompt contains.
func validatePrompt(raw string) (string, error) {
	if raw == "" {
		return "", sdk.InvalidInput("prompt is required")
	}
	if len(raw) > maxPromptBytes {
		return "", sdk.InvalidInput("prompt is %d bytes, over the %d byte limit", len(raw), maxPromptBytes)
	}
	for _, r := range raw {
		if r == 0 {
			return "", sdk.InvalidInput("prompt contains a NUL byte")
		}
	}
	return raw, nil
}

// validateModel bounds the optional model input, which becomes a --model
// flag value on the subprocess's argv - never shell text, since
// exec.CommandContext (inside the codex library) never invokes a shell to
// interpret it, but still bounded and checked for control characters no
// legitimate model identifier contains.
func validateModel(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if len(raw) > maxModelBytes {
		return "", sdk.InvalidInput("model is %d bytes, over the %d byte limit", len(raw), maxModelBytes)
	}
	for _, r := range raw {
		if r == 0 || unicode.IsControl(r) {
			return "", sdk.InvalidInput("model contains a control character")
		}
	}
	return raw, nil
}
