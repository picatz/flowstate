// Package fragments holds the interactive documents `flow mcp` serves as MCP
// Apps UI resources.
//
// Same reasoning as the reference package next door, and the same shape: what a
// binary serves has to come from the binary. `flow` is installed with
// `go install` and run from a home directory, a container, or a CI job with no
// checkout near it, so a handler that read a file off disk would serve the card
// on a maintainer's laptop and nothing anywhere else. The documents are compiled
// in.
//
// Unlike reference/mirror, nothing here is a copy of anything: the card is
// written here, in HTML, and this is its only home. There is no generator, no
// bundler and no toolchain step between the file in the diff and the bytes a
// host renders, which is deliberate. A fragment is a security surface that runs
// in someone else's browser, and the property worth having is that reviewing it
// means reading it.
package fragments

import (
	_ "embed"
)

// approvalCard is the card served as ui://flowstate/approval-card. See the
// document's own header comment for what it is, what it refuses to be, and the
// accessibility posture it holds itself to.
//
//go:embed approval-card.html
var approvalCard string

// ApprovalCard returns the approval card document.
//
// A string rather than a reader because every consumer wants the whole of it:
// MCP has no partial read of a resource, and the digest that versions it is
// taken over exactly these bytes.
func ApprovalCard() string { return approvalCard }
