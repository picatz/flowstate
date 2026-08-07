package flowstatev1

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// entityWorkflowIDPrefix marks a workflow id as entity-addressed rather than the
// `flowstate-workflow-<uuid>` an ordinary run gets. Read by nothing — it exists
// so an operator looking at `flow list` output, or a log line, can tell the two
// addressing schemes apart on sight.
const entityWorkflowIDPrefix = "flowstate-entity-"

// entitySeparator joins a namespace to an entity key inside a workflow id.
//
// # Why this character, and why it is unforgeable
//
// [auth.ValidateNamespace] and [ValidateEntityKey] share one grammar — lowercase
// ASCII letters, digits, and a dash that is never first — and neither permits an
// underscore. So `namespace + entitySeparator + entityKey` has exactly one
// underscore in it: the separator. A reader splitting on the first (or last, or
// only) underscore always recovers the same two halves the writer joined, because
// there is nowhere else in the string an underscore could have come from.
//
// This is the discipline `secrets.ValidateNamespace` and the `_default`/`_local`
// markers in `pkg/flowstate/v1/auth` (#248) already established, cited here rather
// than reinvented: a namespace or a component that could contain the marker byte
// makes the marker forgeable, so the fix is never a cleverer separator — it is a
// grammar where the separator's byte is illegal on both sides of it. CLAUDE.md's
// env-provider incident is the negative case this is built to refuse: `prefix +
// NAMESPACE + "_" + name` let a namespace claiming to be `team_a` and a name of
// `key` land on the identical string a namespace of `team` and a name of `a_key`
// would, because "_" was legal in both halves. Here it is legal in neither.
const entitySeparator = "_"

// MaxEntityKeyLen is the longest permitted entity key.
//
// Not [auth.MaxNamespaceLen]: an entity key names one instance of a workflow — an
// order number, a subscription id — and instance identifiers run longer than
// tenant names in practice. Still small enough that many entity-addressed
// workflow ids fit comfortably under [maxWorkflowIDBytes] together with the
// longest legal namespace; see the compile-time assertion below.
const MaxEntityKeyLen = 128

// ValidateEntityKey reports whether key is well formed enough to enter a
// workflow id.
//
// This is the Go-side twin of the `entity_key` field's protovalidate pattern on
// [RunRequest] and [SignalWithStartRequest] — both already refuse a malformed key
// at the schema layer before a handler ever sees one, so in practice this only
// ever reports success there. It exists as its own function, rather than being
// inlined into [EntityWorkflowID], because [EntityWorkflowID] is what a test
// calls directly to check the join is unambiguous, without constructing a
// [RunRequest] to get there — and because a caller embedding this package
// (invariant: `pkg/flowstate/embed`) builds requests in Go, not by parsing
// protovalidate errors, so the same rule needs a Go-shaped door too.
func ValidateEntityKey(key string) error {
	if key == "" {
		return fmt.Errorf("entity key must not be empty")
	}
	if len(key) > MaxEntityKeyLen {
		return fmt.Errorf("entity key is longer than %d characters", MaxEntityKeyLen)
	}
	for i, c := range key {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' && i > 0:
		default:
			return fmt.Errorf(
				"entity key %q may only contain lowercase letters, digits, and dashes, and may not start with a dash",
				key,
			)
		}
	}
	return nil
}

// maxWorkflowIDBytes is what Temporal's default dynamic config
// (`limit.workflowIDLength`) accepts for a workflow id.
//
// Asserted against below, the same way [maxFairnessKeyBytes] is asserted against
// [auth.MaxNamespaceLen] in `pkg/flowstate/v1/server` — a bound checked once, at
// compile time, rather than trusted to stay compatible as the two numbers evolve
// in different files owned by different concerns.
const maxWorkflowIDBytes = 1000

// A compile-time check: the longest possible entity-addressed workflow id —
// the prefix, the longest legal namespace, the separator, and the longest legal
// entity key — must fit under Temporal's own limit, or raising either grammar's
// length cap would silently start producing workflow ids Temporal refuses at
// submit. Failing the build here is cheaper than failing a caller's first Run.
var _ [maxWorkflowIDBytes - len(entityWorkflowIDPrefix) - auth.MaxNamespaceLen - len(entitySeparator) - MaxEntityKeyLen]struct{}

// EntityWorkflowID composes the workflow id for an entity-addressed run.
//
// namespace must come from the authenticated caller's identity —
// [FlowstateServer.identityFor] in `pkg/flowstate/v1/server`, never from a
// request field — the same rule [fairnessFor] already applies to scheduling
// priority. entityKey is whatever [RunRequest.entity_key] or
// [SignalWithStartRequest.entity_key] the caller supplied, already checked by
// protovalidate against the schema's own copy of [ValidateEntityKey]'s grammar,
// and checked again here so this function is safe to call directly (the embed
// path calls it without going through protovalidate at all).
//
// The empty namespace — single-tenant, or `--insecure-no-auth` — composes just
// as any other namespace does: `flowstate-entity-_<key>`, with the separator
// still present and still the only underscore in the string, so a
// single-tenant deployment's entity ids stay just as unambiguous as a
// multi-tenant one's. There is no tenant to collide with in that mode, but the
// same function producing the same shape either way means there is only one
// code path to have gotten this right.
func EntityWorkflowID(namespace, entityKey string) (string, error) {
	if err := auth.ValidateNamespace(namespace); err != nil {
		return "", fmt.Errorf("entity workflow id: %w", err)
	}
	if err := ValidateEntityKey(entityKey); err != nil {
		return "", fmt.Errorf("entity workflow id: %w", err)
	}

	id := entityWorkflowIDPrefix + namespace + entitySeparator + entityKey
	if len(id) > maxWorkflowIDBytes {
		return "", fmt.Errorf("entity workflow id: %d bytes exceeds Temporal's %d byte limit", len(id), maxWorkflowIDBytes)
	}

	return id, nil
}
