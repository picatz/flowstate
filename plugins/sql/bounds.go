package main

import "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

// Bounds this plugin enforces on attacker-chosen input, in one file for the
// reason plugins/git/validate.go gives for doing the same: every one of
// these has to match the shape of what an attacker (a Flowfile author, or a
// value a previous step computed) actually controls, and a bound nobody can
// find is a bound nobody can review.
const (
	// maxQueryBytes and maxStatementBytes bound the SQL text itself, before
	// it ever reaches a driver. Generous relative to anything hand-written;
	// small enough that a workflow cannot make this plugin buffer megabytes
	// of text before the first real check runs.
	maxQueryBytes     = 64 << 10 // 64 KiB
	maxStatementBytes = 64 << 10 // 64 KiB

	// maxParams bounds how many parameters one statement may bind, and
	// maxParamBytes bounds any single parameter's own encoded size -
	// independently, the same split plugins/codex/bounds.go draws between
	// event count and one event's own summary size: a query can have many
	// small parameters or few large ones, and one bound cannot stand in for
	// the other.
	maxParams     = 500
	maxParamBytes = 1 << 20 // 1 MiB

	// maxStatements bounds how many statements one sql.exec call may run in
	// its single transaction.
	maxStatements = 100

	// defaultMaxRows does not exist: max_rows is required on every
	// sql.query call (see validate.go) precisely so there is no default to
	// name here - an unbounded query is a request this plugin refuses to
	// have an opinion about on a workflow author's behalf.
	//
	// maxMaxRows is the hard ceiling no call may exceed, matching
	// plugins/codex/bounds.go's "refuse over the ceiling rather than
	// silently reduce it" reasoning: a silently reduced bound looks like a
	// working request that quietly returns less than it asked for.
	maxMaxRows = 100_000

	// maxRowBytes bounds one row's own decoded size (the sum of its column
	// values' encoded lengths, plus the structural overhead below), and
	// maxResultBytes bounds the running total across every row read so far
	// - both checked while rows are still being scanned, before the result
	// is ever assembled into an output. See doc.go, "Bounded results," for
	// why this and the wire-level bound in driver.go are different bounds
	// catching different things, and for what this plugin could not close
	// for either engine.
	maxRowBytes    = 1 << 20  // 1 MiB
	maxResultBytes = 16 << 20 // 16 MiB

	// perRowOverheadBytes and perCellOverheadBytes account for what a
	// row's own map(string]any costs beyond the sum of its values' encoded
	// lengths - the gap TestSQLQueryRefusesAWideAllNullRowOnStructureAlone
	// exists to close. A column holding NULL or a tiny scalar reports ~0
	// bytes from convertColumnValue, but every column still costs a map
	// entry: a string key header, an `any` interface header (type pointer
	// + data pointer), and the runtime's own per-entry bucket bookkeeping -
	// real allocation that a byte count of *values alone* is blind to. A
	// hundred thousand rows with hundreds of NULL columns can consume
	// gigabytes of actual heap while resultBytes, without this, sits at
	// zero - the exact bypass a query returning wide, sparse rows would
	// otherwise have through the byte ceiling. These are conservative
	// estimates of Go's own runtime.hmap/runtime.string overhead on a
	// 64-bit build, not an exact accounting - the point is that the bound
	// can no longer be reached at zero cost, not that it predicts resident
	// memory to the byte.
	perRowOverheadBytes  = 64
	perCellOverheadBytes = 40

	// maxWireBytes bounds bytes read from a postgres connection's own
	// network socket, for the duration of one call - see driver.go's dialer
	// wrapper. There is no equivalent for sqlite: it is an embedded engine
	// with no wire, and the honest analogue is maxResultBytes above, applied
	// as data is decoded rather than as it arrives over a socket.
	maxWireBytes = 64 << 20 // 64 MiB

	// queryTimeout backstops a query or exec call that hangs, overriding
	// nothing a step's own `timeout:` already provides - the same
	// relationship plugins/codex's runTimeout has to a step's own deadline.
	queryTimeout = 30 // seconds; see connectTimeout below for context.Context wiring
)

// clampMaxRows validates a requested row bound, refusing anything absent,
// non-positive, or over the ceiling rather than silently reducing it.
func clampMaxRows(requested int32) (int, error) {
	if requested <= 0 {
		return 0, sdk.InvalidInput(
			"max_rows is required and must be positive; there is no default, because an "+
				"unbounded query is not one this task will shape an opinion about (got %d)", requested)
	}
	if requested > maxMaxRows {
		return 0, sdk.InvalidInput("max_rows is %d, over the %d row ceiling this task enforces", requested, maxMaxRows)
	}
	return int(requested), nil
}
