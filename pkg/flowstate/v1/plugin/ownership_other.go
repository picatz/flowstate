//go:build !unix

package plugin

import "io/fs"

// ownedByTrustedUser cannot decide here, and says so rather than answering.
//
// File ownership is not represented portably, so there is nothing to compare a
// uid against. Reporting "trusted" would be the failure cmd/flow's
// acme_cache_other.go spends six lines refusing: the one thing worse than a
// check that cannot run is a check that cannot run and looks like it passed.
// Reporting "not trusted" would refuse every plugin on a supported platform.
//
// So it reports neither, and [Discover] logs once that ownership went
// unchecked — the permission-bit checks still apply, and an operator is told
// which half of the guarantee they hold themselves.
func ownedByTrustedUser(fs.FileInfo) (trusted, decidable bool) { return false, false }
