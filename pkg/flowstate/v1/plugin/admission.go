package plugin

import (
	"fmt"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Admission is the question [Config.PinnedDigests] answers and the measurement
// in [execImage] does not: not "what ran", but "was this entitled to run".
//
// Discovery resolves a name to whatever binary sits on the search path under it,
// so a name is a mutable reference — the shape `uses: foo/bar@v4` has, and the
// one CVE-2025-30066 exploited by repointing a tag. The digests this package
// already takes describe the bytes that answered to a name; nothing until now
// let an operator declare which bytes were *allowed* to. First resolution was
// therefore trust-on-first-use, and the recorded digest was an audit trail
// rather than a control (#146).
//
// A pin turns one name into an immutable reference. It is checked here, at the
// top of [launch], before a socket directory exists, before the process is
// started, and so before any handshake: a plugin's own announcement of itself
// can play no part in the decision to admit it, which is the precedent worth
// copying from that history.
//
// # The cost this chooses
//
// Pinning is opt-in per name, exactly as [Config.Only] is. A name with no pin
// launches as it always has, which keeps trust-on-first-use the default for a
// deployment that has not decided otherwise. That is a deliberate trade: a
// zero-configuration worker keeps working and an operator who pins nothing gains
// nothing, in exchange for a pin being something a deployment adopts one plugin
// at a time rather than a flag day for every one of them. Where a pin *is*
// present it fails closed — a mismatch, an unmeasurable image, or no digest at
// all is a refusal, never an admission.
//
// # What a pin does not say
//
// This is admission of *bytes*, and it is deliberately silent about provenance:
// a digest says these exact bytes and says nothing about who built them or
// whether anyone vouches for them. Whether a handshake should instead carry a
// signature, and what a deployment would trust to verify one, is the open half
// of #146 and is not decided here.

// admit reports whether the executable behind a plugin name is the one this
// deployment declared may answer to it.
//
// The digest compared is the one [execImage.digest] already took from the open
// descriptor the process is about to be exec'd from — the same value recorded on
// [flowstatev1.ResolvedPlugin] — rather than a second hash of the path. Two
// hashes of one name are two lookups with a window between them, which is the
// hazard [execImage] exists to close; re-opening the path to check a pin would
// reintroduce it in the one place that is supposed to be certain.
func admit(cfg Config, name, path string, image *execImage) error {
	want, pinned := cfg.PinnedDigests[name]
	if !pinned {
		return nil
	}

	if image == nil {
		// A caller with no image is one with no digest to protect. That is a
		// legitimate launch path in general and never a legitimate one for a
		// pinned name: admitting it would mean admitting a binary nothing
		// measured on the strength of a pin nobody checked.
		return pluginError(name, path, fmt.Errorf(
			"%w: %q is pinned to %s, but this launch measured no image, so nothing can be compared; refusing rather than admitting an unmeasured binary",
			ErrDigestPin, name, want,
		))
	}

	if !image.pinned {
		// The digest is of the open descriptor; the exec runs image.execPath,
		// which when the image is not descriptor-pinned is the *path* — a name
		// the kernel resolves again at cmd.Start. A pin verified against the
		// descriptor's bytes therefore says nothing about the bytes that will
		// run: an atomic replace between this hash and the exec matches the pin
		// on the old inode while the new one runs. That is the launch's own
		// TOCTOU window ([execImage]'s doc comment), and admission cannot be
		// layered over a launch that reintroduces it.
		//
		// So a pinned name the platform cannot pin to a descriptor is refused
		// rather than downgraded to trust-on-first-use, because a security
		// control that silently weakens where it cannot be enforced is the
		// fail-open this package's own doctrine forbids. The cost is stated
		// plainly and is the honest one: pinning requires descriptor execution,
		// which today means Linux and a directly-executed binary — not a script
		// or other interpreter-run image (see pinToDescriptor's format gate).
		// A deployment that must pin such a plugin has to run it somewhere that
		// can, and being told so at launch beats being given a guarantee that
		// does not hold.
		return pluginError(name, path, fmt.Errorf(
			"%w: %q is pinned to %s, but this platform cannot execute the exact bytes it measured "+
				"(it runs the path, which can be replaced between the hash and the exec), so the pin cannot be honored; "+
				"digest pinning requires descriptor execution — a directly-executed binary on a platform that supports it — "+
				"rather than a script or other interpreter-run image",
			ErrDigestPin, name, want,
		))
	}

	got, err := image.digest()
	if err != nil {
		return pluginError(name, path, fmt.Errorf(
			"%w: %q is pinned to %s and its image could not be hashed: %w",
			ErrDigestPin, name, want, err,
		))
	}

	if got != want {
		// Both digests are named. A digest is not a secret — it is a hash of a
		// file an operator installed, and the whole point of printing it is that
		// the operator can compare it against what they meant to install, decide
		// which of the two is wrong, and fix it. Saying only "mismatch" would
		// leave them running sha256sum by hand to learn what this already knows.
		return pluginError(name, path, fmt.Errorf(
			"%w: %q is pinned to %s, but the binary at this path is %s; refusing to launch it. "+
				"Either install the pinned binary or, if this one is the intended upgrade, update the pin to its digest",
			ErrDigestPin, name, want, got,
		))
	}

	return nil
}

// validateDigestPin checks one entry of [Config.PinnedDigests] for a spelling
// this package can compare against.
//
// It runs when a host is constructed, so a typo is a startup failure rather than
// a refusal the first time a workflow needs that plugin — a pin that can never
// match is the same outage as a missing binary, arriving later and looking like
// a compromise.
func validateDigestPin(name, digest string) error {
	if !validPluginName(name) {
		// Keyed by the same rule [Discover] names a plugin with, because a pin
		// is admission for whatever answers to this name and a key no plugin
		// can ever be called is a pin that matches nothing — which leaves the
		// plugin an operator meant to pin running unpinned. `PinnedDigests` set
		// under "GitHub" for a binary discovered as "github" is the failing
		// case: accepted today, matched never, fails open. Refusing it here
		// turns that typo into a startup error naming the rule.
		return fmt.Errorf(
			"%w: PinnedDigests has an entry under %q, which is not a valid plugin name; "+
				"a plugin name is lower-case letters, digits and interior hyphens, at most %d characters, "+
				"so no discovered plugin could ever match this key",
			ErrDigestPin, truncate(name, MaxNameLen+16), MaxNameLen,
		)
	}

	hex, ok := strings.CutPrefix(digest, flowstatev1.ContentDigestPrefix)
	if !ok {
		return fmt.Errorf(
			"%w: the pin for %q is %q, which does not begin with %q; a pin is the algorithm, a colon, and the hash, exactly as this package prints a digest",
			ErrDigestPin, name, truncate(digest, 96), flowstatev1.ContentDigestPrefix,
		)
	}

	if len(hex) != flowstatev1.ContentDigestHexLen {
		return fmt.Errorf(
			"%w: the pin for %q carries %d hex characters, want %d",
			ErrDigestPin, name, len(hex), flowstatev1.ContentDigestHexLen,
		)
	}

	// Lower-case only, and checked rather than normalized. What a comparison
	// needs is that the pin is spelled the way [flowstatev1.ContentDigestOf]
	// spells its answer; accepting upper-case here and folding it would make
	// this package's own rendering one of two accepted spellings, and a second
	// spelling of one value is what everything else in this tree refuses.
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf(
			"%w: the pin for %q holds %q, which is not lower-case hexadecimal",
			ErrDigestPin, name, string(c),
		)
	}

	return nil
}
