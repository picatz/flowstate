package plugin

import (
	"fmt"
	"maps"
	"slices"
)

// MeasureDistributions returns the distribution digest of every plugin binary
// [Discover] finds for cfg, keyed by plugin name, without launching any of them.
//
// It hashes the same bytes through the same open-and-hash path a launch takes
// ([openExecImage]), so a digest printed here is the digest a pin must carry to
// admit that binary. Nothing is executed: an operator asking whether a binary
// has drifted must not hand the possibly-swapped binary the host's environment
// and egress policy to answer. The result is a measurement, not a vetting; see
// [PinsConfig].
//
// cfg.Only narrows the answer to the named plugins, as it narrows a launch, and
// a name with no binary is an [ErrLaunch] error as it is there.
func MeasureDistributions(cfg Config) (map[string]string, error) {
	found, err := Discover(cfg)
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(found))

	// As a launch does: a name asked for with no binary is a typo or a missing
	// install, and an empty answer for it would read as a clean measurement.
	for _, name := range cfg.Only {
		if !slices.ContainsFunc(found, func(f Found) bool { return f.Name == name }) {
			return nil, fmt.Errorf("%w: no %s%s on the search path %v", ErrLaunch, BinaryPrefix, name, cfg.SearchPath)
		}
	}

	for _, f := range found {
		if len(cfg.Only) > 0 && !slices.Contains(cfg.Only, f.Name) {
			continue
		}

		sum, err := measureOne(f, cfg)
		if err != nil {
			return nil, fmt.Errorf("measuring plugin %q at %s: %w", f.Name, f.Path, err)
		}

		out[f.Name] = sum
	}

	return out, nil
}

func measureOne(f Found, cfg Config) (string, error) {
	im, err := openExecImage(f.Path, cfg.logger())
	if err != nil {
		return "", err
	}
	defer im.close()

	return im.digest()
}

// ValidatePins applies to a set of pins, as read from a pins file, the check
// [Config.PinnedDigests] gets when a host is built: each key must be a name a
// plugin can have and each value a well-formed digest. Sorted, so a file with
// several bad entries reports the same one every time.
func ValidatePins(pins map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(pins)) {
		if err := validateDigestPin(name, pins[name]); err != nil {
			return err
		}
	}

	return nil
}
