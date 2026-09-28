package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

// newCodecCommand is `flow codec`: the operator's side of payload encryption.
// Generating a key and asking what a configuration resolves to are local
// operations on files this process can read; neither dials anything.
func newCodecCommand() *cobra.Command {
	codecCmd := &cobra.Command{
		Use:   "codec",
		Short: "Generate payload encryption keys and inspect a payload keyring",
		Long: "Payload encryption seals every payload a run writes to Temporal history under " +
			"keys this deployment holds. `flow codec keygen` writes a new key; `flow codec status` " +
			"reports what a keyring resolves to, by key id and fingerprint, never by material. " +
			"See docs/ENCRYPTION.md.",
	}
	codecCmd.AddCommand(newCodecKeygenCommand(), newCodecStatusCommand(), newCodecServeCommand())
	return codecCmd
}

func newCodecKeygenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Write a new payload wrapping key, or an HPKE escrow key pair",
		Long: "Without --hpke, write a local wrapping key: 32 random bytes, base64-encoded on one " +
			"line, to `--out` at file mode 0600, printing nothing of the key.\n\n" +
			"With --hpke, write an HPKE (RFC 9180) key pair for escrow: the private key to `--out` " +
			"at mode 0600, to be kept offline and given only to a recovery process, and the public " +
			"key beside it with a .pub suffix, which every worker's keyring names so each data key " +
			"is also wrapped to it. The default KEM is the post-quantum hybrid ML-KEM-768 + X25519.\n\n" +
			"Refuses to overwrite an existing file: rotating a key is adding a new one to the " +
			"keyring beside the old, which must stay while any history sealed under it is needed.",
		Args: cobra.NoArgs,
		Example: `# A local wrapping key for the default namespace, named for when it was made:
flow codec keygen --out /etc/flowstate/payload-keys/default-2026-09.key

# An escrow key pair; keep break-glass.key offline, distribute break-glass.key.pub:
flow codec keygen --hpke --out break-glass.key`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out, _ := cmd.Flags().GetString("out")
			surface := newSurface(cmd)
			if useHPKE, _ := cmd.Flags().GetBool("hpke"); useHPKE {
				kem, _ := cmd.Flags().GetUint16("kem")
				private, public, err := hpke.Generate(kem)
				if err != nil {
					return err
				}
				defer clear(private)
				if err := writeKeyFile(out, private); err != nil {
					return err
				}
				if err := writeFileExclusive(out+".pub", public, 0o644); err != nil {
					return err
				}
				fmt.Fprintf(surface.Err, "wrote an HPKE private key to %s (mode 0600) and its public key to %s.pub; "+
					"keep the private key offline and name the public key under escrow_keys\n", out, out)
				return nil
			}
			if err := writeKeyFile(out, local.Generate()); err != nil {
				return err
			}
			fmt.Fprintf(surface.Err, "wrote a new payload key to %s (mode 0600); add it to the keyring under an id of its own\n", out)
			return nil
		},
	}
	cmd.Flags().String("out", "", "path to write the key to (required)")
	cmd.Flags().Bool("hpke", false, "write an HPKE escrow key pair instead of a local wrapping key")
	cmd.Flags().Uint16("kem", hpke.DefaultKEM, "with --hpke, the HPKE KEM id: 0x647a (ML-KEM-768 + X25519), "+
		"0x0050 (ML-KEM-768 + P-256), 0x0020 (X25519), among others")
	_ = cmd.MarkFlagRequired("out")
	return cmd
}

func newCodecStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report what a payload keyring resolves to, without revealing any key",
		Long: "Load the keyring the way `flow server` and `flow worker` would, reading every key, " +
			"and report each namespace's current key id, every key it can decrypt with, and a " +
			"one-way fingerprint of each. Two processes whose fingerprints for one id differ hold " +
			"different keys under that id. Exits non-zero if the keyring would refuse to start.",
		Args: cobra.NoArgs,
		Example: `flow codec status --payload-keyring /etc/flowstate/payload-keyring.yaml
flow codec status -o json`,
		RunE: runCodecStatus,
	}
	addPayloadEncryptionFlags(cmd)
	addOutputFlag(cmd)
	return cmd
}

func runCodecStatus(cmd *cobra.Command, _ []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	flags := payloadEncryptionFlagsOf(cmd)

	// The same resolution and checks every command that dials Temporal
	// performs, on one opening of the keyring, so a status that succeeds is a
	// configuration those commands will accept.
	status := &v1.PayloadEncryptionStatus{}
	if flags.keyring == "" {
		if _, err := payloadCodecConfig(flags); err != nil {
			return err
		}
	} else {
		keyring, err := openPayloadKeyring(flags.keyring)
		if err != nil {
			return err
		}
		if err := keyring.PayloadCodecConfig().Validate(); err != nil {
			return err
		}
		status = keyring.Status()
	}
	status.Required = flags.required

	surface := newSurface(cmd)
	if format.Machine() {
		return writeJSON(surface, format, status)
	}

	if !status.GetEnabled() {
		fmt.Fprintf(surface.Out, "payload encryption: off (no keyring configured); payloads are written to history unencrypted\n")
		return nil
	}
	fmt.Fprintf(surface.Out, "payload encryption: on (%s)\n", map[bool]string{true: "required", false: "not required"}[status.GetRequired()])
	if status.GetFips140() {
		fmt.Fprintf(surface.Out, "FIPS 140-3 mode: only approved suites are used\n")
	}
	for _, ns := range status.GetNamespaces() {
		fmt.Fprintf(surface.Out, "\n")
		fmt.Fprintf(surface.Out, "namespace %s\n", ns.GetNamespace())
		fmt.Fprintf(surface.Out, "  seals with %s; reads %s\n", suiteName(ns.GetSuite()), suiteNames(ns.GetDecryptSuites()))
		if dk := ns.GetDataKey(); dk != nil {
			fmt.Fprintf(surface.Out, "  data keys roll over every %s, %d payloads or %d bytes", dk.GetMaxAge().AsDuration(),
				dk.GetMaxMessages(), dk.GetMaxBytes())
			if grace := dk.GetStaleGrace().AsDuration(); grace > 0 {
				fmt.Fprintf(surface.Out, ", and keep sealing up to %s longer while the key provider is unreachable", grace)
			}
			fmt.Fprintf(surface.Out, "\n")
		}
		if ns.GetCurrentKeyId() == "" {
			fmt.Fprintf(surface.Out, "  decode-only: no current key, so nothing is written\n")
		}
		if ns.GetAcceptUnencrypted() {
			fmt.Fprintf(surface.Out, "  reads unencrypted payloads written before encryption was turned on\n")
		}
		for _, key := range ns.GetKeys() {
			role := "decrypt only"
			switch {
			case key.GetCurrent():
				role = "current"
			case key.GetEscrow() && key.GetCanUnwrap():
				role = "escrow, can recover"
			case key.GetEscrow():
				role = "escrow, wrap only"
			case !key.GetCanUnwrap():
				role = "cannot unwrap"
			}
			id := key.GetFingerprint()
			if key.GetVersion() > 0 {
				id = fmt.Sprintf("v%d", key.GetVersion())
			}
			fmt.Fprintf(surface.Out, "  %-24s %-6s %-16s  %s\n", key.GetId(), key.GetKind(), id, role)
		}
	}
	return nil
}

// suiteName is a suite without its enum prefix.
func suiteName(s v1.PayloadSuite) string {
	return strings.TrimPrefix(s.String(), "PAYLOAD_SUITE_")
}

func suiteNames(ss []v1.PayloadSuite) string {
	names := make([]string, len(ss))
	for i, s := range ss {
		names[i] = suiteName(s)
	}
	return strings.Join(names, ", ")
}

// writeKeyFile creates path with mode 0600 and writes text to it, refusing to
// replace a file that exists: a key file that may still protect history is
// never overwritten.
func writeKeyFile(path string, text []byte) error {
	defer clear(text)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%s already exists; write a new key under a new file name instead of "+
				"overwriting one that may still protect history", path)
		}
		return fmt.Errorf("creating %s: %w", path, err)
	}
	if _, err := file.Write(text); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}

	// See writePrivateKeyPEM for why Windows is exempt from this check.
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("verifying permissions of %s: %w", path, err)
		}
		if info.Mode().Perm() != 0o600 {
			return fmt.Errorf("%s was created with mode %s instead of 0600; refusing to leave a key at that path",
				path, info.Mode().Perm())
		}
	}
	return nil
}

// writeFileExclusive creates path with mode and writes data to it, refusing
// to replace a file that exists. For a public key: nothing secret, but a key
// pair's halves should never silently disagree.
func writeFileExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return file.Close()
}
