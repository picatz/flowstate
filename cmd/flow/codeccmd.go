package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
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
	codecCmd.AddCommand(newCodecKeygenCommand(), newCodecStatusCommand())
	return codecCmd
}

func newCodecKeygenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Write a new 256-bit payload encryption key to a file",
		Long: "Write 32 random bytes, base64-encoded on one line, to `--out` at file mode 0600, " +
			"and print nothing of the key. Refuses to overwrite an existing file: rotating a key " +
			"is adding a new one to the keyring beside the old, which must stay while any " +
			"history sealed under it is still needed.",
		Args: cobra.NoArgs,
		Example: `# A key for the default namespace, named for when it was made:
flow codec keygen --out /etc/flowstate/payload-keys/default-2026-09.key`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out, _ := cmd.Flags().GetString("out")
			if err := writeKeyFile(out, envelope.GenerateKey()); err != nil {
				return err
			}
			fmt.Fprintf(newSurface(cmd).Err, "wrote a new payload key to %s (mode 0600); add it to the keyring under an id of its own\n", out)
			return nil
		},
	}
	cmd.Flags().String("out", "", "path to write the key to (required)")
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

	// The same resolution every command that dials Temporal performs, so a
	// status that succeeds is a configuration those commands will accept.
	if _, err := payloadCodecConfig(flags); err != nil {
		return err
	}

	status := &v1.PayloadEncryptionStatus{}
	if flags.keyring != "" {
		keyring, err := envelope.LoadFile(flags.keyring)
		if err != nil {
			return fmt.Errorf("payload keyring: %w", err)
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
	for _, ns := range status.GetNamespaces() {
		fmt.Fprintf(surface.Out, "\n")
		fmt.Fprintf(surface.Out, "namespace %s\n", ns.GetNamespace())
		if ns.GetAcceptUnencrypted() {
			fmt.Fprintf(surface.Out, "  reads unencrypted payloads written before encryption was turned on\n")
		}
		for _, key := range ns.GetKeys() {
			role := "decrypt only"
			if key.GetCurrent() {
				role = "current"
			}
			fmt.Fprintf(surface.Out, "  %-24s %s  %s\n", key.GetId(), key.GetFingerprint(), role)
		}
	}
	return nil
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
