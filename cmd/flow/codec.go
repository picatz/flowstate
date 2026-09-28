package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// Where the payload codec is resolved, once, for every command in this binary.
//
// # One resolution point, both drivers
//
// `flow server` and `flow worker` reach this through [temporalConfig], which is
// the single place a Temporal client's options are built, including the pool
// that dials one client per mapped namespace, so a codec cannot end up covering
// some tenants and not others. `flow run local` reaches it through
// [localPayloadCodec] below. Both call this, so a deployment cannot rehearse
// under a different codec configuration than it runs under.
//
// # What it resolves
//
// A payload keyring ([v1.PayloadKeyring], `--payload-keyring` or
// FLOWSTATE_PAYLOAD_KEYRING) becomes the envelope codec: every payload a client
// writes is sealed under its namespace's current key, and every namespace a
// client is dialed for must be in the keyring. Without one, payloads are
// written unencrypted, which is the development default and is said at
// startup; `--require-payload-encryption` (FLOWSTATE_REQUIRE_PAYLOAD_ENCRYPTION)
// turns that default into a refusal, so a production deployment cannot come up
// in plaintext because a variable went missing. See docs/ENCRYPTION.md.
func payloadCodecConfig(ctx context.Context, flags payloadEncryptionFlags) (payloadcodec.Config, error) {
	cfg, err := resolvePayloadCodec(ctx, flags)
	if err != nil {
		return payloadcodec.Config{}, err
	}

	if flags.required && !cfg.Enabled() {
		return payloadcodec.Config{}, errors.New("payload encryption is required (--require-payload-encryption or " +
			requirePayloadEncryptionEnv + ") and no payload keyring is configured: set --payload-keyring or " +
			payloadKeyringEnv + " to a keyring file. Generate a key with `flow codec keygen`; see docs/ENCRYPTION.md")
	}

	// Validated at resolution rather than at the first payload: a codec that
	// cannot come up must stop the command, not fail the first run that reaches
	// it. This is also where a codec whose ciphertext would not fit inside
	// Temporal's blob limit is refused, before a run can wedge on it. See
	// `payloadcodec.Config.Validate`.
	if err := cfg.Validate(); err != nil {
		return payloadcodec.Config{}, err
	}

	return cfg, nil
}

// resolvePayloadCodec is the lookup itself, held in a variable so that the
// checking above it can be tested against a codec that fails it.
var resolvePayloadCodec = func(ctx context.Context, flags payloadEncryptionFlags) (payloadcodec.Config, error) {
	if flags.keyring == "" {
		return payloadcodec.Config{}, nil
	}
	keyring, err := openPayloadKeyring(ctx, flags.keyring)
	if err != nil {
		return payloadcodec.Config{}, err
	}
	return keyring.PayloadCodecConfig(), nil
}

// openPayloadKeyring opens the keyring file at path. Opening reads its keys,
// asks every key provider to describe its keys, and wraps each namespace's
// first data key, bounded by [envelope.StartupBudget] and cancelled with ctx,
// the command's own: a provider that has not answered by then is one this
// process cannot start against, and an interrupted command stops waiting.
func openPayloadKeyring(ctx context.Context, path string) (*envelope.Keyring, error) {
	keyring, err := envelope.LoadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("payload keyring: %w", err)
	}
	return keyring, nil
}

// localPayloadCodec resolves the codec for a local run, and applies it nowhere.
//
// # Why a local run does not encrypt anything, on purpose
//
// A codec is a boundary transform: it runs where a payload stops being a Go
// value in this process and becomes bytes somebody else stores. The local driver
// has no such boundary. `flow run local` calls [v1.RunWithInputs] in process;
// step outputs, `vars:`, and a signal's payload are live protobuf messages
// passed between function calls, never serialized, never persisted, and gone
// when the process exits. There is nothing for a codec to encrypt, and a codec
// invoked on a value that is about to be handed to the next function call would
// be encrypting and immediately decrypting for no reader.
//
// So the parity claim is not "the local driver encrypts too". It is the one both
// drivers can actually keep: *the same configuration is resolved, validated, and
// refused in the same way*, and a keyring that cannot come up fails
// `flow run local` exactly as it fails `flow worker`. A local run reads the same
// environment variables the worker's flags default to.
//
// This is a no-op by design and by argument, not by omission. If the local
// driver ever grows durable state, a local history, a resumable run, a
// `flow test` fixture written to disk, that state is a boundary, and the codec
// belongs on it. That is the moment to change this function, and the reason to
// come looking for it.
//
// A local run names no Temporal namespace, so it cannot ask the question
// `flow worker` asks of its own ([payloadcodec.Config.ForWriting]); it asks
// the one that can be answered without one, and refuses a keyring no
// namespace of which can write, which no worker could start with.
func localPayloadCodec(ctx context.Context) (payloadcodec.Config, error) {
	cfg, err := payloadCodecConfig(ctx, payloadEncryptionFromEnv())
	if err != nil {
		return payloadcodec.Config{}, err
	}
	if !cfg.CanWrite() {
		return payloadcodec.Config{}, errors.New("payload keyring: every namespace is decode-only (none names a " +
			"current key), so no worker could start with it: this is a recovery keyring, for `flow codec serve` " +
			"and `flow codec status` only")
	}
	return cfg, nil
}

const (
	payloadKeyringFlag           = "payload-keyring"
	payloadKeyringEnv            = "FLOWSTATE_PAYLOAD_KEYRING"
	requirePayloadEncryptionFlag = "require-payload-encryption"
	requirePayloadEncryptionEnv  = "FLOWSTATE_REQUIRE_PAYLOAD_ENCRYPTION"
)

// payloadEncryptionFlags is the payload encryption a command was asked for.
type payloadEncryptionFlags struct {
	keyring  string
	required bool
}

// addPayloadEncryptionFlags registers the two flags on a command that dials
// Temporal, defaulted from the environment.
func addPayloadEncryptionFlags(cmd *cobra.Command) {
	env := payloadEncryptionFromEnv()
	cmd.Flags().String(payloadKeyringFlag, env.keyring,
		"payload keyring file: encrypt every payload written to Temporal history under the keys it names "+
			"(default $"+payloadKeyringEnv+"; unset writes payloads unencrypted)")
	cmd.Flags().Bool(requirePayloadEncryptionFlag, env.required,
		"refuse to start without a payload keyring, so history is never written unencrypted "+
			"(default $"+requirePayloadEncryptionEnv+")")
}

// payloadEncryptionFlagsOf reads the flags off cmd, or the environment for a
// command that does not declare them.
func payloadEncryptionFlagsOf(cmd *cobra.Command) payloadEncryptionFlags {
	out := payloadEncryptionFromEnv()
	if f := cmd.Flags().Lookup(payloadKeyringFlag); f != nil {
		out.keyring = f.Value.String()
	}
	if f := cmd.Flags().Lookup(requirePayloadEncryptionFlag); f != nil {
		out.required, _ = strconv.ParseBool(f.Value.String())
	}
	return out
}

func payloadEncryptionFromEnv() payloadEncryptionFlags {
	// An unparseable value requires encryption rather than silently not: the
	// operator who set the variable meant something by it.
	raw := strings.TrimSpace(os.Getenv(requirePayloadEncryptionEnv))
	required, err := strconv.ParseBool(raw)
	if raw != "" && err != nil {
		required = true
	}
	return payloadEncryptionFlags{keyring: os.Getenv(payloadKeyringEnv), required: required}
}

// announcePayloadEncryption says once, at startup, what protection this
// process writes history under, without naming any key material: each
// namespace's current key id when a keyring is configured, and plainly that
// payloads are unencrypted when it is not. Once, because `flow server` resolves
// its Temporal configuration twice when it builds a tenant pool.
func announcePayloadEncryption(cfg payloadcodec.Config) {
	announcePayloadEncryptionOnce.Do(func() {
		if !cfg.Enabled() {
			infraLogger().Warn("payload encryption is off: step outputs, signals and failures are written to " +
				"Temporal history unencrypted; configure --" + payloadKeyringFlag + " and --" +
				requirePayloadEncryptionFlag + " for production (see docs/ENCRYPTION.md)")
			return
		}
		for _, ns := range slices.Sorted(maps.Keys(cfg.Namespaces)) {
			codec := cfg.Namespaces[ns]
			infraLogger().Info("payload encryption is on", "codec", codec.Name(),
				"temporal_namespace", ns, "current_key", codec.CurrentKeyID())
		}
	})
}

var announcePayloadEncryptionOnce sync.Once
