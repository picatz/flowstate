package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/google/uuid"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
	"github.com/spf13/cobra"
)

const (
	secretEnvAllowEnv = "FLOWSTATE_SECRET_ENV_ALLOW"
	secretDirEnv      = "FLOWSTATE_SECRET_DIR"

	secretKeychainEnv         = "FLOWSTATE_SECRET_KEYCHAIN"
	secretKeychainServiceEnv  = "FLOWSTATE_SECRET_KEYCHAIN_SERVICE"
	secretOnePasswordEnv      = "FLOWSTATE_SECRET_OP"
	secretOnePasswordVaultEnv = "FLOWSTATE_SECRET_OP_VAULT"
	secretCommandEnv          = "FLOWSTATE_SECRET_COMMAND"
	secretVaultAddrEnv        = "FLOWSTATE_SECRET_VAULT_ADDR"
	secretVaultTokenFileEnv   = "FLOWSTATE_SECRET_VAULT_TOKEN_FILE"
	secretVaultTokenEnv       = "FLOWSTATE_SECRET_VAULT_TOKEN"
	secretVaultK8sRoleEnv     = "FLOWSTATE_SECRET_VAULT_KUBERNETES_ROLE"
	secretVaultK8sMountEnv    = "FLOWSTATE_SECRET_VAULT_KUBERNETES_MOUNT"
	secretVaultMountEnv       = "FLOWSTATE_SECRET_VAULT_MOUNT"
	secretVaultPathPrefixEnv  = "FLOWSTATE_SECRET_VAULT_PATH_PREFIX"
	secretVaultNamespaceEnv   = "FLOWSTATE_SECRET_VAULT_NAMESPACE"
	secretVaultCAFileEnv      = "FLOWSTATE_SECRET_VAULT_CA_FILE"
)

func addSecretFlags(cmd *cobra.Command) {
	cmd.Flags().StringSlice("secret-env", splitComma(os.Getenv(secretEnvAllowEnv)),
		"environment secret names this process may resolve (comma-separated or repeatable; values come from FLOWSTATE_SECRET_<NAME>)")
	cmd.Flags().String("secret-dir", os.Getenv(secretDirEnv),
		"directory containing file: secrets (default $"+secretDirEnv+")")
	cmd.Flags().StringSlice("secret-env-namespace", nil,
		"tenant-to-prefix mapping NAMESPACE=PREFIX for env: secrets (repeatable)")
	cmd.Flags().Bool("secret-dir-namespaced", false,
		"resolve file: secrets below a separate <secret-dir>/<namespace>/ directory")
	cmd.Flags().Bool("secret-require-namespace", false,
		"refuse every secret read whose authenticated identity has no tenant namespace")

	// keychain: — local development on macOS. The tool itself gates the platform;
	// see checkKeychainPlatform for why the CLI checks first anyway.
	cmd.Flags().Bool("secret-keychain", os.Getenv(secretKeychainEnv) != "",
		"resolve keychain: secrets from the macOS keychain (default $"+secretKeychainEnv+", macOS only)")
	cmd.Flags().String("secret-keychain-service", os.Getenv(secretKeychainServiceEnv),
		"keychain service name entries are stored under (default $"+secretKeychainServiceEnv+", then \""+
			secrets.DefaultKeychainService+"\")")
	cmd.Flags().Bool("secret-keychain-namespaced", false,
		"give each tenant its own keychain service, <service>/<namespace>")

	// op: — 1Password, for local development shared across a team.
	cmd.Flags().Bool("secret-op", os.Getenv(secretOnePasswordEnv) != "",
		"resolve op: secrets through the 1Password CLI (default $"+secretOnePasswordEnv+")")
	cmd.Flags().String("secret-op-vault", os.Getenv(secretOnePasswordVaultEnv),
		"1Password vault read when a run has no namespace (default $"+secretOnePasswordVaultEnv+", then \""+
			secrets.DefaultOnePasswordVault+"\")")
	cmd.Flags().Bool("secret-op-namespaced", false,
		"give each tenant its own 1Password vault, named after the namespace")

	// command: — the escape hatch: sops, age, aws kms, aws secretsmanager, doppler,
	// anything reachable as one external command.
	cmd.Flags().StringArray("secret-command", splitSearchPath(os.Getenv(secretCommandEnv)),
		"argv of the command that resolves command: secrets, repeatable in order (executable first);"+
			"\"{{name}}\" and, with --secret-command-namespaced, \"{{namespace}}\" are substituted "+
			"literally into one argument, never through a shell (default $"+secretCommandEnv+
			", "+string(os.PathListSeparator)+"-separated)")
	cmd.Flags().Bool("secret-command-namespaced", false,
		"substitute \"{{namespace}}\" in --secret-command with the tenant's namespace")

	// vault: — HashiCorp Vault or OpenBao, the regulated-deployment backend.
	cmd.Flags().String("secret-vault-addr", os.Getenv(secretVaultAddrEnv),
		"address of the Vault or OpenBao instance vault: secrets are read from, such as "+
			"https://vault.example.com:8200 (default $"+secretVaultAddrEnv+")")
	cmd.Flags().String("secret-vault-token-file", os.Getenv(secretVaultTokenFileEnv),
		"file holding a static Vault client token, re-read per login (default $"+secretVaultTokenFileEnv+
			"; falls back to $"+secretVaultTokenEnv+" directly, for a development vault or a test)")
	cmd.Flags().String("secret-vault-kubernetes-role", os.Getenv(secretVaultK8sRoleEnv),
		"Vault role to authenticate as via the Kubernetes auth method, using this pod's "+
			"projected service account token (default $"+secretVaultK8sRoleEnv+
			"; exactly one of this or a token must be configured)")
	cmd.Flags().String("secret-vault-kubernetes-mount", os.Getenv(secretVaultK8sMountEnv),
		"where the Kubernetes auth method is mounted (default $"+secretVaultK8sMountEnv+", then \""+
			vault.DefaultKubernetesAuthMount+"\")")
	cmd.Flags().String("secret-vault-mount", os.Getenv(secretVaultMountEnv),
		"where the KV v2 engine is mounted (default $"+secretVaultMountEnv+", then \""+vault.DefaultMount+"\")")
	cmd.Flags().String("secret-vault-path-prefix", os.Getenv(secretVaultPathPrefixEnv),
		"path prefix inside the mount, above the namespace segment (default $"+secretVaultPathPrefixEnv+")")
	cmd.Flags().String("secret-vault-namespace", os.Getenv(secretVaultNamespaceEnv),
		"Vault Enterprise or OpenBao namespace header (default $"+secretVaultNamespaceEnv+
			"; this is the vault's own namespace, not the tenant namespace a run authenticates with)")
	cmd.Flags().String("secret-vault-ca-file", os.Getenv(secretVaultCAFileEnv),
		"PEM CA bundle to verify the vault's certificate against, instead of the system roots "+
			"(default $"+secretVaultCAFileEnv+")")
}

func splitComma(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// secretRegistry constructs the built-in providers before plugins register
// theirs. The store is deliberately built only after plugin startup, because it
// snapshots this registry.
func secretRegistry(cmd *cobra.Command) (*secrets.Registry, bool, func(), error) {
	registry := secrets.NewRegistry()
	var fileProvider *secrets.FileProvider
	closeProviders := func() {
		if fileProvider != nil {
			_ = fileProvider.Close()
		}
	}
	names, _ := cmd.Flags().GetStringSlice("secret-env")
	dir, _ := cmd.Flags().GetString("secret-dir")
	namespaceEntries, _ := cmd.Flags().GetStringSlice("secret-env-namespace")
	namespacePrefixes, err := parseNamespacePrefixes(namespaceEntries)
	if err != nil {
		return nil, false, closeProviders, err
	}

	if len(names) > 0 || len(namespacePrefixes) > 0 {
		opts := []secrets.EnvOption{secrets.WithEnvAllow(names...)}
		if len(namespacePrefixes) > 0 {
			opts = append(opts, secrets.WithEnvNamespaces(namespacePrefixes))
		}
		provider, err := secrets.NewEnvProvider(opts...)
		if err != nil {
			return nil, false, closeProviders, fmt.Errorf("configuring environment secrets: %w", err)
		}
		if err := registry.Register(provider); err != nil {
			return nil, false, closeProviders, err
		}
	}
	if dir != "" {
		var opts []secrets.FileOption
		namespaced, _ := cmd.Flags().GetBool("secret-dir-namespaced")
		if namespaced {
			opts = append(opts, secrets.WithFileNamespaced())
		}
		provider, err := secrets.NewFileProvider(dir, opts...)
		if err != nil {
			return nil, false, closeProviders, err
		}
		fileProvider = provider
		if err := registry.Register(provider); err != nil {
			closeProviders()
			return nil, false, func() {}, err
		}
	}

	configured := len(names) > 0 || len(namespacePrefixes) > 0 || dir != ""

	keychainConfigured, err := registerKeychainProvider(cmd, registry)
	if err != nil {
		closeProviders()
		return nil, false, func() {}, err
	}
	configured = configured || keychainConfigured

	opConfigured, err := registerOnePasswordProvider(cmd, registry)
	if err != nil {
		closeProviders()
		return nil, false, func() {}, err
	}
	configured = configured || opConfigured

	commandConfigured, err := registerCommandProvider(cmd, registry)
	if err != nil {
		closeProviders()
		return nil, false, func() {}, err
	}
	configured = configured || commandConfigured

	vaultConfigured, err := registerVaultProvider(cmd, registry)
	if err != nil {
		closeProviders()
		return nil, false, func() {}, err
	}
	configured = configured || vaultConfigured

	return registry, configured, closeProviders, nil
}

// checkKeychainPlatform reports a clear, specific error when --secret-keychain is
// set on a platform the keychain provider cannot serve.
//
// [secrets.NewKeychainProvider] already refuses to construct when the "security"
// tool is missing, which is every non-macOS machine, so this check is not what
// makes the worker fail closed — it is what makes the failure legible. Without it,
// an operator who baked --secret-keychain into a Linux image sees `"security" is
// not installed or not on PATH`, which reads like a broken image rather than a
// platform mismatch; with it, the message names the platform and the flag.
func checkKeychainPlatform() error {
	if runtime.GOOS == "darwin" {
		return nil
	}
	return fmt.Errorf(
		"--secret-keychain only works on macOS (this worker is running on %s); "+
			"the security tool the keychain provider shells out to does not exist here",
		runtime.GOOS,
	)
}

// registerKeychainProvider wires the macOS keychain provider when --secret-keychain
// (or $FLOWSTATE_SECRET_KEYCHAIN) asks for it.
func registerKeychainProvider(cmd *cobra.Command, registry *secrets.Registry) (bool, error) {
	enabled, _ := cmd.Flags().GetBool("secret-keychain")
	if !enabled {
		return false, nil
	}

	if err := checkKeychainPlatform(); err != nil {
		return false, err
	}

	var opts []secrets.KeychainOption
	if service, _ := cmd.Flags().GetString("secret-keychain-service"); service != "" {
		opts = append(opts, secrets.WithKeychainService(service))
	}
	if namespaced, _ := cmd.Flags().GetBool("secret-keychain-namespaced"); namespaced {
		opts = append(opts, secrets.WithKeychainNamespaced())
	}

	provider, err := secrets.NewKeychainProvider(opts...)
	if err != nil {
		return false, fmt.Errorf("configuring keychain secrets: %w", err)
	}
	if err := registry.Register(provider); err != nil {
		return false, err
	}

	return true, nil
}

// registerOnePasswordProvider wires the 1Password provider when --secret-op (or
// $FLOWSTATE_SECRET_OP) asks for it.
func registerOnePasswordProvider(cmd *cobra.Command, registry *secrets.Registry) (bool, error) {
	enabled, _ := cmd.Flags().GetBool("secret-op")
	if !enabled {
		return false, nil
	}

	var opts []secrets.OnePasswordOption
	if v, _ := cmd.Flags().GetString("secret-op-vault"); v != "" {
		opts = append(opts, secrets.WithOnePasswordVault(v))
	}
	if namespaced, _ := cmd.Flags().GetBool("secret-op-namespaced"); namespaced {
		opts = append(opts, secrets.WithOnePasswordNamespaced())
	}

	provider, err := secrets.NewOnePasswordProvider(opts...)
	if err != nil {
		return false, fmt.Errorf("configuring 1Password secrets: %w", err)
	}
	if err := registry.Register(provider); err != nil {
		return false, err
	}

	return true, nil
}

// registerCommandProvider wires the command: escape hatch when --secret-command (or
// $FLOWSTATE_SECRET_COMMAND) names a command to run.
func registerCommandProvider(cmd *cobra.Command, registry *secrets.Registry) (bool, error) {
	args, _ := cmd.Flags().GetStringArray("secret-command")
	if len(args) == 0 {
		return false, nil
	}

	var opts []secrets.CommandOption
	if namespaced, _ := cmd.Flags().GetBool("secret-command-namespaced"); namespaced {
		opts = append(opts, secrets.WithCommandNamespaced())
	}

	provider, err := secrets.NewCommandProvider(args, opts...)
	if err != nil {
		return false, fmt.Errorf("configuring command secrets: %w", err)
	}
	if err := registry.Register(provider); err != nil {
		return false, err
	}

	return true, nil
}

// registerVaultProvider wires the Vault/OpenBao provider when --secret-vault-addr
// (or $FLOWSTATE_SECRET_VAULT_ADDR) names an instance to read from.
func registerVaultProvider(cmd *cobra.Command, registry *secrets.Registry) (bool, error) {
	addr, _ := cmd.Flags().GetString("secret-vault-addr")
	if addr == "" {
		return false, nil
	}

	var opts []vault.Option

	tokenFile, _ := cmd.Flags().GetString("secret-vault-token-file")
	role, _ := cmd.Flags().GetString("secret-vault-kubernetes-role")

	switch {
	case tokenFile != "" && role != "":
		return false, fmt.Errorf(
			"configure one Vault authentication method, not both --secret-vault-token-file and " +
				"--secret-vault-kubernetes-role")
	case tokenFile != "":
		token, err := readToken(tokenFile)
		if err != nil {
			return false, fmt.Errorf("reading %s: %w", secretVaultTokenFileEnv, err)
		}
		opts = append(opts, vault.WithToken(token))
	case role != "":
		opts = append(opts, vault.WithKubernetesAuth(role))
		if mount, _ := cmd.Flags().GetString("secret-vault-kubernetes-mount"); mount != "" {
			opts = append(opts, vault.WithKubernetesAuthMount(mount))
		}
	case os.Getenv(secretVaultTokenEnv) != "":
		opts = append(opts, vault.WithToken(os.Getenv(secretVaultTokenEnv)))
	default:
		return false, fmt.Errorf(
			"--secret-vault-addr is set but no authentication is configured: pass "+
				"--secret-vault-token-file, $%s, or --secret-vault-kubernetes-role for a worker in a cluster",
			secretVaultTokenEnv)
	}

	if mount, _ := cmd.Flags().GetString("secret-vault-mount"); mount != "" {
		opts = append(opts, vault.WithMount(mount))
	}
	if prefix, _ := cmd.Flags().GetString("secret-vault-path-prefix"); prefix != "" {
		opts = append(opts, vault.WithPathPrefix(prefix))
	}
	if ns, _ := cmd.Flags().GetString("secret-vault-namespace"); ns != "" {
		opts = append(opts, vault.WithVaultNamespace(ns))
	}
	if caFile, _ := cmd.Flags().GetString("secret-vault-ca-file"); caFile != "" {
		opts = append(opts, vault.WithRootCAsFile(caFile))
	}

	provider, err := vault.NewProvider(addr, opts...)
	if err != nil {
		return false, fmt.Errorf("configuring vault secrets: %w", err)
	}
	if err := registry.Register(provider); err != nil {
		return false, err
	}

	return true, nil
}

func parseNamespacePrefixes(entries []string) (map[string]string, error) {
	prefixes := make(map[string]string, len(entries))
	for _, entry := range entries {
		namespace, prefix, found := strings.Cut(entry, "=")
		if !found || namespace == "" || prefix == "" {
			return nil, fmt.Errorf("invalid --secret-env-namespace %q: want NAMESPACE=PREFIX", entry)
		}
		if _, duplicate := prefixes[namespace]; duplicate {
			return nil, fmt.Errorf("duplicate --secret-env-namespace for %q", namespace)
		}
		prefixes[namespace] = prefix
	}
	return prefixes, nil
}

func localWorkloadIdentity(cmd *cobra.Command) (auth.WorkloadIdentity, error) {
	subject, _ := cmd.Flags().GetString("as-subject")
	issuer, _ := cmd.Flags().GetString("as-issuer")
	namespace, _ := cmd.Flags().GetString("as-namespace")
	deployment, _ := cmd.Flags().GetString("as-deployment")
	entries, _ := cmd.Flags().GetStringArray("as-claim")
	claims := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" || value == "" {
			return auth.WorkloadIdentity{}, fmt.Errorf("invalid --as-claim %q: want NAME=VALUE", entry)
		}
		if _, duplicate := claims[name]; duplicate {
			return auth.WorkloadIdentity{}, fmt.Errorf("duplicate --as-claim %q", name)
		}
		claims[name] = value
	}
	// NewLocalWorkloadIdentity, not a struct literal, is what marks this
	// identity as a local rehearsal rather than a server-attested run: the
	// distinction lives in an unexported field only this constructor can set,
	// so nothing --as-subject, --as-namespace, --as-deployment, or --as-claim
	// supplies can turn it off. See [auth.WorkloadIdentity] and
	// [auth.WorkloadIdentity.SubjectFor].
	identity := auth.NewLocalWorkloadIdentity(subject, issuer, namespace, deployment, claims)
	if err := identity.Validate(); err != nil {
		return auth.WorkloadIdentity{}, fmt.Errorf("local rehearsal identity: %w", err)
	}
	return identity, nil
}

func newSecretStore(cmd *cobra.Command, registry *secrets.Registry) (*secrets.Store, error) {
	var opts []secrets.StoreOption
	required, _ := cmd.Flags().GetBool("secret-require-namespace")
	if required {
		opts = append(opts, secrets.WithRequiredNamespace())
	}
	return secrets.NewStoreFromRegistry(registry, opts...)
}

func runtimePolicy(cmd *cobra.Command, secretsConfigured bool) (*auth.Policy, *auth.SecretPolicy, error) {
	path, _ := cmd.Flags().GetString("auth-policy")
	if path == "" {
		if secretsConfigured {
			return nil, nil, fmt.Errorf("secret providers are configured but no access policy is: pass --auth-policy with a secrets section")
		}
		return nil, nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading auth policy: %w", err)
	}
	policy, err := auth.ParsePolicy(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing auth policy %s: %w", path, err)
	}
	if policy.Secrets == nil {
		if secretsConfigured {
			return nil, nil, fmt.Errorf("auth policy %s has no secrets section, so it permits no provider configured on this worker", path)
		}
		return &policy, nil, nil
	}
	compiled, err := policy.Secrets.Compile()
	if err != nil {
		return nil, nil, fmt.Errorf("compiling secret access policy: %w", err)
	}
	return &policy, compiled, nil
}

func workerRuntime(cmd *cobra.Command, registry *secrets.Registry, configured bool) (engine.TaskRuntimeConfig, error) {
	policy, secretAccess, err := runtimePolicy(cmd, configured || len(registry.Schemes()) > 0)
	if err != nil {
		return engine.TaskRuntimeConfig{}, err
	}
	broker, err := identityBroker(authFlagsOf(cmd), policy)
	if err != nil {
		return engine.TaskRuntimeConfig{}, err
	}
	var store *secrets.Store
	if len(registry.Schemes()) > 0 {
		store, err = newSecretStore(cmd, registry)
		if err != nil {
			return engine.TaskRuntimeConfig{}, err
		}
	}
	return engine.NewTaskRuntimeConfig(store, secretAccess, broker)
}

func withLocalTaskRuntime(cmd *cobra.Command, ctx context.Context, workflow *v1.Workflow) (context.Context, func(), error) {
	noop := func() {}
	identity, err := localWorkloadIdentity(cmd)
	if err != nil {
		return nil, noop, err
	}

	// Installed before any of the branches below, and on every one of their
	// returns, rather than only alongside [v1.ContextWithTaskRuntime] near the
	// bottom: a local rehearsal with no secret backend and no broker
	// configured — the common case for a plugin-only workflow on a laptop —
	// returns early, below, without ever building a TaskRuntime at all. A
	// plugin task still runs on that path, and the wire still carries
	// Identity and Namespace fields on every ExecuteRequest, so it needs a
	// caller here regardless of whether this run also resolves secrets. One
	// source: the same identity [v1.TaskRuntime.Identity] gets, converted by
	// [v1.ProtoWorkloadIdentity] to the wire shape [plugin.NewContextWithIdentity]
	// carries, per the same rule engine/runtime.go's taskActivities.context
	// follows for the durable driver.
	ctx = plugin.NewContextWithIdentity(ctx, v1.ProtoWorkloadIdentity(identity))

	registry, configured, closeProviders, err := secretRegistry(cmd)
	if err != nil {
		return nil, closeProviders, err
	}
	policy, secretAccess, err := runtimePolicy(cmd, configured)
	if err != nil {
		closeProviders()
		return nil, noop, err
	}
	broker, err := identityBroker(authFlagsOf(cmd), policy)
	if err != nil {
		closeProviders()
		return nil, noop, err
	}
	if !configured && broker == nil {
		if policy != nil {
			if err := v1.ValidateCredentialTargets(workflow, nil); err != nil {
				closeProviders()
				return nil, noop, err
			}
		}
		return ctx, closeProviders, nil
	}
	var targets []string
	if broker != nil {
		targets = broker.Targets()
	}
	if err := v1.ValidateCredentialTargets(workflow, targets); err != nil {
		closeProviders()
		return nil, noop, err
	}
	var store *secrets.Store
	if configured {
		store, err = newSecretStore(cmd, registry)
		if err != nil {
			closeProviders()
			return nil, noop, err
		}
	}
	return v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
		Store: store, Policy: secretAccess, Broker: broker,
		Identity: identity,
		Step:     auth.StepRef{Workflow: workflow.GetName(), Run: uuid.NewString()},
	}), closeProviders, nil
}
