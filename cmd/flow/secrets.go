package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/spf13/cobra"
)

const (
	secretEnvAllowEnv = "FLOWSTATE_SECRET_ENV_ALLOW"
	secretDirEnv      = "FLOWSTATE_SECRET_DIR"
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
	return registry, len(names) > 0 || len(namespacePrefixes) > 0 || dir != "", closeProviders, nil
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
