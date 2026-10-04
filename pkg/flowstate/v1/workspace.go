package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/uuid"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/artifacts"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// ArtifactsOutput is the output name a step with `produce:` exposes its
// snapshots under: `${steps.<id>.artifacts.<name>}`.
//
// Reserved on any step that declares `produce:`. A task whose own outputs
// already use the name cannot be combined with it, and the compiler says so.
const ArtifactsOutput = "artifacts"

// WorkspaceRootDot names the workspace root itself as a `workspace:` mount.
const WorkspaceRootDot = "."

// Bounds on what one step may declare. They are the schema's `max_pairs`,
// restated because the runtime refuses a hand-built task the schema never saw.
const (
	// MaxWorkspaceMounts bounds the artifacts materialized into one workspace.
	MaxWorkspaceMounts = 16
	// MaxProducedArtifacts bounds the snapshots one step takes.
	MaxProducedArtifacts = 16
)

// ArtifactFlagHelp is how the denial an unconfigured worker returns names the
// way an operator turns artifacts on, from the one spelling the commands
// register (the lesson [ExecPolicyFlag] records).
const (
	ArtifactStoreFlag = "--artifact-store"
	ArtifactStoreEnv  = "FLOWSTATE_ARTIFACT_STORE"
)

// ArtifactRuntime is a worker's artifact capability: the tenant-scoped store a
// `workspace:` materializes from and a `produce:` snapshots into, and the
// directory per-attempt workspaces are created under.
//
// It is configuration the operator owns, installed with
// [ContextWithArtifacts] on the context a task runs under, and never part of a
// specification. Without one every step that declares `workspace:` or
// `produce:` is denied with a message naming [ArtifactStoreFlag]: the default is
// that no workflow can write to a worker's disk.
type ArtifactRuntime struct {
	// Store is the store every tenant's artifacts live in; each use binds it to
	// the run's namespace with [artifacts.Store.For].
	Store *artifacts.Store

	// WorkspaceRoot is the absolute directory under which a fresh directory is
	// created for every attempt. For `exec` to run in a workspace the operator's
	// exec policy must list this directory (or a parent) among its roots: the
	// policy confines the working directory exactly as it does any other.
	WorkspaceRoot string
}

// Validate reports whether the runtime can be used: a store, and an absolute
// directory that exists.
func (r *ArtifactRuntime) Validate() error {
	if r == nil || r.Store == nil {
		return errors.New("artifact runtime has no store")
	}
	if !filepath.IsAbs(r.WorkspaceRoot) {
		return fmt.Errorf("artifact workspace root %q is not an absolute path", r.WorkspaceRoot)
	}
	info, err := os.Stat(r.WorkspaceRoot)
	if err != nil {
		return fmt.Errorf("artifact workspace root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("artifact workspace root %q is not a directory", r.WorkspaceRoot)
	}
	return nil
}

// NewMemoryArtifactRuntime returns a runtime whose store lives in this process
// and is gone when it exits, with workspaces created under workspaceRoot (made
// if absent). It is for rehearsals and tests: nothing a durable worker on
// another machine could reach, which is the point of a rehearsal store.
func NewMemoryArtifactRuntime(workspaceRoot string) (*ArtifactRuntime, error) {
	store, err := artifacts.NewStore(artifacts.NewMemoryBackend(nil), artifacts.DefaultLimits())
	if err != nil {
		return nil, err
	}
	return newArtifactRuntime(store, workspaceRoot)
}

// NewLocalArtifactRuntime returns a runtime whose store is the
// content-addressed directory storeDir on this machine, with workspaces created
// under workspaceRoot. Both are made if absent.
func NewLocalArtifactRuntime(storeDir, workspaceRoot string) (*ArtifactRuntime, error) {
	backend, err := artifacts.NewLocalBackend(storeDir)
	if err != nil {
		return nil, err
	}
	store, err := artifacts.NewStore(backend, artifacts.DefaultLimits())
	if err != nil {
		return nil, err
	}
	return newArtifactRuntime(store, workspaceRoot)
}

func newArtifactRuntime(store *artifacts.Store, workspaceRoot string) (*ArtifactRuntime, error) {
	abs, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("artifact workspace root: %w", err)
	}
	// Resolved, because an exec policy compares roots with symbolic links
	// resolved and a workspace under a link would otherwise be "outside" them.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	runtime := &ArtifactRuntime{Store: store, WorkspaceRoot: abs}
	return runtime, runtime.Validate()
}

type (
	artifactRuntimeKey struct{}
	artifactRunKey     struct{}
	workspaceDirKey    struct{}
)

// ContextWithArtifacts installs a worker's artifact capability on ctx.
func ContextWithArtifacts(ctx context.Context, runtime *ArtifactRuntime) context.Context {
	return context.WithValue(ctx, artifactRuntimeKey{}, runtime)
}

// ArtifactsFromContext returns the artifact capability installed on ctx, nil
// when there is none.
func ArtifactsFromContext(ctx context.Context) *ArtifactRuntime {
	runtime, _ := ctx.Value(artifactRuntimeKey{}).(*ArtifactRuntime)
	return runtime
}

// WorkspaceDirFromContext returns the directory of the workspace the running
// task was given, empty when its step declared none. A task that works on files
// (exec) reads it to default its working directory.
func WorkspaceDirFromContext(ctx context.Context) string {
	dir, _ := ctx.Value(workspaceDirKey{}).(string)
	return dir
}

// ContextWithWorkspaceDir records the workspace directory for the running task.
// Exported for a task stub, which stands in for a program that would have
// written there.
func ContextWithWorkspaceDir(ctx context.Context, dir string) context.Context {
	return context.WithValue(ctx, workspaceDirKey{}, dir)
}

// artifactRun is one local run's claim on the store: a pin key unique to the
// run, and the tenants it pinned under, so the run's end can release them.
type artifactRun struct {
	key string

	mu         sync.Mutex
	namespaces map[string]struct{}
}

func (r *artifactRun) note(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.namespaces == nil {
		r.namespaces = map[string]struct{}{}
	}
	r.namespaces[namespace] = struct{}{}
}

// releaseTimeout bounds the pin release at the end of a local run. The release
// runs on a context that ignores the run's cancellation (a cancelled run must
// still let go of what it pinned), so it needs a bound of its own.
const releaseTimeout = 30 * time.Second

// beginArtifactRun gives a local run its own pin key when an artifact
// capability is installed, and returns the function that releases the run's
// pins. Without a capability it does nothing.
//
// The key is a fresh UUID because a local run has no address of its own (every
// local run answers [LocalRunAddress]), and two runs sharing a key would release
// each other's pins.
func beginArtifactRun(ctx context.Context) (context.Context, func()) {
	runtime := ArtifactsFromContext(ctx)
	if runtime == nil {
		return ctx, func() {}
	}
	if _, nested := ctx.Value(artifactRunKey{}).(*artifactRun); nested {
		return ctx, func() {}
	}

	run := &artifactRun{key: "local-" + uuid.NewString()}
	ctx = context.WithValue(ctx, artifactRunKey{}, run)

	return ctx, func() {
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()

		run.mu.Lock()
		namespaces := slices.Sorted(maps.Keys(run.namespaces))
		run.mu.Unlock()

		for _, namespace := range namespaces {
			// Best effort: the pins of a local run on a memory store die with the
			// process, and on a local directory a failed release leaves blobs
			// pinned until `flow artifacts gc --release` names the run.
			_ = ReleaseArtifacts(release, runtime, namespace, run.key)
		}
	}
}

// ArtifactNamespace is the tenant an identity's artifacts live under.
//
// The identity's namespace, and for the unnamespaced default tenant the same
// reserved name the secrets store uses ([secrets.DefaultNamespaceDir]): no real
// namespace can begin with an underscore, so the default tenant cannot collide
// with, or be mistaken for, a named one. It comes only from the run's attested
// identity; nothing a workflow writes reaches it.
func ArtifactNamespace(identityNamespace string) string {
	if identityNamespace == "" {
		return secrets.DefaultNamespaceDir
	}
	return identityNamespace
}

// ArtifactRunKey is the key a durable run's pins are held under: the run's
// address, which survives Continue-As-New (the first run id of the chain), so
// every segment of one run shares a claim and the run's end releases it once.
func ArtifactRunKey(address *RunAddress) (string, error) {
	workflowID, runID := address.GetWorkflowId(), address.GetRunId()
	if workflowID == "" || runID == "" || workflowID == LocalRunAddress {
		return "", errors.New("the run has no durable address to hold artifact pins under")
	}
	return workflowID + "/" + runID, nil
}

// ReleaseArtifacts lets go of everything a run pinned in one tenant. The blobs
// stay until a sweep removes the unpinned ones older than its grace window.
// Idempotent.
func ReleaseArtifacts(ctx context.Context, runtime *ArtifactRuntime, namespace, runKey string) error {
	if runtime == nil || runtime.Store == nil {
		return errors.New("no artifact store is configured on this worker")
	}
	ns, err := runtime.Store.For(namespace)
	if err != nil {
		return err
	}
	return ns.Unpin(ctx, runKey)
}

// TaskUsesArtifacts reports whether a task declares `workspace:` or
// `produce:`, which is what sends it to an activity that carries the worker's
// artifact capability and the run's address.
func TaskUsesArtifacts(task *Task) bool {
	return len(task.GetWorkspace()) > 0 || len(task.GetProduce()) > 0
}

// WorkflowUsesArtifacts reports whether any step of the workflow, including the
// steps of a called workflow embedded in it, declares `workspace:` or
// `produce:`. The durable driver asks it once per run to decide whether the
// run's end has pins to release.
func WorkflowUsesArtifacts(w *Workflow) bool {
	return workflowUsesArtifacts(w, 0, map[*Workflow]struct{}{})
}

// workflowUsesArtifacts follows calls to [MaxCallDepth], and visits a callee
// message once however many calls share it: a specification built in memory may
// point many calls at one callee, and walking it per call would cost the
// product of the fan-outs rather than the size of the specification.
func workflowUsesArtifacts(w *Workflow, depth int, seen map[*Workflow]struct{}) bool {
	if w == nil || depth > MaxCallDepth {
		return false
	}
	if _, done := seen[w]; done {
		return false
	}
	seen[w] = struct{}{}

	var uses bool
	WalkWorkflow(w, Walk{Node: func(node *Node) {
		if uses {
			return
		}
		if TaskUsesArtifacts(node.GetTask()) || TaskUsesArtifacts(node.GetUndo().GetTask()) {
			uses = true
			return
		}
		if call := node.GetCall(); call != nil && workflowUsesArtifacts(call.GetWorkflow(), depth+1, seen) {
			uses = true
		}
	}})
	return uses
}

// CheckWorkspaceMounts reports whether the directories of a `workspace:` can
// coexist: each a clean relative path (or the root, ".") with no component
// that is "..", none nested inside another, the root alone, and none that would
// collide on a case-insensitive filesystem.
//
// Shared by the compiler, which applies it to the keys an author wrote, and the
// worker, which applies it again to whatever a specification carries, since the
// worker is the one that creates the directories.
func CheckWorkspaceMounts(mounts []string) error {
	if len(mounts) > MaxWorkspaceMounts {
		return fmt.Errorf("workspace: %d entries, over the limit of %d", len(mounts), MaxWorkspaceMounts)
	}
	folded := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		if err := CheckWorkspacePath(mount); err != nil {
			return fmt.Errorf("workspace: %w", err)
		}
		folded = append(folded, strings.ToLower(mount))
	}
	slices.Sort(folded)
	for i, mount := range folded {
		if mount == WorkspaceRootDot && len(folded) > 1 {
			return fmt.Errorf("workspace: %q is the whole workspace, so it cannot be combined with another entry", WorkspaceRootDot)
		}
		if i > 0 {
			prev := folded[i-1]
			if mount == prev {
				return fmt.Errorf("workspace: %q is named twice (paths are compared without regard to case)", mount)
			}
			if strings.HasPrefix(mount, prev+"/") {
				return fmt.Errorf("workspace: %q is inside %q; entries may not nest", mount, prev)
			}
		}
	}
	return nil
}

// CheckWorkspacePath reports whether p can name a directory inside a
// workspace: the root ("."), or a clean relative slash-separated path within the
// artifact store's own path bounds.
func CheckWorkspacePath(p string) error {
	if p == WorkspaceRootDot {
		return nil
	}
	if err := artifacts.ValidatePath(p, artifacts.DefaultLimits()); err != nil {
		return fmt.Errorf("%q is not a path inside the workspace (a clean relative path such as `src` or `out/bin`, or %q for the workspace itself)", p, WorkspaceRootDot)
	}
	return nil
}

// artifactRefLiteralKeys are the only keys an artifact reference holds when it
// is written as a map, so a map with anything else is refused rather than
// partly read.
var artifactRefLiteralKeys = []string{"digest", "entry_count", "size_bytes"}

// ArtifactRefLiteral renders a reference as the CEL map a step exposes it as:
// `{digest, size_bytes, entry_count}`.
func ArtifactRefLiteral(ref *ArtifactRef) map[string]any {
	return map[string]any{
		"digest":      ref.GetDigest(),
		"size_bytes":  ref.GetSizeBytes(),
		"entry_count": int64(ref.GetEntryCount()),
	}
}

// artifactRefFromLiteral reads a CEL map back into a reference, strictly: the
// three keys and nothing else, the digest 64 lowercase hex digits, the numbers
// inside the schema's ranges. A reference is the only thing a workspace entry
// may be, and a map that is not exactly one is an error, not a guess.
func artifactRefFromLiteral(lit *expr.Value) (*ArtifactRef, error) {
	entries := lit.GetMapValue().GetEntries()
	if lit.GetMapValue() == nil {
		return nil, fmt.Errorf("is %s, not an artifact (a map of digest, size_bytes and entry_count, such as ${steps.build.artifacts.src})", literalKindName(lit))
	}

	fields := make(map[string]*expr.Value, len(entries))
	for _, entry := range entries {
		key, ok := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !ok {
			return nil, errors.New("is a map with a key that is not a string, so it is not an artifact")
		}
		fields[key.StringValue] = entry.GetValue()
	}
	if got := slices.Sorted(maps.Keys(fields)); !slices.Equal(got, artifactRefLiteralKeys) {
		return nil, fmt.Errorf("is a map with keys %v, but an artifact has exactly %v", got, artifactRefLiteralKeys)
	}

	digest, ok := fields["digest"].GetKind().(*expr.Value_StringValue)
	if !ok {
		return nil, errors.New("has a digest that is not a string")
	}
	size, err := literalInt(fields["size_bytes"])
	if err != nil {
		return nil, fmt.Errorf("has a size_bytes that is %w", err)
	}
	count, err := literalInt(fields["entry_count"])
	if err != nil {
		return nil, fmt.Errorf("has an entry_count that is %w", err)
	}
	if count > int64(^uint32(0)>>1) {
		return nil, errors.New("has an entry_count out of range")
	}

	ref := &ArtifactRef{Digest: digest.StringValue, SizeBytes: size, EntryCount: int32(count)}
	if err := Validate(ref); err != nil {
		return nil, fmt.Errorf("is not a valid artifact: %w", err)
	}
	return ref, nil
}

func literalInt(v *expr.Value) (int64, error) {
	switch kind := v.GetKind().(type) {
	case *expr.Value_Int64Value:
		return kind.Int64Value, nil
	case *expr.Value_Uint64Value:
		if kind.Uint64Value > uint64(1<<62) {
			return 0, errors.New("out of range")
		}
		return int64(kind.Uint64Value), nil
	}
	return 0, fmt.Errorf("%s, not a whole number", literalKindName(v))
}

// ArtifactRefValue wraps a reference as a [Value].
func ArtifactRefValue(ref *ArtifactRef) *Value {
	return &Value{Kind: &Value_ArtifactRef{ArtifactRef: ref}}
}

// resolveWorkspace returns the task's `workspace:` with every entry a
// [Value_ArtifactRef]: an expression is evaluated against the scope, a literal
// map is read as a reference, and a reference passes through.
//
// Evaluated workflow-side like an input, deterministically and in sorted order
// so the first failure is the same on every replay. What it hands back is
// inert: a digest and two numbers, which is all history ever holds of an
// artifact.
func resolveWorkspace(ctx context.Context, task *Task, scope *Scope) (map[string]*Value, error) {
	if len(task.GetWorkspace()) == 0 {
		return nil, nil
	}

	resolved := make(map[string]*Value, len(task.GetWorkspace()))
	ev := DefaultEvaluator()
	for _, mount := range slices.Sorted(maps.Keys(task.GetWorkspace())) {
		value := task.GetWorkspace()[mount]

		var lit *expr.Value
		switch kind := value.GetKind().(type) {
		case *Value_ArtifactRef:
			resolved[mount] = value
			continue
		case *Value_Expr:
			out, err := ev.EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
			if err != nil {
				return nil, fmt.Errorf("workspace %q: %w", mount, err)
			}
			if lit, err = cel.RefValueToValue(out); err != nil {
				return nil, fmt.Errorf("workspace %q: converting result: %w", mount, err)
			}
		case *Value_Literal:
			lit = kind.Literal
		default:
			return nil, fmt.Errorf("workspace %q: a %T cannot be an artifact", mount, value.GetKind())
		}

		ref, err := artifactRefFromLiteral(lit)
		if err != nil {
			return nil, fmt.Errorf("workspace %q: %w", mount, err)
		}
		resolved[mount] = ArtifactRefValue(ref)
	}
	return resolved, nil
}

// workspaceResolved reports whether every workspace entry is already a
// reference, so a second resolution pass has nothing to do.
func workspaceResolved(task *Task) bool {
	for _, value := range task.GetWorkspace() {
		if _, ok := value.GetKind().(*Value_ArtifactRef); !ok {
			return false
		}
	}
	return true
}

// callWithWorkspace runs a task's function inside the workspace its step
// declared: a fresh directory filled before the call and snapshotted after it,
// then removed.
//
// This is the one place the drivers share (every task body is reached through
// [Task.EvalInScope], local and durable alike), so the two cannot disagree about
// what a workspace is. Per attempt by construction: the directory is created
// here, inside the attempt, and a retry or a replay arrives at a new one.
//
// Every failure is classified and none is a panic or a partial success:
//   - no capability on this worker, or no tenant to scope it to: denied;
//   - a reference this tenant's store cannot produce (unknown here, bytes
//     corrupted, another worker's disk): invalid input, permanent. A retry
//     cannot find on this worker what is not on its disk;
//   - a bound of the store exceeded: limit exceeded, never truncation;
//   - a produced path that is missing, or holds a link or special file: invalid
//     output of the step, permanent;
//   - the disk failing: upstream, retryable.
func (t *Task) callWithWorkspace(ctx context.Context, def TaskDef, scope *Scope) (*Node_Outputs, error) {
	if !TaskUsesArtifacts(t) {
		return def.Fn(ctx, t.Inputs, scope)
	}

	fail := func(kind ErrorKind, err error) error { return NewTaskError(t.Name, kind, err) }

	runtime := ArtifactsFromContext(ctx)
	if runtime == nil || runtime.Store == nil {
		return nil, fail(ErrorKindPolicyDenied, fmt.Errorf("this worker has no artifact store, so a step cannot declare "+
			"workspace: or produce:; an operator enables them by starting the worker with %s <directory> (or setting %s)",
			ArtifactStoreFlag, ArtifactStoreEnv))
	}

	// The tenant is the run's attested identity and nothing else. A scope that
	// names none is the default tenant, exactly as the secrets store reads it.
	ns, err := runtime.Store.For(ArtifactNamespace(scope.GetIdentity().GetNamespace()))
	if err != nil {
		return nil, fail(ErrorKindPolicyDenied, fmt.Errorf("the run's tenant could not be established: %w", err))
	}

	runKey, err := artifactRunKeyFor(ctx, scope, ns.Namespace())
	if err != nil {
		return nil, fail(ErrorKindPolicyDenied, err)
	}

	mounts := slices.Sorted(maps.Keys(t.Workspace))
	if err := CheckWorkspaceMounts(mounts); err != nil {
		return nil, fail(ErrorKindInvalidInput, err)
	}
	if len(t.Produce) > MaxProducedArtifacts {
		return nil, fail(ErrorKindInvalidInput, fmt.Errorf("produce: %d entries, over the limit of %d", len(t.Produce), MaxProducedArtifacts))
	}
	for _, name := range slices.Sorted(maps.Keys(t.Produce)) {
		if err := CheckWorkspacePath(t.Produce[name]); err != nil {
			return nil, fail(ErrorKindInvalidInput, fmt.Errorf("produce %q: %w", name, err))
		}
	}

	dir, err := os.MkdirTemp(runtime.WorkspaceRoot, "ws-")
	if err != nil {
		return nil, fail(ErrorKindUpstream, fmt.Errorf("creating the workspace: %w", err))
	}
	// The workspace never outlives the attempt that made it, whatever happens to
	// it: removed after the snapshot below, and just the same when the task
	// fails, times out, or panics.
	defer os.RemoveAll(dir)

	for _, mount := range mounts {
		ref := t.Workspace[mount].GetArtifactRef()
		if ref == nil {
			return nil, fail(ErrorKindInvalidInput, fmt.Errorf("workspace %q was never resolved to an artifact", mount))
		}
		target := dir
		if mount != WorkspaceRootDot {
			target = filepath.Join(dir, filepath.FromSlash(mount))
			if err := os.MkdirAll(target, 0o755); err != nil {
				return nil, fail(ErrorKindUpstream, fmt.Errorf("creating workspace %q: %w", mount, err))
			}
		}
		// Pinned before it is read, so the run holds what it uses and a sweep
		// cannot collect it between this step and the next. Also the check that
		// the blobs exist in *this* tenant's store on *this* worker.
		if err := ns.PinArtifact(ctx, runKey, ref.GetDigest()); err != nil {
			return nil, artifactFailure(t.Name, fmt.Sprintf("workspace %q", mount), ref.GetDigest(), err)
		}
		noteArtifactNamespace(ctx, ns.Namespace())
		if err := ns.Materialize(ctx, ref.GetDigest(), target); err != nil {
			return nil, artifactFailure(t.Name, fmt.Sprintf("workspace %q", mount), ref.GetDigest(), err)
		}
	}

	out, err := def.Fn(ContextWithWorkspaceDir(ctx, dir), t.Inputs, scope)
	if err != nil || out == nil {
		return out, err
	}

	if len(t.Produce) == 0 {
		return out, nil
	}
	if _, taken := out.GetNamedValues()[ArtifactsOutput]; taken {
		return nil, fail(ErrorKindInvalidInput, fmt.Errorf("the %s task already has an output named %q, so produce: cannot add its snapshots under that name",
			t.Name, ArtifactsOutput))
	}

	produced := make(map[string]any, len(t.Produce))
	for _, name := range slices.Sorted(maps.Keys(t.Produce)) {
		path := t.Produce[name]
		source, err := producedDir(dir, path)
		if err != nil {
			return nil, fail(ErrorKindInvalidInput, fmt.Errorf("produce %q: %w", name, err))
		}
		ref, err := ns.Snapshot(ctx, runKey, source)
		if err != nil {
			return nil, artifactFailure(t.Name, fmt.Sprintf("produce %q (%s)", name, path), "", err)
		}
		noteArtifactNamespace(ctx, ns.Namespace())
		produced[name] = ArtifactRefLiteral(&ArtifactRef{
			Digest: ref.Digest, SizeBytes: ref.SizeBytes, EntryCount: int32(ref.EntryCount),
		})
	}

	if out.NamedValues == nil {
		out.NamedValues = map[string]*Value{}
	}
	out.NamedValues[ArtifactsOutput] = NewLiteralMap(produced)

	return out, nil
}

// producedDir resolves a `produce:` path under the workspace directory,
// refusing any component that is a symbolic link.
//
// The snapshot itself refuses a link at the tree root and inside the tree, but
// the OS would follow one in a *parent* component: a task that replaced `out`
// with a link to a directory elsewhere would otherwise have that directory
// snapshotted as its output.
func producedDir(workspace, path string) (string, error) {
	current := workspace
	if path == WorkspaceRootDot {
		return current, nil
	}
	for part := range strings.SplitSeq(path, "/") {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return "", fmt.Errorf("the task did not create %q in the workspace", path)
		case err != nil:
			return "", err
		case info.Mode()&os.ModeSymlink != 0:
			return "", fmt.Errorf("%q is, or passes through, a symbolic link; a snapshot holds only files and directories", path)
		case !info.IsDir():
			return "", fmt.Errorf("%q is not a directory; produce: snapshots a directory", path)
		}
	}
	return current, nil
}

// artifactRunKeyFor is the key this task's pins are held under: the local
// run's own, or the durable run's address.
func artifactRunKeyFor(ctx context.Context, scope *Scope, _ string) (string, error) {
	if run, ok := ctx.Value(artifactRunKey{}).(*artifactRun); ok {
		return run.key, nil
	}
	return ArtifactRunKey(scope.GetAddress())
}

func noteArtifactNamespace(ctx context.Context, namespace string) {
	if run, ok := ctx.Value(artifactRunKey{}).(*artifactRun); ok {
		run.note(namespace)
	}
}

// artifactFailure classifies a store error. digest may be empty.
func artifactFailure(task, what, digest string, err error) error {
	switch {
	case errors.Is(err, artifacts.ErrLimitExceeded):
		return NewTaskError(task, ErrorKindLimitExceeded, fmt.Errorf("%s: %w", what, err))
	case errors.Is(err, artifacts.ErrNotFound):
		return NewTaskError(task, ErrorKindInvalidInput, fmt.Errorf("%s: the artifact %s is not in this worker's store for the run's "+
			"tenant (it may have been produced on another worker, by another tenant, or swept): %w", what, shortDigest(digest), err))
	case errors.Is(err, artifacts.ErrDigestMismatch), errors.Is(err, artifacts.ErrInvalidManifest),
		errors.Is(err, artifacts.ErrInvalidDigest), errors.Is(err, artifacts.ErrInvalidPath):
		return NewTaskError(task, ErrorKindInvalidInput, fmt.Errorf("%s: the artifact is damaged or malformed: %w", what, err))
	case errors.Is(err, artifacts.ErrSymlink), errors.Is(err, artifacts.ErrHardlink), errors.Is(err, artifacts.ErrSpecialFile):
		return NewTaskError(task, ErrorKindInvalidInput, fmt.Errorf("%s: %w; a snapshot holds only regular files and directories", what, err))
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		return NewTaskError(task, ErrorKindUpstream, fmt.Errorf("%s: %w", what, err))
	}
}

func shortDigest(d string) string {
	if len(d) > 12 {
		return d[:12] + "…"
	}
	return d
}
