package auth

import (
	"context"
	"unicode/utf8"
)

// Attributes a rule sees about what a credential is being used for, beside the
// workload.
const (
	// attrTask is the task whose step is using the credential, such as
	// "slack.post". It is the empty string where no task is known, which no
	// rule naming a task matches, so a rule that pins a secret to a task denies
	// a use that cannot say which task it is.
	attrTask = "task"

	// attrCredential is the plugin credential the use is for: the plugin that
	// declared it and its name there, both empty for a use that is not a
	// plugin's credential input (a built-in task's `bearer:`, say).
	attrCredential = "credential"

	// credentialTypeName is how that object is named in CEL.
	credentialTypeName = "auth.credentialUse"
)

// credentialUse is the credential half of the attributes a secret or assumption
// rule sees. The field tags are the names rules use.
type credentialUse struct {
	// Plugin is the plugin that declared the credential.
	Plugin string `cel:"plugin"`

	// Name is the credential's name in the plugin's declaration.
	Name string `cel:"name"`
}

// maxUseBytes bounds each component of a [CredentialUse] a rule reads. A task
// name is at most a plugin name, a dot and a task name, each bounded by their
// schemas, so an honest value is far below it; the bound keeps a context that
// carries something else from becoming a large attribute.
const maxUseBytes = 160

// CredentialUse says what a credential is being used for: the task whose step
// reads it and, when the input claims a plugin credential, which one. It is
// what the `task` and `credential` attributes of a secret access rule and of an
// assumption rule are read from.
//
// It travels on the context, set where the use is known: the task dispatch
// names the task and the plugin host names the credential of the input it is
// resolving. It carries names and never a value or a reference, so a rule reads
// no more than the author's own Flowfile already stated.
type CredentialUse struct {
	// Task is the qualified task name.
	Task string

	// Plugin and Credential name the plugin credential the input claims. Both
	// are empty for an input that claims none.
	Plugin     string
	Credential string
}

type credentialUseKey struct{}

// WithCredentialUse returns a context carrying use. A use set later replaces
// one set earlier, except that an empty Task keeps the task already there, so a
// layer that knows only the credential does not erase the task a layer above it
// named.
func WithCredentialUse(ctx context.Context, use CredentialUse) context.Context {
	if use.Task == "" {
		use.Task = CredentialUseFrom(ctx).Task
	}

	return context.WithValue(ctx, credentialUseKey{}, use)
}

// CredentialUseFrom returns the use ctx carries, the zero value when none.
func CredentialUseFrom(ctx context.Context) CredentialUse {
	use, _ := ctx.Value(credentialUseKey{}).(CredentialUse)

	return use
}

// addCredentialUse sets the task and credential attributes of vars from ctx.
func addCredentialUse(ctx context.Context, vars map[string]any) {
	use := CredentialUseFrom(ctx)
	vars[attrTask] = boundUse(use.Task)
	vars[attrCredential] = credentialUse{Plugin: boundUse(use.Plugin), Name: boundUse(use.Credential)}
}

// boundUse is a component a rule reads: itself when it is text within the bound,
// and the empty string, which names nothing, when it is not. It is deliberately
// not sanitised or truncated into something shorter: that would let a name that
// is not the pinned one compare equal to it once its bad bytes were dropped.
func boundUse(s string) string {
	if len(s) > maxUseBytes || !utf8.ValidString(s) {
		return ""
	}

	return s
}
