package flowstatev1

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// The bounds on a workflow's modules. A module is a file the compiler reads, so the
// bounds are on what a file's author can make the compiler read.
const (
	// MaxUseDepth is how deeply modules may use modules: the workflow uses a module
	// (depth 1), which uses another (2), and so on up to this. Deeper is refused at
	// the `use:` that would exceed it.
	MaxUseDepth = 4

	// MaxUsesPerFile is the most modules one file names under `use:`.
	MaxUsesPerFile = 16

	// MaxModules is the most modules a compiled workflow records, counting a module
	// once for each way it is reached. It matches the schema's bound on
	// [Workflow.Modules] and bounds the work of compiling a workflow whatever the
	// shape of the files it uses.
	MaxModules = 64
)

// moduleAliasPattern is a word the declarations of a module are qualified by: a
// lowerCamel identifier of at most 32 characters. No underscore, which is what
// keeps the alias of a module reached through another (`ids_core`) from ever being
// one an author could have written.
var moduleAliasPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,31}$`)

// moduleChainPattern is the alias of any recorded module: one alias, or a chain of
// at most [MaxUseDepth] of them joined by underscores.
var moduleChainPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,31}(_[a-z][A-Za-z0-9]{0,31}){0,3}$`)

// QualifiedName is the name a module's declaration is carried under: the module's
// alias, a dot and the declaration's own name.
func QualifiedName(alias, name string) string { return alias + "." + name }

// SplitQualified splits a carried declaration's name into the alias of the module
// that declared it and the name it was declared under. ok is false for a name
// written in the workflow itself.
func SplitQualified(name string) (alias, bare string, ok bool) {
	return strings.Cut(name, ".")
}

// moduleRecordBare is the name a module gives a type or an error: capitalised.
var moduleRecordBare = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]{0,127}$`)

// IsModuleName reports whether name has the shape of a type or an error carried from
// a module: one alias, a dot and a capitalised name (`ids.Uuid`, `ids_core.Id`).
// A fully qualified Protobuf name (`google.protobuf.Timestamp`) has more dots, and
// is told apart from this by its shape and, where it matters, by whether the
// workflow declares it.
func IsModuleName(name string) bool {
	alias, bare, ok := SplitQualified(name)

	return ok && moduleChainPattern.MatchString(alias) && moduleRecordBare.MatchString(bare)
}

// IsCarried reports whether a declaration's name says it came from a module. Such a
// declaration is not written back by `flow fmt`: the file that declared it is.
func IsCarried(name string) bool { return strings.Contains(name, ".") }

// ModuleChain is the alias a module reached through a parent is recorded and
// qualified under: the parent's, an underscore, and the alias the parent used.
func ModuleChain(parent, alias string) string {
	if parent == "" {
		return alias
	}

	return parent + "_" + alias
}

// IsDirectModule reports whether an alias is one a file wrote under its own `use:`
// rather than one made by [ModuleChain].
func IsDirectModule(alias string) bool { return !strings.Contains(alias, "_") }

// reservedModuleAliases are the words an alias may not be, because the name already
// means something where an alias is read: the roots an expression resolves, the
// literals and operator of CEL, the words that name a type, and the namespace of
// every library the profile has (`math` for `math.greatest`). Computed once for the
// current profile from the same environment expressions are checked in, so a library
// the profile gains is reserved without a second list to update.
var reservedModuleAliases = sync.OnceValue(func() map[string]bool {
	words := map[string]bool{
		"now": true, "secret": true, "credential": true,
		"true": true, "false": true, "null": true, "in": true,
		"string": true, "int": true, "uint": true, "double": true, "bool": true, "bytes": true,
		"list": true, "map": true, "type": true, "dyn": true, "timestamp": true, "duration": true,
		"enum": true, "struct": true, "cel": true, "optional": true,
	}
	for _, root := range DeclarationRoots() {
		words[root] = true
	}
	words[OutputsRoot] = true
	for _, word := range CELUnusableStepIDs() {
		words[word] = true
	}
	libs, err := ProfileLibraries(CurrentProfile)
	if err != nil {
		return words
	}
	env, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return words
	}
	for name := range profileFunctionNames(env) {
		if namespace, _, qualified := strings.Cut(name, "."); qualified {
			words[namespace] = true
		}
	}

	return words
})

// OutputsRoot is the word a workflow's outputs go by. Reserved as an alias for the
// reason the other roots are: a run's outputs and a module should never be spelled
// the same.
const OutputsRoot = "outputs"

// ValidModuleAlias reports why a word cannot be the alias of a module a file uses,
// or nil.
func ValidModuleAlias(alias string) error {
	switch {
	case !moduleAliasPattern.MatchString(alias):
		return fmt.Errorf("%q is not a module alias; write a lowerCamel word such as `ids` or `billing`, up to 32 letters and digits, with no underscore", alias)
	case reservedModuleAliases()[alias]:
		return fmt.Errorf("%q is a word the language already uses (a root such as `inputs` and `steps`, a CEL literal or type, or the namespace of a built-in library); choose another alias", alias)
	}

	return nil
}

// A ModuleIssue is one thing wrong with how a workflow records its modules.
type ModuleIssue struct {
	// Alias is the module the issue is about, empty when it is about the set.
	Alias string

	Message string
}

// ModuleIssues reports what is wrong with the modules wf records and with every
// declaration that claims to come from one, for a specification a client compiled
// and for one that never was a Flowfile alike. A spec that carries `ids.Uuid` with
// no module `ids` to have carried it from names something nothing resolved, and
// cannot be told from one that lies about where its rules came from.
func ModuleIssues(wf *Workflow) []ModuleIssue {
	var issues []ModuleIssue
	add := func(alias, format string, args ...any) {
		issues = append(issues, ModuleIssue{Alias: alias, Message: fmt.Sprintf(format, args...)})
	}

	modules := wf.GetModules()
	if len(modules) > MaxModules {
		add("", "records %d modules; the most a workflow records is %d", len(modules), MaxModules)

		return issues
	}

	own := ownNames(wf)
	aliases := make(map[string]bool, len(modules))
	for _, module := range modules {
		alias := module.GetAlias()
		if aliases[alias] {
			add(alias, "module alias %q is recorded twice", alias)

			continue
		}
		aliases[alias] = true

		if !moduleChainPattern.MatchString(alias) {
			add(alias, "module alias %q is not a lowerCamel word, or a chain of them joined by `_`", alias)

			continue
		}
		if segments := strings.Split(alias, "_"); len(segments) > MaxUseDepth {
			add(alias, "module %q is reached through %d modules; the most modules may use one another is %d", alias, len(segments), MaxUseDepth)
		} else if IsDirectModule(alias) {
			if err := ValidModuleAlias(alias); err != nil {
				add(alias, "%s", err)
			}
			if where, taken := own[alias]; taken {
				add(alias, "module alias %q is also this workflow's %s; an alias is a word of its own, so rename one of them", alias, where)
			}
		}
		if err := ValidateContentDigest(module.GetSourceDigest()); err != nil {
			add(alias, "module %q records the digest %q, which %s", alias, module.GetSourceDigest(), err)
		}
	}
	for _, module := range modules {
		alias := module.GetAlias()
		if last := strings.LastIndex(alias, "_"); last >= 0 && !aliases[alias[:last]] {
			add(alias, "module %q is reached through %q, which is not recorded", alias, alias[:last])
		}
	}

	check := func(kind, name string) {
		alias, _, qualified := SplitQualified(name)
		if qualified && !aliases[alias] {
			add(alias, "%s %q names module %q, which this workflow does not record; a name with a dot is carried from a module", kind, name, alias)
		}
	}
	for _, d := range wf.GetDeclaredTypes() {
		check("type", d.GetName())
	}
	for _, d := range wf.GetDeclaredFunctions() {
		check("function", d.GetName())
	}
	for _, d := range wf.GetDeclaredErrors() {
		check("error", d.GetName())
	}

	return issues
}

// ownNames are the words a workflow gives a meaning to that an alias must not
// repeat, and what each is: its inputs, outputs, vars, step ids and the functions it
// declares itself. An alias that was also a step id would read two ways in the
// same expression, and every one of them is one rename away from not.
func ownNames(wf *Workflow) map[string]string {
	names := map[string]string{}
	for _, d := range wf.GetDeclaredFunctions() {
		if !IsCarried(d.GetName()) {
			names[d.GetName()] = "function"
		}
	}
	for _, d := range wf.GetDeclaredInputs() {
		names[d.GetName()] = "input"
	}
	for _, d := range wf.GetDeclaredOutputs() {
		names[d.GetName()] = "output"
	}
	for name := range wf.GetVars() {
		names[name] = "var"
	}
	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		if id := node.GetId(); id != "" {
			names[id] = "step"
		}
	}})

	return names
}

// CheckModules is [ModuleIssues] as one error, for the door a specification that
// never was a Flowfile enters through. Nil when wf records its modules soundly.
func CheckModules(wf *Workflow) error {
	issues := ModuleIssues(wf)
	if len(issues) == 0 {
		return nil
	}

	messages := make([]string, 0, len(issues))
	for _, issue := range issues {
		messages = append(messages, issue.Message)
	}

	return fmt.Errorf("modules: %s", strings.Join(messages, "; "))
}
