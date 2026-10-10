package flowfile

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// The `use:` block: the modules a file takes its vocabulary from.
//
//	use:
//	  ids: {path: ./lib/ids.yaml}
//
//	inputs:
//	  customer: {type: ids.Customer, required: true}
//	steps:
//	  - id: check
//	    if: ${!ids.isUuid(inputs.customer.id)}
//	    fail: {error: ids.NotFound}
//
// A module is a Flowfile with no steps ([v1.IsModule]): its types, functions and
// errors, which a file reaches only through the alias, `ids.Uuid`, `ids.isUuid(x)`,
// `ids.NotFound`. There is no bare import and no re-export, so a name always says
// where it comes from and a module can add a declaration without changing what any
// other file means.
//
// # What it is, and is not
//
// A `use:` is read at compile time, by whoever compiles this file, the way a
// `call:` is, and for the same reason ([v1.Call]): the specification is frozen at
// submit, so nothing a worker does depends on a file. The module's declarations are
// carried into the specification under their qualified names and inlined at every
// use exactly as the file's own are, so the runtime sees plain CEL, plain steps and
// plain `must` strings, and both drivers run what they ran before. [v1.Workflow.Modules]
// records which bytes the declarations came from, for whoever audits a run; nothing
// evaluates it.
//
// The path is resolved by [ResolveCallTarget] with the rule a call is held to:
// relative to this file, never absolute, never above its directory as written or
// through a symlink. A module that is not a file this machine can read is not a
// language concept; vendor it. Nothing here touches the network.
//
// # Bounds
//
// A file names at most [v1.MaxUsesPerFile] modules; modules use modules to
// [v1.MaxUseDepth]; a workflow's modules are at most [v1.MaxModules] together, and a
// module reached by two routes is compiled once and carried under both. A cycle is
// caught by the chain of files being compiled, the way a call's is. What a module
// carries counts against this file's bounds on types, functions, errors and
// expansion nodes, and a refusal is reported at the `use:` that crossed one.
//
// # Transitive modules
//
// A module that uses another carries the other's record types too, because a field
// of the first may be typed by one. They are carried under both aliases joined by an
// underscore (`ids_core.Id`), which no author can write: an alias has no underscore.
// This file cannot name them, so nothing is re-exported, and `flow fmt` writes back
// only the `use:` entries this file wrote.

var useKeys = []string{"path", "digest"}

// A modulePin is the `digest:` an author wrote on a `use:` entry, with what it
// takes to report against it.
type modulePin struct {
	alias string
	node  ast.Node
	path  string
	ref   ref
}

// moduleSession is the account of the modules one workflow uses, shared by the file
// being compiled and every module it reaches.
type moduleSession struct {
	// depth is how many modules deep the file being compiled is: 0 for a workflow.
	depth int

	// loaded holds each module compiled so far by the path it resolved to, so a
	// module reached twice is read and compiled once and both uses see the same
	// bytes. Shared by every file of the workflow's compile; its size bounds the
	// files a workflow's author can make the compiler read.
	loaded map[string]*loadedModule

	// failed holds, by the same key, the reason each module that was read and did
	// not compile gave, so a module that fails is read and parsed once however many
	// times it is named, and a tree of uses cannot multiply the work of failing.
	// It counts against the same bound as loaded. A module refused only for where
	// it was reached from (a cycle, the depth) is checked before this and never
	// recorded.
	failed map[string]string

	// reads counts the files read for modules, shared like loaded.
	reads *int

	// ignorePins skips verifying `use:` digests, for [ParseAtWithoutModulePins]
	// only. Shared by every module the compile reaches.
	ignorePins bool

	// cache, when set, holds the modules compiled before, for a validating entry
	// point ([ModuleCache]). Nil compiles every module.
	cache *ModuleCache

	// deps records, in order, each module this file's `use:` entries loaded, which
	// is what a cached copy of this file is later checked against. Not shared:
	// each file has its own.
	deps []moduleDep
}

func newModuleSession() *moduleSession {
	return &moduleSession{loaded: map[string]*loadedModule{}, failed: map[string]string{}, reads: new(int)}
}

// A loadedModule is a module that compiled and validated.
type loadedModule struct {
	workflow *v1.Workflow
	digest   string

	// path is where the module resolved to, and iface is the digest of what it
	// declares ([interfaceDigest]).
	path  string
	iface string
}

// A usedModule is one alias this file wrote.
type usedModule struct {
	alias  string
	source string
	module *v1.Workflow
}

// typeNames are the types the module declares for itself, in the order written.
func (u usedModule) typeNames() []string {
	var names []string
	for _, d := range u.module.GetDeclaredTypes() {
		if !v1.IsCarried(d.GetName()) {
			names = append(names, d.GetName())
		}
	}

	return names
}

// functionNames are the functions the module declares for itself.
func (u usedModule) functionNames() []string {
	var names []string
	for _, d := range u.module.GetDeclaredFunctions() {
		if !v1.IsCarried(d.GetName()) {
			names = append(names, d.GetName())
		}
	}

	return names
}

// usedModules is what a file's `use:` brought in.
type usedModules struct {
	aliases map[string]usedModule
	order   []string

	// refused holds the aliases of the entries that were not used because this
	// block stopped at one it could not (a pin that does not verify, a module that
	// does not compile), so a name that reaches one says so rather than telling the
	// author to add a `use:` that is already written.
	refused map[string]bool

	// span is where `use:` is written, for a refusal about the block as a whole.
	span Span

	records   []*v1.Module
	types     []*v1.TypeDeclaration
	functions []*v1.FunctionDeclaration
	errors    []*v1.ErrorDeclaration
}

func (u *usedModules) withTypes(own []*v1.TypeDeclaration) []*v1.TypeDeclaration {
	return nilIfEmpty(slices.Concat(u.types, own))
}

func (u *usedModules) withFunctions(own []*v1.FunctionDeclaration) []*v1.FunctionDeclaration {
	return nilIfEmpty(slices.Concat(u.functions, own))
}

func (u *usedModules) withErrors(own []*v1.ErrorDeclaration) []*v1.ErrorDeclaration {
	return nilIfEmpty(slices.Concat(u.errors, own))
}

func nilIfEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}

	return s
}

// carriedCount completes a sentence about how many declarations a file makes with
// how many it was handed, so a refusal for going over a bound names both halves.
func (u *usedModules) carriedCount(kind string, n int) string {
	if n == 0 {
		return ""
	}

	return fmt.Sprintf(" beside the %d %s%s its modules carry", n, kind, plural(n))
}

// useModules compiles the `use:` block: for each alias, resolves the module,
// compiles it, and carries its declarations in.
func (c *compiler) useModules(f field) {
	const path = "use"
	r := ref{path: path, label: "use"}

	block := c.resolveQuiet(f.value)
	c.pos.record(path, spanOfNode(block))
	c.uses.span = spanOfNode(f.key)

	entries, ok := c.entries(f.value, path, r)
	if !ok {
		return
	}
	if len(entries) > v1.MaxUsesPerFile {
		c.report(spanOfNode(block), r,
			"uses %d modules; the most a file uses is %d", len(entries), v1.MaxUsesPerFile)

		return
	}
	if len(entries) > 0 && c.session.depth >= v1.MaxUseDepth {
		c.report(c.uses.span, r,
			"uses a module from a module that is already %d modules deep; modules may use one another to a depth of %d, "+
				"so a module this deep can only declare", c.session.depth, v1.MaxUseDepth)

		return
	}

	c.uses.aliases = make(map[string]usedModule, len(entries))
	c.uses.refused = map[string]bool{}
	c.initTypeScope()
	for i, e := range entries {
		before := len(c.diags)
		c.useModule(e, path)
		if len(c.diags) > before {
			// The first module that cannot be used is the one to fix; the rest of
			// the block would only repeat it, and a block of failing modules would
			// otherwise multiply its report by its fan-out at every level.
			for _, left := range entries[i:] {
				c.uses.refused[left.name] = true
			}

			return
		}
	}
}

// maxFirstProblem bounds the part of a module's own error that a use of it repeats.
const maxFirstProblem = 240

// firstProblem is the first line of a module's error, cut to [maxFirstProblem]
// bytes on a rune boundary: enough to say what to fix, and a size that does not
// grow with how deeply modules use one another.
func firstProblem(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	line = strings.TrimSpace(line)
	if len(line) <= maxFirstProblem {
		return line
	}
	cut := maxFirstProblem
	for cut > 0 && !utf8.RuneStart(line[cut]) {
		cut--
	}

	return line[:cut] + "..."
}

// useModule compiles one `alias: {path: ...}` entry.
func (c *compiler) useModule(e entry, parent string) {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "use " + e.name}
	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	if err := v1.ValidModuleAlias(e.name); err != nil {
		c.report(spanOfNode(e.key), r, "is not an alias: %s", err)

		return
	}

	entries, ok := c.entries(e.value, path, r)
	if !ok {
		c.report(spanOfNode(e.value), r, "is written as a mapping with the module's `path:`, such as `%s: {path: ./lib/%s.yaml}`", e.name, e.name)

		return
	}
	fields := c.check(entries, r, useKeys)

	pathField, found := fields.get("path")
	if !found {
		c.report(spanOfNode(e.key), r, "has no `path:`; write the module's file, relative to this one, such as `path: ./lib/%s.yaml`", e.name)

		return
	}
	pathPath := fieldPath(path, "path")
	pathRef := ref{path: pathPath, label: "use " + e.name + " path"}
	c.pos.record(pathPath, spanOfNode(c.resolveQuiet(pathField.value)))
	target, ok := c.text(pathField.value, pathPath, pathRef)
	if !ok {
		return
	}

	var pin *modulePin
	if digestField, pinned := fields.get("digest"); pinned && !c.session.ignorePins {
		pinPath := fieldPath(path, "digest")
		pin = &modulePin{alias: e.name, node: digestField.value, path: pinPath, ref: ref{path: pinPath, label: "use " + e.name + " digest"}}
	}

	module, ok := c.loadModule(pathField.value, pathRef, target, pin)
	if !ok {
		return
	}

	dep := moduleDep{target: target, path: module.path, iface: module.iface}
	if pin != nil {
		dep.pinned = module.digest
	}
	c.session.deps = append(c.session.deps, dep)

	c.carry(usedModule{alias: e.name, source: target, module: module.workflow}, module.digest, pathField.value, pathRef)
}

// loadModule resolves, reads, compiles and validates the module a `use:` names, or
// reports why it cannot. It fails closed: a module that does not resolve to a
// readable, valid module contributes nothing, and the file is not compiled as if it
// had been used.
func (c *compiler) loadModule(pathNode ast.Node, r ref, target string, pin *modulePin) (*loadedModule, bool) {
	span := spanOfNode(pathNode)

	located := ResolveCallTarget(c.filePath, target)
	if message := explainRefusal(located, target, usingFile); message != "" {
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r, "%s", message)

		return nil, false
	}
	resolved := located.Path

	ancestors := c.ancestors()
	if err := v1.CheckCallDepth(len(ancestors)); err != nil {
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r, "%s", err.Error())

		return nil, false
	}
	if slices.Contains(ancestors, resolved) {
		chain := append(slices.Clone(ancestors), resolved)
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
			"uses %q, which leads back to this file: %s; a module may not use itself, directly or through other modules",
			target, strings.Join(chain, " uses "))

		return nil, false
	}

	if loaded, ok := c.session.loaded[resolved]; ok {
		// A pin is held to the bytes this workflow's compile read, however many
		// entries name the module, and each entry is held to its own.
		if pin != nil && !c.verifyModulePin(pin, target, loaded.digest) {
			return nil, false
		}

		return loaded, true
	}
	if message, ok := c.session.failed[resolved]; ok {
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r, "%s", message)

		return nil, false
	}
	if attempted := len(c.session.loaded) + len(c.session.failed); attempted >= v1.MaxModules {
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
			"uses a module that would make %d, which is more than the %d a workflow reads; "+
				"a workflow's modules are bounded together however they nest", attempted+1, v1.MaxModules)

		return nil, false
	}

	// refuse records why this module cannot be used, once, for every later use of it.
	refuse := func(format string, args ...any) (*loadedModule, bool) {
		message := fmt.Sprintf(format, args...)
		c.session.failed[resolved] = message
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r, "%s", message)

		return nil, false
	}

	*c.session.reads++
	data, err := readBoundedSource(resolved)
	if err != nil {
		return refuse("uses %q, which could not be read: %s", target, err.Error())
	}
	digest := formatSourceDigest(data)

	// Checked before the module is compiled, on the digest of the bytes just read
	// and never on a second read: the bytes that were verified are the bytes that
	// are parsed. Not recorded as a failure of the module, because a pin belongs to
	// the entry that wrote it and another entry may name the same file unpinned.
	if pin != nil && !c.verifyModulePin(pin, target, digest) {
		return nil, false
	}

	// A module that compiled before, whose dependencies are what they were, is not
	// compiled again; see [ModuleCache].
	if loaded, ok := c.reuseModule(resolved, ancestors, digest); ok {
		c.session.loaded[resolved] = loaded

		return loaded, true
	}

	child := c.session.child()
	c.session.cache.count(func(s *ModuleCacheStats) { s.Compiles++ })
	module, positions, err := parse(data, resolved, ancestors, c.callBudget, child)
	if err != nil {
		count := 1
		if compiled, ok := errors.AsType[Diagnostics](err); ok {
			count = len(compiled)
		}

		return refuse("uses %q, which has %d error%s; first: %s", target, count, plural(count), firstProblem(err.Error()))
	}

	switch {
	case len(module.GetSteps()) > 0:
		return refuse("uses %q, which has steps and so is a workflow and not a module; a module declares only types, functions and errors. "+"To run it as a step, write `call: %s` on a step", target, target)
	case !v1.IsModule(module):
		return refuse("uses %q, which is not a module; a module declares types, functions or errors and nothing else "+"(no inputs, outputs, vars, triggers or steps)", target)
	}

	if ds := ValidateModule(module); len(ds) > 0 {
		positionDiagnostics(ds, positions)
		return refuse("uses %q, which has %d error%s; first: %s", target, len(ds), plural(len(ds)), firstProblem(ds.Error()))
	}

	loaded := &loadedModule{workflow: module, digest: digest, path: resolved, iface: interfaceDigest(module)}
	c.session.loaded[resolved] = loaded
	if cache := c.session.cache; cache != nil && !c.session.ignorePins {
		cache.store(cache.keyFor(resolved, digest, c.session.depth+1), loaded, child.deps)
	}

	return loaded, true
}

// verifyModulePin checks the `digest:` of a `use:` entry against the digest of the
// module's bytes, and reports whether the module may go on being used.
//
// The same contract as a `call:` pin ([compiler.verifySourcePin]): the written text
// goes through [v1.CanonicalContentDigest], so one spelling is accepted and
// nothing else (no prefix, no other algorithm, no padding), and it is compared with
// the digest of bytes already read. Fail closed: a pin that does not verify means
// the author has not authorised these bytes, so the module contributes nothing.
func (c *compiler) verifyModulePin(pin *modulePin, target, actual string) bool {
	before := len(c.diags)
	written, ok := c.text(pin.node, pin.path, pin.ref)
	if !ok {
		// A pin that is not text is a pin that does not verify: coded the same.
		for i := before; i < len(c.diags); i++ {
			c.diags[i].Code = v1.DiagnosticCodeModulePinMismatch
		}

		return false
	}

	span := spanOfNode(pin.node)
	canonical, err := v1.CanonicalContentDigest(written)
	if err != nil {
		c.reportCode(v1.DiagnosticCodeModulePinMismatch, span, pin.ref,
			"is %s, which is not the shape of a pin; write `sha256:` and the 64 hex characters of the module's SHA-256, "+
				"which for module %s (%s) is `digest: %s` right now",
			describeWrittenPin(written), pin.alias, target, actual)

		return false
	}
	if canonical != actual {
		c.reportCode(v1.DiagnosticCodeModulePinMismatch, span, pin.ref,
			"pins module %s (%s) at %s, but that file hashes to %s right now; a mismatch means the module changed since the pin was written, "+
				"so read what it declares now and then run `flow fix --repin` on this file, or write `digest: %s`, to adopt it",
			pin.alias, target, canonical, actual, actual)

		return false
	}

	return true
}

// ancestors is the chain of files compiling this one, this file included, each
// canonicalised so two names for one file compare equal: a cycle through a symlink
// is the cycle it is.
func (c *compiler) ancestors() []string {
	self := c.filePath
	if real, err := filepath.EvalSymlinks(c.filePath); err == nil {
		self = real
	}

	return append(slices.Clone(c.callStack), self)
}

// reportCode is [compiler.report] for a diagnostic with a code of its own.
func (c *compiler) reportCode(code v1.DiagnosticCode, span Span, r ref, format string, args ...any) {
	c.report(span, r, format, args...)
	c.diags[len(c.diags)-1].Code = code
}

// carry puts what a module declares into this file's declarations under the
// module's alias, and makes the alias's types nameable here. Nothing is carried
// unless everything is: a module that would take the file past a bound adds no
// part of itself.
func (c *compiler) carry(used usedModule, digest string, pathNode ast.Node, r ref) {
	span := spanOfNode(pathNode)
	module := used.module

	records := []*v1.Module{{Alias: used.alias, Source: used.source, SourceDigest: digest}}
	for _, inner := range module.GetModules() {
		chain := v1.ModuleChain(used.alias, inner.GetAlias())
		if strings.Count(chain, "_")+1 > v1.MaxUseDepth {
			c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
				"uses %q, whose own modules nest %d deep; modules may use one another to a depth of %d",
				used.source, strings.Count(inner.GetAlias(), "_")+2, v1.MaxUseDepth)

			return
		}
		records = append(records, &v1.Module{Alias: chain, Source: inner.GetSource(), SourceDigest: inner.GetSourceDigest()})
	}
	if len(c.uses.records)+len(records) > v1.MaxModules {
		c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
			"uses %q, which brings the modules this file records to %d; the most a workflow records is %d",
			used.source, len(c.uses.records)+len(records), v1.MaxModules)

		return
	}

	names := make(map[string]string, len(module.GetDeclaredTypes()))
	for _, d := range module.GetDeclaredTypes() {
		names[d.GetName()] = carriedName(used.alias, d.GetName())
	}

	var (
		types     []*v1.TypeDeclaration
		functions []*v1.FunctionDeclaration
		errs      []*v1.ErrorDeclaration
		weight    int
	)
	for _, d := range module.GetDeclaredTypes() {
		if d.IsScalar() && v1.IsCarried(d.GetName()) {
			// A use of a scalar was lowered to its base and rule where the module
			// that used it was compiled, so a record that is typed by one holds
			// them already, and nothing here can name it.
			continue
		}
		carried := proto.Clone(d).(*v1.TypeDeclaration)
		carried.Name = names[d.GetName()]
		carried.MustSource = nil
		weight += v1.NodeCount(carried.GetMust())
		for _, field := range carried.GetFields() {
			retypeField(field, names)
			weight += v1.NodeCount(field.GetMust())
		}
		types = append(types, carried)
	}

	set, _ := v1.NewFunctionSet(module.GetProfile(), module.GetDeclaredFunctions())
	for _, f := range module.GetDeclaredFunctions() {
		if v1.IsCarried(f.GetName()) {
			continue
		}
		body, ok := set.Inlined(f.GetName())
		if !ok {
			c.reportCode(v1.DiagnosticCodeModuleRefused, span, r, "uses %q, whose function %q could not be carried; this is a defect in the compiler, not in the module", used.source, f.GetName())

			return
		}
		carried := &v1.FunctionDeclaration{
			Name:        carriedName(used.alias, f.GetName()),
			Description: f.Description,
			Result:      proto.Clone(f.GetResult()).(*v1.Type),
			Body:        body,
		}
		renameMessages(carried.Result, names)
		for _, p := range f.GetParameters() {
			parameter := proto.Clone(p).(*v1.FunctionParameter)
			renameMessages(parameter.Type, names)
			carried.Parameters = append(carried.Parameters, parameter)
		}
		weight += v1.ExprNodeCount(body)
		functions = append(functions, carried)
	}

	for _, d := range module.GetDeclaredErrors() {
		if v1.IsCarried(d.GetName()) {
			continue
		}
		carried := proto.Clone(d).(*v1.ErrorDeclaration)
		carried.Name = carriedName(used.alias, d.GetName())
		errs = append(errs, carried)
	}

	for _, bound := range []struct {
		kind      string
		have, add int
		most      int
	}{
		{"type", len(c.uses.types), len(types), v1.MaxRecordTypes},
		{"function", len(c.uses.functions), len(functions), v1.MaxFunctions},
		{"error", len(c.uses.errors), len(errs), v1.MaxDeclaredErrors},
	} {
		if bound.have+bound.add > bound.most {
			c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
				"uses %q, which carries %d %s%s and brings this file's modules to %d; the most a workflow declares is %d",
				used.source, bound.add, bound.kind, plural(bound.add), bound.have+bound.add, bound.most)

			return
		}
	}

	// One budget with every expansion in the file: what a module declares is spent
	// where it is used, and what it weighs is spent here.
	c.expandedNodes += weight
	if c.expandedNodes > v1.MaxFunctionExpansionNodes {
		if !c.expansionOverflowed {
			c.expansionOverflowed = true
			c.reportCode(v1.DiagnosticCodeModuleRefused, span, r,
				"uses %q, whose declarations add up to %d CEL nodes with what this file already spends; the most a file expands to is %d",
				used.source, c.expandedNodes, v1.MaxFunctionExpansionNodes)
		}

		return
	}

	c.uses.records = append(c.uses.records, records...)
	c.uses.types = append(c.uses.types, types...)
	c.uses.functions = append(c.uses.functions, functions...)
	c.uses.errors = append(c.uses.errors, errs...)
	c.uses.aliases[used.alias] = used
	c.uses.order = append(c.uses.order, used.alias)

	// What this file may name: the module's own types, qualified. Not the ones it
	// carried for itself (`ids_core.Id`), which are not the module's to give away.
	for _, d := range types {
		if !strings.HasPrefix(d.GetName(), used.alias+".") {
			continue
		}
		c.typeNames[d.GetName()] = true
		if d.IsScalar() {
			c.scalarNames[d.GetName()] = true
			c.scalarTypes[d.GetName()] = d
		}
	}
}

// carriedName is the name a module's declaration has in the file that uses it: the
// alias and a dot for a declaration the module made, and the alias joined to the
// module's own alias for one the module carried from another.
func carriedName(alias, name string) string {
	if inner, bare, carried := v1.SplitQualified(name); carried {
		return v1.QualifiedName(v1.ModuleChain(alias, inner), bare)
	}

	return v1.QualifiedName(alias, name)
}

// retypeField renames the types a carried field names to the names they have here,
// and drops the call-form text the field was written with, which names functions of
// the module and means nothing in the file that uses it. Nothing evaluates it.
func retypeField(field *v1.InputDeclaration, names map[string]string) {
	renameMessages(field.GetValueType(), names)
	field.MustSource = nil
	if field.TypeSource != nil {
		if name, ok := names[field.GetTypeSource()]; ok {
			field.TypeSource = &name
		}
	}
}

// renameMessages rewrites the record names in t, in place.
func renameMessages(t *v1.Type, names map[string]string) {
	switch k := t.GetKind().(type) {
	case *v1.Type_Message:
		if name, ok := names[k.Message]; ok {
			k.Message = name
		}
	case *v1.Type_List:
		renameMessages(k.List, names)
	case *v1.Type_Map_:
		renameMessages(k.Map.GetValue(), names)
	}
}

// initTypeScope makes the maps that hold the names a file may give a type.
func (c *compiler) initTypeScope() {
	if c.typeNames != nil {
		return
	}
	c.typeNames = map[string]bool{}
	c.scalarNames = map[string]bool{}
	c.scalarTypes = map[string]*v1.TypeDeclaration{}
}

// ensureTypeEnv builds the type environment for a file whose only type names came
// from its modules; a file with a `types:` block built it already.
func (c *compiler) ensureTypeEnv() {
	if c.typeEnv != nil || len(c.typeNames) == 0 {
		return
	}
	env, err := newTypeEnv(c.typeNames)
	if err != nil {
		c.report(c.uses.span, ref{path: "use", label: "use"}, "type environment: %s", err)

		return
	}
	c.typeEnv = env
}

// carriedFunctionSet makes the set a file's expressions are expanded through when
// the file declares no functions of its own, so a module's can still be called.
func (c *compiler) carriedFunctionSet() {
	if c.functions != nil || len(c.uses.functions) == 0 {
		return
	}
	set, errs := v1.NewFunctionSet(v1.CurrentProfile, c.uses.functions)
	for _, fe := range errs {
		c.report(c.uses.span, ref{path: "use", label: "function " + fe.Function}, "%s", forAFunctionAuthor(fe.Err.Error()))
	}
	if len(set.Names()) > 0 {
		c.functions = set
	}
}

// qualifiedReference finds `alias.Name` in the text of a type.
var qualifiedReference = regexp.MustCompile(`\b([a-z][A-Za-z0-9_]*)\.([A-Z][A-Za-z0-9_]*)`)

// typeProblem is the checker's sentence about a type text, with what it cannot know
// added when the text names a module: that the module is not used here, or does not
// declare the name, and the nearest name it does declare.
func (c *compiler) typeProblem(text string, err error) string {
	message := err.Error()
	for _, m := range qualifiedReference.FindAllStringSubmatch(text, -1) {
		alias, name := m[1], m[2]
		used, ok := c.uses.aliases[alias]
		if !ok {
			if c.uses.refused[alias] {
				message += fmt.Sprintf("; `%s.%s` names module `%s`, which was refused where this file's `use:` names it", alias, name, alias)

				continue
			}
			message += fmt.Sprintf("; `%s.%s` names module `%s`, which this file does not use: add `use: {%s: {path: ...}}`", alias, name, alias, alias)

			continue
		}
		declared := used.typeNames()
		if slices.Contains(declared, name) {
			continue
		}
		message += fmt.Sprintf("; `%s.%s` is not declared by module %s (%s)", alias, name, alias, used.source)
		if suggestion, ok := nearest.Name(name, declared); ok {
			message += fmt.Sprintf("; did you mean `%s.%s`?", alias, suggestion)
		} else if len(declared) > 0 {
			message += "; it declares " + strings.Join(declared, ", ")
		}
	}

	return message
}

// checkQualifiedText is [compiler.checkQualifiedCalls] for CEL source, which is what
// a `must:` and an `allow:` predicate are stored as.
func (c *compiler) checkQualifiedText(src string, span Span, r ref) bool {
	if len(c.uses.aliases) == 0 {
		return true
	}
	value := v1.NewExpr(src)
	if value.Error() != nil {
		// Reported by the compile of the text itself.
		return true
	}

	return c.checkQualifiedCalls(value.GetExpr(), span, r)
}

// checkQualifiedCalls reports a call to `alias.name` where alias is a module this
// file uses and the module declares no such function, with the nearest one it does.
// A call to a name that is not a module of this file is the checker's to report.
func (c *compiler) checkQualifiedCalls(parsed *exprpb.ParsedExpr, span Span, r ref) bool {
	if len(c.uses.aliases) == 0 || parsed == nil {
		return true
	}

	ok := true
	walk := []*exprpb.Expr{parsed.GetExpr()}
	for len(walk) > 0 {
		e := walk[len(walk)-1]
		walk = walk[:len(walk)-1]
		if e == nil {
			continue
		}
		switch kind := e.GetExprKind().(type) {
		case *exprpb.Expr_CallExpr:
			call := kind.CallExpr
			if ident := call.GetTarget().GetIdentExpr(); ident != nil {
				if used, isAlias := c.uses.aliases[ident.GetName()]; isAlias {
					declared := used.functionNames()
					if !slices.Contains(declared, call.GetFunction()) {
						message := fmt.Sprintf("calls `%s.%s`, which module %s (%s) does not declare", ident.GetName(), call.GetFunction(), used.alias, used.source)
						if suggestion, found := nearest.Name(call.GetFunction(), declared); found {
							message += fmt.Sprintf("; did you mean `%s.%s`?", used.alias, suggestion)
						} else if len(declared) > 0 {
							message += "; it declares " + strings.Join(declared, ", ")
						}
						c.reportCode(v1.DiagnosticCodeUnresolvedReference, span, r, "%s", message)
						ok = false
					}
				}
			}
			walk = append(walk, call.GetTarget())
			walk = append(walk, call.GetArgs()...)
		case *exprpb.Expr_SelectExpr:
			walk = append(walk, kind.SelectExpr.GetOperand())
		case *exprpb.Expr_ListExpr:
			walk = append(walk, kind.ListExpr.GetElements()...)
		case *exprpb.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				walk = append(walk, entry.GetMapKey(), entry.GetValue())
			}
		case *exprpb.Expr_ComprehensionExpr:
			comp := kind.ComprehensionExpr
			walk = append(walk, comp.GetIterRange(), comp.GetAccuInit(), comp.GetLoopCondition(), comp.GetLoopStep(), comp.GetResult())
		}
	}

	return ok
}

// validateModules reports what is wrong with the modules a compiled workflow
// records, by the rule submit applies to a specification that never was a Flowfile
// ([v1.ModuleIssues]); here each lands on the alias that wrote the `use:`.
func validateModules(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics
	for _, issue := range v1.ModuleIssues(wf) {
		field := "use"
		if issue.Alias != "" && v1.IsDirectModule(issue.Alias) {
			field = fieldPath(field, issue.Alias)
		}
		ds = append(ds, Diagnostic{Field: field, Message: issue.Message, Code: v1.DiagnosticCodeModuleRefused})
	}

	return ds
}

// ownDeclarations are the declarations a file wrote itself: the ones whose names
// carry no module's alias. A carried declaration belongs to the file that declared
// it, and writing it here would declare it twice.
func ownDeclarations[T any](declared []T, name func(T) string) []T {
	return slices.DeleteFunc(slices.Clone(declared), func(d T) bool { return v1.IsCarried(name(d)) })
}

// useToYAML is the inverse of [compiler.useModules]: the `use:` block as written,
// in the order the file wrote it. Only the aliases the file wrote are written; the
// ones recorded for modules those modules use are not this file's to spell.
func useToYAML(modules []*v1.Module) yaml.MapSlice {
	var out yaml.MapSlice
	for _, m := range modules {
		if !v1.IsDirectModule(m.GetAlias()) {
			continue
		}
		out = append(out, yaml.MapItem{Key: m.GetAlias(), Value: yaml.MapSlice{{Key: "path", Value: textToYAML(m.GetSource())}}})
	}

	return out
}
