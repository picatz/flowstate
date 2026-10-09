package flowfile

import (
	"cmp"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Consolidating a repeated credential into one `plugins:` binding.
//
// A plugin task that claims a credential takes it as an input, and a Flowfile that
// calls the plugin more than once used to write the same reference on every step.
// `plugins:` can bind it once (docs/PLUGINS.md, "Binding a credential once"), and
// this rewrite moves a file from the first spelling to the second:
//
//	plugins:
//	  slack: v0.2.0
//	steps:
//	  - id: notify
//	    slack.post: {token: ${secret('env:SLACK_BOT_TOKEN')}, ...}
//	  - id: report
//	    slack.update: {token: ${secret('env:SLACK_BOT_TOKEN')}, ...}
//
// becomes
//
//	plugins:
//	  slack:
//	    version: v0.2.0
//	    credentials:
//	      bot_token: ${secret('env:SLACK_BOT_TOKEN')}
//	steps: ...with no token: on either step
//
// # What makes it safe
//
// A binding is expanded into every step that claims the credential and does not
// write it ([v1.BindPluginCredentials]), so a step whose only difference from the
// bound one is the reference it repeats compiles to exactly the specification it did
// before: the same reference on the same input. Nothing about resolution, policy or
// audit moves, because the spec the engine reads is the same one.
//
// The rewrite reads which input claims which credential from the plugin's own task
// definition, so it needs the plugin loaded (`flow fix --plugin-dir`, or a
// catalog); without one it sees no credential and changes nothing, which is the
// safe direction.
//
// # What it leaves alone
//
//   - A step that writes a different reference is an override and stays as written.
//     The reference most steps repeat becomes the binding; a tie has no such
//     reference, so nothing is bound.
//   - Fewer than two steps repeating it, because one use is not a repetition.
//   - A site it cannot remove without losing something: an input written in flow
//     style, over several lines, with a trailing comment or a comment above it, or
//     as anything other than one whole `${secret('...')}` or `${credential('...')}`.
//     That site keeps its own reference, which is the binding's value, so the file
//     still means the same.
//   - A plugin the file does not list under `plugins:` in a form this can extend
//     (the entry is written in flow style, binds the credential already, or its
//     `credentials:` is not a block mapping). The file is not guessed at; a note
//     says which.
//   - Steps in `undo:`, which this walk does not enter, and a callee's steps, which
//     belong to another document.

// wholeReferenceText matches one whole secret or credential reference written the
// way every example writes it, in single quotes. It is deliberately narrow: a
// reference with anything around it is an expression, and an expression is not
// something to hoist.
var wholeReferenceText = regexp.MustCompile(`^\$\{\s*(?:secret|credential)\('[^'"\\$\{\}]*'\)\s*\}$`)

// credentialSite is one step input that claims a credential and holds a whole
// reference.
type credentialSite struct {
	plugin     string
	credential string
	task       string
	reference  string
	line       int // the input's line, which holds its key and its value
	removable  bool
	raw        string

	// taskLine is the line of the step's task key, and sole says the input is the
	// only thing under it, on the very next line. Removing it would leave a bare
	// `task:`, which reads as unfinished, so the task is written `task: {}` instead.
	taskLine int
	sole     bool
}

// credentialSite records the credential inputs of one plugin task step. It reads
// only; nothing is decided until the whole document has been seen.
func (f *fixer) collectCredentialSites(name string, entry *ast.MappingValueNode) {
	plugin, _, qualified := strings.Cut(name, ".")
	if !qualified || plugin == "" {
		return
	}
	def, known := v1.LookupTask(name)
	if !known || def.Inputs == nil {
		return
	}
	claims, err := v1.TaskCredentialInputs(def)
	if err != nil || len(claims) == 0 {
		return
	}
	block, ok := unwrapAnchor(entry.Value).(*ast.MappingNode)
	if !ok || block.IsFlowStyle {
		return
	}
	for _, input := range block.Values {
		inputName, named := keyNameOf(input.Key)
		credential, claimed := claims[inputName]
		if !named || !claimed {
			continue
		}
		value, isString := input.Value.(*ast.StringNode)
		if !isString || !wholeReferenceText.MatchString(value.Value) {
			continue
		}
		keySpan := spanOfNode(input.Key)
		valueSpan := spanOfNode(value)
		if !keySpan.IsValid() || !valueSpan.IsValid() || keySpan.Start.Line != valueSpan.Start.Line || valueSpan.End.Line != valueSpan.Start.Line {
			continue
		}

		line := f.line(keySpan.Start.Line)
		if valueSpan.Start.Column < 1 || valueSpan.Start.Column > len(line) {
			continue
		}
		raw := strings.TrimSpace(line[valueSpan.Start.Column-1:])
		site := credentialSite{
			plugin:     plugin,
			credential: credential,
			task:       name,
			reference:  value.Value,
			line:       keySpan.Start.Line,
			raw:        raw,
			taskLine:   spanOfNode(entry.Key).Start.Line,
		}
		if taskLine := strings.TrimRight(f.line(site.taskLine), " "); len(block.Values) == 1 && site.taskLine+1 == site.line && strings.HasSuffix(taskLine, ":") {
			site.sole = true
		}
		// The whole rest of the line is the reference, bare or in double quotes: a
		// trailing comment or a quoting this does not reproduce stays where it is.
		// A comment on the line above is the author's note about this input and
		// would be left describing the line below it.
		above := strings.TrimSpace(f.line(keySpan.Start.Line - 1))
		site.removable = (raw == value.Value || raw == `"`+value.Value+`"`) && !strings.HasPrefix(above, "#")
		f.credentialSites = append(f.credentialSites, site)
	}
}

// consolidateCredentials binds each credential that several steps of a plugin
// repeat once, in the plugin's `plugins:` entry, and removes the repetitions.
func (f *fixer) consolidateCredentials(workflow *ast.MappingNode) {
	if len(f.credentialSites) == 0 {
		return
	}

	// Plugin, then credential, so the result is the same on every run.
	type key struct{ plugin, credential string }
	groups := make(map[key][]credentialSite)
	for _, site := range f.credentialSites {
		k := key{site.plugin, site.credential}
		groups[k] = append(groups[k], site)
	}
	keys := slices.SortedFunc(maps.Keys(groups), func(a, b key) int {
		return cmp.Or(cmp.Compare(a.plugin, b.plugin), cmp.Compare(a.credential, b.credential))
	})

	type binding struct {
		credential string
		raw        string
		removed    []credentialSite
	}
	byPlugin := make(map[string][]binding)
	var plugins []string
	for _, k := range keys {
		sites := groups[k]

		counts := make(map[string]int)
		for _, site := range sites {
			counts[site.reference]++
		}
		chosen, best, tied := "", 0, false
		for _, reference := range slices.Sorted(maps.Keys(counts)) {
			switch n := counts[reference]; {
			case n > best:
				chosen, best, tied = reference, n, false
			case n == best:
				tied = true
			}
		}
		if tied || best < 2 {
			continue
		}

		var removed []credentialSite
		raw := ""
		for _, site := range sites {
			if site.reference == chosen && site.removable && !f.covered(site.line) {
				removed = append(removed, site)
				raw = cmp.Or(raw, site.raw)
			}
		}
		if len(removed) < 2 {
			continue
		}
		if len(byPlugin[k.plugin]) == 0 {
			plugins = append(plugins, k.plugin)
		}
		byPlugin[k.plugin] = append(byPlugin[k.plugin], binding{credential: k.credential, raw: raw, removed: removed})
	}

	for _, plugin := range plugins {
		bindings := byPlugin[plugin]
		entries := make([][2]string, len(bindings))
		for i, b := range bindings {
			entries[i] = [2]string{b.credential, b.raw}
		}

		bound, ok := f.bindInPlugins(workflow, plugin, entries)
		if !ok {
			continue
		}
		for _, b := range bindings {
			if !slices.Contains(bound, b.credential) {
				continue
			}
			for _, site := range b.removed {
				if site.sole {
					f.record(site.taskLine, site.line, []string{strings.TrimRight(f.line(site.taskLine), " ") + " {}"},
						fmt.Sprintf("%s: `%s:` removed, its reference now bound once as the %s plugin's %s credential", site.task, f.inputAt(site), plugin, b.credential),
						fmt.Sprintf("%s: `%s:` would be removed, its reference bound once as the %s plugin's %s credential", site.task, f.inputAt(site), plugin, b.credential))

					continue
				}
				f.record(site.line, site.line, nil,
					fmt.Sprintf("%s: `%s:` removed, its reference now bound once as the %s plugin's %s credential", site.task, f.inputAt(site), plugin, b.credential),
					fmt.Sprintf("%s: `%s:` would be removed, its reference bound once as the %s plugin's %s credential", site.task, f.inputAt(site), plugin, b.credential))
			}
		}
	}
}

// inputAt is the name of the input written on a site's line.
func (f *fixer) inputAt(site credentialSite) string {
	name, _, _ := strings.Cut(strings.TrimSpace(f.line(site.line)), ":")

	return strings.Trim(name, `"'`)
}

// covered reports whether an edit already consumes a line.
func (f *fixer) covered(line int) bool {
	for start, edit := range f.edits {
		if start <= line && line <= edit.through {
			return true
		}
	}

	return false
}

// bindInPlugins writes the bindings into the plugin's entry of the top-level
// `plugins:` block and reports which credentials it bound. It binds nothing, and
// says why in a note, when the entry is not one it can extend without guessing.
func (f *fixer) bindInPlugins(workflow *ast.MappingNode, plugin string, entries [][2]string) ([]string, bool) {
	var pluginsKey *ast.MappingValueNode
	for _, v := range workflow.Values {
		if name, ok := keyNameOf(v.Key); ok && name == "plugins" {
			pluginsKey = v
		}
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e[0]
	}
	skip := func(why string) ([]string, bool) {
		line, column := 1, 1
		if pluginsKey != nil {
			span := spanOfNode(pluginsKey.Key)
			line, column = span.Start.Line, span.Start.Column
		}
		f.note(line, column, "the %s plugin's %s credential is repeated on several steps but not consolidated: %s", plugin, strings.Join(names, ", "), why)

		return nil, false
	}
	if pluginsKey == nil {
		return skip("the file has no `plugins:` entry to bind it in; list the plugin with its version and run this again")
	}
	block, ok := pluginsKey.Value.(*ast.MappingNode)
	if !ok || block.IsFlowStyle {
		return skip("`plugins:` is not a block mapping")
	}
	var entry *ast.MappingValueNode
	for _, v := range block.Values {
		if name, ok := keyNameOf(v.Key); ok && name == plugin {
			entry = v
		}
	}
	if entry == nil {
		return skip("the plugin is not listed under `plugins:`; list it with its version and run this again")
	}

	pluginsSpan, keySpan := spanOfNode(pluginsKey.Key), spanOfNode(entry.Key)
	if !pluginsSpan.IsValid() || !keySpan.IsValid() {
		return nil, false
	}
	keyIndent := keySpan.Start.Column - 1
	step := keyIndent - (pluginsSpan.Start.Column - 1)
	if step <= 0 {
		step = 2
	}
	bindingLines := func(indent int, withHeader bool) []string {
		var lines []string
		if withHeader {
			lines = append(lines, strings.Repeat(" ", indent)+"credentials:")
			indent += step
		}
		for _, e := range entries {
			lines = append(lines, strings.Repeat(" ", indent)+e[0]+": "+e[1])
		}

		return lines
	}
	message := fmt.Sprintf("`plugins:` binds the %s plugin's %s once, replacing the reference repeated on its steps", plugin, strings.Join(names, ", "))
	pending := fmt.Sprintf("`plugins:` would bind the %s plugin's %s once, replacing the reference repeated on its steps", plugin, strings.Join(names, ", "))

	switch value := entry.Value.(type) {
	case *ast.StringNode:
		// `slack: v0.2.0`: the scalar becomes a mapping that keeps the version.
		valueSpan := spanOfNode(value)
		if keySpan.Start.Line != valueSpan.Start.Line || valueSpan.End.Line != keySpan.Start.Line || f.covered(keySpan.Start.Line) {
			return skip("its `plugins:` entry is not one line")
		}
		line := f.line(keySpan.Start.Line)
		if valueSpan.Start.Column < 1 || valueSpan.Start.Column > len(line) {
			return nil, false
		}
		child := keyIndent + step
		replacement := []string{
			strings.TrimRight(line[:valueSpan.Start.Column-1], " "),
			strings.Repeat(" ", child) + "version: " + strings.TrimSpace(line[valueSpan.Start.Column-1:]),
		}
		replacement = append(replacement, bindingLines(child, true)...)
		f.record(keySpan.Start.Line, keySpan.Start.Line, replacement, message, pending)
	case *ast.MappingNode:
		if value.IsFlowStyle || len(value.Values) == 0 {
			return skip("its `plugins:` entry is written in flow style")
		}
		first := spanOfNode(value.Values[0].Key)
		child := first.Start.Column - 1
		var credentials *ast.MappingValueNode
		for _, v := range value.Values {
			if name, ok := keyNameOf(v.Key); ok && name == "credentials" {
				credentials = v
			}
		}
		if credentials == nil {
			through := f.blockEnd(keySpan.Start.Line, keyIndent)
			if f.covered(through) {
				return nil, false
			}
			replacement := append([]string{f.line(through)}, bindingLines(child, true)...)
			f.record(through, through, replacement, message, pending)

			break
		}
		existing, ok := credentials.Value.(*ast.MappingNode)
		if !ok || existing.IsFlowStyle || len(existing.Values) == 0 {
			return skip("its `credentials:` is not a block mapping")
		}
		for _, v := range existing.Values {
			if name, ok := keyNameOf(v.Key); ok && slices.Contains(names, name) {
				return skip("it already binds one of them")
			}
		}
		credSpan := spanOfNode(credentials.Key)
		through := f.blockEnd(credSpan.Start.Line, credSpan.Start.Column-1)
		if f.covered(through) {
			return nil, false
		}
		indent := spanOfNode(existing.Values[0].Key).Start.Column - 1
		replacement := append([]string{f.line(through)}, bindingLines(indent, false)...)
		f.record(through, through, replacement, message, pending)
	default:
		return skip("its `plugins:` entry is not a version or a mapping")
	}

	return names, true
}
