package lsp

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The `plugins:` block's mapping form, which binds a plugin's credentials once.
//
//	plugins:
//	  slack:
//	    version: v0.2.0
//	    credentials:
//	      bot_token: ${secret('env:SLACK_BOT_TOKEN')}
//
// What an editor can say about it comes from the same place the compiler reads it:
// the registry's task definitions, whose input descriptors carry each task's
// `credential` claim ([v1.TaskCredentialInputs]) and whose federated list says
// which kind of reference a credential takes. There is no second table. What the
// registry does not carry is a credential's prose description, which lives in the
// plugin's manifest and the catalog and is shown by `flow plugins`; the editor
// names the inputs a credential reaches instead.

// pluginCredential is one credential a plugin declares, as the registry shows it.
type pluginCredential struct {
	name      string
	federated bool

	// inputs are the `task.input` pairs that receive it, sorted.
	inputs []string
}

// reference is the spelling a binding for it takes.
func (c pluginCredential) reference() string {
	if c.federated {
		return "${credential('target')}"
	}

	return "${secret('env:NAME')}"
}

// pluginCredentials lists what the named plugin's registered tasks claim, sorted
// by name. A plugin with no registered task yields nothing: this process has not
// loaded it, and saying nothing is the honest answer.
func pluginCredentials(tasks *v1.Registry, plugin string) []pluginCredential {
	if tasks == nil || plugin == "" {
		return nil
	}

	byName := make(map[string]*pluginCredential)
	for _, def := range tasks.All() {
		if p, _, qualified := strings.Cut(def.Name, "."); !qualified || p != plugin || def.Inputs == nil {
			continue
		}
		claims, err := v1.TaskCredentialInputs(def)
		if err != nil {
			continue
		}
		for input, credential := range claims {
			c := byName[credential]
			if c == nil {
				c = &pluginCredential{name: credential}
				byName[credential] = c
			}
			c.federated = c.federated || v1.CredentialFederated(def, credential)
			c.inputs = append(c.inputs, def.Name+"."+input)
		}
	}

	out := make([]pluginCredential, 0, len(byName))
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		c := *byName[name]
		slices.Sort(c.inputs)
		out = append(out, c)
	}

	return out
}

// credentialDocFor renders what a credential is for an editor popup.
func credentialDocFor(plugin string, c pluginCredential) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**`%s`** · credential of the `%s` plugin", c.name, plugin)
	if c.federated {
		fmt.Fprintf(&b, "\n\nFederated: bind it to a whole `%s`, never a stored secret.", c.reference())
	} else {
		fmt.Fprintf(&b, "\n\nBind it to a whole `%s`, never a literal or an expression.", c.reference())
	}
	fmt.Fprintf(&b, "\n\nBound once here, it is the value of %s on every step of this file that does not write its own.",
		joinNames(c.inputs))

	return b.String()
}

// pluginsEntryKeys are the keys of the mapping form of a `plugins:` entry.
var pluginsEntryKeys = []struct{ name, detail, docs string }{
	{"version", "string", "The minimum semantic version of the plugin this workflow requires, written vMAJOR.MINOR.PATCH."},
	{"credentials", "map", "Binds each credential the plugin declares to a whole secret or credential reference, once for every step of the plugin."},
}

// pluginsCandidates completes keys inside the `plugins:` block's mapping form: the
// keys of an entry, and the credential names of a plugin's `credentials:`. The
// second return reports whether the path is one of those places at all.
func pluginsCandidates(path []string, prefix string, replace lsp.Range, tasks *v1.Registry) ([]lsp.CompletionItem, bool) {
	if len(path) < 2 || path[0] != "plugins" {
		return nil, false
	}

	var items []lsp.CompletionItem
	switch {
	case len(path) == 2:
		for i, k := range pluginsEntryKeys {
			if !strings.HasPrefix(k.name, prefix) {
				continue
			}
			items = append(items, lsp.CompletionItem{
				Label:         k.name,
				Kind:          lsp.CIKKeyword,
				Detail:        k.detail,
				Documentation: k.docs,
				SortText:      sortAt(i*slotSpacing, k.name),
				TextEdit:      &lsp.TextEdit{Range: replace, NewText: k.name + ": "},
			})
		}
	case len(path) == 3 && path[2] == "credentials":
		for i, c := range pluginCredentials(tasks, path[1]) {
			if !strings.HasPrefix(c.name, prefix) {
				continue
			}
			detail := "credential · " + c.reference()
			if c.federated {
				detail = "federated credential · " + c.reference()
			}
			items = append(items, lsp.CompletionItem{
				Label:         c.name,
				Kind:          lsp.CIKProperty,
				Detail:        detail,
				Documentation: plainText(credentialDocFor(path[1], c)),
				SortText:      sortAt(i*slotSpacing, c.name),
				TextEdit:      &lsp.TextEdit{Range: replace, NewText: c.name + ": "},
			})
		}
	default:
		return nil, false
	}

	return items, true
}

// hoverPluginCredential describes the credential name under the cursor when it is
// a key of a plugin's `credentials:` binding.
func hoverPluginCredential(doc *document, pos lsp.Position) *lsp.Hover {
	line := doc.index.line(pos.Line)
	scanned, ok := scanKeyLine(line)
	if !ok {
		return nil
	}
	path := keyPath(doc.index, pos.Line)
	if len(path) != 3 || path[0] != "plugins" || path[2] != "credentials" {
		return nil
	}

	rng := lsp.Range{
		Start: lsp.Position{Line: pos.Line, Character: doc.index.utf16OfByte(pos.Line, scanned.keyStart)},
		End:   lsp.Position{Line: pos.Line, Character: doc.index.utf16OfByte(pos.Line, scanned.keyEnd)},
	}
	if !contains(rng, pos) {
		return nil
	}
	for _, c := range pluginCredentials(doc.tasks, path[1]) {
		if c.name == scanned.key {
			return markdownHover(credentialDocFor(path[1], c), rng)
		}
	}

	return nil
}
