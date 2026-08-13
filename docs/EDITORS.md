# Editor setup

Flowstate ships a language server for the `Flowfile` DSL. It gives you diagnostics
as you type, hover documentation generated from the task registry, and completion
that only ever offers things the engine will accept.

## What the server provides

| Feature | What you get |
| --- | --- |
| **Diagnostics** | YAML syntax errors, CEL syntax errors underlined inside the expression, unknown tasks, duplicate and unusable step ids, references to steps that do not exist or have not run yet, inputs a task does not declare (with a spelling suggestion), required inputs left out, an input a task used to accept and no longer does — reported as a key that can be deleted rather than as a misspelling of something else, malformed step timeouts and retry intervals, a `log:` message that interpolates an input declared `sensitive:` — the `sensitive-in-log` lint, which names what to log instead — a step that is no kind of work or more than one, a step named the retired bare way rather than under `steps.` — reported as a migration with the command that performs it, not as an unknown name — and an `edition:` this build does not compile, which is reported on its own since every other complaint would be describing the wrong grammar. |
| **Hover** | A task's summary and full typed signature; an input's type, whether it is required, and the value constraints the schema enforces; what a `${steps.<id>.<output>}` reference resolves to, what type it produces when the registry declares one (an output shaped by the step's own `outputs:` has no type to claim), and which line declared it; what the root `steps` itself is; what a loop's iterator binds; what `now` is inside a wait's expressions (`wait_until:`, an expression-valued `sleep:`, a signal's `timeout:`) and why it is bound only there; what a `${secret('scheme:name')}` reference names; what each `Flowfile` key means. |
| **Completion** | Task names where a step's keys go, alongside `id`/`if`/`timeout` and the other kinds; input keys under the task's own name, required ones first, already-written ones omitted; the names in scope inside `${...}` (see the scoping rules below); and the document's own keys (`id`, `if`, `timeout`, `retry`, `for_each`, `parallel`, …). |
| **Go to definition** | Jump from a `${steps.<id>.<output>}` reference to that step's `id:` declaration, from a loop's bare iterator name to the loop that binds it, and from a `call:` target to the called Flowfile — opened at its `name:`, and resolved relative to the calling file's own directory by the same rule the compiler uses, so the file you arrive in is the file the run compiles. A call the compiler would refuse, or one naming a file that is not there, navigates nowhere rather than somewhere wrong. |
| **Document symbols** | An outline of the workflow's steps, each labelled with the task it runs, and for a nested step the block it belongs to. |
| **Formatting** | Rewrites the whole document into the form `flow fmt` and `flowfile.Format` write. Comments are kept, carried onto the rewritten document at the key, value or list entry they were written against; whitespace is not, so a blank line, a mapping's key order, and a string literal's quote style are all normalized away. A document that does not compile draws no edit at all, never a partial or guessed one, and neither does one carrying a comment the rewrite cannot keep. Because of the rewrite, this is opt-in in most editors' configuration rather than run on every save; see the per-editor notes below for how to bind it deliberately. |
| **Code actions** | The migration `flow fix` performs, offered from the editor. Two kinds of the same thing: a `source.fixAll` action titled *Migrate to edition …*, and a `quickfix` on each line the migration rewrites, so it is reachable from the diagnostic that told you to run the command. Both carry one whole-document edit holding exactly what `flow fix` writes — comments and untouched lines copied through byte for byte, unlike formatting. A document already in the current edition, one that does not parse, and one where the rewriter *refuses* — a `task:` written in flow style, a binding through an alias it cannot resolve — each draw no action at all, because the only edit that could be offered there is the guess `flow fix` declined to make. |

Everything above is read from the task registry and the Protobuf schema at the
moment you ask for it, so a task added to the engine shows up in your editor with
no change to the language server.

The rules about what a workflow may *say* — step ids, unknown tasks, references that
cannot resolve, inputs a task does not declare, literals of the wrong type,
durations, step structure — all come from the same validator `flow validate` uses.
The language server's contribution there is position: the same message, under the
token at fault. A misspelled input underlines the key, because the key is what is
wrong; a literal of the wrong type underlines the value. Which of the two is at
fault is decided from the schema, not from the wording of the message.

All of it also works inside a `for_each` body, a `loop:` body, and a `parallel`
branch, not only at the top level.

### Completion inside an expression has three levels

A reference has a root, so completing one is a walk rather than a single list, and
each level is answered from a different place:

1. **Bare, at the start of an expression.** The names bound where the cursor
   stands, then `steps` (and `vars`, when the file declares any), then the
   profile's CEL library functions — which dominate the list by count, which is
   why bindings come first: they are the nearer thing, and inside a loop the
   item is usually what is wanted. The binding depends on where the cursor is:
   inside a `for_each` body it is the iterator; inside a `loop:`'s
   `until:`/`update:` it is the carried state under its `as:` name; inside a
   wait's own expressions (`wait_until:`, an expression-valued `sleep:`, a
   signal's `timeout:`) it is `now`; and inside a `wait_for_signal:`'s
   `outputs:` shaping it is `payload`, `sender`, and `timed_out`, with `now`
   still bound because the shaping is evaluated in the wait's own scope.
2. **After `steps.`** — the ids of the steps whose outputs exist at that point,
   labelled with what each one runs: the task's name, or `for_each`, `loop` or
   `parallel` for a block.
3. **After `steps.<id>.`** — that step's real output names. Declared outputs are
   read from the registry with their types; a step whose `outputs:` shapes them
   gets the shaped names read from the document instead, marked `shaped output`
   and deliberately typeless — their shape is the author's expression, which the
   registry does not describe. Past the output nothing is offered, because the
   shape inside a value is not something the schema describes and a guess would
   be a wrong one.

Two things follow from the split that are easy to miss. `now` is offered inside a
wait's expressions and nowhere else: rightly not in a task input, where the
validator refuses it because a task input is resolved inside an activity that has
no clock surviving a retry. The set matches what validates: all three of a wait's
expressions (`wait_until:`, a computed `sleep:`, and a signal's `timeout:`)
plus the signal's `outputs:` shaping, which is evaluated in the wait's own scope.
Completion and hover used to stop at `wait_until:`, which was
[#319](https://github.com/picatz/flowstate/issues/319).

And a bare qualifier gets nothing: `${item.` could only be a binding, whose element
type is not statically known, or a step reference written the retired way — and
offering that step's outputs would keep an author writing a form `flow validate`
refuses. The diagnostic on it names the step and says to run `flow fix`; hover stays
silent for the same reason, rather than saying it twice in a smaller box.

### Scoping inside loops and parallel blocks

Completion, hover, and go-to-definition follow the engine's scoping rules, so a
name the editor offers is always one the workflow can resolve:

- Inside a `for_each` body, the current item is in scope under the loop's
  `iterator:` name (`item` by default), written bare because it is a binding rather
  than a step; earlier body steps are in scope within the iteration, under `steps.`.
  The two are separate namespaces, so an iterator may share a step's id and the
  editor still offers both.
- A loop body's step outputs **do not** escape the loop. After the block, only the
  loop's own id is referenceable, and it reports `results` — one entry per
  iteration, so `${steps.<loop>.results}` — plus, for a `loop:` that carries
  state, `state`, the final carried value. Body step ids are not offered to later
  steps.
- A `loop:`'s carried state (its `as:` name) is bound in three scopes, not one:
  `init:` evaluates in the enclosing scope, before the loop, so it cannot read
  the state it defines; the body reads the state bare; and `until:`/`update:`
  run after the body, so they see the body's own step outputs as well as the
  state. [DSL.md](DSL.md) carries the full story.
- A `parallel` block's branch outputs **do** merge into the enclosing scope once
  the block joins, so a later step can reference them as `${steps.<branch_step>.…}`.
  One branch cannot reference a sibling's, because branches are unordered.
- A step cannot reference the block that contains it, since that block has not
  finished while the step runs.

Each diagnostic carries a stable code you can filter on: `yaml-syntax`,
`cel-syntax`, `document-too-large`, and `flowfile` for everything the shared
validator reports — the same problems `flow validate` prints, with the same wording,
positioned onto the token at fault. Most of what you will see is `flowfile`, and that
is the point: the editor and the command line cannot disagree about a file.

The two syntax codes are the exceptions, and they earn it: a document that will not
parse has no model for the validator to run against, so there is nothing for it to
report and no risk of two answers. Everywhere else, the rule has one home and this
server only improves the position — including where the validator names an element
of a list it has no coordinates for, so the squiggle lands on the element rather
than on the whole value. One known gap: errors in a `wait_for_signal:`'s
`outputs:` shaping are positioned on the step id by the validator itself rather
than on the expression at fault
([#318](https://github.com/picatz/flowstate/issues/318)).

Not implemented, and deliberately not advertised: rename, references, and workspace
symbols. The server's own expression pass is parse-only — it adds no type
judgments of its own, because a step's output types are not statically known for
every task, and a wrong squiggle under working code is worse than no squiggle.
The shared validator's diagnostics are published as-is, though, and those do
include type-checking against the types the profile declares — so a real CEL
type error, like "found no matching overload for '_+_' applied to
'(int, string)'", squiggles in the editor with exactly the wording
`flow validate` prints.

Secret references are described on hover but **not** offered by completion, and the
reason is narrower than it once was: a secret is consumed for real now — the `http`
task's `bearer:` takes a reference whole and the worker resolves it inside the
activity — so a `${secret('env:API_KEY')}` marker in the one place built to receive
one runs rather than failing. What completion would still have to know is *where* it
may be offered, and that is the hard half: a reference has to be the whole value of a
task input the schema declares as taking one, and every other position — a header
entry, an `if:`, a loop's `items` — is a compile error the validator explains at
length. A completion list that cannot make that distinction offers the refusal as
often as the working form, so it stays unoffered until it can. Hover reads the
reference through the same parser the compiler uses, so it cannot describe a form a
worker would refuse, and it names the scheme rather than a backend: which provider
serves `vault` is a deployment's choice, made worker-side.
Misplaced references — combined into a larger expression, or used in `if` or
`for_each.items`, where resolving them would put the secret into workflow history —
are reported by the validator with its own explanation.

Inputs a task evaluates itself are not reference-checked, because they resolve
against a scope the document does not model: the `http` task's `expect` and `outputs`
expressions see the response under `response.` — `response.status_code`,
`response.headers`, `response.body`, and, when the step asked for `parse_json`,
`response.json` — none of which exist until the request has been made. That root is
bound by the task rather than by the workflow's scope, which is why a step's ordinary
inputs cannot see it: there is no response yet when those are resolved. `steps.` is
reachable alongside it, so a shaping expression can combine the response with an
earlier step's output. Which inputs are deferred this way comes from the task's own
definition, so this cannot go stale. `outputs` is written as a mapping of name to
value — one name per line, the same shape a `wait_for_signal:` shapes its result in
— and because the names are written down, the editor completes them after
`steps.<id>.` and reports a reference to one the step does not produce.

## Install the binary

```console
$ go install github.com/picatz/flowstate/cmd/flow@latest
```

That puts `flow` in `$(go env GOPATH)/bin`. Confirm it is on your `PATH`:

```console
$ flow lsp --help
```

Or build from a checkout:

```console
$ go build -o /usr/local/bin/flow ./cmd/flow
```

The server speaks the Language Server Protocol over stdin and stdout, so the
command every editor needs is just `flow lsp`. It takes no arguments unless you
have plugins, which is the next section. Logs go to stderr; document contents are
never logged, since a Flowfile input can hold a credential.

## Which files are Flowfiles

A Flowfile is YAML, so no editor detects one on extension alone. Point the client
at the files you actually use. The configurations below match, in order of how
specific they are:

- a file literally named `Flowfile` or `Flowfile.yaml`
- `workflow.yaml` or `workflow.yml`
- anything under a `workflows/` directory

Adjust these to your layout. Pointing the server at every `*.yaml` in a repository
works, but you will get Flowfile diagnostics on your Kubernetes manifests.

## Plugin tasks: `flow lsp --plugin-dir`

A plugin's tasks are named with a dot — `slack.post` is the `post` task of the
plugin discovered as `flowstate-plugin-slack` — and by default your editor has
never heard of one. It underlines the name, lists the tasks it does know, and the
worker that runs the file executes the step perfectly. That disagreement is real
and the server is being honest about it: a process that has launched no plugins
cannot know what they provide, so `flow validate` in a terminal says the same
thing. The diagnostic says as much rather than calling it a typo — it names
installation, not spelling, because it genuinely cannot tell the two apart.

Pass `--plugin-dir` and it can:

```console
$ flow lsp --plugin-dir /usr/local/lib/flowstate/plugins
```

The flag is the same one `flow worker`, `flow server` and `flow plugins` take,
doing the same thing through the same discovery: the directory is searched for
executables named `flowstate-plugin-<name>`, each is launched, and what it
advertises is registered before the server answers its first request. From then
on a plugin's task is a task — completion offers it where a step's keys go, hover
shows the signature built from the descriptors the plugin shipped, its inputs are
checked against them, and the unknown-task diagnostic goes away. Point it at the
same directory your workers use and the editor and the worker agree.

`--plugin`, `--plugin-scheme` and `--allow-insecure-plugin-dir` are accepted too,
with the meanings [CLI.md](CLI.md) gives them. A plugin that will not start —
including one you pinned with `--plugin` that is not installed — fails the command
at startup rather than leaving you with a server that quietly knows less than you
think it does. If your editor reports that the language server exited immediately
after you added the flag, run the same command line in a terminal and it will tell
you which plugin and why.

**It is opt-in, and the command line is the only way in.** Launching plugins means
executing binaries, and there are two ways that could have happened by itself,
both refused:

- **Not per request.** Plugins start once, at server startup. Checking a file must
  never launch a process, because an editor asks this server a question on every
  keystroke.
- **Not from the workspace.** No setting in a repository, and no LSP configuration
  request, reaches this. A project you cloned to read must not be able to decide
  what your editor executes.

What is left is you, typing a flag for your own machine, in the editor
configuration that starts the server. So that is where it goes — in the same place
as the `lsp` argument, not in a project file:

| Editor | Where |
| --- | --- |
| Neovim | `cmd = { 'flow', 'lsp', '--plugin-dir', '/path/to/plugins' }` |
| VS Code | `args: ['lsp', '--plugin-dir', '/path/to/plugins']` in the extension's server options |
| Helix | `args = ["lsp", "--plugin-dir", "/path/to/plugins"]` under `[language-server.flowstate]` |
| Zed | `"arguments": ["lsp", "--plugin-dir", "/path/to/plugins"]` in the `binary` block |
| Emacs | `'(flowfile-mode . ("flow" "lsp" "--plugin-dir" "/path/to/plugins"))` |

`$FLOWSTATE_PLUGIN_DIR` is read when no `--plugin-dir` is given, the same as for a
worker — which is a convenience for a machine where every Flowstate command should
see the same plugins, and still an operator's decision about their own environment
rather than a repository's about yours.

## Neovim

### Without a plugin (Neovim 0.11+)

`vim.lsp.config` and `vim.lsp.enable` are built in — no `nvim-lspconfig` needed.
Put this in `init.lua`:

```lua
vim.lsp.config.flowstate = {
  cmd = { 'flow', 'lsp' },
  filetypes = { 'flowfile' },
  root_markers = { 'go.mod', '.git' },
}

vim.lsp.enable('flowstate')

-- Give Flowfiles their own filetype so the server is not attached to every YAML
-- file. The pattern is what decides where you get diagnostics.
vim.filetype.add({
  filename = {
    ['Flowfile'] = 'flowfile',
    ['Flowfile.yaml'] = 'flowfile',
    ['workflow.yaml'] = 'flowfile',
    ['workflow.yml'] = 'flowfile',
  },
  pattern = {
    ['.*/workflows/.*%.ya?ml'] = 'flowfile',
  },
})

-- Flowfiles are YAML, so keep YAML's indentation and comment rules.
vim.api.nvim_create_autocmd('FileType', {
  pattern = 'flowfile',
  callback = function()
    vim.bo.commentstring = '# %s'
    vim.bo.expandtab = true
    vim.bo.shiftwidth = 2
  end,
})
```

Neovim 0.11 already binds `K` to hover and `gO` to document symbols when a server
attaches, and `<C-]>` reaches definition through `tagfunc`. Add anything else you
want, and turn completion on — it is off by default:

```lua
vim.api.nvim_create_autocmd('LspAttach', {
  callback = function(args)
    local opts = { buffer = args.buf }
    vim.keymap.set('n', 'gd', vim.lsp.buf.definition, opts)
    vim.keymap.set('n', 'gO', vim.lsp.buf.document_symbol, opts)
    -- Completion is not automatic unless you ask for it.
    vim.lsp.completion.enable(true, args.data.client_id, args.buf, { autotrigger = true })
  end,
})
```

### With nvim-lspconfig

```lua
local configs = require('lspconfig.configs')
local util = require('lspconfig.util')

configs.flowstate = {
  default_config = {
    cmd = { 'flow', 'lsp' },
    filetypes = { 'flowfile' },
    root_dir = util.root_pattern('go.mod', '.git'),
    single_file_support = true,
  },
}

require('lspconfig').flowstate.setup({})
```

You still need the `vim.filetype.add` block above; `nvim-lspconfig` decides *how*
to start the server, not which files are Flowfiles.

### Attaching to a single file, no config

Useful for trying it out:

```vim
:lua vim.lsp.start({ name = 'flowstate', cmd = { 'flow', 'lsp' } })
```

## Visual Studio Code

There is no published extension. A minimal one is about twenty lines.

Create a folder, then `package.json`:

```json
{
  "name": "flowstate",
  "displayName": "Flowstate Flowfile",
  "version": "0.0.1",
  "engines": { "vscode": "^1.75.0" },
  "categories": ["Programming Languages"],
  "activationEvents": ["onLanguage:flowfile"],
  "main": "./extension.js",
  "contributes": {
    "languages": [
      {
        "id": "flowfile",
        "aliases": ["Flowfile"],
        "filenames": ["Flowfile", "Flowfile.yaml"],
        "filenamePatterns": ["**/workflow.yaml", "**/workflow.yml", "**/workflows/*.yaml"],
        "configuration": "./language-configuration.json"
      }
    ],
    "grammars": [
      {
        "language": "flowfile",
        "scopeName": "source.yaml",
        "path": "./syntaxes/empty.tmLanguage.json"
      }
    ]
  },
  "dependencies": { "vscode-languageclient": "^9.0.1" }
}
```

`language-configuration.json`, so comments and indentation behave like YAML:

```json
{
  "comments": { "lineComment": "#" },
  "brackets": [["{", "}"], ["[", "]"]],
  "indentationRules": {
    "increaseIndentPattern": "^\\s*[-\\w\"']+\\s*:\\s*$",
    "decreaseIndentPattern": "^\\s+\\}"
  }
}
```

`extension.js`:

```javascript
const { LanguageClient, TransportKind } = require('vscode-languageclient/node');

let client;

function activate(context) {
  client = new LanguageClient(
    'flowstate',
    'Flowstate Flowfile',
    // `flow` must be on the PATH, or give an absolute path here.
    { command: 'flow', args: ['lsp'], transport: TransportKind.stdio },
    { documentSelector: [{ scheme: 'file', language: 'flowfile' }] },
  );
  context.subscriptions.push(client);
  client.start();
}

function deactivate() {
  return client && client.stop();
}

module.exports = { activate, deactivate };
```

Then:

```console
$ npm install
$ code --extensionDevelopmentPath="$PWD" .
```

Package it with `npx @vscode/vsce package` and install the `.vsix` when you are
happy with it.

To reuse YAML's syntax highlighting rather than shipping a grammar, drop the
`grammars` block and instead map the language to YAML's tokenizer in your settings:

```json
{
  "files.associations": { "**/workflow.yaml": "flowfile" }
}
```

### Without writing an extension

Install a generic LSP client extension and configure it to run `flow lsp` for
your Flowfile pattern. The exact settings key depends on the extension, but every
one of them needs the same three things: the command `flow`, the argument `lsp`,
and stdio transport.

## Helix

Add to `~/.config/helix/languages.toml`:

```toml
[language-server.flowstate]
command = "flow"
args = ["lsp"]

[[language]]
name = "flowfile"
scope = "source.flowfile"
injection-regex = "flowfile"
file-types = [
  { glob = "Flowfile" },
  { glob = "Flowfile.yaml" },
  { glob = "workflow.yaml" },
  { glob = "workflow.yml" },
  { glob = "workflows/*.yaml" },
]
language-servers = ["flowstate"]
comment-token = "#"
indent = { tab-width = 2, unit = "  " }
grammar = "yaml"
```

Check it was picked up:

```console
$ hx --health flowfile
```

Helix binds `gd` to definition, `K` to hover, and `<space>s` to document symbols
out of the box.

## Zed

Zed needs an extension to register a language server, but you can point it at the
binary through an existing YAML setup by adding to `settings.json`:

```json
{
  "languages": {
    "YAML": {
      "language_servers": ["flowstate", "..."]
    }
  },
  "lsp": {
    "flowstate": {
      "binary": { "path": "flow", "arguments": ["lsp"] }
    }
  }
}
```

This attaches the server to all YAML files, so expect Flowfile diagnostics
elsewhere until a proper Flowfile extension exists.

## Emacs (eglot)

```elisp
(define-derived-mode flowfile-mode yaml-mode "Flowfile")

(add-to-list 'auto-mode-alist '("/Flowfile\\'" . flowfile-mode))
(add-to-list 'auto-mode-alist '("/workflow\\.ya?ml\\'" . flowfile-mode))
(add-to-list 'auto-mode-alist '("/workflows/.*\\.ya?ml\\'" . flowfile-mode))

(with-eval-after-load 'eglot
  (add-to-list 'eglot-server-programs
               '(flowfile-mode . ("flow" "lsp"))))

(add-hook 'flowfile-mode-hook #'eglot-ensure)
```

## Checking it works

Open a Flowfile and try each of these:

1. **Diagnostics.** Change a task name to something that does not exist. You should
   get an error under the name listing the tasks that do exist.
2. **Completion.** Type `message: ${` in a step after the first. You should be
   offered `steps` and, after it, the CEL library functions — plus the iterator,
   if you are inside a `for_each` body, or a `loop:`'s carried state inside its
   `until:`/`update:`. Accept `steps` and only the ids of steps *above* the
   cursor are offered; add a `.` after one and you get that step's real output
   names — typed when the registry declares them, typeless when the step's own
   `outputs:` shaped them.
3. **Hover.** Put the cursor on a task name. You should see its summary and a table
   of typed inputs and outputs, with required inputs marked.
4. **Go to definition.** With the cursor on a `${steps.<id>.<output>}` reference,
   jump to that step's `id:`.
5. **Outline.** Open your editor's symbol list. Each step appears, labelled with
   its task.
6. **Formatting.** Run your editor's "Format Document" command on a file with a
   comment or two in it. The comments disappear and the document comes back as
   `flow fmt` would write it — that is the whole-document rewrite the capability
   table describes, not a bug. Because of that rewrite, none of the editor
   configurations above bind this to format-on-save; invoke it deliberately
   (`:lua vim.lsp.buf.format()` in Neovim, `:format` in Helix, the Command
   Palette's "Format Document" in VS Code, `M-x eglot-format-buffer` in Emacs) and
   review the diff before committing it, the same as running `flow fmt` from the
   command line.
7. **Code actions.** Open a Flowfile written in an older edition — one this build
   refuses, with a diagnostic saying to run `flow fix` — and ask your editor for
   the actions at the underlined line (`vim.lsp.buf.code_action()` in Neovim,
   `<space>a` in Helix, the lightbulb or `Ctrl+.` in VS Code, `M-x
   eglot-code-actions` in Emacs). You should be offered *Migrate to edition …*
   plus a line-level entry naming the change under the cursor. Applying either one
   rewrites the buffer to exactly what `flow fix` writes, comments and all.

   The `source.fixAll` action is the one editors bind to fix-on-save
   (`editor.codeActionsOnSave` in VS Code, `lsp-format`/`code_action` hooks
   elsewhere). None of the configurations above turn that on, and the reason is
   not that the rewrite is unsafe — `flow fix` refuses rather than guesses, which
   is why an editor that cannot be sure offers nothing. It is that a migration is
   a thing people read in review. The command changes what a file *says* it is
   written in and moves an author's steps around to say the same thing in the
   current grammar, and the moment to see that is when it happens, not in a
   `git diff` after a save you were not thinking about. Invoke it deliberately,
   read the diff, commit it on its own.

If nothing happens, check that the server starts and answers at all. The `sleep`
matters: the server exits as soon as its input closes, so a plain heredoc ends the
process before it has written the reply.

```console
$ body='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
$ { printf 'Content-Length: %d\r\n\r\n%s' "${#body}" "$body"; sleep 1; } | flow lsp
Content-Length: 383

{"id":1,"result":{"capabilities":{"textDocumentSync":{"openClose":true,"change":1,"save":{"includeText":true}},"hoverProvider":true,"completionProvider":{"triggerCharacters":[":"," ",".","{","[",",","-"]},"definitionProvider":true,"documentSymbolProvider":true,"documentFormattingProvider":true,"codeActionProvider":{"codeActionKinds":["quickfix","source.fixAll"]}}},"jsonrpc":"2.0"}
```

If you get that response, the server is fine and the problem is in the editor's
file matching — it is only attached to the filetypes your configuration names.
Check what filetype your editor thinks the buffer is (`:set ft?` in Vim,
`:lang-info` in Helix, the language indicator in VS Code's status bar).

The same checks are available without an editor:

```console
$ flow validate examples/*/workflow.yaml
$ flow tasks
```

`flow validate` runs the same semantic validation the language server reports, so
if a file is clean there and not in your editor, the editor is looking at a
different file than you think.

## Agents

An AI agent editing Flowfiles is not an editor client, and pointing one at
`flow lsp` is the wrong shape: LSP answers questions about a buffer at a
position, and an agent has no cursor. It wants the same guarantees through a
protocol built for it, which is `flow mcp` — one tool per RPC with schemas
derived from the protobuf schema, `flowstate_run_local` to rehearse a file in
process, and read-only resources carrying the DSL reference, the task catalog,
and the examples.

The validation is the same validation. `flow validate`, the language server's
`flowfile` diagnostics, and `flowstate_validate` are one implementation with
three front doors, so an agent and the person reviewing its pull request cannot
be told different things about the same file.

Client configuration — Claude Code, Claude Desktop, and the generic stdio shape —
is in [CLI.md](CLI.md#flow-mcp-the-same-surface-for-an-agent), along with what a
local run may reach and the authoring loop the surface is shaped around.
