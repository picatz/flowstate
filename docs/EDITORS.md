# Editor setup

Flowstate ships a language server for the `Flowfile` DSL. It gives you diagnostics
as you type, hover documentation generated from the task registry, and completion
that only ever offers things the engine will accept.

## What the server provides

| Feature | What you get |
| --- | --- |
| **Diagnostics** | YAML syntax errors, CEL syntax errors underlined inside the expression, unknown tasks, duplicate and unusable step ids, references to steps that do not exist or have not run yet, inputs a task does not declare (with a spelling suggestion), required inputs left out, unknown CEL libraries, malformed step timeouts and retry intervals, and a step that is no kind of work or more than one. |
| **Hover** | A task's summary and full typed signature; an input's type, whether it is required, and the value constraints the schema enforces; what a `${step.output}` reference resolves to and what type it produces; what a loop's iterator binds; what a `${secret('scheme:name')}` reference names; what a CEL library provides; what each `Flowfile` key means. |
| **Completion** | Task names after `name:`; input keys for the step's task, required ones first, already-written ones omitted; the names in scope inside `${...}` (see the scoping rules below); a step's actual output names after `${step.`; CEL library names in `libs`; and the document's own keys (`id`, `if`, `timeout`, `retry`, `for_each`, `parallel`, …). |
| **Go to definition** | Jump from a `${step.output}` reference to that step's `id:` declaration. |
| **Document symbols** | An outline of the workflow's steps, each labelled with the task it runs, and for a nested step the block it belongs to. |

Everything above is read from the task registry and the Protobuf schema at the
moment you ask for it, so a task added to the engine shows up in your editor with
no change to the language server. The rules about what a *workflow* may say — step
ids, unknown tasks, references that cannot resolve, durations, step structure — come
from the same validator `flow validate` uses, so the editor and the command line
never disagree about a file. The language server's contribution there is the
position: the same message, under the token at fault.

All of it also works inside a `for_each` body and a `parallel` branch, not only at
the top level.

### Scoping inside loops and parallel blocks

Completion, hover, and go-to-definition follow the engine's scoping rules, so a
name the editor offers is always one the workflow can resolve:

- Inside a `for_each` body, the current item is in scope under the loop's
  `iterator:` name (`item` by default), and earlier body steps are in scope within
  the iteration.
- A loop body's step outputs **do not** escape the loop. After the block, only the
  loop's own id is referenceable, and its single output is `results` — one entry
  per iteration. Body step ids are not offered to later steps.
- A `parallel` block's branch outputs **do** merge into the enclosing scope once
  the block joins, so a later step can reference them directly. One branch cannot
  reference a sibling's, because branches are unordered.
- A step cannot reference the block that contains it, since that block has not
  finished while the step runs.

Each diagnostic carries a stable code you can filter on: `yaml-syntax`,
`cel-syntax`, `unknown-cel-library`, `unknown-input`, `missing-input`,
`document-too-large`, and `flowfile` for everything the shared validator reports —
the same problems `flow validate` prints, with the same wording, positioned onto
the token at fault.

Not implemented, and deliberately not advertised: formatting, rename, code actions,
references, and workspace symbols. The server also never type-checks expressions —
it only parses them — because a step's output types are not statically known for
every task, and a wrong squiggle under working code is worse than no squiggle.

Secret references are described on hover but **not** offered by completion. A
`${secret('env:API_KEY')}` marker compiles and validates today, but no task consumes
one yet, so a workflow using it fails at run time — suggesting it would be offering
a trap. Hover reads the reference through the same parser the compiler uses, so it
cannot describe a form a worker would refuse, and it names the scheme rather than a
backend: which provider serves `vault` is a deployment's choice, made worker-side.
Misplaced references — combined into a larger expression, or used in `if` or
`for_each.items`, where resolving them would put the secret into workflow history —
are reported by the validator with its own explanation.

Inputs a task evaluates itself are not reference-checked, because they resolve
against a scope the document does not model: the `http` task's `outputs` expression
sees `status_code`, `body`, and `headers` from the response, not step outputs.
Which inputs those are comes from the task's own definition, so this cannot go
stale. Note that `outputs` has to be written as a quoted whole-value expression —
`outputs: "${ {'status': status_code} }"` — since an unquoted value would read the
colons inside as YAML mapping syntax.

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
command every editor needs is just `flow lsp`. It takes no arguments. Logs go to
stderr; document contents are never logged, since a Flowfile input can hold a
credential.

## Which files are Flowfiles

A Flowfile is YAML, so no editor detects one on extension alone. Point the client
at the files you actually use. The configurations below match, in order of how
specific they are:

- a file literally named `Flowfile` or `Flowfile.yaml`
- `workflow.yaml` or `workflow.yml`
- anything under a `workflows/` directory

Adjust these to your layout. Pointing the server at every `*.yaml` in a repository
works, but you will get Flowfile diagnostics on your Kubernetes manifests.

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
2. **Completion.** Type `message: ${` in a step after the first. Only the ids of
   steps *above* it should be offered. Add a `.` after one and you get that step's
   real output names.
3. **Hover.** Put the cursor on a task name. You should see its summary and a table
   of typed inputs and outputs, with required inputs marked.
4. **Go to definition.** With the cursor on a `${step.output}` reference, jump to
   that step's `id:`.
5. **Outline.** Open your editor's symbol list. Each step appears, labelled with
   its task.

If nothing happens, check that the server starts and answers at all. The `sleep`
matters: the server exits as soon as its input closes, so a plain heredoc ends the
process before it has written the reply.

```console
$ body='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
$ { printf 'Content-Length: %d\r\n\r\n%s' "${#body}" "$body"; sleep 1; } | flow lsp
Content-Length: 279

{"id":1,"result":{"capabilities":{"textDocumentSync":{ ... }}},"jsonrpc":"2.0"}
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
