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

Each diagnostic carries a stable code you can filter on. Three of them belong to
this server, for the problems it finds itself: `yaml-syntax`, `cel-syntax`, and
`document-too-large`. Everything else is the shared validator's own code, published
unchanged — `unknown-task`, `unresolved-reference`, `type-mismatch`,
`constraint-violation`, `placement-refusal`, `retired-key`, `sensitive-in-log`,
`sensitive-in-prompt`, and `general` for a class that has not earned one.
[reference/diagnostics.md](reference/diagnostics.md) is the generated list.

That the codes pass through is the point rather than an implementation detail. The
server used to overwrite every validator code with a single spelling, `flowfile`,
which meant an editor could filter every Flowfile problem or none while a program
reading the same file over `flow validate --output json` could tell an unknown task
from an unresolved reference. Two surfaces disagreeing about what a problem *is* is
the drift the schema type exists to prevent, so what you see under the squiggle is
what the JSON says.

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
- `*.test.yaml` and `testdefaults.yaml` — `flow test`'s suite format and its
  shared directory fixture, which the server recognizes by name and checks with
  the test loader rather than the workflow grammar, so a test file never draws a
  workflow's diagnostics

Adjust these to your layout. Pointing the server at every `*.yaml` in a repository
works, but you will get Flowfile diagnostics on your Kubernetes manifests.

### What the server provides for a test file

A `*.test.yaml` and a `testdefaults.yaml` speak their own, narrower language, and
the table above does not apply to them — a step's `for_each:` or a task's own
inputs would be wrong answers with confidence in a document that has no `steps:`
at all. What they get instead:

| Feature | What you get |
| --- | --- |
| **Diagnostics** | Everything the flowtest loader — the same one `flow test` runs — checks: a misspelled key, a malformed stub or `starter:`/`sender:`, an over-limit `check:` list, and the rest. Syntax, strict-key, and semantic problems use the loader's own positions; there is no case-name/prose heuristic. A problem in an included `testdefaults.yaml` is published on that file's URI and line, including from an unsaved defaults buffer, rather than mapped onto the suite that included it. Live defaults edits revalidate at most 32 open suites; another suite is checked against saved defaults and gets an explicit warning instead of silently creating unbounded per-keystroke work. If that bounded fallback finds a suite-specific defaults refusal, it reports the refusal at the overflow suite's start and labels it as a saved-defaults fallback rather than putting disk coordinates on a newer live defaults buffer. A task name absent from the catalog is not diagnosed merely for being absent: tests may provide synthetic tasks, so doing so would be false. |
| **Completion** | The document's own keys at every level — a suite's `edition`/`vars`/`defaults`/`tests`/`coverage`, a case's `name`/`workflow`/`inputs`/`stubs`/`expect`/…, `expect:`'s own keys (`outputs`, `failed`, `ran`, `check`, …), and the rest of the shape (`defaults:`, a stub's `fails:`, a `signals:` entry, `starter:`/`sender:`, a `check:` claim, a `cases:` row) — plus a stub's `task:` value, completed from the same task registry a workflow step's task name is. `testdefaults.yaml`'s own top level is narrower, since `tests:` and `coverage:` are not legal there. |
| **Document symbols** | An outline naming every runnable case: an entry with no `cases:` rows by its own `name:`, and an entry that declares rows by `<entry name>/<row name>` for each row — the same identity `flow test`'s own report uses, since an entry with rows is a template the rows are merged over and does not itself run. |
| **Hover** | Every real test-language key, using the same guarded key table completion reads, including `cases:`, `expect:`, `stubs:`, `vars:`, and `testdefaults.yaml`'s narrower root. A stub's `task:` value shows the same registry-derived documentation a workflow step's task name shows; an unknown or synthetic task stays silent rather than inventing documentation. |
| **Go to definition, formatting, code actions** | Not yet answered for a test file — there is no flowtest analogue of `flowfile.Marshal` for formatting to render against, no suggested-edit machinery for code actions to read, and go-to-definition's one candidate (a case's `workflow:` naming a sibling Flowfile) is unbuilt. |

Everything in that table is derived the same way the workflow table's is: the
document-shape keys are read by reflection off the `flowtest` package's own
`yaml:` struct tags — the identical tags the loader's strict decoder consults to
decide "known" from "unknown field" — and a two-way guard fails if the table or
loader changes alone. Completion and key hover read that one table; stub task
completion and hover read the server's one task registry.

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

`$FLOWSTATE_PLUGIN_DIR` is **not** read by `flow lsp`, and the path it is given must
be **absolute**. A worker reads the variable, because a worker's environment is one
an operator arranged; an editor starts the language server with the opened workspace
as its working directory, so a relative `--plugin-dir` would name a directory inside
whatever repository you happen to have open, and an inherited variable is one more
way for something other than this command line to choose what your editor executes.
The command line in the table above is the whole of the opt-in, which is why there
is no configuration path to the same effect.

## Neovim

This is the configuration CI runs. It lives at
[`tools/editorsmoke/init.lua`](../tools/editorsmoke/init.lua), and
`tools/editorsmoke/probe.lua` asserts on every pull request that the block below
is that file byte for byte — so the instructions cannot quietly stop matching the
thing that was tested. See [What has been verified](#what-has-been-verified) for
what the run actually checks.

### Neovim 0.11+, no plugin

`vim.lsp.config`, `vim.lsp.enable` and `vim.lsp.completion` are built in, so
`nvim-lspconfig` buys nothing here — the whole configuration is one `init.lua`:

```lua
-- Give Flowfiles their own filetype, so the server is not attached to every YAML
-- file you open. These patterns are what decide where you get diagnostics.
vim.filetype.add({
  filename = {
    ['Flowfile'] = 'flowfile',
    ['Flowfile.yaml'] = 'flowfile',
    ['workflow.yaml'] = 'flowfile',
    ['workflow.yml'] = 'flowfile',
    ['testdefaults.yaml'] = 'flowfile',
  },
  pattern = {
    ['.*/workflows/.*%.ya?ml'] = 'flowfile',
    ['.*%.test%.ya?ml'] = 'flowfile',
  },
})

vim.lsp.config.flowstate = {
  cmd = { 'flow', 'lsp' },
  filetypes = { 'flowfile' },
  root_markers = { 'go.mod', '.git' },
}

vim.lsp.enable('flowstate')

-- Flowfiles are YAML, so keep YAML's indentation and comment rules.
vim.api.nvim_create_autocmd('FileType', {
  pattern = 'flowfile',
  callback = function()
    vim.bo.commentstring = '# %s'
    vim.bo.expandtab = true
    vim.bo.shiftwidth = 2
  end,
})

vim.api.nvim_create_autocmd('LspAttach', {
  callback = function(args)
    local client = vim.lsp.get_client_by_id(args.data.client_id)
    if not client or client.name ~= 'flowstate' then
      return
    end
    local opts = { buffer = args.buf }
    vim.keymap.set('n', 'gd', vim.lsp.buf.definition, opts)
    vim.keymap.set('n', 'gO', vim.lsp.buf.document_symbol, opts)
    vim.keymap.set('n', '<leader>a', vim.lsp.buf.code_action, opts)
    -- Completion is not automatic unless you ask for it.
    vim.lsp.completion.enable(true, args.data.client_id, args.buf, { autotrigger = true })
  end,
})
```

Two things in there are less arbitrary than they look. The `LspAttach` callback
checks the client name before binding anything, because that autocommand fires for
*every* server that attaches to *any* buffer — without the guard, opening a Go file
gets flowstate's keymaps and its completion setting. And `vim.filetype.add` comes
before `vim.lsp.enable`, because `filetypes = { 'flowfile' }` is only useful once
something classifies a file that way; the order does not matter to Neovim, but
reading it in the other order invites deleting the block that does the work.

Neovim 0.11 already binds `K` to hover and `gO` to document symbols when a server
attaches, and `<C-]>` reaches definition through `tagfunc`, so the keymaps above are
convenience rather than necessity. Completion genuinely is off until you enable it.

**Version.** `vim.lsp.config` arrived in Neovim 0.11, and this was verified on
0.12.4. It is worth checking `nvim --version` before assuming: Ubuntu 24.04's
`apt` package is 0.9.5, four minor versions behind, and on it this configuration
fails with `attempt to index field 'config' (a nil value)`. Install from the
[release tarball](https://github.com/neovim/neovim/releases) — which is what CI
does — or use the "single file" recipe below, which works back to 0.8.

### Attaching to a single file, no config

Useful for trying it out. `flow lsp` attaches to whatever buffer is current, so the
filetype does not have to be `flowfile`:

```vim
:lua vim.lsp.start({ name = 'flowstate', cmd = { 'flow', 'lsp' } })
```

## Visual Studio Code

`editors/vscode/` in this repository is a thin client over `flow lsp`, built to
the design in [#585](https://github.com/picatz/flowstate/issues/585): the
language client, the same filename/pattern association as the table above, and
palette commands (`Flowstate: Validate/Test/Fix/Run Local`) that shell out to the
matching subcommand and show its own output. See `editors/vscode/README.md` for
what it does, what it deliberately leaves out (a workflow tree view and a
step-graph webview are both designed but not shipped yet), and exactly what has
been compiled and unit-tested here versus what still needs a human with a real
editor window — this repository's CI has no display to open one on.

**It is not published to any marketplace.** Install it from source:

```console
$ cd editors/vscode
$ npm ci
$ npm run compile
$ code --extensionDevelopmentPath="$PWD" /path/to/a/repo/with/flowfiles
```

`flow` must be on your `PATH`, or point `flowstate.path` at it in your *user*
settings. That setting and `flowstate.lsp.args` are `machine`, so a
workspace's own `.vscode/settings.json` cannot choose what your editor executes —
the same argument this page makes about Neovim's `--plugin-dir` above.

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
Configured language servers:
  ✓ flowstate: /usr/local/bin/flow
Configured debug adapter: None
Configured formatter: None
Tree-sitter parser: ✓
Highlight queries: ✘
Textobject queries: ✘
Indent queries: ✘
```

Those three crosses are the part that used to go unsaid. `grammar = "yaml"` gets
you YAML's *parser*, and Helix then looks its queries up by the **language** name
rather than by the grammar's — so a language called `flowfile` has no highlight
queries anywhere and a Flowfile opens as undifferentiated text. Borrow YAML's, in
two one-line files under your runtime directory:

```console
$ mkdir -p ~/.config/helix/runtime/queries/flowfile
$ echo '; inherits: yaml' > ~/.config/helix/runtime/queries/flowfile/highlights.scm
$ echo '; inherits: yaml' > ~/.config/helix/runtime/queries/flowfile/indents.scm
```

`hx --health flowfile` then reports highlight and indent queries present. Textobject
queries stay absent, which costs you `mi`/`ma` motions inside a Flowfile and nothing
else; add a third `; inherits: yaml` file named `textobjects.scm` if you want them.

Helix binds `gd` to definition, `K` to hover, and `<space>s` to document symbols
out of the box.

Verified on Helix 25.07.1: the config above is picked up, the server resolves, and
the queries fix moves both crosses to ticks. Not verified: the `workflows/*.yaml`
glob matching a real nested path, and the editor behaviour itself — Helix is a
terminal UI with no headless mode, so `--health` is as far as a scripted check
reaches.

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

> **Untested.** Zed is a GUI application and was not run while writing this. The
> settings shape is from Zed's documentation, not from a session anyone had.

## Emacs (eglot)

`eglot` is built into Emacs 29 and later; nothing needs installing for the client
half. `yaml-mode` is **not** built in — it is a MELPA package — so the derivation
below fails on a stock Emacs with `Symbol's function definition is void:
yaml-mode`. Emacs 29 ships `yaml-ts-mode` instead, which needs the tree-sitter YAML
grammar installed once with `M-x treesit-install-language-grammar RET yaml`. Pick
whichever of the three you actually have:

```elisp
;; Name the parent you actually have. `define-derived-mode` is a macro and takes
;; a literal symbol here, so this cannot be a `cond` that picks one at load time:
;;
;;   yaml-mode     from MELPA
;;   yaml-ts-mode  Emacs 29+, after M-x treesit-install-language-grammar RET yaml
;;   conf-mode     always present, comments and indentation but no highlighting
(define-derived-mode flowfile-mode yaml-ts-mode "Flowfile")

(add-to-list 'auto-mode-alist '("/Flowfile\\'" . flowfile-mode))
(add-to-list 'auto-mode-alist '("/workflow\\.ya?ml\\'" . flowfile-mode))
(add-to-list 'auto-mode-alist '("/workflows/.*\\.ya?ml\\'" . flowfile-mode))

(with-eval-after-load 'eglot
  (add-to-list 'eglot-server-programs
               '(flowfile-mode . ("flow" "lsp"))))

(add-hook 'flowfile-mode-hook #'eglot-ensure)
```

Partly verified on GNU Emacs 29.3: `eglot` is present without installing anything,
and eglot connects to `flow lsp` and reports *"Connected! Server … now managing
`(flowfile-mode)` buffers"*. Not verified: that diagnostics reach flymake, because
driving eglot to that point under `emacs --batch` depends on idle timers and
`post-command-hook` and proves more about batch mode than about anybody's editor.
Take the connection as confirmed and the squiggles as expected.

## Stepping a run: `flow dap`

Everything above is `flow lsp`, which answers questions about a *file*. `flow dap`
is the other half: it speaks the Debug Adapter Protocol, so an editor's step and
continue buttons drive a real local run of the workflow you are looking at.

```console
$ flow dap
```

Run by hand it prints a banner saying so and waits — like `flow lsp`, it is meant
to be launched by an editor rather than typed. For a terminal debugger, use
`flow run local --debug`, which is the same session behind the same commands.

**Breakpoints are step ids, not source lines.** The debugger is handed steps and
not files — the engine calls it with a node, and a node carries an `id` and no
position — so there is nothing to hang a gutter dot on. Set them as *function*
breakpoints named after a step. A line breakpoint is answered rather than
ignored, unverified and carrying that reason, so an editor shows a hollow marker
instead of a filled one you would wait at forever.

Two more consequences of the same seam, so nothing here is discovered: a stack
frame names the step and cannot be navigated to, and a run is one thread even
where a `parallel:` block is running several steps at once — the debugger
deliberately does not stop inside one.

### Visual Studio Code

Add to `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "flowstate",
      "request": "launch",
      "name": "Debug this Flowfile",
      "program": "${workspaceFolder}/examples/hello-world/workflow.yaml"
    }
  ]
}
```

`program` is the workflow to run, and it is read from the launch configuration
rather than from the adapter's own arguments — one `flow dap` serves whatever you
point it at. Registering the `flowstate` debug type needs an extension
contribution; `editors/vscode/` does not ship one yet.

Function breakpoints go in the Breakpoints view's own section — the **+** beside
*Function Breakpoints* — typed as a step's `id`.

### Helix

Add a `[language.debugger]` stanza to the `[[language]]` block from
[Helix](#helix) above:

```toml
[language.debugger]
name = "flowstate"
transport = "stdio"
command = "flow"
args = ["dap"]

[[language.debugger.templates]]
name = "workflow"
request = "launch"
completion = [{ name = "workflow", completion = "filename" }]
args = { program = "{0}" }
```

`hx --health flowfile` then reports the adapter where it used to say
`Configured debug adapter: None`, and Helix's debug menu (`<space>g` by default)
has something to open.

### What the debug console can do

The `evaluate` request is wired to the same CEL evaluator the terminal debugger's
`inspect` uses, against the scope the run is actually paused in — so the debug
console is a REPL over the paused run:

```
> steps.build.value
"web.tar.gz"
> steps.build.value.endsWith('.tar.gz')
true
```

The variables pane is the same scope, grouped as `scope` groups it: `steps`,
`vars`, `inputs`, the workflow's declared vars, `run` and `trigger`. A very large
scope is rendered up to a bound and then says how many it did not render, rather
than stopping silently.

Secrets are withheld here exactly as they are at the terminal prompt. The
redaction is a property of the session rather than of the front end, so a value a
`flow test` run would not print is not one an editor's variables pane can read
out of it either.

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

## What has been verified

Setup instructions rot silently, because nothing fails when they do — the person
they fail for is somebody who never files a bug about it. So one editor's
instructions are run on every pull request, and the rest of this page says which
version it was checked on and what was not checked at all.

**Neovim is the one CI runs.** `.github/workflows/editors.yml` installs a pinned
Neovim release, builds `flow`, and drives the real editor through
`tools/editorsmoke/probe.lua` — the configuration under
[Neovim 0.11+, no plugin](#neovim-011-no-plugin) is
`tools/editorsmoke/init.lua`, loaded as `-u`, and the first thing the probe
asserts is that the two are the same text. It then checks, on a Flowfile in
`tools/editorsmoke/fixtures/`:

| | |
| --- | --- |
| **filetype** | each documented filename and the `workflows/` pattern classify as `flowfile`, and a `kustomization.yaml` does *not* — the negative direction, because a rule that matches everything also matches every one of your Kubernetes manifests |
| **initialize** | the client attaches, and the server advertises hover, completion, definition, document symbol, formatting and code actions |
| **publishDiagnostics** | a deliberate `lag:` draws one error, carrying the code `unknown-task`, positioned on the task name rather than on the file — and a Flowfile with nothing wrong with it draws nothing, which is what separates a working server from one that complains about everything |
| **hover** | over `log` returns the summary and typed signature built from the task registry |
| **completion** | inside `${` offers `vars` and `steps` among 35 items; where a step's keys go it offers the registry's tasks and the grammar's keys among 20 |
| **documentSymbol** | both steps appear in the outline |

You can run the same thing yourself; it needs `flow` on `PATH` and nothing else:

```console
$ go build -o /usr/local/bin/flow ./cmd/flow
$ nvim --clean --headless -u tools/editorsmoke/init.lua -l tools/editorsmoke/probe.lua
…
36 checks, 0 failed
```

**Verified by hand, not by CI:** Helix 25.07.1 (`hx --health flowfile`, including
the missing-queries finding above) and GNU Emacs 29.3 (eglot connects). Both are
recorded in their own sections with the part that was *not* reached.

**`flow dap` is exercised end to end, and not inside an editor.**
`cmd/flow/dap_test.go` runs the real binary as a subprocess and speaks the
protocol to it over real `Content-Length` framing — initialize, launch, a
function breakpoint on a step id, `configurationDone`, continue, the stopped
event, a stack frame naming the step, an `evaluate` reading an earlier step's
output, and the variables pane — so the conversation is checked on every pull
request the way the Neovim job checks `flow lsp`. What is *not* checked is the
half above that is a settings key: neither the `launch.json` nor the Helix
`[language.debugger]` stanza has been loaded by the editor it is written for.

**Not verified inside a real editor:** Visual Studio Code and Zed. Both are GUI
applications with no headless mode worth scripting. The VS Code extension under
`editors/vscode/` compiles (`tsc`, `strict: true`) and its unit tests pass under
Node's built-in test runner — argv construction for each palette command, and
binary-resolution and availability-probe behavior — but nothing has confirmed the
extension actually activates in a VS Code window, that diagnostics appear as you
type, or that a palette command's task renders as expected; see
`editors/vscode/README.md`'s "Verified, and not" section. Zed's section below has
not been run by anyone. Both are marked as such where they appear rather than
here, so nobody reads a confident paragraph without the caveat attached to it.

The asymmetry is deliberate rather than lazy. The load-bearing half of every one of
these configurations is `flow lsp` itself, and that half is exercised by the Neovim
job on every pull request. What the untested sections risk is a wrong settings key,
which costs a reader ten minutes — not a wrong claim about what the server does,
which would cost them their trust in the page.

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
