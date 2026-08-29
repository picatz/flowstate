# Flowstate Flowfile — VS Code client

A thin client over `flow lsp`, following the design in
[#585](https://github.com/picatz/flowstate/issues/585). It is **not published to
any marketplace**, has no publish step wired to anything, and holds no token or
secret. Install it by hand from source, as described below.

Everything it adds beyond starting the server has to justify not being in the
engine (`CLAUDE.md`, "A capability is not done until it is reachable from a
Flowfile"; #585 §3). Concretely: no YAML parsing here, no diagnostics of this
extension's own, no policy evaluation. If it looks like the extension is
deciding something about a Flowfile, that decision belongs in
`flowfile/validate.go`, not here.

## What it does

1. **Language client.** Activates on the `flowfile` language, launches
   `flow lsp` over stdio, and wires it to VS Code's language-client surface.
   Diagnostics, hover, completion, go-to-definition, document symbols,
   formatting and the fix-all action all arrive from the server — see
   `docs/EDITORS.md` for what each one covers. If the configured binary is
   missing or the server fails to start, the extension shows an error message
   naming the problem and offers to open the setting, rather than silently
   doing nothing.
2. **Syntax association and language configuration.** `Flowfile`,
   `Flowfile.yaml`, `workflow.yaml`, `workflow.yml`, `workflows/*.yaml`,
   `*.test.yaml` and `testdefaults.yaml` are recognized
   (`docs/EDITORS.md`'s "Which files are Flowfiles" list, mirrored here);
   the server checks the two test-file shapes with `flow test`'s own loader
   rather than the workflow grammar. Comment toggling (`#`) and bracket/indent behavior
   come from `language-configuration.json`. There is no bundled grammar, so
   Flowfiles render as plain text unless you also map the language to YAML's
   tokenizer:
   ```json
   { "files.associations": { "**/workflow.yaml": "flowfile" } }
   ```
3. **Commands that shell out.** `Flowstate: Validate/Test/Fix/Run Local` run
   `flow validate|test|fix|run local <file>` on the active Flowfile as a VS
   Code task, and show the CLI's own output in a dedicated terminal panel. The
   extension never re-parses that output to decide pass or fail — the
   process's exit status is the answer, same as running it yourself.

## What it deliberately does not do

Per #585 §2–3, left out of this first slice on purpose:

- **The workflow tree view and the step-graph webview.** Both are named in
  the design as worth having, but the design also flags the graph as the
  riskiest slice to get right (it must render `flow compile`'s protojson, not
  re-parsed YAML) and recommends shipping the LSP client and language
  contribution alone first. This PR is that first slice plus the palette
  commands; the tree view and graph are follow-ups, not abandoned.
- **Run progress / watch integration**, **deployment management**, and **any
  "ask AI" surface** — all explicitly out of scope in #585 §2–3.
- **Bundling `flow`.** The binary must be on `PATH`, or pointed to with
  `flowstate.path`. Shipping a Go binary through npm is a second, per-platform
  distribution channel the design recommends against for a first version.

## Settings

| Setting | Default | Scope |
| --- | --- | --- |
| `flowstate.path` | `flow` | machine |
| `flowstate.lsp.args` | `[]` | machine |

Both are `machine`: VS Code ignores them when set in a
workspace's `.vscode/settings.json`, so a repository you cloned to read
cannot choose what your editor executes. This is the same argument
`docs/EDITORS.md` makes about Neovim's `--plugin-dir`.

## Supply chain

- One runtime dependency: `vscode-languageclient` (Microsoft's own package;
  it pulls `vscode-jsonrpc` and `vscode-languageserver-protocol`, both from
  the same publisher). Every other package in `package-lock.json` is a
  devDependency needed only to compile and type-check
  (`typescript`, `@types/node`, `@types/vscode`, and their own transitive
  deps — 13 packages total, `npm ls` will show the full tree).
- `package-lock.json` is committed, `.npmrc` sets `save-exact=true`, and CI
  runs `npm ci`, which fails rather than resolving when the lockfile and
  manifest disagree.
- No postinstall scripts, no bundler, no publish tooling. Nothing here talks
  to the network except `npm ci` itself. `.npmrc` sets `ignore-scripts=true`
  as of the supply-chain depth pass, so even a compromised or typosquatted
  future dependency cannot run an install-time lifecycle script — the
  single most-used npm supply-chain vector — without that being a visible,
  reviewed change to `.npmrc` itself. Verified against this exact dependency
  set: `rm -rf node_modules && npm ci`, `npm run compile`, and `npm test`
  all still pass with the setting on, and none of the four packages here
  (`vscode-languageclient`, `typescript`, `@types/node`, `@types/vscode`)
  ships one.
- Dependency bumps arrive through `.github/dependabot.yml`'s npm entry, on a
  cooldown, same as the rest of this repo's supply-chain posture.
- Three related settings considered and decided against, for now:
  - `engine-strict=true` — the extension declares `engines.vscode`, not
    `engines.node`. npm's engine-strict check enforces `engines.node` and
    `engines.npm`; a `vscode` key isn't one it understands, so turning this
    on today would have nothing to enforce and would protect nothing.
    Revisit if a `node` engine constraint is ever added to `package.json`.
  - `npm audit signatures` as a CI step — considered, not added. It runs in
    about two seconds against this thirteen-package tree and would have
    verified registry signatures/attestations for the whole install every
    run, which is cheap. Left out because the `vscode` job's isolation
    (no repository-write token, no Go cache reachable, `npm ci` pinned to an
    exact lockfile — see `editors.yml`) already bounds what an unsigned or
    unverifiable package could do here, and a step that can only ever
    pass-or-warn on a thirteen-package devDependency-heavy tree wasn't worth
    the extra CI minute on every PR touching this directory. Easy to add
    later if the dependency count grows or a real signature gap surfaces.
  - `provenance` — not applicable. There is deliberately no publish step
    (see "What it deliberately does not do" below); provenance attestation
    is something a `npm publish` step would set, and adding one now with
    nothing publishing would be dead configuration. If this extension is
    ever published to a marketplace, that PR should set
    `--provenance`/`provenance: true` at the same time it adds the publish
    step, not before.

## Verified, and not

Verified in this repository, without a display:

- `npm ci` installs from the committed lockfile.
- `npm run compile` (`tsc -p .`, `strict: true`) type-checks clean.
- `npm test` runs the unit tests under Node's built-in test runner
  (`node --test`) against the pure logic in `src/commandLine.ts` and
  `src/binary.ts` — argv construction for each command, and binary-resolution
  and binary-availability-probe behavior including the not-found path.

**Not verified, because this environment has no display and cannot run VS
Code:** that the extension activates correctly inside a real VS Code window,
that the language client actually attaches to `flow lsp` and diagnostics
appear as you type, that the commands' tasks render correctly in the
terminal panel, or that the settings UI behaves as described. A human with
an editor needs to open this folder with
`code --extensionDevelopmentPath=$PWD .` (after `npm ci && npm run compile`)
and try it against a real Flowfile before trusting any of that — exactly the
same caveat #584 gives for its own editor stories.

## Manual install

There is no packaging step in this PR (see "What it deliberately does not
do"). To try it:

```console
$ npm ci
$ npm run compile
$ code --extensionDevelopmentPath="$PWD" /path/to/a/repo/with/flowfiles
```

`flow` must be on your `PATH`, or set `flowstate.path` to it in your user
settings (not the workspace's).
