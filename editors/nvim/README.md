# Neovim: tree-sitter for Flowfiles

Highlights and CEL injections for a `flowfile` buffer, on top of the LSP setup in
[docs/EDITORS.md](../../docs/EDITORS.md). A Flowfile is YAML, so the buffer is
parsed with the stock `yaml` parser, and the `cel` parser from
[`editors/tree-sitter-cel`](../tree-sitter-cel/README.md) is injected into
`must:` values and into any scalar that is exactly one `${...}` fence. A fence in
the middle of text is left to the semantic tokens `flow lsp` serves.

| Path | What it is |
| --- | --- |
| `plugin/flowfile.lua` | Gives `flowfile` a language of its own that reuses the yaml parser, so plain YAML files get none of this. |
| `queries/flowfile/` | YAML's highlights, plus the Flowfile injections. |
| `queries/cel/highlights.scm` | Highlights for the injected CEL. |
| `test/ts_test.lua` | The CI check: asks Neovim which ranges got a `cel` tree and what the highlight query captures in them. |

The query files are copies of the ones beside the grammar, which stay the
source; the test fails if a copy drifts.

## Install

Put this directory on the `runtimepath`, and make sure both parsers are installed
as `parser/yaml.so` and `parser/cel.so` on it. The yaml parser comes from
nvim-treesitter or your package manager. For `cel`, build it with the
`tree-sitter` CLI:

```console
$ cd editors/tree-sitter-cel
$ tree-sitter generate
$ cc -shared -fPIC -O2 -Isrc -o ~/.local/share/nvim/site/parser/cel.so src/parser.c
```

CI does the same from source with a pinned `tree-sitter` binary and a pinned
tree-sitter-yaml revision, then runs `test/ts_test.lua` under Neovim 0.12.
Neovim 0.11 or later is needed for the `flowfile` language and for `#offset!` on
an injection; nothing here was run on an older one.
