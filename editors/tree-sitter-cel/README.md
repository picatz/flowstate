# tree-sitter-cel

A [tree-sitter](https://tree-sitter.github.io/) grammar for CEL as a Flowfile
writes it: the bare value of `must:` and the inside of a `${...}` fence. It
follows the [CEL language definition](https://github.com/google/cel-spec/blob/master/doc/langdef.md#syntax)
and knows nothing Flowstate-specific; `inputs` and `steps` are identifiers here.
Scope-aware colour comes from `flow lsp` semantic tokens, which an editor layers
over these queries.

There is no Flowfile grammar. A Flowfile is YAML, and a second YAML
representation would drift, so the Flowfile queries run over the stock
[tree-sitter-yaml](https://github.com/tree-sitter-grammars/tree-sitter-yaml)
parser and inject this grammar.

| Path | What it is |
| --- | --- |
| `grammar.js` | The grammar. The only source; the parser is generated and not checked in. |
| `test/corpus/` | Parse-tree tests, run by `tree-sitter test`. |
| `queries/highlights.scm` | Highlights for `cel`. |
| `queries-flowfile/injections.scm` | YAML injections: `must:` values and a scalar that is exactly one `${...}` fence. |

```console
$ cd editors/tree-sitter-cel
$ tree-sitter generate
$ tree-sitter test
```

## Limits, stated

- An injection takes a node's whole range or a fixed offset from it, so a fence
  in the middle of text (`"hi ${x}"`) is not injected. The semantic tokens from
  `flow lsp` colour those.
- The grammar parses CEL's syntax, not its meaning: macros such as `exists` are
  ordinary calls, and a bare `x` is an identifier whether or not it is bound.
- CI (`.github/workflows/editors.yml`) generates the parser with a pinned,
  digest-verified `tree-sitter` binary, runs the corpus, and checks that the
  highlight query compiles. It does not load `queries-flowfile/injections.scm`,
  which needs the YAML parser; that query was run by hand against
  tree-sitter-yaml. A YAML-backed check, and wiring the queries into Neovim,
  Helix and Zed, are separate work.
- Inside a flow mapping, YAML forbids `{` and `}` in a plain scalar, so a
  `${...}` fence in `{a: ...}` must be quoted; the injection then sees it as a
  quoted scalar.
