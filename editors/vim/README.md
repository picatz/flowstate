# Flowfile for Vim

Filetype detection, a small syntax file and an `ftplugin` for Flowfiles. It
colours the YAML with Vim's own `yaml` syntax and adds the two places a Flowfile
holds CEL: a `${...}` fence anywhere in a scalar, and the bare value after
`must:`. Anything the editor's regexes cannot know — what a name resolves to —
comes from `flow lsp` if your client asks it for semantic tokens.

Nothing here is published to a plugin registry. Install it from a checkout:

```vim
" ~/.vimrc, any plugin manager that takes a path, or a native package:
set runtimepath+=/path/to/flowstate/editors/vim
```

or copy `ftdetect/`, `ftplugin/` and `syntax/` into `~/.vim/`.

Detection is the list in [docs/EDITORS.md](../../docs/EDITORS.md#which-files-are-flowfiles).
For any other file, `:set filetype=flowfile` or a `# vim: ft=flowfile` modeline.

## Language server

The server is `flow lsp` over stdio; Vim needs an LSP client. Each client needs
the filetype above and nothing else.

[vim-lsp](https://github.com/prabirshrestha/vim-lsp):

```vim
if executable('flow')
  autocmd User lsp_setup call lsp#register_server({
        \ 'name': 'flowstate',
        \ 'cmd': {server_info -> ['flow', 'lsp']},
        \ 'allowlist': ['flowfile'],
        \ })
endif
```

[coc.nvim](https://github.com/neoclide/coc.nvim), in `coc-settings.json`:

```json
{
  "languageserver": {
    "flowstate": {
      "command": "flow",
      "args": ["lsp"],
      "filetypes": ["flowfile"]
    }
  }
}
```

[yegappan/lsp](https://github.com/yegappan/lsp) (Vim 9):

```vim
call LspAddServer([#{
      \ name: 'flowstate',
      \ filetype: ['flowfile'],
      \ path: 'flow',
      \ args: ['lsp'],
      \ }])
```

Put `--plugin-dir /absolute/path` after `lsp` for plugin tasks, in your own
editor configuration and never in a project file: see
[Plugin tasks](../../docs/EDITORS.md#plugin-tasks-flow-lsp---plugin-dir).

## Test

```console
$ vim -es -u NONE -N -S editors/vim/test/syntax_test.vim
```

It asks the syntax engine which group each position of `test/fixture.yaml`
holds and exits non-zero on the first miss. Neovim runs the same file
(`nvim --headless -u NONE -S ...`); its own setup is in
[docs/EDITORS.md](../../docs/EDITORS.md#neovim).
