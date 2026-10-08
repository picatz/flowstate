" A Flowfile keeps YAML's comment, indentation and fold rules.
if exists('b:did_ftplugin')
  finish
endif

runtime! ftplugin/yaml.vim ftplugin/yaml_*.vim ftplugin/yaml/*.vim
let b:did_ftplugin = 1
setlocal commentstring=#\ %s expandtab shiftwidth=2 softtabstop=2
let b:undo_ftplugin = get(b:, 'undo_ftplugin', 'exe') . '|setl commentstring< expandtab< shiftwidth< softtabstop<'
