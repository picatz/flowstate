" Flowfiles are YAML, so no editor can tell one from any other YAML file by
" extension. These are the names docs/EDITORS.md lists as Flowfiles; anything
" else is yours to opt in with `:set filetype=flowfile` or a modeline. It is `set
" filetype`, not `setfiletype`, because the stock detection has already called these
" files YAML by the time this runs and `setfiletype` never overrides.
autocmd BufRead,BufNewFile Flowfile,Flowfile.yaml,workflow.yaml,workflow.yml,testdefaults.yaml set filetype=flowfile
autocmd BufRead,BufNewFile */workflows/*.yaml,*/workflows/*.yml,*.test.yaml,*.test.yml set filetype=flowfile
