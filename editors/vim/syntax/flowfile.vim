" Vim syntax file
" Language: Flowfile (YAML with CEL in ${...} fences and in bare-CEL values)
"
" YAML's own groups do the YAML. This file adds the two places a Flowfile holds
" CEL, which docs/LANGUAGE.md defines: a ${...} fence anywhere in a scalar, and
" the bare value after `must:` under an input, output or type. It is deliberately
" lexical and small; `flow lsp` knows what a name means and colours accordingly
" in an editor that asks it for semantic tokens.
if exists('b:current_syntax')
  finish
endif

runtime! syntax/yaml.vim
unlet! b:current_syntax

syn case match

" CEL ------------------------------------------------------------------------
syn match   flowfileCelComment  contained /\/\/.*$/
syn region  flowfileCelString   contained start=/\c\%(\<\%(rb\|br\|r\|b\)\)\=\z(["']\)/ skip=/\\./ end=/\z1/ oneline
syn match   flowfileCelNumber   contained /\<\%(0[xX]\x\+\|\d\+\%(\.\d\+\)\=\%([eE][+-]\=\d\+\)\=\)[uU]\=\>/
syn keyword flowfileCelKeyword  contained true false null in
syn keyword flowfileCelRoot     contained inputs vars steps run event trigger response this now sender
syn match   flowfileCelFunction contained /\<\h\w*\ze\s*(/
syn match   flowfileCelMember   contained /\.\zs\h\w*/ contains=NONE
syn match   flowfileCelOperator contained /&&\|||\|==\|!=\|<=\|>=\|\.?\|[-+*%!<>?:]/
syn region  flowfileCelBraces   contained matchgroup=flowfileCelOperator start=/{/ end=/}/ oneline transparent contains=@flowfileCel

syn cluster flowfileCel contains=flowfileCelComment,flowfileCelString,flowfileCelNumber,flowfileCelKeyword,flowfileCelRoot,flowfileCelFunction,flowfileCelMember,flowfileCelOperator,flowfileCelBraces

" A fence. Nested braces (a map literal) are consumed by flowfileCelBraces, so the
" first unmatched } is the fence's own.
syn region flowfileFence matchgroup=flowfileFenceDelim start=/\${/ end=/}/ contains=@flowfileCel oneline containedin=yamlFlowString,yamlPlainScalar,yamlBlockString,yamlFlowMappingVal,yamlFlowMappingKey,yamlString

" A bare-CEL value: everything after `must:` up to a trailing YAML comment.
syn match flowfileMustKey /^\s*\%(-\s\+\)\=must:\ze\s/ nextgroup=flowfileMustVal skipwhite
syn match flowfileMustVal contained /\%([^#]\|\S#\)\+/ contains=@flowfileCel nextgroup=flowfileMustComment skipwhite
syn match flowfileMustComment contained /#.*$/

hi def link flowfileMustKey yamlBlockMappingKey
hi def link flowfileMustComment yamlComment
hi def link flowfileFenceDelim PreProc
hi def link flowfileCelComment Comment
hi def link flowfileCelString  String
hi def link flowfileCelNumber  Number
hi def link flowfileCelKeyword Keyword
hi def link flowfileCelRoot    Identifier
hi def link flowfileCelFunction Function
hi def link flowfileCelMember  Type
hi def link flowfileCelOperator Operator

let b:current_syntax = 'flowfile'
