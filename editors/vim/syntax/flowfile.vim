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
syn region  flowfileCelStringD  contained start=/\c\%(\<\%(rb\|br\|r\|b\)\)\="/ skip=/\\./ end=/"/ oneline
syn region  flowfileCelStringS  contained start=/\c\%(\<\%(rb\|br\|r\|b\)\)\='/ skip=/\\./ end=/'/ oneline
syn match   flowfileCelNumber   contained /\<\%(0[xX]\x\+\|\d\+\%(\.\d\+\)\=\%([eE][+-]\=\d\+\)\=\)[uU]\=\>/
syn keyword flowfileCelKeyword  contained true false null in
syn keyword flowfileCelRoot     contained inputs vars steps run event trigger response this now sender
syn match   flowfileCelFunction contained /\<\h\w*\ze\s*(/
syn match   flowfileCelMember   contained /\.\zs\h\w*/ contains=NONE
syn match   flowfileCelOperator contained /&&\|||\|==\|!=\|<=\|>=\|\.?\|[-+*%!<>?:]/
syn region  flowfileCelBraces   contained matchgroup=flowfileCelOperator start=/{/ end=/}/ oneline transparent contains=@flowfileCel

syn cluster flowfileCelCore contains=flowfileCelComment,flowfileCelNumber,flowfileCelKeyword,flowfileCelRoot,flowfileCelFunction,flowfileCelMember,flowfileCelOperator,flowfileCelBraces
syn cluster flowfileCel contains=@flowfileCelCore,flowfileCelStringD,flowfileCelStringS
" Inside a YAML-quoted predicate the outer quote is YAML's, so only the other
" quote can open a CEL string.
syn cluster flowfileCelInDq contains=@flowfileCelCore,flowfileCelStringS
syn cluster flowfileCelInSq contains=@flowfileCelCore,flowfileCelStringD

" A fence. A $ before the ${ makes it the escaped opening $${, which is literal
" text (interp.go). Nested braces (a map literal) are consumed by flowfileCelBraces,
" so the first unmatched } is the fence's own. A fence may span lines in a block
" scalar; an unfinished one stops where the next line reads as a YAML key or item.
syn region flowfileFence matchgroup=flowfileFenceDelim start=/\%(\$\)\@1<!\${/ end=/}/ end=/\n\ze\s*\%(-\s\+\)\=[[:alnum:]_"'.-]\+:\%(\s\|$\)/ end=/\n\ze\s*-\s/ contains=@flowfileCel containedin=yamlFlowString,yamlPlainScalar,yamlBlockString,yamlFlowMappingVal,yamlFlowMappingKey,yamlString

" A bare-CEL value: everything after `must:` up to a trailing YAML comment. A
" value that opens with a quote is a YAML-quoted predicate; the quotes are YAML's.
syn match flowfileMustKey /^\s*\%(-\s\+\)\=must:\ze\s/ nextgroup=flowfileMustVal,flowfileMustDq,flowfileMustSq skipwhite
syn match flowfileMustVal contained /[^"'#[:space:]]\%([^#]\|\S#\)*/ contains=@flowfileCel nextgroup=flowfileMustComment skipwhite
syn region flowfileMustDq contained matchgroup=yamlString start=/"/ skip=/\\./ end=/"/ oneline contains=@flowfileCelInDq nextgroup=flowfileMustComment skipwhite
syn region flowfileMustSq contained matchgroup=yamlString start=/'/ skip=/''/ end=/'/ oneline contains=@flowfileCelInSq nextgroup=flowfileMustComment skipwhite
syn match flowfileMustComment contained /#.*$/

hi def link flowfileMustKey yamlBlockMappingKey
hi def link flowfileMustComment yamlComment
hi def link flowfileFenceDelim PreProc
hi def link flowfileCelComment Comment
hi def link flowfileCelStringD String
hi def link flowfileCelStringS String
hi def link flowfileCelNumber  Number
hi def link flowfileCelKeyword Keyword
hi def link flowfileCelRoot    Identifier
hi def link flowfileCelFunction Function
hi def link flowfileCelMember  Type
hi def link flowfileCelOperator Operator

let b:current_syntax = 'flowfile'
