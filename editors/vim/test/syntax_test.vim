" Run from the repository root:
"
"   vim -es -u NONE -N -S editors/vim/test/syntax_test.vim
"   nvim --headless -u NONE -S editors/vim/test/syntax_test.vim
"
" Exits non-zero on the first failed expectation. It asks the syntax engine what
" group each position holds rather than comparing highlight output, so the claim
" is about where CEL starts and stops, which is the part this file owns.
set nocompatible
filetype on
syntax on
let s:root = fnamemodify(expand('<sfile>:p'), ':h:h')
execute 'set rtp^=' . fnameescape(s:root)
runtime ftdetect/flowfile.vim

let s:failures = []

function! s:Groups(line, needle) abort
  let l = getline(a:line)
  let c = match(l, '\V' . a:needle) + 1
  if c == 0
    return ['<needle not found>']
  endif
  return map(synstack(a:line, c), 'synIDattr(v:val, "name")')
endfunction

function! s:Expect(line, needle, group, ...) abort
  let want = a:0 ? a:1 : 1
  let groups = s:Groups(a:line, a:needle)
  let has = index(groups, a:group) >= 0
  if has != want
    call add(s:failures, printf('line %d %s: want %s%s, got %s',
          \ a:line, a:needle, want ? '' : 'no ', a:group, join(groups, ' > ')))
  endif
endfunction

" The filetype comes from the file's name, not from a call in this script.
execute 'edit ' . fnameescape(s:root . '/test/fixture.yaml')
setfiletype flowfile
syntax sync fromstart
call s:Expect(1, 'name', 'yamlPlainScalar', 0)

" An unrelated YAML name is not a Flowfile.
enew
file /tmp/other.yaml
doautocmd BufRead
if &filetype ==# 'flowfile'
  call add(s:failures, 'other.yaml was detected as a flowfile')
endif
for name in ['Flowfile', 'workflow.yaml', 'a/workflows/b.yaml', 'a/c.test.yaml']
  enew
  execute 'file ' . name
  doautocmd BufRead
  if &filetype !=# 'flowfile'
    call add(s:failures, name . ' was not detected as a flowfile (' . &filetype . ')')
  endif
endfor

execute 'edit ' . fnameescape(s:root . '/test/fixture.yaml')
setfiletype flowfile
syntax sync fromstart

" must: is bare CEL, up to a trailing comment.
call s:Expect(5, 'this', 'flowfileCelRoot')
call s:Expect(5, 'size', 'flowfileCelFunction')
call s:Expect(5, 'xs', 'flowfileCelMember')
call s:Expect(5, 'why', 'yamlComment', 0)
call s:Expect(5, 'why', 'flowfileMustComment')
" description: is a literal even when it reads like an expression.
call s:Expect(6, 'this', 'flowfileCelRoot', 0)
" A fence in a plain scalar, in a double-quoted one, and nested braces in it.
call s:Expect(9, 'inputs', 'flowfileCelRoot')
call s:Expect(9, '1', 'flowfileCelNumber')
call s:Expect(11, 'size', 'flowfileCelFunction')
call s:Expect(11, "'a'", 'flowfileCelString')
call s:Expect(11, '.a}', 'flowfileFence')
" Text between fences is YAML's.
call s:Expect(12, 'plain', 'flowfileFence', 0)
" A fence-looking comment is a comment.
call s:Expect(13, 'inputs', 'flowfileFence', 0)

if empty(s:failures)
  echo 'ok'
  qall!
endif
call writefile(s:failures, '/dev/stderr')
cquit
