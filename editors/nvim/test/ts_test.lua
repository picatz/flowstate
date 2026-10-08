-- Run from the repository root with both parsers built (see the workflow):
--
--   TS_PARSERS=/dir/with/parser/yaml.so/and/parser/cel.so \
--     nvim --headless -u NONE -l editors/nvim/test/ts_test.lua
--
-- It asks Neovim what it parsed rather than what it painted: that the yaml
-- parser serves a flowfile buffer, that the cel parser is injected into exactly
-- the ranges editors/tree-sitter-cel/queries-flowfile/injections.scm names, and
-- that the CEL highlight query then captures something inside them. Exits
-- non-zero on the first failed expectation.
local root = vim.fn.fnamemodify(debug.getinfo(1, 'S').source:sub(2), ':p:h:h')
local parsers = assert(os.getenv('TS_PARSERS'), 'TS_PARSERS must name the parser directory')
vim.opt.runtimepath:prepend(parsers)
vim.opt.runtimepath:prepend(root)
vim.cmd('runtime plugin/flowfile.lua')

local failures = {}
local function check(ok, msg)
  if not ok then
    table.insert(failures, msg)
  end
end

-- The copies under editors/nvim are the files an editor loads; the sources live
-- with the grammar. They must not drift.
local function read(path)
  return table.concat(vim.fn.readfile(path), '\n')
end
local repo = vim.fn.fnamemodify(root, ':h:h')
check(read(root .. '/queries/flowfile/injections.scm') == read(repo .. '/editors/tree-sitter-cel/queries-flowfile/injections.scm'),
  'editors/nvim/queries/flowfile/injections.scm differs from its source')
check(read(root .. '/queries/cel/highlights.scm') == read(repo .. '/editors/tree-sitter-cel/queries/highlights.scm'),
  'editors/nvim/queries/cel/highlights.scm differs from its source')

assert(vim.treesitter.language.add('cel'), 'the cel parser is not on the runtimepath')

vim.cmd('edit ' .. vim.fn.fnameescape(root .. '/test/fixture.yaml'))
vim.bo.filetype = 'flowfile'
vim.treesitter.start(0, 'flowfile')
local parser = vim.treesitter.get_parser(0)
parser:parse(true)

local cel = parser:children().cel
check(cel ~= nil, 'no cel injection in a flowfile buffer')

local texts = {}
if cel then
  for _, tree in ipairs(cel:trees()) do
    -- Each injected tree is one expression; its root's text is the injected text.
    table.insert(texts, vim.treesitter.get_node_text(tree:root(), 0))
  end
end
table.sort(texts)
local want = {
  'inputs.n > 1',
  'inputs.t',
  'this == "a"',
  'this >= 1',
  'this > 0 && size(vars.xs) == 1',
}
table.sort(want)
check(vim.deep_equal(texts, want),
  'injected expressions: want ' .. vim.inspect(want) .. ', got ' .. vim.inspect(texts))

-- description: and a fence inside text are YAML's, not CEL.
for _, t in ipairs(texts) do
  check(not t:find('hi ', 1, true), 'a mid-text fence was injected: ' .. t)
end
check(#texts == #want, 'want ' .. #want .. ' injected expressions, got ' .. #texts)
-- Scalars whose YAML decoding differs from their raw text are not injected: they
-- would reach the CEL parser undecoded.
for _, t in ipairs(texts) do
  check(not t:find('\\', 1, true) and not t:find("''", 1, true), 'an escaped scalar was injected: ' .. t)
end
if cel then
  for _, tree in ipairs(cel:trees()) do
    check(not tree:root():has_error(), 'an injected expression does not parse: ' .. vim.treesitter.get_node_text(tree:root(), 0))
  end
end

-- The CEL highlight query reaches inside an injection.
local function captures(row, needle)
  local line = vim.api.nvim_buf_get_lines(0, row, row + 1, false)[1]
  local col = assert(line:find(needle, 1, true), needle .. ' not on line ' .. row + 1) - 1
  local names = {}
  for _, c in ipairs(vim.treesitter.get_captures_at_pos(0, row, col)) do
    names[c.capture] = true
  end
  return names
end
check(captures(4, 'size')['function.call'], 'size( is not a function call')
check(captures(4, '0').number, '0 is not a number')
check(captures(9, 'inputs')['variable'], 'inputs in a fence is not a variable')

if #failures == 0 then
  io.stdout:write('ok\n')
  vim.cmd('qall!')
end
io.stderr:write(table.concat(failures, '\n') .. '\n')
vim.cmd('cquit')
