-- A headless drive of `flow lsp` through Neovim's built-in LSP client, so the
-- setup instructions in docs/EDITORS.md are a thing CI runs rather than a thing
-- somebody wrote down once.
--
--     nvim --clean --headless -u tools/editorsmoke/init.lua \
--         -l tools/editorsmoke/probe.lua
--
-- It drives a real editor rather than speaking JSON-RPC at the server directly,
-- because what is under test is the *configuration* — the filetype patterns, the
-- `vim.lsp.config` entry, the `LspAttach` wiring. A hand-rolled JSON-RPC client
-- would prove the server works and say nothing about the document.
--
-- `flow` must be on PATH. Exits non-zero on the first failed check.

local failures = 0
local checks = 0

local function fail(name, detail)
  failures = failures + 1
  io.stderr:write(('  FAIL  %s\n        %s\n'):format(name, detail or ''))
end

local function ok(name, detail)
  io.stdout:write(('  ok    %s%s\n'):format(name, detail and ('  — ' .. detail) or ''))
end

local function check(name, cond, detail)
  checks = checks + 1
  if cond then
    ok(name, detail)
  else
    fail(name, detail)
  end
  return cond
end

local function section(name)
  io.stdout:write(('\n%s\n'):format(name))
end

-- The repository root, taken from this script's own path so the probe can be run
-- from anywhere. `<sfile>` is not set under `nvim -l`, so ask Lua instead.
local this = debug.getinfo(1, 'S').source:gsub('^@', '')
local root = vim.fn.fnamemodify(vim.fn.fnamemodify(this, ':p'), ':h:h:h')

local function read(path)
  local fd = io.open(path, 'r')
  if not fd then
    return nil
  end
  local body = fd:read('*a')
  fd:close()
  return body
end

-- 1. The document and the file CI runs must be the same text.
--
-- docs/EDITORS.md quotes this configuration in a fenced block. A quoted block is
-- a copy, and a copy drifts, so the copy is checked rather than trusted.
section('docs/EDITORS.md quotes tools/editorsmoke/init.lua verbatim')
do
  local lua = read(root .. '/tools/editorsmoke/init.lua')
  local doc = read(root .. '/docs/EDITORS.md')
  if not check('both files readable', lua ~= nil and doc ~= nil, root) then
    os.exit(1)
  end
  -- The markers themselves stay out of the document — what has to match is the
  -- configuration between them.
  local marked = lua:match('%-%- FLOWSTATE%-DOC%-BEGIN nvim%-builtin\n(.-)%-%- FLOWSTATE%-DOC%-END nvim%-builtin')
  check('init.lua carries the doc markers', marked ~= nil)
  if marked then
    check(
      'the fenced block in docs/EDITORS.md matches it byte for byte',
      doc:find(vim.trim(marked), 1, true) ~= nil,
      'if this fails, copy the marked region of tools/editorsmoke/init.lua into the ```lua block under "Neovim 0.11+, no plugin"'
    )
  end
end

-- 2. The filetype patterns the document names actually classify a Flowfile.
section('vim.filetype.add classifies Flowfiles')
do
  for _, name in ipairs({ 'Flowfile', 'Flowfile.yaml', 'workflow.yaml', 'workflow.yml' }) do
    local ft = vim.filetype.match({ filename = '/repo/' .. name })
    check(('%s -> flowfile'):format(name), ft == 'flowfile', tostring(ft))
  end
  local nested = vim.filetype.match({ filename = '/repo/workflows/nightly.yaml' })
  check('workflows/nightly.yaml -> flowfile', nested == 'flowfile', tostring(nested))
  local manifest = vim.filetype.match({ filename = '/repo/deploy/kustomization.yaml' })
  check('deploy/kustomization.yaml is left alone', manifest ~= 'flowfile', tostring(manifest))
end

local fixtures = root .. '/tools/editorsmoke/fixtures/workflows'

--- Open a fixture and wait for the flowstate client to attach.
local function open(name)
  vim.cmd.edit(vim.fn.fnameescape(fixtures .. '/' .. name))
  local buf = vim.api.nvim_get_current_buf()
  local client
  local attached = vim.wait(30000, function()
    client = vim.lsp.get_clients({ bufnr = buf, name = 'flowstate' })[1]
    return client ~= nil and client.initialized
  end, 100)
  return buf, client, attached
end

--- Find a 0-based (line, col) for the first occurrence of `needle`.
local function locate(buf, needle, offset)
  for i, line in ipairs(vim.api.nvim_buf_get_lines(buf, 0, -1, false)) do
    local at = line:find(needle, 1, true)
    if at then
      return i - 1, at - 1 + (offset or 0)
    end
  end
  return nil
end

-- 3. initialize: the server attaches and says what it can do.
section('initialize')
local buf, client, attached = open('broken.yaml')
if not check('flow lsp attached to the buffer', attached, client and client.name or 'no client') then
  os.exit(1)
end
check('the buffer is a flowfile', vim.bo[buf].filetype == 'flowfile', vim.bo[buf].filetype)
do
  local caps = client.server_capabilities or {}
  for _, cap in ipairs({
    'hoverProvider',
    'completionProvider',
    'definitionProvider',
    'documentSymbolProvider',
    'documentFormattingProvider',
    'codeActionProvider',
  }) do
    local advertised = caps[cap]
    check(
      ('advertises %s'):format(cap),
      advertised ~= nil and advertised ~= false,
      (vim.inspect(advertised):gsub('%s+', ' '))
    )
  end
end

-- 4. textDocument/publishDiagnostics on a file with a deliberate mistake.
section('textDocument/publishDiagnostics')
do
  local got = vim.wait(30000, function()
    return #vim.diagnostic.get(buf) > 0
  end, 100)
  local diags = vim.diagnostic.get(buf)
  if check('a diagnostic was published for broken.yaml', got, ('%d diagnostics'):format(#diags)) then
    local d = diags[1]
    io.stdout:write(('        %d:%d  [%s] %s\n'):format(d.lnum + 1, d.col + 1, tostring(d.code), d.message))
    check('it is an error', d.severity == vim.diagnostic.severity.ERROR, tostring(d.severity))
    -- The code is the validator's own, not a single spelling the server
    -- overwrites them all with — see the comment on diagnosticSource in
    -- flowfile/lsp/diagnostics.go, and docs/reference/diagnostics.md for the set.
    check('it carries the validator code for the class', d.code == 'unknown-task', tostring(d.code))
    check('it names the unknown task', d.message:lower():find('lag', 1, true) ~= nil, d.message)
    local lnum, col = locate(buf, 'lag:')
    check(
      'it is positioned on the task name, not the file',
      d.lnum == lnum and d.col == col,
      ('diagnostic at %d:%d, `lag:` at %d:%d'):format(d.lnum, d.col, lnum or -1, col or -1)
    )
  end
end

-- A file with nothing wrong with it must draw nothing. Without this, a server
-- that complained about every document would pass the check above.
section('a clean Flowfile draws no diagnostic')
local goodbuf, goodclient = open('good.yaml')
do
  check('flow lsp attached to good.yaml', goodclient ~= nil)
  -- There is no event for "the server decided there was nothing to say", so
  -- settle for a bounded wait and then assert the buffer is clean.
  vim.wait(3000)
  local diags = vim.diagnostic.get(goodbuf)
  check('good.yaml is clean', #diags == 0, vim.inspect(vim.tbl_map(function(d)
    return d.message
  end, diags)))
end

local function request(method, params)
  local res = goodclient:request_sync(method, params, 10000, goodbuf)
  if not res then
    return nil, 'no response within 10s'
  end
  if res.err then
    return nil, vim.inspect(res.err)
  end
  return res.result
end

-- 5. textDocument/hover over a task name.
section('textDocument/hover')
do
  local lnum, col = locate(goodbuf, 'log:')
  local result, err = request('textDocument/hover', {
    textDocument = vim.lsp.util.make_text_document_params(goodbuf),
    position = { line = lnum, character = col },
  })
  if check('the server answered hover over `log`', result ~= nil, err) then
    local value = result.contents and (result.contents.value or result.contents[1]) or ''
    io.stdout:write('        ' .. (value:gsub('\n', '\n        ')) .. '\n')
    local function has(needle)
      return value:lower():find(needle, 1, true) ~= nil
    end
    check('it names the task', has('task `log`'))
    check('it carries the summary from the registry', has('emit a message for a person to read'))
    check('it carries the typed signature', has('message') and has('string') and has('required'))
  end
end

-- 6. textDocument/completion inside an expression.
section('textDocument/completion')
do
  -- Immediately after the `${` of `message: ${vars.greeting}` — a bare position,
  -- where EDITORS.md says the bindings, `steps` and `vars` are offered.
  local lnum, col = locate(goodbuf, '${', 2)
  local result, err = request('textDocument/completion', {
    textDocument = vim.lsp.util.make_text_document_params(goodbuf),
    position = { line = lnum, character = col },
    context = { triggerKind = 1 },
  })
  local items = result and (result.items or result) or nil
  if check('the server answered completion inside `${`', items ~= nil and #items > 0, err) then
    local labels = {}
    for _, item in ipairs(items) do
      labels[item.label] = true
    end
    io.stdout:write(('        %d items, including: %s\n'):format(
      #items,
      table.concat(vim.list_slice(vim.tbl_map(function(i)
        return i.label
      end, items), 1, 8), ', ')
    ))
    check('offers `vars`, because the file declares some', labels['vars'] == true)
    check('offers `steps`', labels['steps'] == true)
  end

  -- Where a step's keys go, the answer comes from the task registry instead —
  -- a different level of the walk, answered from a different place.
  local klnum, kcol = locate(goodbuf, 'log:')
  local result2, err2 = request('textDocument/completion', {
    textDocument = vim.lsp.util.make_text_document_params(goodbuf),
    position = { line = klnum, character = kcol },
    context = { triggerKind = 1 },
  })
  local items2 = result2 and (result2.items or result2) or nil
  if check('the server answered completion where a step\'s keys go', items2 ~= nil and #items2 > 0, err2) then
    local labels = {}
    for _, item in ipairs(items2) do
      labels[item.label] = true
    end
    io.stdout:write(('        %d items, including: %s\n'):format(
      #items2,
      table.concat(vim.list_slice(vim.tbl_map(function(i)
        return i.label
      end, items2), 1, 8), ', ')
    ))
    check('offers the tasks the registry knows', labels['log'] == true and labels['http'] == true)
    check('offers the keys the grammar knows', labels['id'] == true and labels['if'] == true)
  end
end

-- 7. textDocument/documentSymbol, the outline the document promises.
section('textDocument/documentSymbol')
do
  local result, err = request('textDocument/documentSymbol', {
    textDocument = vim.lsp.util.make_text_document_params(goodbuf),
  })
  if check('the server answered documentSymbol', result ~= nil and #result > 0, err) then
    local names = vim.tbl_map(function(s)
      return s.name
    end, result)
    io.stdout:write('        ' .. table.concat(names, ', ') .. '\n')
    check('both steps are in the outline', vim.tbl_contains(names, 'greet') and vim.tbl_contains(names, 'shout'), table.concat(names, ', '))
  end
end

io.stdout:write(('\n%d checks, %d failed\n'):format(checks, failures))
os.exit(failures == 0 and 0 or 1)
