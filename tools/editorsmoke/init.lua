-- The Neovim configuration docs/EDITORS.md tells a reader to write, kept here as
-- a file so CI can run the thing the document claims. probe.lua asserts that the
-- fenced Lua block in docs/EDITORS.md is this file byte for byte, so the two
-- cannot drift the way a copied command list does.
--
-- Everything between the BEGIN and END markers is what appears in the document.

-- FLOWSTATE-DOC-BEGIN nvim-builtin
-- Give Flowfiles their own filetype, so the server is not attached to every YAML
-- file you open. These patterns are what decide where you get diagnostics.
vim.filetype.add({
  filename = {
    ['Flowfile'] = 'flowfile',
    ['Flowfile.yaml'] = 'flowfile',
    ['workflow.yaml'] = 'flowfile',
    ['workflow.yml'] = 'flowfile',
  },
  pattern = {
    ['.*/workflows/.*%.ya?ml'] = 'flowfile',
  },
})

vim.lsp.config.flowstate = {
  cmd = { 'flow', 'lsp' },
  filetypes = { 'flowfile' },
  root_markers = { 'go.mod', '.git' },
}

vim.lsp.enable('flowstate')

-- Flowfiles are YAML, so keep YAML's indentation and comment rules.
vim.api.nvim_create_autocmd('FileType', {
  pattern = 'flowfile',
  callback = function()
    vim.bo.commentstring = '# %s'
    vim.bo.expandtab = true
    vim.bo.shiftwidth = 2
  end,
})

vim.api.nvim_create_autocmd('LspAttach', {
  callback = function(args)
    local client = vim.lsp.get_client_by_id(args.data.client_id)
    if not client or client.name ~= 'flowstate' then
      return
    end
    local opts = { buffer = args.buf }
    vim.keymap.set('n', 'gd', vim.lsp.buf.definition, opts)
    vim.keymap.set('n', 'gO', vim.lsp.buf.document_symbol, opts)
    vim.keymap.set('n', '<leader>a', vim.lsp.buf.code_action, opts)
    -- Completion is not automatic unless you ask for it.
    vim.lsp.completion.enable(true, args.data.client_id, args.buf, { autotrigger = true })
  end,
})
-- FLOWSTATE-DOC-END nvim-builtin
