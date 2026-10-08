-- Flowfiles are YAML, but the CEL injections must not reach plain YAML files, so
-- a Flowfile gets a language of its own that reuses the yaml parser. Neovim looks
-- queries up by language, which is what makes queries/flowfile/ apply here and
-- nowhere else. The yaml and cel parsers must be on the runtimepath (parser/yaml.so,
-- parser/cel.so); see editors/nvim/README.md.
local yaml = vim.api.nvim_get_runtime_file('parser/yaml.*', false)[1]
if yaml then
  vim.treesitter.language.add('flowfile', { path = yaml, symbol_name = 'yaml' })
  vim.treesitter.language.register('flowfile', 'flowfile')
end
