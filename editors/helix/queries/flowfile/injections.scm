; inherits: yaml
; CEL inside a Flowfile, for Helix. UNVERIFIED: written from Helix's documented
; injection directives, not run in an editor (Helix has no headless mode). The
; Neovim twin in editors/nvim/queries/flowfile/injections.scm is the tested one.
;
; Only the bare value of `must:` is injected. The Neovim query also injects
; quoted `must:` values and a scalar that is exactly one ${...} fence, but those
; need `#offset!` to skip the quotes and the fence's `${`/`}`, and whether Helix
; honours that directive is the part that could not be checked here. Add them
; once a Helix session confirms it; `flow lsp` semantic tokens colour the rest.

; must: this > 0
((_
  key: (flow_node (plain_scalar (string_scalar) @_key))
  value: (flow_node (plain_scalar (string_scalar) @injection.content)))
  (#eq? @_key "must")
  (#set! injection.language "cel"))
