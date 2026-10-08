; Flowfile injections over the stock tree-sitter-yaml parser; no YAML fork.
; A block pair and an inline `{type: int, must: this > 0}` pair (flow_pair) share
; the `key:`/`value:` fields, so `(_ ...)` matches both. Install as queries/yaml/injections.scm with `; extends` (see README.md).
;
; Covered: the bare value of `must:` (plain, "double" or 'single' quoted) and a
; scalar that is exactly one ${...} fence. A fence in the middle of text
; ("hi ${x}") cannot be expressed here, because an injection takes a node's
; whole range or a fixed offset from it; `flow lsp` semantic tokens colour those.

; must: this > 0
((_
  key: (flow_node (plain_scalar (string_scalar) @_key))
  value: (flow_node (plain_scalar (string_scalar) @injection.content)))
  (#eq? @_key "must")
  (#set! injection.language "cel"))

; must: "this > 0"
((_
  key: (flow_node (plain_scalar (string_scalar) @_key))
  value: (flow_node [(double_quote_scalar) (single_quote_scalar)] @injection.content))
  (#eq? @_key "must")
  (#offset! @injection.content 0 1 0 -1)
  (#set! injection.language "cel"))

; if: ${inputs.n > 1}
((_
  value: (flow_node (plain_scalar (string_scalar) @injection.content)))
  (#match? @injection.content "^\\$\\{[^$]*\\}$")
  (#offset! @injection.content 0 2 0 -1)
  (#set! injection.language "cel"))

; if: "${inputs.n > 1}"
((_
  value: (flow_node [(double_quote_scalar) (single_quote_scalar)] @injection.content))
  (#match? @injection.content "^[\"']\\$\\{[^$]*\\}[\"']$")
  (#offset! @injection.content 0 3 0 -2)
  (#set! injection.language "cel"))
