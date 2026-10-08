; inherits: yaml
; Flowfile injections over the stock tree-sitter-yaml parser; no YAML fork.
; A block pair and an inline `{type: int, must: this > 0}` pair (flow_pair) share
; the `key:`/`value:` fields, so `(_ ...)` matches both. Install as
; queries/flowfile/injections.scm, a language that reuses the yaml parser, so
; plain YAML files are left alone (see editors/nvim/README.md).
;
; A quoted scalar is injected only when YAML decoding would not change its text:
; an injection takes the raw range, so a backslash escape in a "double" scalar or
; a doubled quote in a 'single' one would reach the CEL parser undecoded. Those
; scalars are left to the semantic tokens. Covered: the bare value of `must:` (plain, "double" or 'single' quoted) and a
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
  value: (flow_node (double_quote_scalar) @injection.content))
  (#eq? @_key "must")
  (#not-match? @injection.content "\\\\")
  (#offset! @injection.content 0 1 0 -1)
  (#set! injection.language "cel"))

; must: 'this > 0'
((_
  key: (flow_node (plain_scalar (string_scalar) @_key))
  value: (flow_node (single_quote_scalar) @injection.content))
  (#eq? @_key "must")
  (#not-match? @injection.content "^'.+''.*'$")
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
  value: (flow_node (double_quote_scalar) @injection.content))
  (#match? @injection.content "^\"\\$\\{[^$\\\\]*\\}\"$")
  (#offset! @injection.content 0 3 0 -2)
  (#set! injection.language "cel"))

; if: '${inputs.n > 1}'
((_
  value: (flow_node (single_quote_scalar) @injection.content))
  (#match? @injection.content "^'\\$\\{[^$']*\\}'$")
  (#offset! @injection.content 0 3 0 -2)
  (#set! injection.language "cel"))
