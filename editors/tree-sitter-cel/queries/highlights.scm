; Highlights for CEL. Capture names follow the nvim-treesitter / Helix
; conventions; an editor maps them onto its theme. Names such as `inputs` and
; `steps` are plain identifiers to the parser, so scope-aware colour comes from
; `flow lsp` semantic tokens, which an editor layers over these.

(comment) @comment

(string) @string
(bytes) @string.special
(integer) @number
(unsigned_integer) @number
(float) @number.float
(boolean) @boolean
(null) @constant.builtin

(identifier) @variable

(field_access
  field: (identifier) @property)

(call
  function: (identifier) @function.call)

(method_call
  method: (identifier) @function.method.call)

(type_name
  (identifier) @type)

(field_initializer
  field: (identifier) @property)

(map_entry
  key: (string) @property)

[
  "&&"
  "||"
  "!"
  "=="
  "!="
  "<"
  "<="
  ">"
  ">="
  "+"
  "-"
  "*"
  "/"
  "%"
  "?"
  ":"
  ".?"
  "[?"
] @operator

"in" @keyword.operator

[
  "("
  ")"
  "["
  "]"
  "{"
  "}"
] @punctuation.bracket

[
  ","
  "."
] @punctuation.delimiter
