/**
 * Tree-sitter grammar for the Common Expression Language as Flowstate writes
 * it: the bare value of `must:` and the inside of a `${...}` fence. It follows
 * the CEL language definition's grammar
 * (https://github.com/google/cel-spec/blob/master/doc/langdef.md#syntax):
 * precedence from the conditional down to member access, optional field and
 * index syntax, message construction, and the four literal families.
 *
 * It parses text, not meaning. `inputs`, `steps` and the macros are ordinary
 * identifiers here; `flow lsp` knows what they mean and says so with semantic
 * tokens. Keep this file the syntax of CEL and nothing Flowstate-specific.
 */

/** Comma-separated list with an optional trailing comma. */
const commaSep = (rule) => optional(seq(rule, repeat(seq(',', rule)), optional(',')));

const PREC = {
  ternary: 1,
  or: 2,
  and: 3,
  relation: 4,
  add: 5,
  mul: 6,
  unary: 7,
  member: 8,
};

module.exports = grammar({
  name: 'cel',

  extras: ($) => [/\s/, $.comment],

  word: ($) => $.identifier,

  // `a.b{` is a message type; `a.b` alone is field access. The parser forks at
  // the identifier and keeps whichever alternative survives.
  conflicts: ($) => [[$._primary, $.type_name]],

  rules: {
    source_file: ($) => $._expression,

    comment: (_) => token(seq('//', /[^\n]*/)),

    _expression: ($) =>
      choice(
        $.conditional,
        $.binary_expression,
        $.unary_expression,
        $._member,
      ),

    conditional: ($) =>
      prec.right(
        PREC.ternary,
        seq($._expression, '?', $._expression, ':', $._expression),
      ),

    binary_expression: ($) => {
      const table = [
        [PREC.or, '||'],
        [PREC.and, '&&'],
        [PREC.relation, choice('<', '<=', '>=', '>', '==', '!=', 'in')],
        [PREC.add, choice('+', '-')],
        [PREC.mul, choice('*', '/', '%')],
      ];
      return choice(
        ...table.map(([p, op]) =>
          prec.left(p, seq(field('left', $._expression), field('operator', op), field('right', $._expression))),
        ),
      );
    },

    // CEL allows a chain of unary operators: !!x, --x.
    unary_expression: ($) =>
      prec(PREC.unary, seq(field('operator', choice('!', '-')), field('operand', $._expression))),

    _member: ($) =>
      choice(
        $.field_access,
        $.method_call,
        $.index_access,
        $._primary,
      ),

    field_access: ($) =>
      prec.left(
        PREC.member,
        seq(field('object', $._member), field('operator', choice('.', '.?')), field('field', $.identifier)),
      ),

    method_call: ($) =>
      prec.left(
        PREC.member,
        seq(
          field('object', $._member),
          '.',
          field('method', $.identifier),
          field('arguments', $.arguments),
        ),
      ),

    index_access: ($) =>
      prec.left(
        PREC.member,
        seq(field('object', $._member), choice('[', '[?'), field('index', $._expression), ']'),
      ),

    arguments: ($) => seq('(', commaSep($._expression), ')'),

    _primary: ($) =>
      choice(
        $.call,
        $.message,
        $.identifier,
        $._literal,
        $.list,
        $.map,
        $.parenthesized,
      ),

    // A leading dot makes an identifier or call resolve from the root scope.
    call: ($) => seq(optional('.'), field('function', $.identifier), field('arguments', $.arguments)),

    parenthesized: ($) => seq('(', $._expression, ')'),

    list: ($) => seq('[', commaSep(seq(optional('?'), $._expression)), ']'),

    map: ($) => seq('{', commaSep($.map_entry), '}'),

    map_entry: ($) =>
      seq(optional('?'), field('key', $._expression), ':', field('value', $._expression)),

    // Message construction: pkg.Type{field: value}. The type name is a dotted
    // identifier, so it is parsed as one rather than as field access.
    message: ($) =>
      seq(
        field('type', $.type_name),
        '{',
        commaSep($.field_initializer),
        '}',
      ),

    type_name: ($) => seq(optional('.'), $.identifier, repeat(seq('.', $.identifier))),

    field_initializer: ($) =>
      seq(optional('?'), field('field', $.identifier), ':', field('value', $._expression)),

    _literal: ($) =>
      choice($.integer, $.unsigned_integer, $.float, $.string, $.bytes, $.boolean, $.null),

    identifier: (_) => /[a-zA-Z_][a-zA-Z0-9_]*/,

    integer: (_) => token(choice(/0[xX][0-9a-fA-F]+/, /[0-9]+/)),

    unsigned_integer: (_) => token(choice(/0[xX][0-9a-fA-F]+[uU]/, /[0-9]+[uU]/)),

    float: (_) =>
      token(
        choice(
          /[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?/,
          /[0-9]+[eE][+-]?[0-9]+/,
          /\.[0-9]+([eE][+-]?[0-9]+)?/,
        ),
      ),

    boolean: (_) => choice('true', 'false'),

    null: (_) => 'null',

    // Strings: single or double quoted, triple-quoted, each optionally raw.
    string: ($) => choice($._string_quoted, $._string_raw),
    bytes: ($) => choice($._bytes_quoted, $._bytes_raw),

    _string_quoted: (_) =>
      token(
        choice(
          seq('"""', /([^"\\]|\\[\s\S]|"[^"]|""[^"])*/, '"""'),
          seq("'''", /([^'\\]|\\[\s\S]|'[^']|''[^'])*/, "'''"),
          seq('"', repeat(choice(/[^"\\\n]/, /\\./)), '"'),
          seq("'", repeat(choice(/[^'\\\n]/, /\\./)), "'"),
        ),
      ),

    _string_raw: (_) =>
      token(
        choice(
          seq(/[rR]/, '"""', /([^"]|"[^"]|""[^"])*/, '"""'),
          seq(/[rR]/, "'''", /([^']|'[^']|''[^'])*/, "'''"),
          seq(/[rR]/, '"', /[^"\n]*/, '"'),
          seq(/[rR]/, "'", /[^'\n]*/, "'"),
        ),
      ),

    _bytes_quoted: (_) =>
      token(
        choice(
          seq(/[bB]/, '"""', /([^"\\]|\\[\s\S]|"[^"]|""[^"])*/, '"""'),
          seq(/[bB]/, "'''", /([^'\\]|\\[\s\S]|'[^']|''[^'])*/, "'''"),
          seq(/[bB]/, '"', repeat(choice(/[^"\\\n]/, /\\./)), '"'),
          seq(/[bB]/, "'", repeat(choice(/[^'\\\n]/, /\\./)), "'"),
        ),
      ),

    _bytes_raw: (_) =>
      token(
        choice(
          seq(/([bB][rR]|[rR][bB])/, '"""', /([^"]|"[^"]|""[^"])*/, '"""'),
          seq(/([bB][rR]|[rR][bB])/, "'''", /([^']|'[^']|''[^'])*/, "'''"),
          seq(/([bB][rR]|[rR][bB])/, '"', /[^"\n]*/, '"'),
          seq(/([bB][rR]|[rR][bB])/, "'", /[^'\n]*/, "'"),
        ),
      ),
  },
});
