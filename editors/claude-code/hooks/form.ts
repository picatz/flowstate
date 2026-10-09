import { isFlowfile } from './flowfile'
import { clean } from './runs'

/**
 * The run form (roadmap slice 8). Pure and bounded: the pane reads a Flowfile's
 * declared inputs from `flow compile --schema inputs` (a JSON Schema 2020-12
 * projection of the `inputs:` block, pkg/flowstate/v1/jsonschema.go) and draws
 * one control per input. Everything in that schema is data from a file: names,
 * descriptions, defaults and enum values are cleaned and bounded before they are
 * drawn, and a value the mod would have to alter to show or send is refused
 * instead. The mod checks only what the declared type alone settles; the
 * engine binds and validates the run and its message is shown as it is.
 */

/** A Flowfile list shows this many files; the rest is "and N more". */
export const MAX_FILES = 12
/** Directory entries read per listing; a bigger directory is not scanned past them. */
export const MAX_SCAN = 500
/** A form draws at most this many inputs; a workflow with more is run from a terminal. */
export const MAX_INPUTS = 24
/** One typed value, in characters; a longer one is refused rather than cut, since a cut value is a different value. */
export const MAX_VALUE = 1000
/** All values of one run together. */
export const MAX_TOTAL = 8000
/** The schema document is read up to this size (characters); a larger one is no form. */
export const MAX_SCHEMA = 262144
/** An enum offers at most this many choices. */
export const MAX_CHOICES = 50
/** The longest a run may take: `$.process.run`'s own default, so a run that outlasts it is killed and its outcome is unknown. */
export const RUN_TIMEOUT_MS = 30000
/** What a result card shows of a run's output. */
export const MAX_OUTPUT_LINES = 12
const MAX_LINE = 200
const MAX_HELP = 240
const MAX_NAME = 64

/** An input name as the mod will put it after `--input=`: an identifier, so no `=`, space or flag-looking text. */
export const INPUT_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/
/**
 * The files the form may run: a plain relative path in the working directory
 * (or its `workflows/` directory) with no leading `-` or `.`, so it can be
 * neither a flag nor a parent path. Listing membership is checked as well.
 */
export const RUN_FILE = /^(?:workflows\/)?[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$/
/** Characters `clean` would drop; a value holding one is refused, never rewritten. */
const hasHidden = (s: string): boolean => clean(s, s.length + 1) !== s

export interface Entry {
  name: string
  kind: string
}

export interface Candidates {
  files: string[]
  /** Flowfiles found but not offered: past the cap, or with a name outside RUN_FILE. */
  more: number
}

/**
 * The Flowfiles of the working directory and, when it has one, its `workflows/`
 * directory, as `$.fs.list` reported them. Only regular files count (a link is
 * `other`), `isFlowfile` decides what a Flowfile is, and a name the allowlist
 * does not admit is counted, not offered.
 */
export const candidates = (top: readonly Entry[], workflows: readonly Entry[] = []): Candidates => {
  const found = new Set<string>()
  let skipped = 0
  const scan = (entries: readonly Entry[], prefix: string) => {
    for (const e of entries.slice(0, MAX_SCAN)) {
      if (e.kind !== 'file' || typeof e.name !== 'string') continue
      const path = prefix + e.name
      if (!isFlowfile(path)) continue
      if (RUN_FILE.test(path)) found.add(path)
      else skipped++
    }
  }
  scan(top, '')
  scan(workflows, 'workflows/')
  const all = [...found].toSorted()
  return { files: all.slice(0, MAX_FILES), more: Math.max(0, all.length - MAX_FILES) + skipped }
}

export type Kind = 'bool' | 'enum' | 'string' | 'int' | 'number' | 'json'

/** One declared input as the form draws it. */
export interface Field {
  name: string
  kind: Kind
  required: boolean
  /** The declared type as a word, for the label. */
  type: string
  /** The author's description and `must:` rule, cleaned and bounded. */
  help: string
  /** The declared default as the text the control starts with; empty for none. */
  initial: string
  /** The declared example, cleaned, shown dim in an empty field. */
  example: string
  choices: string[]
  /** For `kind: json`: what the declared type requires of the document. */
  shape: 'array' | 'object' | 'any'
  minLength: number
  maxLength: number
  sensitive: boolean
  /** Why this input cannot be offered or sent as declared; empty when it can. */
  refused: string
}

export type Parsed = { fields: Field[] } | { error: string }

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const count = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) && v > 0 ? Math.trunc(v) : 0)

/** A default or example as the text a control holds, or undefined where it is not that kind of value. */
const textOf = (kind: Kind, v: unknown): string | undefined => {
  switch (kind) {
    case 'bool':
      return typeof v === 'boolean' ? String(v) : undefined
    case 'int':
      return typeof v === 'number' && Number.isInteger(v) ? String(v) : undefined
    case 'number':
      return typeof v === 'number' && Number.isFinite(v) ? String(v) : undefined
    case 'string':
    case 'enum':
      return typeof v === 'string' ? v : undefined
    default:
      return v === undefined ? undefined : JSON.stringify(v)
  }
}

const TYPE_WORD: Record<Kind, string> = { bool: 'bool', enum: 'enum', string: 'string', int: 'int', number: 'number', json: 'JSON' }

const fieldOf = (name: string, p: Record<string, unknown>, required: boolean): Field => {
  const type = typeof p.type === 'string' ? p.type : ''
  const kind: Kind = Array.isArray(p.enum)
    ? 'enum'
    : type === 'boolean'
      ? 'bool'
      : type === 'integer'
        ? 'int'
        : type === 'number'
          ? 'number'
          : type === 'string'
            ? 'string'
            : 'json'
  const shape = type === 'array' ? 'array' : type === 'object' || typeof p.$ref === 'string' ? 'object' : 'any'
  const sensitive = p['x-flowstate-sensitive'] === true
  const choices: string[] = []
  let refused = ''
  if (kind === 'enum') {
    const values = p.enum as unknown[]
    if (values.length > MAX_CHOICES) refused = `it allows more than ${MAX_CHOICES} values`
    for (const v of values.slice(0, MAX_CHOICES)) {
      if (typeof v === 'string' && v !== '' && v.length <= MAX_VALUE && !hasHidden(v)) choices.push(v)
      else refused ||= 'an allowed value cannot be shown or sent as declared'
    }
  }
  const dflt = sensitive ? undefined : textOf(kind, p.default)
  if (dflt !== undefined && (dflt.length > MAX_VALUE || hasHidden(dflt))) refused ||= 'its declared default cannot be shown or sent as declared'
  const must = typeof p['x-flowstate-must'] === 'string' ? clean(p['x-flowstate-must'], 120) : ''
  const help = [clean(p.description, MAX_HELP), must && `(rule: ${must})`].filter(Boolean).join(' ')
  const sample = sensitive || !Array.isArray(p.examples) ? undefined : textOf(kind, p.examples[0])
  const hint = p.format === 'date-time' ? 'RFC 3339, e.g. 2026-10-09T10:00:00Z' : ''
  return {
    name,
    kind,
    required,
    type: TYPE_WORD[kind],
    help,
    initial: refused ? '' : (dflt ?? ''),
    example: clean(sample, 80) || hint,
    choices,
    shape,
    minLength: count(p.minLength),
    maxLength: count(p.maxLength),
    sensitive,
    refused,
  }
}

/**
 * Reads `flow compile --schema inputs` output. Anything that is not an object
 * schema is no form at all (the pane then draws no control and no Run button),
 * and a workflow with more inputs than the form draws is refused whole, since
 * hiding an input could hide a required one. A name outside INPUT_NAME is
 * refused with a reason that blocks Run, never rewritten into another name.
 */
export const parseInputs = (stdout: string): Parsed => {
  if (stdout.length > MAX_SCHEMA) return { error: 'the schema is larger than the form reads' }
  let doc: unknown
  try {
    doc = JSON.parse(stdout)
  } catch {
    return { error: 'flow compile did not print a JSON schema' }
  }
  if (!isRecord(doc) || doc.type !== 'object') return { error: 'flow compile did not print an object schema' }
  const props = doc.properties === undefined ? {} : doc.properties
  if (!isRecord(props)) return { error: 'the schema has no readable properties' }
  const names = Object.keys(props)
  if (names.length > MAX_INPUTS) return { error: `the workflow declares ${names.length} inputs; the form draws at most ${MAX_INPUTS}` }
  const required = new Set(Array.isArray(doc.required) ? doc.required.filter((r): r is string => typeof r === 'string') : [])
  const fields = names.map(name => {
    const p = props[name]
    const f = fieldOf(name.slice(0, MAX_NAME), isRecord(p) ? p : {}, required.has(name))
    return INPUT_NAME.test(name) ? f : { ...f, name: clean(name, MAX_NAME), refused: 'its name is not a plain identifier' }
  })
  return { fields }
}

/** What the control shows and the run sends: the typed value, else the declared default. */
export const valueOf = (f: Field, values: Readonly<Record<string, string>>): string => (Object.hasOwn(values, f.name) ? values[f.name] : f.initial)

const INT = /^-?\d+$/
const NUMBER = /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/
const I64 = { min: -(2n ** 63n), max: 2n ** 63n - 1n }

/** Why `raw` cannot be a value of the declared type, or empty. The engine checks the rest (`must:`, bounds on items, records). */
export const fieldError = (f: Field, raw: string): string => {
  if (f.refused) return f.refused
  if (raw === '') return f.required && f.initial === '' ? 'required' : ''
  if (raw.length > MAX_VALUE) return `longer than ${MAX_VALUE} characters`
  if (hasHidden(raw)) return 'contains a control or invisible character, which cannot be sent as typed'
  switch (f.kind) {
    case 'bool':
      return raw === 'true' || raw === 'false' ? '' : 'must be true or false'
    case 'enum':
      return f.choices.includes(raw) ? '' : 'must be one of the allowed values'
    case 'int':
      return INT.test(raw) && BigInt(raw) >= I64.min && BigInt(raw) <= I64.max ? '' : 'must be a whole number, e.g. 3'
    case 'number':
      return NUMBER.test(raw) && Number.isFinite(Number(raw)) ? '' : 'must be a number, e.g. 1.5'
    case 'string': {
      const n = [...raw].length
      if (f.minLength > 0 && n < f.minLength) return `shorter than ${f.minLength} characters`
      if (f.maxLength > 0 && n > f.maxLength) return `longer than ${f.maxLength} characters`
      return ''
    }
    default: {
      let doc: unknown
      try {
        doc = JSON.parse(raw)
      } catch {
        return 'must be valid JSON'
      }
      if (f.shape === 'array' && !Array.isArray(doc)) return 'must be a JSON list, e.g. [1, 2]'
      if (f.shape === 'object' && !isRecord(doc)) return 'must be a JSON object, e.g. {"key": "value"}'
      return ''
    }
  }
}

export interface Checked {
  /** Per-input reasons, for the inputs that have one. */
  errors: Record<string, string>
  /** The one sentence that says why Run is unavailable; empty when it is available. */
  blocked: string
}

/** Checks every control against its declared type. A sensitive input is never collected, so a required one blocks. */
export const checkForm = (fields: readonly Field[], values: Readonly<Record<string, string>>): Checked => {
  const errors: Record<string, string> = {}
  let total = 0
  for (const f of fields) {
    if (f.sensitive) {
      if (f.required && !f.refused) errors[f.name] = 'sensitive and required: the pane never collects a sensitive value; run it from a terminal'
      continue
    }
    const raw = valueOf(f, values)
    total += raw.length
    const why = fieldError(f, raw)
    if (why) errors[f.name] = why
  }
  const first = Object.entries(errors)[0]
  const blocked = first ? `${first[0]}: ${first[1]}` : total > MAX_TOTAL ? `the values together are longer than ${MAX_TOTAL} characters` : ''
  return { errors, blocked }
}

export interface Pair {
  name: string
  value: string
}

/** What a run sends: each declared, non-sensitive input holding a value, in declared order. An empty optional input is left to the engine's default. */
export const submission = (fields: readonly Field[], values: Readonly<Record<string, string>>): Pair[] =>
  fields.flatMap(f => {
    const value = f.sensitive ? '' : valueOf(f, values)
    return value === '' ? [] : [{ name: f.name, value }]
  })

/**
 * The one argv of a local run, or undefined when anything is outside the
 * allowlist: refused, never sanitised into another run. Each input is a single
 * `--input=name=value` element (the `=` form binds the text as the flag's value
 * whatever it starts with, and `--input` is a repeatable string array, so a
 * comma or `=` in a value is not split), and the file is the one operand, after
 * `--`. There is no shell, no server address and no other flag but `--no-color`.
 */
export const runArgv = (flow: string, file: string, inputs: readonly Pair[], fields: readonly Field[]): string[] | undefined => {
  if (!RUN_FILE.test(file) || !isFlowfile(file) || inputs.length > MAX_INPUTS) return undefined
  const declared = new Set(fields.filter(f => !f.sensitive && f.refused === '').map(f => f.name))
  const seen = new Set<string>()
  let total = 0
  for (const { name, value } of inputs) {
    if (!INPUT_NAME.test(name) || !declared.has(name) || seen.has(name)) return undefined
    if (value === '' || value.length > MAX_VALUE || hasHidden(value)) return undefined
    seen.add(name)
    total += value.length
  }
  if (total > MAX_TOTAL) return undefined
  return [flow, 'run', 'local', '--no-color', ...inputs.map(i => `--input=${i.name}=${i.value}`), '--', file]
}

/** What the card asks before anything runs: the verb, the file and every value that will be sent, line by line. */
export const confirmLines = (file: string, inputs: readonly Pair[]): string[] => [
  `Run \`flow run local\` on ${clean(file, 200)}? It executes the workflow's tasks, which can have side effects.`,
  ...(inputs.length === 0 ? ['with no inputs (the declared defaults apply)'] : ['with these inputs:', ...inputs.map(i => `  ${i.name} = ${i.value}`)]),
  'Nothing runs until you confirm. A run is stopped after 30 seconds and then reported as outcome unknown.',
]

/** One of the three ways a press ends. */
export interface Result {
  kind: 'ok' | 'failed' | 'unknown' | 'notrun'
  /** The one-line account. */
  text: string
  /** The CLI's output, cleaned and bounded, one entry per line. */
  lines: string[]
}

const ANSI = /\u001b\[[0-9;?]*[ -/]*[@-~]|\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)/g

/** Output text as lines a terminal can show safely: escape sequences and controls dropped, each line and the count bounded. */
export const cleanLines = (text: string, maxLines = MAX_OUTPUT_LINES, maxLine = MAX_LINE): { lines: string[]; cut: boolean } => {
  const all = text
    .slice(0, 65536)
    .replace(ANSI, '')
    .split(/\r\n|\n|\r/)
    .map(l => l.replaceAll('\t', ' ').trimEnd())
    .filter(l => l.trim() !== '')
  const lines = all.slice(0, maxLines).map(l => clean(l, maxLine))
  return { lines, cut: all.length > maxLines || all.some(l => l.length > maxLine) || text.length > 65536 }
}

/** The answer to a finished `flow run local`: success with its output, or the engine's own refusal or failure. */
export const resultOf = (ran: { exitCode: number; stdout: string; stderr: string; isStdoutTruncated?: boolean; isStderrTruncated?: boolean }, file: string): Result => {
  if (ran.exitCode === 0) {
    const out = cleanLines(ran.stdout)
    return { kind: 'ok', text: `ran ${clean(file, 200)}${out.cut || ran.isStdoutTruncated ? ' (output cut)' : ''}`, lines: out.lines }
  }
  const err = cleanLines(ran.stderr.trim() === '' ? ran.stdout : ran.stderr)
  const lines = err.lines[0] === 'ERROR' ? err.lines.slice(1) : err.lines
  return {
    kind: 'failed',
    text: `failed (exit ${Math.trunc(ran.exitCode)}) ${clean(file, 200)}${err.cut || ran.isStderrTruncated ? ' (output cut)' : ''}`,
    lines: lines.length > 0 ? lines : ['flow printed no message'],
  }
}

/** A run that threw or outlasted its time proves nothing: its tasks may have started, finished or half-finished. */
export const unknownResult = (err: unknown, file: string): Result => ({
  kind: 'unknown',
  text: `outcome unknown for ${clean(file, 200)}: ${clean(String(err), 100) || 'no answer'}`,
  lines: ['The run may have started, finished or stopped part way, and its tasks may have had effects. Check them before running again.'],
})
