import { MAX_SCHEMA } from './form'
import { clean } from './runs'
import { statusFor } from './vocab'
import type { Status } from './vocab'

/**
 * Output cards (roadmap slice 9). Pure and bounded: the declared outputs come
 * from `flow compile --schema=outputs` (a JSON Schema projection of the
 * `outputs:` block, pkg/flowstate/v1/jsonschema.go) and their values from
 * `.runOutputs` of the document `flow run local` writes to stdout. Both are
 * data from a file or a run, so every name and value is cleaned and bounded
 * before it is drawn, and a sensitive output is never read, drawn or kept.
 * Nothing here runs anything: it renders the result of a run already confirmed.
 */

/** A run shows at most this many cards; the rest is "and N more". */
export const MAX_CARDS = 24
/** One card's raw value, in characters. */
export const MAX_RAW = 1000
/** The one fact a card leads with. */
export const MAX_FACT = 160
/** All cards' raw values together. */
export const MAX_TOTAL_RAW = 6000
/** Nested lists and objects are shown this deep; deeper is `…`. */
export const MAX_DEPTH = 5
/** Items or keys shown per list or object. */
export const MAX_ITEMS = 20
/** The run document is read up to this size (characters); a larger one is no cards. */
export const MAX_RESULT = 65536
const MAX_NAME = 64

/** Words for the schema's types, in the vocabulary the Flowfile uses. */
const TYPE_WORD: Record<string, string> = { string: 'string', integer: 'int', number: 'number', boolean: 'bool', array: 'list', object: 'map' }

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
/** An own property's value; a `__proto__` key parsed from JSON is an own property, and an inherited one is never read. */
const own = (o: object, key: string): unknown => Object.getOwnPropertyDescriptor(o, key)?.value

/** One declared output. `key` is the name as the run document spells it; it is only ever used to look a value up. */
export interface Spec {
  key: string
  title: string
  type: string
  help: string
  sensitive: boolean
}

export type Declared = { specs: Spec[]; more: number } | { error: string }

/** Reads `flow compile --schema=outputs`. Anything but an object schema is no cards; outputs past the cap are counted, not drawn. */
export const parseOutputs = (stdout: string): Declared => {
  if (stdout.length > MAX_SCHEMA) return { error: 'the schema is larger than the cards read' }
  let doc: unknown
  try {
    doc = JSON.parse(stdout)
  } catch {
    return { error: 'flow compile did not print a JSON schema' }
  }
  if (!isRecord(doc) || doc.type !== 'object') return { error: 'flow compile did not print an object schema' }
  const props = doc.properties === undefined ? {} : doc.properties
  if (!isRecord(props)) return { error: 'the schema has no readable properties' }
  const keys = Object.keys(props)
  const specs = keys.slice(0, MAX_CARDS).map((key): Spec => {
    const p = isRecord(own(props, key)) ? (own(props, key) as Record<string, unknown>) : {}
    const type = Array.isArray(p.enum) ? 'enum' : typeof p.type === 'string' ? (TYPE_WORD[p.type] ?? 'value') : typeof p.$ref === 'string' ? 'record' : 'value'
    return {
      key,
      title: clean(typeof p.title === 'string' ? p.title : key, MAX_NAME),
      type,
      help: clean(p.description, 120),
      sensitive: p['x-flowstate-sensitive'] === true,
    }
  })
  return { specs, more: Math.max(0, keys.length - MAX_CARDS) }
}

/** Stands in the parsed run document for a number, which keeps its own digits: a double would round 9007199254740993. */
const NUM = '\u0000n:'
const TOKENS = /"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g

/** `.runOutputs` of the run document, numbers as their original text; undefined for anything else. */
const readOutputs = (stdout: string): Record<string, unknown> | undefined => {
  // A NUL in the text could forge a number marker, so such a document is no cards.
  if (stdout.length > MAX_RESULT || stdout.includes('\u0000') || /\\u0000/i.test(stdout)) return undefined
  try {
    const doc: unknown = JSON.parse(stdout.replace(TOKENS, t => (t[0] === '"' ? t : JSON.stringify(NUM + t))))
    if (!isRecord(doc)) return undefined
    const out = own(doc, 'runOutputs')
    return out === undefined ? {} : isRecord(out) ? out : undefined
  } catch {
    return undefined
  }
}

interface Budget {
  /** Characters this value may still use. */
  left: number
  /** The shown value differs from the real one: cut short or cleaned. */
  cut: boolean
}

/** A value as compact JSON, bounded in depth, width and size, with numbers as written. */
const show = (v: unknown, depth: number, b: Budget): string => {
  if (b.left <= 0) {
    b.cut = true
    return '…'
  }
  let s: string
  if (typeof v === 'string') {
    if (v.startsWith(NUM)) {
      const tok = v.slice(NUM.length)
      if (tok.length > MAX_RAW) b.cut = true
      s = tok.slice(0, MAX_RAW)
    }
    else {
      const c = clean(v, MAX_RAW)
      if (c !== v) b.cut = true
      s = JSON.stringify(c)
    }
  } else if (v === null || typeof v === 'boolean') s = String(v)
  else if (depth >= MAX_DEPTH) {
    b.cut = true
    s = '…'
  } else {
    const keys = isRecord(v) ? Object.keys(v) : []
    const items: unknown[] = Array.isArray(v) ? v : keys
    if (items.length > MAX_ITEMS) b.cut = true
    const parts = items.slice(0, MAX_ITEMS).map(x => (Array.isArray(v) ? show(x, depth + 1, b) : `${JSON.stringify(clean(x, MAX_NAME))}:${show(own(v as object, x as string), depth + 1, b)}`))
    if (items.length > MAX_ITEMS) parts.push('…')
    s = Array.isArray(v) ? `[${parts.join(',')}]` : `{${parts.join(',')}}`
  }
  b.left -= s.length
  if (s.length > MAX_RAW) {
    b.cut = true
    s = s.slice(0, MAX_RAW)
  }
  return s
}

export interface Card {
  title: string
  type: string
  help: string
  /** The symbol and word that carry the card's state; colour only repeats it. */
  status: Status
  /** The one key value, a line. */
  fact: string
  /** The whole value as compact JSON; the same bounds apply. */
  raw: string
  /** Fact or raw is not the real value: cut short or cleaned. */
  cut: boolean
}

export interface Cards {
  cards: Card[]
  /** Declared outputs past the cap, not drawn. */
  more: number
}

const HIDDEN = 'hidden (sensitive)'

/**
 * The cards of one finished run: one per declared output, in declared order. A
 * sensitive output is never looked up. A document that is not the run document
 * is an error, never cards guessed from it.
 */
export const cardsOf = (declared: Declared, stdout: string, truncated = false): Cards | { error: string } => {
  if ('error' in declared) return declared
  // Nothing declared, nothing to draw: the run's own output stands as it is.
  if (declared.specs.length === 0) return { cards: [], more: declared.more }
  const values = truncated ? undefined : readOutputs(stdout)
  if (values === undefined) return { error: 'the run printed a document the cards cannot read' }
  let total = MAX_TOTAL_RAW
  const cards = declared.specs.map((s): Card => {
    const base = { title: s.title, type: s.type, help: s.help, cut: false }
    if (s.sensitive) return { ...base, status: statusFor('skipped', 'hidden'), fact: HIDDEN, raw: HIDDEN }
    if (!Object.hasOwn(values, s.key)) return { ...base, status: statusFor('unknown', 'not reported'), fact: 'not reported by this run', raw: 'not reported' }
    const v = own(values, s.key)
    const cap = Math.min(MAX_RAW, Math.max(0, total))
    const b: Budget = { left: cap, cut: false }
    let raw = show(v, 0, b)
    // A string is cut by clean() alone, so the shared budget is enforced on the result.
    if (raw.length > cap) {
      raw = raw.slice(0, cap)
      b.cut = true
    }
    total -= raw.length
    const plain = typeof v === 'string' && !v.startsWith(NUM)
    const full = plain ? clean(v, MAX_FACT + 1) : raw
    return {
      ...base,
      status: statusFor('succeeded', 'reported'),
      fact: full.slice(0, MAX_FACT),
      raw,
      cut: b.cut || full.length > MAX_FACT || (plain && clean(v, v.length + 1) !== v),
    }
  })
  return { cards, more: declared.more }
}

const NOTE = ' (cut or cleaned)'

/** The plain-text form: the same facts as the cards, one line each (`raw` for the whole values). */
export const cardLines = (c: Cards, raw = false): string[] => [
  ...c.cards.map(k => (raw ? `${k.title} = ${k.raw}` : `${k.status.symbol} ${k.title} (${k.type}): ${k.fact}`) + (k.cut ? NOTE : '')),
  ...(c.more > 0 ? [`and ${c.more} more declared outputs not shown`] : []),
]
