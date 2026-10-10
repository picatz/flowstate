import { RUN_FILE } from './form'
import { isFlowfile } from './flowfile'
import { clean } from './runs'

/**
 * The pane's Graph section: one Flowfile's steps, read from `flow graph -o json
 * --workflow <name> -- <file>`, which prints the schema's `flowstate.v1.Graph`
 * (nodes with `id`, `kind`, `label`, `address`, `detail`; edges with `from`,
 * `to`, `kind`, `count`; `partial`; `notes`). It is the one graph model the CLI,
 * `flow explore` and the debugger share, so nothing here builds a second one: a
 * row, its depth and its order are what the document states and nothing more.
 * The document says what is declared, not what ran, so every row carries the
 * neutral not-run mark and no status. All of it is another party's text (labels
 * and details are the author's): cleaned, and bounded where it is spent.
 */

/** Output larger than this is not parsed; a step graph of a sane workflow is a few KiB. */
export const MAX_STDOUT = 256 * 1024
export const MAX_NODES = 200
export const MAX_EDGES = 400
/** The CLI's own notes shown; the view's own truncation notes and the "more notes" line are always shown besides. */
export const MAX_NOTES = 5
const MAX_NOTE = 160
const MAX_LABEL = 60
const MAX_DETAIL = 80
/** Nesting deeper than this is drawn at this depth; the schema's steps nest a few levels, not dozens. */
export const MAX_DEPTH = 8
/** A graph read is stopped after this long and reported as unreadable. */
export const GRAPH_TIMEOUT_MS = 10000

/** The mark of a step nothing has run: the structure is declared, no run is overlaid. */
export const NOT_RUN = '○'

/** A workflow's declared name: the schema's grammar (`^[A-Za-z0-9-_]+$`, 1 to 128). It may start with `-`, so it travels as `--workflow=<name>`. */
export const WORKFLOW_NAME = /^[A-Za-z0-9_-]{1,128}$/

/**
 * The declared name from a Flowfile's text: the first top-level `name:` line,
 * plain or quoted. The file is read only for this; `flow graph` is what checks
 * it (a wrong name is its "no workflow named" refusal). Undefined when there is
 * none, or when it is not a name `--workflow` may be handed.
 */
export const workflowName = (text: string): string | undefined => {
  const m = /^name:[ \t]*(["']?)([^\s"'#]+)\1[ \t]*(?:#.*)?$/m.exec(text.slice(0, 65536))
  return m !== null && WORKFLOW_NAME.test(m[2]) ? m[2] : undefined
}

/**
 * The argv that reads one workflow's steps. `--` precedes the file and the name
 * is checked against the schema's pattern, so neither can be a flag. Undefined
 * unless the file is a plain Flowfile path (the run form's own allowlist).
 */
export const graphArgv = (flow: string, file: string, name: string): string[] | undefined => {
  if (!RUN_FILE.test(file) || !isFlowfile(file) || !WORKFLOW_NAME.test(name)) return undefined
  return [flow, 'graph', '-o', 'json', `--workflow=${name}`, '--', file]
}

/** One line of the drawn graph. */
export interface Row {
  /** Nesting under the workflow: 0 for the workflow itself, 1 for its steps. */
  depth: number
  /** The node's kind word (`workflow`, `step`, `task`, `signal`, or `node` for one this view does not know). */
  kind: string
  label: string
  /** What the step does, in the debugger's words; empty for none. */
  detail: string
}

export interface Graph {
  rows: Row[]
  /** The document says it is missing something, or this view cut it at a bound; `notes` says why. */
  partial: boolean
  notes: string[]
  /** The rows follow the edges' nesting; false when they could only be put in the document's node order. */
  nested: boolean
}

/** A graph that is shown, or the one reason it is not. */
export type Parsed = { graph: Graph } | { unreadable: string }

type Obj = Record<string, unknown>
const obj = (v: unknown): Obj | undefined => (typeof v === 'object' && v !== null && !Array.isArray(v) ? (v as Obj) : undefined)

const KIND: Record<string, string> = {
  GRAPH_NODE_KIND_WORKFLOW: 'workflow',
  GRAPH_NODE_KIND_STEP: 'step',
  GRAPH_NODE_KIND_TASK: 'task',
  GRAPH_NODE_KIND_SIGNAL: 'signal',
}
const CONTAINS = 'GRAPH_EDGE_KIND_CONTAINS'

/**
 * The graph from `flow graph -o json`. Rows are the nodes in the document's
 * order (the workflow first, then its steps in document order, which the schema
 * states for this view), nested under the node that CONTAINS them: a loop's
 * body, a parallel branch or a switch arm sits one level in, under its parent.
 * The edge kinds order nothing else, so no dependency order is derived. Nodes
 * the nesting does not reach (a cycle, a missing parent) are listed at the top
 * level in document order and the graph says so. Never throws.
 */
export const parseGraph = (stdout: unknown, cut = false): Parsed => {
  if (cut) return { unreadable: 'the graph is larger than the view reads' }
  if (typeof stdout !== 'string') return { unreadable: 'no graph document' }
  const text = stdout.trim()
  if (text === '') return { unreadable: 'flow graph printed nothing' }
  if (text.length > MAX_STDOUT) return { unreadable: 'the graph is larger than the view reads' }
  let doc: Obj | undefined
  try {
    doc = obj(JSON.parse(text))
  } catch {
    return { unreadable: 'the output was not a JSON graph' }
  }
  if (doc === undefined || !Array.isArray(doc.nodes) || (doc.edges !== undefined && !Array.isArray(doc.edges))) return { unreadable: 'the output was not a graph document' }

  // The CLI's notes are cut to MAX_NOTES; the view's own notes (a fixed few) are kept apart so they can never be crowded out.
  const own: string[] = []
  const note = (n: string) => {
    own.push(clean(n, MAX_NOTE))
  }
  let partial = doc.partial === true
  const given = Array.isArray(doc.notes) ? doc.notes : []
  const cli = given.flatMap(n => (typeof n === 'string' && clean(n, MAX_NOTE) !== '' ? [clean(n, MAX_NOTE)] : [])).slice(0, MAX_NOTES)
  if (given.length > MAX_NOTES) note(`${given.length - MAX_NOTES} more notes not shown`)
  if (partial && cli.length === 0) note('the graph says it is partial and gives no reason')

  // Nodes by id, in document order; a repeated id keeps its first.
  const index = new Map<string, number>()
  const rows: Row[] = []
  let skipped = 0
  for (const raw of doc.nodes.slice(0, MAX_NODES)) {
    const n = obj(raw)
    if (n === undefined || typeof n.id !== 'string' || n.id === '' || index.has(n.id)) {
      skipped++
      continue
    }
    index.set(n.id, rows.length)
    const kind = typeof n.kind === 'string' && Object.hasOwn(KIND, n.kind) ? KIND[n.kind] : 'node'
    rows.push({ depth: 1, kind, label: clean(n.label, MAX_LABEL) || clean(n.id, MAX_LABEL), detail: clean(n.detail, MAX_DETAIL) })
  }
  if (doc.nodes.length > MAX_NODES) {
    partial = true
    note(`only the first ${MAX_NODES} of ${doc.nodes.length} nodes are shown`)
  }
  if (skipped > 0) {
    partial = true
    note(`${skipped} nodes could not be read`)
  }
  const ids = [...index.keys()]

  // CONTAINS edges only; each node keeps the first parent the edges give it.
  const edges = Array.isArray(doc.edges) ? doc.edges : []
  if (edges.length > MAX_EDGES) {
    partial = true
    note(`only the first ${MAX_EDGES} of ${edges.length} edges are read`)
  }
  const parent = new Map<number, number>()
  for (const raw of edges.slice(0, MAX_EDGES)) {
    const e = obj(raw)
    if (e === undefined || e.kind !== CONTAINS || typeof e.from !== 'string' || typeof e.to !== 'string') continue
    const from = index.get(e.from)
    const to = index.get(e.to)
    if (from === undefined || to === undefined || from === to || parent.has(to)) continue
    parent.set(to, from)
  }

  // Children in node order. A node is placed under its parent only if walking up reaches a root without a loop.
  const children = new Map<number, number[]>()
  const roots: number[] = []
  const order: number[] = []
  const reach = (i: number): boolean => {
    const seen = new Set<number>()
    for (let at: number | undefined = i; at !== undefined; at = parent.get(at)) {
      if (seen.has(at)) return false
      seen.add(at)
    }
    return true
  }
  let stray = 0
  for (let i = 0; i < rows.length; i++) {
    const p = parent.get(i)
    if (p === undefined) roots.push(i)
    else if (reach(i)) children.set(p, [...(children.get(p) ?? []), i])
    else {
      // In a containment cycle: listed at the top level, not nested under itself.
      stray++
      roots.push(i)
    }
  }
  let flattened = false
  const walk = (i: number, depth: number) => {
    const stack: [number, number][] = [[i, depth]]
    while (stack.length > 0) {
      const [at, d] = stack.pop()!
      if (d > MAX_DEPTH) flattened = true
      rows[at].depth = Math.min(d, MAX_DEPTH)
      order.push(at)
      for (const c of (children.get(at) ?? []).toReversed()) stack.push([c, d + 1])
    }
  }
  // A workflow node is the top; a graph without one starts its roots at the first level.
  const top = (i: number) => (rows[i].kind === 'workflow' ? 0 : 1)
  for (const r of roots) walk(r, top(r))
  if (stray > 0) {
    partial = true
    note(`${stray} nodes are contained in a cycle and are listed unnested`)
  }
  if (flattened) {
    partial = true
    note(`nesting deeper than ${MAX_DEPTH} levels is drawn at level ${MAX_DEPTH}`)
  }
  // Without a single CONTAINS edge the nesting is not stated at all.
  const nested = parent.size > 0 || rows.length <= 1
  if (!nested) note('the graph states no nesting, so the nodes are listed in the order given')
  return { graph: { rows: order.map(i => rows[i]), partial, notes: [...cli, ...own], nested } }
}

/** `    ○ label — detail`: one row, indented by depth, the mark first. */
export const rowText = (r: Row): string => {
  const what = r.detail !== '' ? r.detail : r.kind === 'step' || r.kind === 'workflow' ? '' : r.kind
  return `${'  '.repeat(r.depth)}${r.kind === 'workflow' ? 'workflow' : NOT_RUN} ${r.label}${what === '' ? '' : ` — ${what}`}`
}

/** The section's headline: what the rows are, and that nothing has run. */
export const graphHead = (g: Graph, file: string): string => {
  const steps = g.rows.filter(r => r.kind === 'step').length
  return `${clean(file, 200)}: ${steps} step${steps === 1 ? '' : 's'} as declared; ${NOT_RUN} means not run (no run is shown)${g.partial ? '; partial' : ''}`
}

/** The graph as plain text, the same facts as the drawn form. */
export const graphLines = (g: Graph, file: string): string[] => [
  graphHead(g, file),
  ...g.rows.map(rowText),
  ...(g.rows.length === 0 ? ['(no nodes)'] : []),
  ...g.notes.map(n => `note: ${n}`),
]

/**
 * What a finished `flow graph` gave. Only exit 0 with a whole document is a
 * graph; a refusal is its first line, a stopped or throwing read is unreadable,
 * and a cut document is never half-read.
 */
export const graphOf = (ran: { exitCode: number; stdout: string; stderr: string; isStdoutTruncated?: boolean } | undefined, err?: unknown): Parsed => {
  if (ran === undefined) return { unreadable: clean(String(err), 100) || 'no answer' }
  if (ran.exitCode !== 0) {
    const first = ran.stderr.split('\n').map(l => l.trim()).find(l => l !== '' && l !== 'ERROR')
    return { unreadable: clean(first, 120) || `flow graph exited ${Math.trunc(ran.exitCode)}` }
  }
  return parseGraph(ran.stdout, ran.isStdoutTruncated === true)
}
