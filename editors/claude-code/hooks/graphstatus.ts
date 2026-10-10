import type { Execution } from './detail'
import type { Graph, Row } from './graph'
import { MAX_NAME, NOT_RUN, markedText, rowText } from './graph'
import { clean } from './runs'
import { statusFor } from './vocab'
import type { Status } from './vocab'

/**
 * Live status on the Graph rows: one run's `flow timeline` steps laid over the
 * graph the Graph section already draws. It is an overlay on that model, not a
 * second graph: rows, depth and order stay the document's, and a mark is added
 * only where the two sources agree on an exact key.
 *
 * What the key can be is a measured limit, not a choice. A timeline row names a
 * step by the engine's label (`engine/summary.go`): the enclosing step ids and
 * the step id, each in backticks, joined by ` > ` (`` `process` > `label` ``),
 * a ` · sleep` / ` · wait timeout` / ` · undo` suffix on timers and
 * compensations, and a leading `… > ` when the path was cut. The graph's
 * `address` spells the same position differently and finer: a loop iteration
 * `process[0]/label`, a parallel branch `checks#1/check_quota`, a switch arm
 * `decision?0/deploy`. The label carries no iteration, branch or arm index, so
 * a nested label cannot be tied to one address: `` `process` > `label` `` is
 * every iteration, and a graph holds `process[0]/label` alone. Container steps
 * (a loop, a parallel, a switch) are never scheduled themselves, so they have
 * no row. The one sound match is a top-level step: its address is its bare id
 * and its label is that id in backticks. Everything else keeps the not-run
 * mark rather than a guess, and is counted aloud.
 */

/** A step id as the schema spells it: an address of just this is a top-level step. */
const TOP_LEVEL = /^[A-Za-z0-9_-]{1,128}$/

/** The timeline label of the top-level step with this address. */
export const labelOf = (address: string): string => `\`${address}\``

/** A graph row's mark: its status (when a run says one) and how many timeline steps stand behind it. */
export interface Mark {
  status: Status
  /** Timeline steps that carry this address; above 1 means the status is an aggregate. */
  steps: number
  /** The most attempts any of them shows. */
  attempts: number
  /** The step's compensation (`<id> · undo` rows), when the timeline has one. */
  undo?: Status
}

export interface Overlay {
  /** By graph row (the very objects of `graph.rows`). A row absent here has no status. */
  marks: Map<Row, Mark>
  /** Timeline steps that matched no top-level graph step. */
  unmatched: number
  /** Graph step rows left unmarked because two rows share one address. */
  ambiguous: number
  /** Graph step rows below the top level, which the timeline cannot be tied to. */
  nested: number
  /** Timeline labels at the read bound: possibly cut, so never matched. */
  cut: number
  /** Top-level graph steps with no timeline step. */
  silent: number
}

/**
 * Several timeline steps for one address (rows merged by label should not give
 * them, but a caller may): failed beats everything, then running, then waiting;
 * only an all-agreeing set is anything else, and a disagreement is `mixed` with
 * the unknown symbol, never a pick.
 */
export const aggregate = (statuses: readonly Status[]): Status => {
  const failed = statuses.filter(s => s.kind === 'failed')
  if (failed.length > 0) return failed.every(s => s.word === failed[0].word) ? failed[0] : statusFor('failed')
  const running = statuses.find(s => s.kind === 'running')
  if (running) return running
  const waiting = statuses.find(s => s.kind === 'waiting')
  if (waiting) return waiting
  const first = statuses[0]
  if (first !== undefined && statuses.every(s => s.kind === first.kind && s.word === first.word)) return first
  return statusFor('unknown', 'mixed')
}

/**
 * The graph's rows with the run's steps laid on them. Matching is exact string
 * equality of a top-level address's label and a timeline step's name, and
 * nothing else: no prefix, case folding or nested guess. A graph address shared
 * by two rows marks neither. Never throws; bounded by the graph (200 rows) and
 * the steps given (the detail card's own bound).
 */
export const overlayStatus = (graph: Graph, steps: readonly Execution[]): Overlay => {
  const byLabel = new Map<string, Execution[]>()
  let cut = 0
  for (const s of steps) {
    if (s.cut) {
      cut++
      continue
    }
    byLabel.set(s.label, [...(byLabel.get(s.label) ?? []), s])
  }

  const seen = new Map<string, number>()
  for (const r of graph.rows) if (r.kind === 'step' && TOP_LEVEL.test(r.address)) seen.set(r.address, (seen.get(r.address) ?? 0) + 1)

  const marks = new Map<Row, Mark>()
  const used = new Set<string>()
  let ambiguous = 0
  let nested = 0
  let silent = 0
  for (const r of graph.rows) {
    if (r.kind !== 'step') continue
    if (!TOP_LEVEL.test(r.address)) {
      nested++
      continue
    }
    if ((seen.get(r.address) ?? 0) > 1) {
      ambiguous++
      continue
    }
    const key = labelOf(r.address)
    const found = byLabel.get(key)
    if (found === undefined) {
      silent++
      continue
    }
    used.add(key)
    let status = aggregate(found.map(s => s.status))
    // A compensation row for this very id: the effect was undone, so a plain success would mislead.
    const undoKey = `${key} · undo`
    const undos = byLabel.get(undoKey)
    let undo: Status | undefined
    if (undos !== undefined) {
      used.add(undoKey)
      undo = aggregate(undos.map(s => s.status))
      if (status.kind === 'succeeded' && undo.kind === 'succeeded') status = statusFor('compensated')
    }
    marks.set(r, { status, steps: found.length, attempts: Math.max(...found.map(s => s.attempt)), undo })
  }
  // A step whose address was ambiguous was not placed, so it counts as unmatched too.
  const unmatched = [...byLabel.keys()].filter(k => !used.has(k)).length
  return { marks, unmatched: unmatched + cut, ambiguous, nested, cut, silent }
}

/** The words after a marked row: `· succeeded`, with attempts and an aggregate stated. */
export const markText = (m: Mark): string => ` · ${m.status.word}${m.attempts > 1 ? `, attempt ${m.attempts}` : ''}${m.steps > 1 ? `, ${m.steps} executions` : ''}${m.undo !== undefined && m.status.kind !== 'compensated' ? `, undo ${m.undo.word}` : ''}`

/** One graph row as text: marked from the overlay, or with the not-run mark when it has no status. */
export const liveRowText = (r: Row, overlay: Overlay): string => {
  const m = overlay.marks.get(r)
  return m === undefined ? rowText(r) : markedText(r, m.status.symbol, markText(m))
}

/** The same workflow: the run's declared name is the graph's workflow node. Anything less is no overlay. */
export const sameWorkflow = (graph: Graph, runName: unknown): boolean => {
  // Both sides compare uncut (up to the schema's 128). A name past the bound on either side was cut, so it is never equal to anything.
  if (typeof runName !== 'string' || runName.length > MAX_NAME || graph.workflowId.length > MAX_NAME) return false
  return graph.workflowId !== '' && clean(runName, MAX_NAME + 1) === graph.workflowId
}

/** The headline of an overlaid graph; `from` is the run's short id. */
export const liveHead = (graph: Graph, overlay: Overlay, file: string, from: string): string => {
  const steps = graph.rows.filter(r => r.kind === 'step').length
  return `${clean(file, 200)}: ${steps} step${steps === 1 ? '' : 's'} as declared; status from run ${clean(from, 40)}: ${overlay.marks.size} of ${steps} have one; ${NOT_RUN} means no status from that run, not that the step did not run; the file may have changed since that run${graph.partial ? '; partial' : ''}`
}

/** What the overlay could not place, one note each, in a fixed order. `clipped` is the server cutting the timeline. */
export const overlayNotes = (overlay: Overlay, clipped: boolean): string[] => [
  ...(overlay.unmatched > 0 ? [`${overlay.unmatched} timeline step${overlay.unmatched === 1 ? '' : 's'} not in this graph's top-level steps (nested steps, timers, compensations and engine steps are not mapped)`] : []),
  ...(overlay.nested > 0 ? [`${overlay.nested} nested step${overlay.nested === 1 ? '' : 's'} keep ${NOT_RUN}: the timeline does not say which iteration, branch or arm a row belongs to`] : []),
  ...(overlay.cut > 0 ? [`${overlay.cut} timeline step${overlay.cut === 1 ? '' : 's'} had a label too long to read whole and ${overlay.cut === 1 ? 'is' : 'are'} not matched`] : []),
  ...(overlay.ambiguous > 0 ? [`${overlay.ambiguous} graph step${overlay.ambiguous === 1 ? '' : 's'} share an address and get no status`] : []),
  ...(clipped ? ['the server clipped the timeline, so a step without a status may not have been read'] : []),
]

/** The overlaid graph as plain text: the same lines the drawn form shows. */
export const liveLines = (graph: Graph, overlay: Overlay, file: string, from: string, clipped: boolean): string[] => [
  liveHead(graph, overlay, file, from),
  ...graph.rows.map(r => liveRowText(r, overlay)),
  ...(graph.rows.length === 0 ? ['(no nodes)'] : []),
  ...graph.notes.map(n => `note: ${n}`),
  ...overlayNotes(overlay, clipped).map(n => `note: ${n}`),
]
