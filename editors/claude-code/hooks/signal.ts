import { DEFAULT_ADDRESS } from './guard'
import { clean, rejection } from './runs'
import { middleTruncate } from './vocab'

/**
 * The signal gate on a run's card (roadmap slice 7). Pure and bounded: the pane
 * reads the gates a run is parked on from `flow get -o json`
 * (`progress.pendingWaits`), because `flow timeline` carries only the waiting
 * step's id, never the signal name `flow signal` takes. Every string here comes
 * from a server or a workflow and is data: cleaned and bounded before it is
 * drawn, and checked against a strict allowlist before it reaches an argv, where
 * a failing check refuses the gate instead of rewriting it into another target.
 */

/** A card shows this many gates; the rest is "and N more", one `flow get` away. */
export const MAX_GATES = 5
/** The longest prompt a gate shows; the schema's own bound is larger. */
const MAX_PROMPT = 160
/** The signal-name rule of `SignalRequest.name` (service.proto): at most 128 characters. */
export const SIGNAL_NAME = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/
/** A workflow id is 1 to 256 bytes in the schema; the mod sends only the plain subset of it. */
export const WORKFLOW_ID = /^[A-Za-z0-9][A-Za-z0-9._:@=+-]{0,255}$/
/** A server address as `--address` takes it (host:port or a URL), with no space, quote or control character. */
export const SERVER_ADDRESS = /^[A-Za-z0-9][A-Za-z0-9._:/@%[\]-]{0,255}$/

/** One signal wait a run is parked on, as the card draws it. */
export interface Gate {
  /** The waiting step's id, cleaned. */
  step: string
  /** The name `flow signal` takes, cleaned. Only a name that passes SIGNAL_NAME may be sent. */
  signal: string
  /** What the gate asks, cleaned; empty where the author wrote none. */
  prompt: string
  /** The prompt shown is part of the question: the server cut it, or this card did. */
  promptCut: boolean
  /** The workflow declares a `signals:` policy for this name. */
  policed: boolean
  /** When the wait lapses of its own accord; empty for a gate that waits for a person. */
  deadline: string
  /** "1 of 2 approvals" quorum progress; empty for a plain gate. */
  quorum: string
  /** Why this gate offers no button; empty when it does. */
  refused: string
}

export interface Gates {
  gates: Gate[]
  /** Gates reported beyond the ones shown. */
  more: number
  /** The run holds more gates than it reported (`pendingWaitsTruncated`): `more` is a floor. */
  atLeast: boolean
}

/** The line that says gates are not shown, or empty when all are. */
export const moreText = (g: Gates): string =>
  g.more > 0 ? `and ${g.atLeast ? 'at least ' : ''}${g.more} more gates; \`flow get\` with the id above lists them` : g.atLeast ? 'and more gates the run did not report; `flow get` with the id above shows what it holds' : ''

/**
 * Reads `flow get -o json` for `progress.pendingWaits`. Anything that is not
 * that document is no gates at all: the card then shows no button, which is the
 * safe direction. A wait whose signal name fails the allowlist is kept, marked
 * refused, so the card says why instead of silently hiding a gate.
 */
export const parseGates = (stdout: string): Gates => {
  let waits: unknown
  let progress: { pendingWaitsTruncated?: unknown } | undefined
  try {
    progress = (JSON.parse(stdout) as { progress?: { pendingWaits?: unknown; pendingWaitsTruncated?: unknown } } | null)?.progress
    waits = progress?.pendingWaits
  } catch {
    return { gates: [], more: 0, atLeast: false }
  }
  if (!Array.isArray(waits)) return { gates: [], more: 0, atLeast: false }
  const gates: Gate[] = []
  // Bounded: only the first few entries are read; the rest are counted, not parsed.
  const scanned = Math.min(waits.length, MAX_GATES * 4)
  for (const w of waits.slice(0, scanned)) {
    if (typeof w !== 'object' || w === null || typeof w.signalName !== 'string') continue
    const needed = Number.isFinite(w.approvalsNeeded) ? Math.trunc(w.approvalsNeeded) : 0
    const got = Number.isFinite(w.approvals) ? Math.max(0, Math.trunc(w.approvals)) : 0
    gates.push({
      step: clean(w.stepId, 60),
      signal: clean(w.signalName, 128),
      prompt: clean(w.prompt, MAX_PROMPT),
      promptCut: w.promptTruncated === true || (typeof w.prompt === 'string' && w.prompt.length > MAX_PROMPT),
      policed: w.policed === true,
      deadline: clean(w.deadline, 40),
      quorum: needed > 0 ? `${got} of ${needed} approvals` : '',
      refused: SIGNAL_NAME.test(w.signalName) ? '' : 'its signal name is not one `flow signal` accepts',
    })
  }
  const shown = gates.slice(0, MAX_GATES)
  return { gates: shown, more: Math.max(0, waits.length - scanned) + (gates.length - shown.length), atLeast: progress?.pendingWaitsTruncated === true }
}

/** The address a send is aimed at, or why none can be trusted. */
export type Target = { address: string } | { refused: string }

/**
 * `FLOWSTATE_ADDRESS` as read: null is a lookup that failed (the target is unknown, never the default), unset means the CLI's own default, and a value
 * that is not a plain address is refused rather than trimmed into another one.
 */
export const targetOf = (env: string | undefined | null): Target =>
  env === null
    ? { refused: 'FLOWSTATE_ADDRESS could not be read, so the target server is not known' }
    : env === undefined || env === ''
    ? { address: '' }
    : SERVER_ADDRESS.test(env)
      ? { address: env }
      : { refused: 'FLOWSTATE_ADDRESS is not a plain server address' }

/** The server as a sentence: the address, or the default the CLI will use. */
export const where = (address: string): string =>
  address !== '' ? address : `${DEFAULT_ADDRESS} (the default; FLOWSTATE_ADDRESS is unset)`

/** `--address=` so the argv names the server the card names; the `=` form binds the text as the value. */
const serverArgs = (address: string): string[] => (address !== '' ? [`--address=${address}`] : [])

/** The argv of `flow get -o json`, or undefined when the id is not a plain one. Operands follow `--`. */
export const getArgv = (flow: string, address: string, id: string): string[] | undefined =>
  WORKFLOW_ID.test(id) ? [flow, 'get', '-o', 'json', ...serverArgs(address), '--', id] : undefined

/**
 * The one argv that sends a signal, or undefined when the id or name is not a
 * plain one: refused, never sanitised into a different target. `payload`, when a
 * caller has one, travels as the single element `--data=<text>` and nowhere else,
 * so it can never become a second argument or reach a shell. The pane passes
 * none: a `wait_for_signal:` declares no payload schema (docs/DSL.md), so the
 * timeline has nothing to build a field from.
 */
export const signalArgv = (flow: string, address: string, id: string, name: string, payload?: string): string[] | undefined =>
  WORKFLOW_ID.test(id) && SIGNAL_NAME.test(name)
    ? [flow, 'signal', ...serverArgs(address), ...(payload ? [`--data=${payload}`] : []), '--', id, name]
    : undefined

/** What the card asks before anything is sent: the verb, the signal, the run and the server. */
export const confirmText = (id: string, name: string, address: string): string =>
  `Send signal "${clean(name, 128)}" to run ${clean(id, 256)} on server ${where(address)}? Nothing is sent until you confirm.`

/** An id the card already names above is never repeated whole in a sentence: any long quoted token is cut to its ends. */
const shorten = (text: string): string => text.replace(/"([^"]{25,})"/g, (_m, id: string) => `"${middleTruncate(id, 16)}"`)

/** The server refused because the run is over (`FailedPrecondition`: "that workload has already finished", "the execution ... has already finished"). */
const FINISHED = /already (?:finished|completed|closed)|not running|no longer running/i

/** What a refusal says in one short line: the run is over, or a cause with the ids cut. */
const refusalLine = (stderr: string): string => {
  const said = rejection(stderr)
  if (FINISHED.test(said)) return 'Nothing sent: the run had already finished.'
  // Drop the CLI's `signalling "<id>": ` lead and a repeated `code:` prefix of the run id; keep the cause.
  const cause = shorten(said.replace(/^signalling "[^"]*":\s*/, '')).slice(0, 120)
  return `Not sent: ${cause}`
}

/** A run that threw or timed out proves nothing: the server may have taken the signal. */
export const unknownOutcome = (err: unknown, id: string, name: string): { ok: boolean; text: string } => ({
  ok: false,
  text: `delivery unknown for ${clean(name, 128)} on ${middleTruncate(id, 16)}: ${shorten(clean(String(err), 100)) || 'no answer'}; check the timeline before sending again`,
})

/** The one-line answer to a press: what `flow signal` did, or the server's own refusal, cleaned and bounded. */
export const outcomeOf = (ran: { exitCode: number; stderr: string }, id: string, name: string): { ok: boolean; text: string } =>
  ran.exitCode === 0
    ? { ok: true, text: `delivered ${clean(name, 128)} to ${middleTruncate(id, 16)}` }
    : { ok: false, text: refusalLine(ran.stderr) }
