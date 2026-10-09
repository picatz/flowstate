import { DEFAULT_ADDRESS } from './guard'
import { clean, rejection } from './runs'

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
  /** Gates read beyond the ones shown. */
  more: number
}

/**
 * Reads `flow get -o json` for `progress.pendingWaits`. Anything that is not
 * that document is no gates at all: the card then shows no button, which is the
 * safe direction. A wait whose signal name fails the allowlist is kept, marked
 * refused, so the card says why instead of silently hiding a gate.
 */
export const parseGates = (stdout: string): Gates => {
  let waits: unknown
  try {
    waits = (JSON.parse(stdout) as { progress?: { pendingWaits?: unknown } } | null)?.progress?.pendingWaits
  } catch {
    return { gates: [], more: 0 }
  }
  if (!Array.isArray(waits)) return { gates: [], more: 0 }
  const gates: Gate[] = []
  for (const w of waits.slice(0, MAX_GATES * 4)) {
    if (typeof w !== 'object' || w === null || typeof w.signalName !== 'string') continue
    const needed = Number.isFinite(w.approvalsNeeded) ? Math.trunc(w.approvalsNeeded) : 0
    const got = Number.isFinite(w.approvals) ? Math.max(0, Math.trunc(w.approvals)) : 0
    gates.push({
      step: clean(w.stepId, 60),
      signal: clean(w.signalName, 128),
      prompt: clean(w.prompt, MAX_PROMPT),
      policed: w.policed === true,
      deadline: clean(w.deadline, 40),
      quorum: needed > 0 ? `${got} of ${needed} approvals` : '',
      refused: SIGNAL_NAME.test(w.signalName) ? '' : 'its signal name is not one `flow signal` accepts',
    })
  }
  return { gates: gates.slice(0, MAX_GATES), more: Math.max(0, gates.length - MAX_GATES) }
}

/** The address a send is aimed at, or why none can be trusted. */
export type Target = { address: string } | { refused: string }

/**
 * `FLOWSTATE_ADDRESS` as read: unset means the CLI's own default, and a value
 * that is not a plain address is refused rather than trimmed into another one.
 */
export const targetOf = (env: string | undefined): Target =>
  env === undefined || env === ''
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

/** The one-line answer to a press: what `flow signal` did, or the server's own refusal, cleaned and bounded. */
export const outcomeOf = (ran: { exitCode: number; stderr: string }, id: string, name: string): { ok: boolean; text: string } =>
  ran.exitCode === 0
    ? { ok: true, text: `delivered ${clean(name, 128)} to ${clean(id, 256)}` }
    : { ok: false, text: `not sent: ${rejection(ran.stderr)}` }
