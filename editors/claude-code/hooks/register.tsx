import { atom, read, update } from 'claude-code'
import type { Engine, Register } from 'claude-code'

import type { FileReport } from '../types'
import type { RunSummary } from '../types/flowstate'
import { cwdFlowfile, formatContext, mentionedFlowfile, parseTaskNames, reportFor } from './context'
import { UNCHECKED_BASH, UNCHECKED_EDIT, analyzeCommand, askReason, denyReason, namesFlow, secretsIn } from './guard'
import { isFlowfile, parseReports, summarize, toFileReport } from './flowfile'
import { MAX_PAGES, MAX_RUNS, clean, parsePage, runLine, toListing } from './runs'
import type { Listing } from './runs'

const PANE = 'flowstate'
/** The pane and the stored state keep the most recent Flowfiles only. */
const MAX_REPORTS = 50
const reports = atom({ plugin: 'flowstate', key: 'reports' } as const, [])

const validate = async (
  $: Engine,
  flow: string,
  path: string,
): Promise<FileReport> => {
  try {
    const ran = await $.process.run([flow, 'validate', '-o', 'jsonl', '--', path], {
      timeoutMs: 20000,
    })
    const found = parseReports(ran.stdout).find(r => r.file === path)
    if (found) return toFileReport(found)
    return { file: path, diagnostics: [], failure: ran.stderr.trim().split('\n')[0] || 'no report' }
  } catch (err) {
    return { file: path, diagnostics: [], failure: String(err) }
  }
}

/**
 * Asks the server for its newest runs. A bounded scan can come back short with
 * a continuation token, so it follows the token a few pages until it has enough.
 * A failure to answer is a state of the pane, never an error.
 */
const listRuns = async ($: Engine, flow: string): Promise<Listing> => {
  const runs: RunSummary[] = []
  let token = ''
  try {
    for (let page = 0; page < MAX_PAGES && runs.length < MAX_RUNS; page++) {
      const argv = [flow, 'list', '-o', 'json', ...(token ? ['--page-token', token] : [])]
      const ran = await $.process.run(argv, { timeoutMs: 5000 })
      if (ran.exitCode !== 0) return toListing(ran)
      const got = parsePage(ran.stdout)
      runs.push(...got.runs)
      token = got.next
      if (!token) break
    }
    return { runs: runs.slice(0, MAX_RUNS) }
  } catch (err) {
    return { offline: clean(String(err), 100) || 'no answer' }
  }
}

/**
 * The tasks and the last validation for a Flowfile, as one context block. Both
 * legs are local; a leg that fails to run, or exits non-zero without its answer,
 * is left out, and nothing here throws.
 */
const gather = async ($: Engine, flow: string, file: string): Promise<string | undefined> => {
  // Independent legs, started together: a stalled one costs its own timeout, not both.
  const [tasks, report] = await Promise.all([
    $.process.run([flow, 'tasks', '-o', 'json'], { timeoutMs: 10000 }).then(
      ran => (ran.exitCode === 0 ? parseTaskNames(ran.stdout) : []),
      () => [],
    ),
    $.process.run([flow, 'validate', '-o', 'jsonl', '--', file], { timeoutMs: 20000 }).then(
      ran => reportFor(ran.stdout, file),
      () => undefined,
    ),
  ])
  return formatContext({ file, tasks: tasks.length > 0 ? tasks : undefined, report })
}

/** The first Flowfile in the session's working directory, if it has one and can be listed. */
const findInCwd = async ($: Engine): Promise<string | undefined> => {
  try {
    return cwdFlowfile((await $.fs.list()).filter(e => e.kind === 'file').map(e => e.name))
  } catch {
    return undefined
  }
}

export const register: Register = (on, options) => {
  const flow = typeof options.flowBinary === 'string' && options.flowBinary ? options.flowBinary : 'flow'
  const isEnabled = options.validateOnEdit !== false
  const guardsServer = options.guardServerActions !== false

  on('session.start', async ($, e, next) => {
    await $.command.register({
      name: 'flowstate',
      description: 'Show the validation state of Flowfiles touched this session',
    })
    return next(e)
  })

  // Once per conversation, when the working directory holds a Flowfile.
  on('prompt.context', async ($, e, next) => {
    const out = await next(e)
    const file = await findInCwd($)
    const text = file === undefined ? undefined : await gather($, flow, file)
    return text === undefined ? out : { ...out, blocks: [...out.blocks, { name: 'flowstate', text }] }
  })

  // A prompt that names a Flowfile gets the same block beside it; this event
  // sees the prompt, which `prompt.context` does not.
  on('prompt.submit', async ($, e, next) => {
    const file = mentionedFlowfile(e.text)
    const text = file === undefined ? undefined : await gather($, flow, file)
    return next(text === undefined ? e : { ...e, context: [...(e.context ?? []), text] })
  })

  on('command.run', { command: 'flowstate' }, async $ => {
    await $.ui.open({ id: PANE, title: 'Flowstate' })
    return { text: 'Flowstate pane opened.' }
  })

  for (const tool of ['Edit', 'Write', 'MultiEdit'] as const) {
    on('tool.call', { tool }, async ($, e, next) => {
      const ran = await next(e)
      if (ran.deny !== undefined || ran.isError === true || !isEnabled || !isFlowfile(e.file_path)) {
        return ran
      }

      const report = await validate($, flow, e.file_path)
      await update($, reports, list =>
        [...list.filter(r => r.file !== report.file), report].slice(-MAX_REPORTS),
      )
      const broken = report.diagnostics.length > 0 || report.failure !== undefined
      $.ui.status(broken ? `flowstate: ${report.diagnostics.length || '!'} problem(s)` : undefined)

      return broken ? { ...ran, context: [...(ran.context ?? []), summarize(report)] } : ran
    })
  }

  // The decision comes after the rules and settings hooks have spoken, so a
  // `deny` is final here and an `allow` is tightened to a question, never loosened.
  on('tool.check', { tool: 'Bash' }, async ($, e, next) => {
    const decided = await next(e)
    if (!guardsServer || decided.decision === 'deny') return decided
    const command = (e.input as { command?: unknown } | null)?.command
    if (typeof command !== 'string') return decided

    const found = analyzeCommand(command, flow)
    if (found.actions.length === 0 && !found.uncertain) return decided

    let address: string | undefined
    try {
      address = await $.env.get('FLOWSTATE_ADDRESS')
    } catch {
      address = undefined
    }
    return { ...decided, decision: 'ask', reason: askReason(found, address) }
  }).catch(async (_$, e, next) => {
    // A guard that failed must not wave a flow command through; commands that never name flow are left alone.
    const decided = await next(e)
    const command = (e.input as { command?: unknown } | null)?.command
    if (!guardsServer || decided.decision === 'deny' || !namesFlow(command, flow)) return decided
    return { ...decided, decision: 'ask', reason: UNCHECKED_BASH }
  })

  // Refuses a secret literal before it reaches a Flowfile, whatever the mode.
  for (const tool of ['Edit', 'Write', 'MultiEdit'] as const) {
    on('tool.check', { tool }, async ($, e, next) => {
      const decided = await next(e)
      const path = (e.input as { file_path?: unknown } | null)?.file_path
      if (decided.decision === 'deny' || typeof path !== 'string' || !isFlowfile(path)) return decided
      // An edit replaces part of a line as often as a whole one, so scan the file as the edit leaves it.
      let current: string | undefined
      if (tool !== 'Write') {
        try {
          current = await $.fs.read(path)
        } catch {
          current = undefined
        }
      }
      const findings = secretsIn(e.input, current)
      return findings.length === 0 ? decided : { ...decided, decision: 'deny', reason: denyReason(path, findings) }
    }).catch(async (_$, e, next) => {
      // Nothing has run yet, so a failed secret check refuses a Flowfile write rather than allowing it.
      const decided = await next(e)
      const path = (e.input as { file_path?: unknown } | null)?.file_path
      if (decided.decision === 'deny' || typeof path !== 'string' || !isFlowfile(path)) return decided
      return { ...decided, decision: 'deny', reason: UNCHECKED_EDIT }
    })
  }

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text } = $.ui.resolve(e)
    const list = await read($, reports)
    const runs = await listRuns($, flow)

    return (
      <Box flexDirection="column">
        <Text bold>Runs</Text>
        {'offline' in runs ? (
          <Text dimColor>  Runs unavailable ({runs.offline}). Local runs need no server; set FLOWSTATE_ADDRESS to list a server's.</Text>
        ) : runs.runs.length === 0 ? (
          <Text dimColor>  No runs yet.</Text>
        ) : (
          runs.runs.map(r => <Text dimColor>  {runLine(r)}</Text>)
        )}
        <Text bold>Flowfiles</Text>
        {list.length === 0 && <Text dimColor>No Flowfile edited yet this session.</Text>}
        {list.map(r => (
          <Box flexDirection="column">
            <Text bold>
              {r.failure ? 'could not check' : r.diagnostics.length === 0 ? 'valid' : 'invalid'}{' '}
              {r.file}
            </Text>
            {r.failure && <Text dimColor>  {clean(r.failure, 120)}</Text>}
            {r.diagnostics.slice(0, 8).map(d => (
              <Text dimColor>
                {'  '}
                {d.line > 0 ? `${d.line}:${d.column} ` : ''}
                {clean(d.message, 120)}
              </Text>
            ))}
          </Box>
        ))}
      </Box>
    )
  })
}
