import { atom, read, update } from 'claude-code'
import type { Engine, Register } from 'claude-code'

import type { FileReport } from '../types'
import type { RunSummary } from '../types/flowstate'
import { cwdFlowfile, formatContext, mentionedFlowfile, parseTaskNames, reportFor } from './context'
import { UNCHECKED_BASH, UNCHECKED_EDIT, alreadyPresent, analyzeCommand, askReason, denyReason, namesFlow, secretsIn } from './guard'
import { isFlowfile, parseReports, summarize, toFileReport } from './flowfile'
import { MAX_PAGES, MAX_RUNS, clean, parsePage, reason, stderrNote, toListing } from './runs'
import type { Listing } from './runs'
import { MAX_ENTRIES, factsFor, parseTimeline, visibleSteps } from './detail'
import type { Parsed } from './detail'
import { COLOR, duration, middleTruncate, progressBar, runRow, statusOf, story } from './vocab'

const PANE = 'flowstate'
/** The pane and the stored state keep the most recent Flowfiles only. */
const MAX_REPORTS = 50
const reports = atom({ plugin: 'flowstate', key: 'reports' } as const, [])
const selected = atom({ plugin: 'flowstate', key: 'selected' } as const, '')
const summary = atom({ plugin: 'flowstate', key: 'summary' } as const, { name: '', status: '', startTime: '', closeTime: '' })
const filter = atom({ plugin: 'flowstate', key: 'filter' } as const, '')
/** A CEL filter is a sentence, not a document; a longer one is refused rather than cut, since a cut filter is a different query. */
const MAX_FILTER = 2000

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
const listRuns = async ($: Engine, flow: string, expr: string): Promise<Listing> => {
  const runs: RunSummary[] = []
  let token = ''
  if (expr.length > MAX_FILTER) return { offline: `the filter is longer than ${MAX_FILTER} characters` }
  try {
    for (let page = 0; page < MAX_PAGES && runs.length < MAX_RUNS; page++) {
      // `--filter=` binds the text as the flag's value whatever it starts with.
      const argv = [flow, 'list', '-o', 'json', ...(expr ? [`--filter=${expr}`] : []), ...(token ? ['--page-token', token] : [])]
      const ran = await $.process.run(argv, { timeoutMs: 5000 })
      if (ran.exitCode !== 0) return toListing(ran, expr !== '')
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
 * One run's account, from `flow timeline`. Like the listing, a failure to answer
 * is a state of the card, never an error, and `--` keeps an id from being a flag.
 */
const readTimeline = async ($: Engine, flow: string, id: string): Promise<Parsed> => {
  try {
    const argv = [flow, 'timeline', '-o', 'json', '--max-entries', String(MAX_ENTRIES), '--', id]
    const ran = await $.process.run(argv, { timeoutMs: 5000 })
    if (ran.exitCode !== 0) return { error: reason(ran.stderr) }
    const parsed = parseTimeline(ran.stdout)
    const note = stderrNote(ran.stderr)
    return 'detail' in parsed && note !== '' ? { ...parsed, note } : parsed
  } catch (err) {
    return { error: clean(String(err), 100) || 'no answer' }
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
      return findings.length === 0 ? decided : { ...decided, decision: 'deny', reason: denyReason(path, findings, alreadyPresent(current, findings)) }
    }).catch(async (_$, e, next) => {
      // Nothing has run yet, so a failed secret check refuses a Flowfile write rather than allowing it.
      const decided = await next(e)
      const path = (e.input as { file_path?: unknown } | null)?.file_path
      if (decided.decision === 'deny' || typeof path !== 'string' || !isFlowfile(path)) return decided
      return { ...decided, decision: 'deny', reason: UNCHECKED_EDIT }
    })
  }

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text, Button, Input } = $.ui.resolve(e)
    const list = await read($, reports)
    const expr = await read($, filter)
    const id = await read($, selected)
    const memo = await read($, summary)
    // Independent legs, started together: a stalled one costs its own timeout, not both.
    const [runs, account] = await Promise.all([
      listRuns($, flow, expr),
      id === '' ? Promise.resolve(undefined) : readTimeline($, flow, id),
    ])
    const row = 'runs' in runs ? runs.runs.find(r => r.workflowId === id) : undefined
    const detail = account && 'detail' in account ? account.detail : undefined
    // The listing is a window of the newest runs; the pressed run's own summary stands in once it leaves it.
    // Only a terminal status survives the press: a remembered running or waiting one is stale by now, so it reads as unknown.
    const live = ['running', 'waiting'].includes(statusOf(memo.status).kind)
    const known = row ?? (id === '' ? undefined : { workflowId: id, name: memo.name, status: live ? '' : memo.status, startTime: live ? null : memo.startTime || null, closeTime: memo.closeTime || null })
    const facts = factsFor(known ?? { workflowId: id }, detail, Date.now())
    const { shown, more } = visibleSteps(detail?.steps ?? [])
    const head = statusOf(known?.status)

    return (
      <Box flexDirection="column">
        <Text bold>Runs</Text>
        <Input
          key="filter"
          label="Filter"
          placeholder={'CEL, as flow list --filter takes it: status == "FAILED"'}
          value={expr}
          submitLabel="filter"
          onSubmit={v => update($, filter, () => (v.trim() === '' ? '' : v))}
        />
        {expr !== '' && (
          <Box>
            <Text dimColor>  Filter: {clean(expr, 120)}  </Text>
            <Button key="clear-filter" label="Clear filter" plain onPress={() => update($, filter, () => '')}>
              Clear filter
            </Button>
          </Box>
        )}
        {'offline' in runs ? (
          <Text dimColor>  Runs unavailable ({runs.offline}). Local runs need no server; set FLOWSTATE_ADDRESS to list a server's.</Text>
        ) : runs.runs.length === 0 ? (
          <Text dimColor>  {expr === '' ? 'No runs yet.' : 'No runs match the filter.'}</Text>
        ) : (
          runs.runs.map(r => {
            const one = runRow(r)
            return (
              <Button key={`run:${clean(r.workflowId, 200)}`} label={one.text} plain onPress={async () => {
                await update($, summary, () => ({
                  name: clean(r.name, 80),
                  status: clean(r.status, 40),
                  startTime: clean(r.startTime, 40),
                  closeTime: clean(r.closeTime, 40),
                }))
                await update($, selected, () => r.workflowId)
              }}>
                <Text color={COLOR[one.status.tone]}>{one.status.symbol}</Text> {one.text}
              </Button>
            )
          })
        )}
        {id !== '' && (
          <Box flexDirection="column">
            <Text bold>
              <Text color={COLOR[head.tone]}>{head.symbol}</Text> {head.word} {clean(known?.name) || middleTruncate(id)}
            </Text>
            <Text dimColor>  id {clean(id, 256)}</Text>
            <Text>  {story(facts)}</Text>
            {account && 'note' in account && account.note && <Text dimColor>  {account.note}</Text>}
            {account && 'error' in account ? (
              <Text dimColor>  Timeline unavailable ({account.error}).</Text>
            ) : (
              <Box flexDirection="column">
                <Text>
                  {'  '}
                  {progressBar(facts.done, facts.total)} {facts.done}/{facts.total} steps
                </Text>
                {detail?.steps.length === 0 && <Text dimColor>  No steps yet.</Text>}
                {shown.map(s => (
                  <Box flexDirection="column">
                    <Text>
                      {'  '}
                      <Text color={COLOR[s.status.tone]}>{s.status.symbol}</Text> {s.name}{' '}
                      <Text dimColor>
                        {s.status.word}
                        {s.durationMs !== undefined ? `  ${duration(s.durationMs)}` : ''}
                        {s.attempts > 1 ? `  attempt ${s.attempts}` : ''}
                      </Text>
                    </Text>
                    {s.reason !== '' && <Text dimColor>      {s.reason}</Text>}
                  </Box>
                ))}
                {more > 0 && <Text dimColor>  and {more} more; `flow timeline` with the id above lists them all</Text>}
                {detail?.truncated && <Text dimColor>  The server clipped this account; `flow timeline --help` says how to continue it (--run-id, --after-event-id).</Text>}
              </Box>
            )}
            <Button key="close-run" label="Close" plain onPress={() => update($, selected, () => '')}>
              Close
            </Button>
          </Box>
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
