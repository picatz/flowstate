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
import { WORKFLOW_ID, confirmText, getArgv, moreText, outcomeOf, parseGates, unknownOutcome, signalArgv, targetOf } from './signal'
import type { Gates } from './signal'
import { EMPTY, checkOf, hasTestFile, nudgeFor, recordCheck, recordEdit } from './verify'
import { COLOR, duration, middleTruncate, progressBar, runRow, statusFor, statusOf, story } from './vocab'
import { MAX_VALUE, RUN_TIMEOUT_MS, candidates, checkForm, cleanLines, confirmLines, parseInputs, resultOf, runArgv, submission, unknownResult, valueOf } from './form'
import type { Field, Pair, Parsed, Result } from './form'

const PANE = 'flowstate'
/** The pane and the stored state keep the most recent Flowfiles only. */
const MAX_REPORTS = 50
const reports = atom({ plugin: 'flowstate', key: 'reports' } as const, [])
const selected = atom({ plugin: 'flowstate', key: 'selected' } as const, '')
const summary = atom({ plugin: 'flowstate', key: 'summary' } as const, { name: '', status: '', startTime: '', closeTime: '' })
const filter = atom({ plugin: 'flowstate', key: 'filter' } as const, '')
const verify = atom({ plugin: 'flowstate', key: 'verify' } as const, EMPTY)
/** The Send press that awaits its Confirm: which run, which signal, and the server it was aimed at. Empty id for none. */
const NO_CONFIRM = { id: '', signal: '', address: '' }
const confirm = atom({ plugin: 'flowstate', key: 'confirm' } as const, NO_CONFIRM)
/** What the last Confirm did, kept for the card: the server's refusal verbatim (cleaned), or the delivery. */
const NO_OUTCOME = { id: '', signal: '', ok: false, text: '' }
const outcome = atom({ plugin: 'flowstate', key: 'outcome' } as const, NO_OUTCOME)
/** The Run locally press that awaits its Confirm (hooks/form.ts): the file and the exact inputs the question named. Empty file for none. */
const NO_RUN_CONFIRM: { file: string; inputs: Pair[] } = { file: '', inputs: [] }
const runConfirm = atom({ plugin: 'flowstate', key: 'runConfirm' } as const, NO_RUN_CONFIRM)
/** The Flowfile the run form is for, and what has been typed into its controls (only the inputs the file declares are ever written). */
const runFile = atom({ plugin: 'flowstate', key: 'runFile' } as const, '')
const runValues = atom({ plugin: 'flowstate', key: 'runValues' } as const, {} as Record<string, string>)
/** What the last local run did, kept for the form: output, the engine's refusal, or "outcome unknown". */
const NO_RUN_RESULT: { file: string; kind: '' | Result['kind']; text: string; lines: string[] } = { file: '', kind: '', text: '', lines: [] }
const runResult = atom({ plugin: 'flowstate', key: 'runResult' } as const, NO_RUN_RESULT)
const NO_GATES: Gates = { gates: [], more: 0, atLeast: false }
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
 * The signal gates a run is parked on, from `flow get`. Read only, like the
 * timeline; a failure to answer is no gates, which shows no button.
 */
const readGates = async ($: Engine, flow: string, address: string, id: string): Promise<Gates> => {
  const argv = getArgv(flow, address, id)
  if (argv === undefined) return NO_GATES
  try {
    const ran = await $.process.run(argv, { timeoutMs: 5000 })
    return ran.exitCode === 0 ? parseGates(ran.stdout) : NO_GATES
  } catch {
    return NO_GATES
  }
}

/** `FLOWSTATE_ADDRESS` as the session sees it: undefined when unset, null when the lookup fails (the target is then unknown, not the default). */
const envAddress = async ($: Engine): Promise<string | undefined | null> => {
  try {
    return await $.env.get('FLOWSTATE_ADDRESS')
  } catch {
    return null
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

/** The Flowfiles the run form may offer: the working directory's, and its `workflows/` directory's. Nothing here throws. */
const listFlowfiles = async ($: Engine): Promise<ReturnType<typeof candidates> & { stamp: Map<string, number> }> => {
  const stamp = new Map<string, number>()
  try {
    const top = await $.fs.list()
    const sub = top.some(e => e.kind === 'dir' && e.name === 'workflows') ? await $.fs.list('workflows').catch(() => []) : []
    for (const e of top) stamp.set(e.name, e.mtimeMs)
    for (const e of sub) stamp.set(`workflows/${e.name}`, e.mtimeMs)
    return { ...candidates(top, sub), stamp }
  } catch {
    return { files: [], more: 0, stamp }
  }
}

/**
 * A Flowfile's declared inputs, from `flow compile --schema inputs`: read only,
 * bounded, and `--` keeps the path from being a flag. A failure to answer is a
 * state of the form (no controls, no Run), never an error.
 */
const readInputs = async ($: Engine, flow: string, file: string): Promise<Parsed> => {
  try {
    const ran = await $.process.run([flow, 'compile', '-o', 'json', '--schema=inputs', '--', file], { timeoutMs: 10000 })
    if (ran.exitCode !== 0) return { error: cleanLines(ran.stderr, 3, 160).lines.join(' ') || 'flow compile failed' }
    return ran.isStdoutTruncated ? { error: 'the schema is larger than the form reads' } : parseInputs(ran.stdout)
  } catch (err) {
    return { error: clean(String(err), 100) || 'no answer' }
  }
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
  const nudges = options.verifyBeforeDone !== false
  /** One Confirm at a time: a second press while a send is in flight sends nothing. */
  let sending = false
  /** One local run at a time, the same way. */
  let running = false
  /** Schemas by file and modification time, so typing in a control does not recompile the file on every key. */
  const schemas = new Map<string, Parsed>()

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

  // Verify before done. `turn.complete` can only caption an answer already given, so the nudge rides
  // `classic.Stop`, whose `block` hands the model a reason and a chance to run the check. Every leg
  // below fails open: a nudge is advice, never a gate.
  on('turn.start', async ($, e, next) => {
    const out = await next(e)
    if (nudges) await update($, verify, () => EMPTY).catch(() => undefined)
    return out
  })

  for (const tool of ['Edit', 'Write', 'MultiEdit'] as const) {
    on('tool.call', { tool }, async ($, e, next) => {
      const ran = await next(e)
      if (nudges && ran.deny === undefined && ran.isError !== true) {
        await update($, verify, s => recordEdit(s, e.file_path)).catch(() => undefined)
      }
      return ran
    })
  }

  on('tool.call', { tool: 'Bash' }, async ($, e, next) => {
    const ran = await next(e)
    if (!nudges) return ran
    const check = checkOf((e as { command?: unknown }).command, flow)
    if (check === undefined) return ran
    const result = 'result' in ran ? (ran.result as { interrupted?: boolean; backgroundTaskId?: string } | undefined) : undefined
    const passed = ran.deny === undefined && ran.isError !== true && result?.interrupted !== true && result?.backgroundTaskId === undefined
    await update($, verify, s => recordCheck(s, check, passed)).catch(() => undefined)
    return ran
  })

  on('classic.Stop', async ($, e, next) => {
    const out = await next(e)
    // The model was already sent back once by a Stop hook: never loop.
    // An earlier Stop hook's block stands; this one neither replaces it nor spends its once-per-turn nudge.
    if (!nudges || e.stop_hook_active || out.block !== undefined) return out
    try {
      const state = await read($, verify)
      let suite = false
      try {
        suite = hasTestFile((await $.fs.list()).filter(f => f.kind === 'file').map(f => f.name))
      } catch {
        suite = false
      }
      const text = nudgeFor(state, suite)
      if (text === undefined) return out
      await update($, verify, s => ({ ...s, nudged: true }))
      return { ...out, block: text }
    } catch {
      return out
    }
  })

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
    const { Box, Text, Button, Input, Select } = $.ui.resolve(e)
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
    // Gates are read only for a run that is, or may be, parked: a finished run has none to answer.
    const target = targetOf(await envAddress($))
    const parkable = id !== '' && (facts.waitingOn !== undefined || ['running', 'waiting'].includes(head.kind))
    const idOk = WORKFLOW_ID.test(id)
    const found = parkable && idOk && 'address' in target ? await readGates($, flow, target.address, id) : NO_GATES
    const pending = await read($, confirm)
    const last = await read($, outcome)
    const asked = 'address' in target && pending.id === id && pending.address === target.address ? pending : NO_CONFIRM

    // The run form: the Flowfiles on offer, and the selected one's declared inputs.
    const offered = await listFlowfiles($)
    const chosen = await read($, runFile)
    const file = offered.files.includes(chosen) ? chosen : ''
    let schema: Parsed | undefined
    if (file !== '') {
      const key = `${file}@${offered.stamp.get(file) ?? 0}`
      schema = schemas.get(key)
      if (schema === undefined) {
        schema = await readInputs($, flow, file)
        if ('fields' in schema) {
          if (schemas.size >= 16) schemas.clear()
          schemas.set(key, schema)
        }
      }
    }
    const fields: Field[] = schema && 'fields' in schema ? schema.fields : []
    const typed = await read($, runValues)
    const checked = checkForm(fields, typed)
    const sent = submission(fields, typed)
    const asking = await read($, runConfirm)
    const questioned = file !== '' && asking.file === file && JSON.stringify(asking.inputs) === JSON.stringify(sent)
    const ranLast = await read($, runResult)
    /** A change to the form takes any pending question away: Confirm only ever runs what the question named. */
    const setValue = async (name: string, v: string) => {
      await update($, runConfirm, () => NO_RUN_CONFIRM)
      await update($, runValues, s => ({ ...s, [name]: v.slice(0, MAX_VALUE + 1) }))
    }
    const badge = (kind: string) => statusFor(kind === 'ok' ? 'succeeded' : kind === 'failed' ? 'failed' : kind === 'notrun' ? 'skipped' : 'unknown')

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
                await update($, confirm, () => NO_CONFIRM)
                await update($, outcome, () => NO_OUTCOME)
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
            {parkable && !idOk && <Text dimColor>  No signal button: this run's id is not a plain one.</Text>}
            {parkable && 'refused' in target && <Text dimColor>  No signal button: {target.refused}.</Text>}
            {found.gates.map(g => {
              const asking = g.refused === '' && asked.id !== '' && asked.signal === g.signal
              return (
                <Box flexDirection="column">
                  <Text>
                    {'  '}
                    <Text color={COLOR.wait}>◔</Text> Gate <Text bold>{g.signal}</Text> {g.step && `(step ${g.step}) `}waits for a signal
                  </Text>
                  {g.prompt !== '' && <Text>      {g.prompt}{g.promptCut ? ' [prompt truncated]' : ''}</Text>}
                  {g.prompt === '' && g.promptCut && <Text>      [prompt truncated]</Text>}
                  {g.quorum !== '' && <Text dimColor>      {g.quorum}</Text>}
                  <Text dimColor>
                    {'      '}
                    {g.deadline !== '' ? `lapses ${g.deadline}` : 'waits until answered'}
                    {g.policed ? '; the workflow declares who may act' : '; no sender policy declared'}
                  </Text>
                  {g.refused !== '' && <Text dimColor>      No button: {g.refused}.</Text>}
                  {g.refused === '' && !asking && (
                    <Button
                      key={`signal:${g.signal}`}
                      label={`Send signal ${g.signal}`}
                      plain
                      onPress={async () => {
                        // The first press only asks: nothing is sent until Confirm.
                        const t = targetOf(await envAddress($))
                        if (!('address' in t)) return
                        await update($, outcome, () => NO_OUTCOME)
                        await update($, confirm, () => ({ id, signal: g.signal, address: t.address }))
                      }}
                    >
                      Send signal {g.signal}
                    </Button>
                  )}
                  {asking && (
                    <Box flexDirection="column">
                      <Text color={COLOR.wait}>      {confirmText(asked.id, asked.signal, asked.address)}</Text>
                      <Text dimColor>      The server decides whether you may act, and says so if not.</Text>
                      <Box>
                        <Button
                          key={`confirm-signal:${g.signal}`}
                          label={`Confirm: send ${g.signal}`}
                          plain
                          onPress={async () => {
                            if (sending) return
                            sending = true
                            try {
                              const c = await read($, confirm)
                              // This button was drawn for one gate: if the question has moved on, it acts on nothing.
                              if (c.id !== id || c.signal !== g.signal) return
                              await update($, confirm, () => NO_CONFIRM)
                              // The target is read again: if it moved since the question was asked, nothing is sent.
                              const t = targetOf(await envAddress($))
                              const argv = 'address' in t && t.address === c.address ? signalArgv(flow, c.address, c.id, c.signal) : undefined
                              if (c.id === '' || argv === undefined) return
                              let result: { ok: boolean; text: string }
                              try {
                                result = outcomeOf(await $.process.run(argv, { timeoutMs: 10000 }), c.id, c.signal)
                              } catch (err) {
                                result = unknownOutcome(err, c.id, c.signal)
                              }
                              await update($, outcome, () => ({ id: c.id, signal: c.signal, ...result }))
                            } finally {
                              sending = false
                            }
                          }}
                        >
                          Confirm: send {g.signal}
                        </Button>
                        <Button key={`cancel-signal:${g.signal}`} label="Cancel" plain onPress={() => update($, confirm, () => NO_CONFIRM)}>
                          Cancel
                        </Button>
                      </Box>
                    </Box>
                  )}
                </Box>
              )
            })}
            {moreText(found) !== '' && <Text dimColor>  {moreText(found)}</Text>}
            {last.id === id && last.text !== '' && (
              <Text color={last.ok ? COLOR.ok : COLOR.fail}>
                {'  '}
                {last.ok ? '✓' : '✗'} {last.text}
              </Text>
            )}
            <Button key="close-run" label="Close" plain onPress={async () => {
              await update($, confirm, () => NO_CONFIRM)
              await update($, outcome, () => NO_OUTCOME)
              await update($, selected, () => '')
            }}>
              Close
            </Button>
          </Box>
        )}
        <Text bold>Run a Flowfile</Text>
        <Text dimColor>  Runs here with `flow run local`, no server. A run executes the workflow's tasks, so it asks first.</Text>
        {offered.files.length === 0 && <Text dimColor>  No Flowfile in this directory.</Text>}
        {offered.files.length > 0 && (
          <Select
            key="run-file"
            label="Flowfile"
            options={[{ value: '', label: '(choose a Flowfile)' }, ...offered.files.map(f => ({ value: f }))]}
            value={file}
            onSelect={async v => {
              if (v !== '' && !offered.files.includes(v)) return
              await update($, runConfirm, () => NO_RUN_CONFIRM)
              await update($, runResult, () => NO_RUN_RESULT)
              await update($, runValues, () => ({}))
              await update($, runFile, () => v)
            }}
          />
        )}
        {offered.more > 0 && <Text dimColor>  and {offered.more} more Flowfiles not offered (past the list's bound, or a name the form does not send)</Text>}
        {schema && 'error' in schema && <Text dimColor>  Inputs unavailable ({schema.error}). Fix the file, or run it from a terminal.</Text>}
        {schema && 'fields' in schema && (
          <Box flexDirection="column">
            {fields.length === 0 && <Text dimColor>  {file} declares no inputs.</Text>}
            {fields.map(f => {
              const label = `${f.name}${f.required ? ' *' : ''} (${f.type})`
              const why = checked.errors[f.name]
              const raw = valueOf(f, typed)
              return (
                <Box flexDirection="column">
                  {f.sensitive ? (
                    <Text dimColor>  {label} is sensitive: the pane never collects it{f.required ? '' : ' and sends nothing for it'}.</Text>
                  ) : f.refused ? (
                    <Text dimColor>  {label} is not offered: {f.refused}.</Text>
                  ) : f.kind === 'bool' || f.kind === 'enum' ? (
                    <Select
                      key={`in:${f.name}`}
                      label={label}
                      options={[
                        ...(f.initial === '' ? [{ value: '', label: f.required ? '(choose)' : '(not set)' }] : []),
                        ...(f.kind === 'bool' ? ['true', 'false'] : f.choices).map(value => ({ value })),
                      ]}
                      value={raw}
                      onSelect={v => setValue(f.name, v)}
                    />
                  ) : (
                    <Input
                      key={`in:${f.name}`}
                      label={label}
                      placeholder={f.example ? `e.g. ${f.example}` : f.kind === 'json' ? 'JSON' : ''}
                      value={clean(raw, MAX_VALUE + 1)}
                      submitLabel="set"
                      onInput={v => setValue(f.name, v)}
                      onSubmit={v => setValue(f.name, v)}
                    />
                  )}
                  {f.help !== '' && <Text dimColor>      {f.help}</Text>}
                  {f.initial !== '' && !Object.hasOwn(typed, f.name) && <Text dimColor>      default {f.initial}</Text>}
                  {why && !f.sensitive && <Text color={raw === '' ? COLOR.wait : COLOR.fail}>      {raw === '' ? '' : '✗ '}{why}</Text>}
                </Box>
              )
            })}
            {checked.blocked !== '' ? (
              <Text dimColor>  Run locally is unavailable: {checked.blocked}</Text>
            ) : (
              !questioned && (
                <Button
                  key="run-local"
                  label="Run locally"
                  plain
                  onPress={async () => {
                    // The first press only asks: nothing runs until Confirm.
                    const now = await read($, runValues)
                    if (checkForm(fields, now).blocked !== '') return
                    await update($, runResult, () => NO_RUN_RESULT)
                    await update($, runConfirm, () => ({ file, inputs: submission(fields, now) }))
                  }}
                >
                  Run locally
                </Button>
              )
            )}
            {questioned && (
              <Box flexDirection="column">
                {confirmLines(asking.file, asking.inputs).map((l, i) => (
                  <Text color={i === 0 ? COLOR.wait : undefined} dimColor={i !== 0}>
                    {'  '}
                    {l}
                  </Text>
                ))}
                <Box>
                  <Button
                    key="confirm-run"
                    label="Confirm: run locally"
                    plain
                    onPress={async () => {
                      if (running) return
                      running = true
                      try {
                        const c = await read($, runConfirm)
                        // This button was drawn for one question: if it has moved on, it acts on nothing.
                        if (c.file === '' || c.file !== file) return
                        await update($, runConfirm, () => NO_RUN_CONFIRM)
                        const stop = (why: string) => update($, runResult, () => ({ file: c.file, kind: 'notrun' as const, text: `not run: ${why}`, lines: [] }))
                        // Everything is read again: the file must still be a listed Flowfile and its declaration must still accept exactly these values.
                        if (!(await listFlowfiles($)).files.includes(c.file)) return stop('the file is no longer listed')
                        const fresh = await readInputs($, flow, c.file)
                        if (!('fields' in fresh)) return stop(`its inputs could not be read (${fresh.error})`)
                        const values = Object.fromEntries(fresh.fields.map(f => [f.name, c.inputs.find(i => i.name === f.name)?.value ?? '']))
                        const blocked = checkForm(fresh.fields, values).blocked
                        if (blocked !== '') return stop(blocked)
                        const same = JSON.stringify(submission(fresh.fields, values)) === JSON.stringify(c.inputs)
                        const argv = same ? runArgv(flow, c.file, c.inputs, fresh.fields) : undefined
                        if (argv === undefined) return stop('the form no longer matches the file, or a value is outside what the form sends')
                        let result: Result
                        try {
                          result = resultOf(await $.process.run(argv, { timeoutMs: RUN_TIMEOUT_MS }), c.file)
                        } catch (err) {
                          result = unknownResult(err, c.file)
                        }
                        await update($, runResult, () => ({ file: c.file, ...result }))
                      } finally {
                        running = false
                      }
                    }}
                  >
                    Confirm: run locally
                  </Button>
                  <Button key="cancel-run" label="Cancel" plain onPress={() => update($, runConfirm, () => NO_RUN_CONFIRM)}>
                    Cancel
                  </Button>
                </Box>
              </Box>
            )}
          </Box>
        )}
        {ranLast.file === chosen && ranLast.text !== '' && (
          <Box flexDirection="column">
            <Text color={COLOR[badge(ranLast.kind).tone]}>
              {'  '}
              {badge(ranLast.kind).symbol} {ranLast.text}
            </Text>
            {ranLast.lines.map(l => (
              <Text dimColor>
                {'      '}
                {l}
              </Text>
            ))}
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
