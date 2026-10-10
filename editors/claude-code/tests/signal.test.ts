import { clean } from '../hooks/runs'
import { outcomeOf, unknownOutcome, SIGNAL_NAME, WORKFLOW_ID, getArgv, moreText, parseGates, signalArgv, targetOf } from '../hooks/signal'
import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

type Reply = { exitCode: number; stdout: string; stderr: string }
const ok = (stdout: string, stderr = ''): Reply => ({ exitCode: 0, stdout, stderr })
const fail = (stderr: string): Reply => ({ exitCode: 1, stdout: '', stderr })

const list = (id: string) =>
  ok(JSON.stringify({ runs: [{ workflowId: id, runId: 'r', status: 'STATUS_RUNNING', name: 'Deploy', startTime: '2026-10-09T10:00:00Z', closeTime: null }] }))
const timeline = ok(
  JSON.stringify({
    entries: [
      { eventId: '1', time: '2026-10-09T10:00:00Z', kind: 'KIND_STEP_SCHEDULED', step: 'build', attempt: 1, failure: '' },
      { eventId: '2', time: '2026-10-09T10:00:05Z', kind: 'KIND_STEP_COMPLETED', step: 'build', attempt: 1, failure: '' },
      { eventId: '3', time: '2026-10-09T10:00:06Z', kind: 'KIND_TIMER_STARTED', step: 'approval', attempt: 1, failure: '' },
    ],
  }),
)
const wait = (extra: Record<string, unknown> = {}) => ({ stepId: 'approval', signalName: 'deploy-approved', policed: true, prompt: 'Ship build 41 to production?', ...extra })
const got = (...waits: unknown[]) => ok(JSON.stringify({ workflowId: 'wf-1', status: 'STATUS_RUNNING', progress: { stepId: 'approval', pendingWaits: waits } }))

interface Answers {
  list?: Reply
  timeline?: Reply
  get?: Reply
  signal?: Reply | 'deny'
}

// Answer each flow verb separately, record every argv, and pin FLOWSTATE_ADDRESS.
// What FLOWSTATE_ADDRESS answers right now: a string, null for unset, 'fail' for a lookup that errors.
const env: { address: string | null } = { address: null }

const stub = (on: any, answers: Answers, seen: string[][] = [], address: string | null = 'prod.example:9233') => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = answers[e.argv[1] as keyof Answers] ?? fail(`no ${e.argv[1]} stubbed`)
    if (reply === 'deny') return { deny: 'timed out' }
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  env.address = address
  on('env.get', () => (env.address === 'fail' ? { deny: 'unavailable' } : { value: env.address ?? undefined }))
  return seen
}
const signals = (seen: string[][]) => seen.filter(a => a[1] === 'signal')
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')

const open = async ($: any, on: any, answers: Answers, id = 'wf-1', address?: string | null) => {
  const seen: string[][] = []
  stub(on, { list: list(id), timeline, get: got(wait()), signal: ok('delivered deploy-approved to wf-1\n'), ...answers }, seen, address)
  const ui = await mount($)
  await ui.press({ key: `run:${id.replace(/[\u0000-\u001f]/g, '')}` })
  return { ui, seen }
}

test('a waiting run shows its gate in plain words and a verb button, and reads it with flow get', async ($, on) => {
  const { ui, seen } = await open($, on, {})

  expect(seen.some(a => a.join(' ') === 'flow get -o json --address=prod.example:9233 -- wf-1')).toBe(true)
  expect(await ui.find({ type: 'Text', text: /Gate deploy-approved \(step approval\) waits for a signal/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /Ship build 41 to production\?/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /waits until answered; the workflow declares who may act/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Send signal deploy-approved/ })).toBeDefined()
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('no gate means no button, and no flow signal', async ($, on) => {
  // The run is waiting on a timer, not a person: flow get reports no pending waits.
  const { ui, seen } = await open($, on, { get: got() })
  expect(await ui.find({ type: 'Text', text: /approval.*waiting/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /Gate / })).toBeUndefined()
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('a finished run is not asked about gates at all', async ($, on) => {
  const done = list('wf-9')
  done.stdout = done.stdout.replace('STATUS_RUNNING', 'STATUS_COMPLETED')
  const { ui, seen } = await open($, on, { list: done, timeline: ok('{}'), get: got(wait()) }, 'wf-9')
  expect(seen.some(a => a[1] === 'get')).toBe(false)
  expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
  await ui.unmount()
})

for (const [label, reply] of [['fails', fail('ERROR\nno server\n')], ['is not json', ok('not json')], ['is an array', ok('[1]')], ['has no waits list', ok('{"progress":{"pendingWaits":"x"}}')]] as const) {
  test(`flow get that ${label} shows no button`, async ($, on) => {
    const { ui, seen } = await open($, on, { get: reply })
    expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
    expect(signals(seen)).toEqual([])
    await ui.unmount()
  })
}

test('the first press asks and runs NOTHING; the question names the verb, the signal, the run and the server', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })

  const ask = await ui.find({ type: 'Text', text: /Send signal "deploy-approved" to run wf-1 on server prod\.example:9233\? Nothing is sent until you confirm\./ })
  expect(ask).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Confirm: send deploy-approved/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Cancel/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /server decides whether you may act/ })).toBeDefined()
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('without FLOWSTATE_ADDRESS the question names the default server the CLI will use', async ($, on) => {
  const { ui, seen } = await open($, on, {}, 'wf-1', null)
  expect(seen.some(a => a.join(' ') === 'flow get -o json -- wf-1')).toBe(true)
  await ui.press({ key: 'signal:deploy-approved' })
  expect(await ui.find({ type: 'Text', text: /on server localhost:9233 \(the default; FLOWSTATE_ADDRESS is unset\)/ })).toBeDefined()
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(signals(seen)).toEqual([['flow', 'signal', '--', 'wf-1', 'deploy-approved']])
  await ui.unmount()
})

test('Cancel runs nothing and takes the question away', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'cancel-signal:deploy-approved' })

  expect(signals(seen)).toEqual([])
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(await ui.find({ type: 'Button', text: /Send signal deploy-approved/ })).toBeDefined()
  await ui.unmount()
})

test('Confirm runs exactly one flow signal with the expected argv, then the card refreshes from flow get and the timeline', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  const before = seen.filter(a => a[1] === 'timeline').length
  await ui.press({ key: 'confirm-signal:deploy-approved' })

  expect(signals(seen)).toEqual([['flow', 'signal', '--address=prod.example:9233', '--', 'wf-1', 'deploy-approved']])
  expect(await ui.find({ type: 'Text', text: /✓ delivered deploy-approved to wf-1/ })).toBeDefined()
  expect(seen.filter(a => a[1] === 'timeline').length).toBeGreaterThan(before)
  // The question is gone: a second Confirm has nothing to confirm.
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  await ui.unmount()
})

test('a failing flow signal shows the server\'s refusal, cleaned and bounded, and is not retried', async ($, on) => {
  const refusal = `ERROR\npermission denied: the starter of a run may not approve it\u001b[31m ${'x'.repeat(600)}\n\nNEXT\n  ask someone else`
  const { ui, seen } = await open($, on, { signal: fail(refusal) })
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'confirm-signal:deploy-approved' })

  expect(signals(seen)).toHaveLength(1)
  const shown = await ui.find({ type: 'Text', text: /✗ Not sent: permission denied: the starter of a run may not approve it/ })
  expect(shown).toBeDefined()
  expect(shown?.text).not.toMatch(/\u001b/)
  expect(shown?.text).not.toMatch(/ask someone else/)
  expect(shown!.text.length).toBeLessThan(330)
  await ui.unmount()
})

test('a flow signal that fails with nothing on stderr still says it was not sent', async ($, on) => {
  const { ui } = await open($, on, { signal: fail('') })
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(await ui.find({ type: 'Text', text: /✗ Not sent: no server answered/ })).toBeDefined()
  await ui.unmount()
})

const NAMES = ['approve; rm -rf /', '--address=evil:1', '$(id)', 'a b', '-x', '', 'ok\u001b[2Jname', 'x'.repeat(129)]
for (const name of NAMES) {
  test(`a hostile gate name is refused: ${JSON.stringify(name.slice(0, 20))}`, async ($, on) => {
    const { ui, seen } = await open($, on, { get: got(wait({ signalName: name, prompt: 'go\u001b]0;pwned\u0007?' })) })
    expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
    expect(await ui.find({ type: 'Text', text: /No button: its signal name is not one `flow signal` accepts/ })).toBeDefined()
    expect(await texts(ui)).not.toMatch(/[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/)
    expect(signals(seen)).toEqual([])
    await ui.unmount()
  })
}

for (const id of ['wf 1', 'wf-1;id', '--help', '$(id)', 'wf\u001b[31m-1']) {
  test(`a hostile run id gets no server verb: ${JSON.stringify(id)}`, async ($, on) => {
    const { ui, seen } = await open($, on, {}, id)
    expect(seen.some(a => a[1] === 'get' || a[1] === 'signal')).toBe(false)
    expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
    await ui.unmount()
  })
}

test('a hostile prompt and step id are cleaned before they are drawn', async ($, on) => {
  const { ui } = await open($, on, { get: got(wait({ prompt: 'ship\u001b[31m it?\n\nNEXT', stepId: 'ap\u001b[2Jproval' })) })
  expect(await ui.find({ type: 'Text', text: /\(step ap\[2Jproval\)/ })).toBeDefined()
  expect(await texts(ui)).not.toMatch(/[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/)
  await ui.unmount()
})

test('a FLOWSTATE_ADDRESS that is not a plain address gets no button', async ($, on) => {
  const { ui, seen } = await open($, on, {}, 'wf-1', 'evil.example:1 --data={}')
  expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /No signal button: FLOWSTATE_ADDRESS is not a plain server address/ })).toBeDefined()
  expect(seen.some(a => a[1] === 'get' || a[1] === 'signal')).toBe(false)
  await ui.unmount()
})

test('a quorum and a deadline are shown; more gates than fit say so', async ($, on) => {
  const waits = Array.from({ length: 7 }, (_, i) => wait({ signalName: `g-${i}`, stepId: `s${i}`, approvals: 1, approvalsNeeded: 2, deadline: '2026-10-10T00:00:00Z' }))
  const { ui } = await open($, on, { get: got(...waits) })
  expect(await ui.find({ type: 'Text', text: /1 of 2 approvals/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /lapses 2026-10-10T00:00:00Z/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /and 2 more gates/ })).toBeDefined()
  await ui.unmount()
})

test('closing the card drops a pending question, so reopening it asks again', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'close-run' })
  await ui.press({ key: 'run:wf-1' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('the argv is plain: the payload is one element, and ids and names outside the allowlist build nothing', () => {
  expect(signalArgv('flow', 'h:1', 'wf-1', 'go')).toEqual(['flow', 'signal', '--address=h:1', '--', 'wf-1', 'go'])
  const payload = '{"approved": true, "by": "a b; $(id) --address=evil"}'
  const argv = signalArgv('flow', '', 'wf-1', 'go', payload)!
  expect(argv).toEqual(['flow', 'signal', `--data=${payload}`, '--', 'wf-1', 'go'])
  expect(argv.filter(a => a.includes('approved'))).toHaveLength(1)
  expect(signalArgv('flow', '', 'wf-1', 'go', '')).toEqual(['flow', 'signal', '--', 'wf-1', 'go'])

  for (const bad of ['', 'a b', '-x', 'a;b', '$(x)', 'a\nb', 'é', 'x'.repeat(129)]) expect(signalArgv('flow', '', 'wf-1', bad)).toBeUndefined()
  for (const bad of ['', 'a b', '--help', 'a;b', 'a`b`', 'a\nb', 'x'.repeat(257)]) expect(signalArgv('flow', '', bad, 'go')).toBeUndefined()
  expect(getArgv('flow', '', 'a b')).toBeUndefined()
  expect(SIGNAL_NAME.test('deploy-approved_2')).toBe(true)
  expect(WORKFLOW_ID.test('flowstate-workflow-3f7c')).toBe(true)
  expect(targetOf(undefined)).toEqual({ address: '' })
  expect(targetOf('https://flow.example:9233')).toEqual({ address: 'https://flow.example:9233' })
  expect('refused' in targetOf('a b')).toBe(true)
  expect('refused' in targetOf('-x')).toBe(true)
})

test('parseGates reads only pendingWaits and bounds its work', () => {
  expect(parseGates('{}')).toEqual({ gates: [], more: 0, atLeast: false })
  expect(parseGates('{"progress":{"pendingWaits":[null,1,{"stepId":"s"}]}}')).toEqual({ gates: [], more: 0, atLeast: false })
  const many = JSON.stringify({ progress: { pendingWaits: Array.from({ length: 500 }, (_, i) => ({ signalName: `g${i}` })) } })
  const parsed = parseGates(many)
  expect(parsed.gates).toHaveLength(5)
  expect(parsed.more).toBeGreaterThan(0)
  expect(parsed.more).toBe(495)
})

test('if the address changes between the question and Confirm, nothing is sent', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  env.address = 'other.example:9233'
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('if the address cannot be read at Confirm, nothing is sent', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  env.address = 'fail'
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('an address lookup that fails is an unknown target, not the default: no get, no button, and it says why', async ($, on) => {
  const { ui, seen } = await open($, on, {}, 'wf-1', 'fail')
  expect(await ui.find({ type: 'Button', text: /Send signal/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /No signal button: FLOWSTATE_ADDRESS could not be read, so the target server is not known/ })).toBeDefined()
  expect(seen.some(a => a[1] === 'get' || a[1] === 'signal')).toBe(false)
  await ui.unmount()
})

test('two Confirm presses at once send exactly one signal', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  await Promise.all([ui.press({ key: 'confirm-signal:deploy-approved' }), ui.press({ key: 'confirm-signal:deploy-approved' })])
  expect(signals(seen)).toHaveLength(1)
  await ui.unmount()
})

const two = ok(JSON.stringify({ runs: ['wf-a', 'wf-b'].map(workflowId => ({ workflowId, runId: 'r', status: 'STATUS_RUNNING', name: '', startTime: '2026-10-09T10:00:00Z', closeTime: null })) }))

test('a question asked on one run is gone when that run is selected again', async ($, on) => {
  const { ui, seen } = await open($, on, { list: two }, 'wf-a')
  await ui.press({ key: 'signal:deploy-approved' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeDefined()
  await ui.press({ key: 'run:wf-b' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  await ui.press({ key: 'run:wf-a' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(await ui.find({ type: 'Button', text: /Send signal deploy-approved/ })).toBeDefined()
  expect(signals(seen)).toEqual([])
  await ui.unmount()
})

test('Send on gate B then a Confirm drawn for gate A sends nothing for A', async ($, on) => {
  const { ui, seen } = await open($, on, { get: got(wait({ signalName: 'gate-a' }), wait({ signalName: 'gate-b', stepId: 'second' })) })
  await ui.press({ key: 'signal:gate-a' })
  expect(await ui.find({ type: 'Button', text: /Confirm: send gate-a/ })).toBeDefined()
  await ui.press({ key: 'signal:gate-b' })
  // The question moved to B: A's Confirm is no longer drawn, and pressing it sends nothing.
  await expect(ui.press({ key: 'confirm-signal:gate-a' })).rejects.toThrow()
  expect(signals(seen)).toEqual([])
  expect(await ui.find({ type: 'Button', text: /Confirm: send gate-b/ })).toBeDefined()
  await ui.press({ key: 'confirm-signal:gate-b' })
  expect(signals(seen)).toEqual([['flow', 'signal', '--address=prod.example:9233', '--', 'wf-1', 'gate-b']])
  await ui.unmount()
})

test('a signal that times out or cannot run is "delivery unknown", never "not sent"', async ($, on) => {
  const { ui, seen } = await open($, on, { signal: 'deny' })
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(signals(seen)).toHaveLength(1)
  const shown = await ui.find({ type: 'Text', text: /delivery unknown for deploy-approved on wf-1/ })
  expect(shown?.text).toMatch(/check the timeline before sending again/)
  expect(shown?.text).not.toMatch(/not sent/)
  await ui.unmount()
})

test('a prompt the server cut, or this card cut, is marked as partial', async ($, on) => {
  const server = await open($, on, { get: got(wait({ promptTruncated: true })) })
  expect(await server.ui.find({ type: 'Text', text: /Ship build 41 to production\? \[prompt truncated\]/ })).toBeDefined()
  await server.ui.unmount()
})

test('a prompt this card cuts at its own bound is marked as partial', async ($, on) => {
  const { ui } = await open($, on, { get: got(wait({ prompt: 'q'.repeat(400) })) })
  expect(await ui.find({ type: 'Text', text: /q{160} \[prompt truncated\]/ })).toBeDefined()
  await ui.unmount()
})

test('a whole prompt carries no marker', async ($, on) => {
  const { ui } = await open($, on, {})
  expect(await texts(ui)).not.toMatch(/prompt truncated/)
  await ui.unmount()
})

test('gates beyond the ones shown are counted from the whole answer, and a truncated answer says "at least"', () => {
  const waits = Array.from({ length: 64 }, (_, i) => ({ signalName: `g${i}` }))
  const all = parseGates(JSON.stringify({ progress: { pendingWaits: waits } }))
  expect(all.gates).toHaveLength(5)
  expect(all.more).toBe(59)
  expect(all.atLeast).toBe(false)
  expect(moreText(all)).toMatch(/^and 59 more gates/)

  const cut = parseGates(JSON.stringify({ progress: { pendingWaits: waits, pendingWaitsTruncated: true } }))
  expect(cut.more).toBe(59)
  expect(moreText(cut)).toMatch(/^and at least 59 more gates/)

  const exact = parseGates(JSON.stringify({ progress: { pendingWaits: waits.slice(0, 3), pendingWaitsTruncated: true } }))
  expect(exact.more).toBe(0)
  expect(moreText(exact)).toMatch(/^and more gates the run did not report/)
  expect(moreText(parseGates(JSON.stringify({ progress: { pendingWaits: waits.slice(0, 3) } })))).toBe('')
})

test('clean drops zero-width and bidi format characters', () => {
  const hostile = 'ap\u202eprove\u200b\u200f\u2066x\u2069\ufeff'
  expect(clean(hostile)).toBe('approvex')
  expect(clean('ünï — ok')).toBe('ünï — ok')
})

const LONG = 'flowstate-workflow-3f7c9a2e-5b1d-4c8e-9a6f-0d2b7e1c4a58-and-more-text'
const FINISHED_STDERR = `ERROR\nsignalling "${LONG}": failed_precondition: delivering a signal to run "${LONG}": that workload has already finished\n\nNEXT\n  flow get ${LONG}`

test('outcomeOf says plainly that the run had already finished, with no id', () => {
  const out = outcomeOf({ exitCode: 1, stderr: FINISHED_STDERR }, LONG, 'deploy-approved')
  expect(out).toEqual({ ok: false, text: 'Nothing sent: the run had already finished.' })
  const stale = `ERROR\nfailed_precondition: delivering a signal to run "${LONG}": the execution named by that run id has already finished; retry without it`
  expect(outcomeOf({ exitCode: 1, stderr: stale }, LONG, 'x').text).toBe('Nothing sent: the run had already finished.')
})

test('any other refusal is "Not sent: <cause>" with ids cut and never repeated whole', () => {
  const out = outcomeOf({ exitCode: 1, stderr: `ERROR\nsignalling "${LONG}": permission denied: run "${LONG}" may not be approved by its starter` }, LONG, 'x')
  expect(out.ok).toBe(false)
  expect(out.text).toMatch(/^Not sent: permission denied: run "/)
  expect(out.text).not.toContain(LONG)
  expect(out.text).not.toMatch(/signalling/)
  expect(out.text.length).toBeLessThan(160)
})

test('delivery unknown keeps its honesty but cuts the id', () => {
  const out = unknownOutcome(new Error(`timed out waiting for "${LONG}"`), LONG, 'deploy-approved')
  expect(out.ok).toBe(false)
  expect(out.text).toMatch(/^delivery unknown for deploy-approved on /)
  expect(out.text).toMatch(/check the timeline before sending again/)
  expect(out.text).not.toContain(LONG)
})

test('a signal that finds the run done says so plainly and the card re-reads the run', async ($, on) => {
  const { ui, seen } = await open($, on, { signal: fail(FINISHED_STDERR) })
  await ui.press({ key: 'signal:deploy-approved' })
  const before = seen.filter(a => a[1] === 'timeline').length
  await ui.press({ key: 'confirm-signal:deploy-approved' })
  expect(signals(seen)).toHaveLength(1)
  expect(await ui.find({ type: 'Text', text: /✗ Nothing sent: the run had already finished\./ })).toBeDefined()
  // The outcome update redraws the pane, which reads the list and the timeline again: the card shows the truth.
  expect(seen.filter(a => a[1] === 'timeline').length).toBeGreaterThan(before)
  expect(await texts(ui)).not.toContain('failed_precondition')
  // The question is consumed: a second press of Confirm is not drawn.
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  await ui.unmount()
})

test('a transport-style error is never the confident finished line', () => {
  for (const cause of ['connection closed before the response', 'worker not running', 'the stream is no longer running', 'workflow execution already completed']) {
    const out = outcomeOf({ exitCode: 1, stderr: `ERROR\nunavailable: ${cause}` }, 'wf-1', 'x')
    expect(out.text).toBe(`Not sent: unavailable: ${cause}`)
  }
})
