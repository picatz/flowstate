import { SIGNAL_NAME, WORKFLOW_ID, getArgv, parseGates, signalArgv, targetOf } from '../hooks/signal'
import { expect, mock, test } from 'claude-code/testing'

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
  signal?: Reply
}

// Answer each flow verb separately, record every argv, and pin FLOWSTATE_ADDRESS.
const stub = (on: any, answers: Answers, seen: string[][] = [], address: string | null = 'prod.example:9233') => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = answers[e.argv[1] as keyof Answers] ?? fail(`no ${e.argv[1]} stubbed`)
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  mock.env(on, address === null ? {} : { FLOWSTATE_ADDRESS: address })
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
  await ui.press({ key: 'confirm-signal' })
  expect(signals(seen)).toEqual([['flow', 'signal', '--', 'wf-1', 'deploy-approved']])
  await ui.unmount()
})

test('Cancel runs nothing and takes the question away', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'cancel-signal' })

  expect(signals(seen)).toEqual([])
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(await ui.find({ type: 'Button', text: /Send signal deploy-approved/ })).toBeDefined()
  await ui.unmount()
})

test('Confirm runs exactly one flow signal with the expected argv, then the card refreshes from flow get and the timeline', async ($, on) => {
  const { ui, seen } = await open($, on, {})
  await ui.press({ key: 'signal:deploy-approved' })
  const before = seen.filter(a => a[1] === 'timeline').length
  await ui.press({ key: 'confirm-signal' })

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
  await ui.press({ key: 'confirm-signal' })

  expect(signals(seen)).toHaveLength(1)
  const shown = await ui.find({ type: 'Text', text: /✗ not sent: permission denied: the starter of a run may not approve it/ })
  expect(shown).toBeDefined()
  expect(shown?.text).not.toMatch(/\u001b/)
  expect(shown?.text).not.toMatch(/ask someone else/)
  expect(shown!.text.length).toBeLessThan(330)
  await ui.unmount()
})

test('a flow signal that fails with nothing on stderr still says it was not sent', async ($, on) => {
  const { ui } = await open($, on, { signal: fail('') })
  await ui.press({ key: 'signal:deploy-approved' })
  await ui.press({ key: 'confirm-signal' })
  expect(await ui.find({ type: 'Text', text: /✗ not sent: no server answered/ })).toBeDefined()
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
  expect(parseGates('{}')).toEqual({ gates: [], more: 0 })
  expect(parseGates('{"progress":{"pendingWaits":[null,1,{"stepId":"s"}]}}')).toEqual({ gates: [], more: 0 })
  const many = JSON.stringify({ progress: { pendingWaits: Array.from({ length: 500 }, (_, i) => ({ signalName: `g${i}` })) } })
  const parsed = parseGates(many)
  expect(parsed.gates).toHaveLength(5)
  expect(parsed.more).toBeGreaterThan(0)
  expect(parsed.more).toBeLessThan(20)
})
