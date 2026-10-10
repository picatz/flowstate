import { clean } from '../hooks/runs'
import { candidates, checkForm, cleanLines, parseInputs, resultOf, runArgv, submission } from '../hooks/form'
import type { Field } from '../hooks/form'
import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 80 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

type Reply = { exitCode: number; stdout: string; stderr: string }
const ok = (stdout: string, stderr = ''): Reply => ({ exitCode: 0, stdout, stderr })
const fail = (stderr: string, exitCode = 1): Reply => ({ exitCode, stdout: '', stderr })

/** An inputs schema as `flow compile --schema inputs` writes it. */
const schemaOf = (properties: Record<string, unknown>, required: string[] = []) =>
  ok(JSON.stringify({ $schema: 'https://json-schema.org/draft/2020-12/schema', title: 'x inputs', type: 'object', additionalProperties: false, properties, ...(required.length ? { required } : {}) }))

const DEPLOY = schemaOf(
  {
    service: { type: 'string', description: 'which service to deploy', minLength: 2 },
    region: { type: 'string', default: 'eu-west-1', description: 'where to deploy it' },
    replicas: { type: 'integer', default: 2, 'x-flowstate-must': 'this >= 1 && this <= 50' },
    ratio: { type: 'number', default: 0.5 },
    dry_run: { type: 'boolean', default: false, description: 'plan without announcing' },
    tier: { type: 'string', enum: ['canary', 'staging', 'production'], description: 'rollout tier' },
    tags: { type: 'array', items: { type: 'string' }, default: [] },
    order: { $ref: '#/$defs/Order', description: 'the order' },
  },
  ['service'],
)

const entry = (name: string, kind = 'file', mtimeMs = 1) => ({ name, kind, size: 1, mtimeMs, isLink: false })

interface World {
  /** Directory listings by path ('' is the working directory). */
  dirs?: Record<string, ReturnType<typeof entry>[]>
  compile?: Reply | 'deny'
  /** The reply to `--schema=outputs`; no declared outputs by default. */
  outputs?: Reply | 'deny'
  run?: Reply | 'deny'
}

const world: World = {}
/** The time limit each `flow run` was started with. */
const limits: unknown[] = []

const stub = (on: any, w: World, seen: string[][] = []) => {
  Object.assign(world, { dirs: { '': [entry('deploy.flow.yaml')] }, compile: DEPLOY, outputs: schemaOf({}), run: ok('COMPLETED workflow x\n'), ...w })
  on('process.run', (_$: unknown, e: { argv: string[]; init?: { timeoutMs?: number }; timeoutMs?: number }) => {
    seen.push(e.argv)
    if (e.argv[1] === 'run') limits.push(e.init?.timeoutMs ?? e.timeoutMs)
    const reply = e.argv[1] === 'compile' ? (e.argv.includes('--schema=outputs') ? world.outputs : world.compile) : e.argv[1] === 'run' ? world.run : fail(`no ${e.argv[1]} stubbed`)
    if (reply === 'deny') return { deny: 'timed out' }
    return { value: { ...reply!, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', (_$: unknown, e: { path: string }) => ({ value: world.dirs![/workflows$/.test(e.path ?? '') ? 'workflows' : ''] ?? world.dirs![''] ?? [] }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  return seen
}
/** The calls that read or run a Flowfile; the pane also lists runs, which is not what these tests are about. */
const formCalls = (seen: string[][]) => seen.filter(a => a[1] === 'compile' || a[1] === 'run')
const runs = (seen: string[][]) => seen.filter(a => a[1] === 'run')
const compiles = (seen: string[][]) => seen.filter(a => a[1] === 'compile')
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')
const control = async (ui: any, type: 'Input' | 'Select', key: string) => (await ui.findAll({ type })).find((c: { props: { key: string } }) => c.props.key === key)?.props
const NO_CONTROL_CHARS = /[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/

const open = async ($: any, on: any, w: World = {}, pick = 'deploy.flow.yaml') => {
  const seen: string[][] = []
  stub(on, w, seen)
  const ui = await mount($)
  if (pick) await ui.select({ plugin: 'flowstate', key: 'run-file', value: pick })
  return { ui, seen }
}
const type = (ui: any, name: string, text: string) => ui.input({ plugin: 'flowstate', key: `in:${name}`, text, kind: 'change' })
const pick = (ui: any, name: string, value: string) => ui.select({ plugin: 'flowstate', key: `in:${name}`, value })

/** A form that is valid as drawn: the one required input typed in. */
const filled = async ($: any, on: any, w: World = {}) => {
  const r = await open($, on, w)
  await type(r.ui, 'service', 'api')
  return r
}

test('a directory with no Flowfile offers none, and a test suite or link is not a Flowfile', async ($, on) => {
  const { ui, seen } = await open($, on, { dirs: { '': [entry('notes.md'), entry('x.test.yaml'), entry('testdefaults.yaml'), entry('a.flow.yaml', 'other')] } }, '')
  expect(await ui.find({ type: 'Text', text: /^\s*No Flowfile here\.$/ })).toBeDefined()
  expect(await ui.find({ type: 'Select' })).toBeUndefined()
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  expect(formCalls(seen)).toEqual([])
  await ui.unmount()
})

test('Flowfiles come from the working directory and its workflows/ directory, listed bounded', async ($, on) => {
  const { ui } = await open($, on, { dirs: { '': [entry('Flowfile'), entry('workflows', 'dir'), entry('b.flow.yaml')], workflows: [entry('deep.yaml'), entry('readme.md')] } }, '')
  const select = await control(ui, 'Select', 'run-file')
  expect(select.options.map((o: { value: string }) => o.value)).toEqual(['', 'Flowfile', 'b.flow.yaml', 'workflows/deep.yaml'])
  await ui.unmount()
})

test('picking a Flowfile reads its inputs with exactly flow compile --schema=inputs, after --', async ($, on) => {
  const { seen, ui } = await open($, on)
  expect(compiles(seen)).toEqual([['flow', 'compile', '-o', 'json', '--schema=inputs', '--', 'deploy.flow.yaml']])
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('each declared type gets its control, the declared default prefilled and the description as help', async ($, on) => {
  const { ui } = await open($, on)
  expect(await control(ui, 'Input', 'in:service')).toMatchObject({ label: 'service * (string)', value: '' })
  expect(await control(ui, 'Input', 'in:region')).toMatchObject({ label: 'region (string)', value: 'eu-west-1' })
  expect(await control(ui, 'Input', 'in:replicas')).toMatchObject({ label: 'replicas (int)', value: '2' })
  expect(await control(ui, 'Input', 'in:ratio')).toMatchObject({ label: 'ratio (number)', value: '0.5' })
  expect(await control(ui, 'Select', 'in:dry_run')).toMatchObject({ label: 'dry_run (bool)', value: 'false', options: [{ value: 'true' }, { value: 'false' }] })
  expect(await control(ui, 'Select', 'in:tier')).toMatchObject({ label: 'tier (enum)', value: '', options: [{ value: '', label: '(not set)' }, { value: 'canary' }, { value: 'staging' }, { value: 'production' }] })
  // A list and a record are one JSON field each.
  expect(await control(ui, 'Input', 'in:tags')).toMatchObject({ label: 'tags (JSON)', value: '[]' })
  expect(await control(ui, 'Input', 'in:order')).toMatchObject({ label: 'order (JSON)', value: '' })
  const all = await texts(ui)
  expect(all).toMatch(/which service to deploy/)
  expect(all).toMatch(/plan without announcing/)
  expect(all).toMatch(/\(rule: this >= 1 && this <= 50\)/)
  expect(all).toMatch(/default eu-west-1/)
  await ui.unmount()
})

for (const [label, reply] of [
  ['flow compile fails', fail('deploy.flow.yaml:3:1: unknown step "x"\n')],
  ['prints something that is not JSON', ok('not json')],
  ['prints an array', ok('[1]')],
  ['prints a schema that is not an object', ok('{"type":"string"}')],
  ['prints properties that are not a map', ok('{"type":"object","properties":[1]}')],
  ['prints a schema larger than the form reads', ok(`{"type":"object","title":"${'x'.repeat(300000)}"}`)],
  ['declares more inputs than the form draws', schemaOf(Object.fromEntries(Array.from({ length: 30 }, (_, i) => [`i${i}`, { type: 'string' }])))],
] as const) {
  test(`no schema, no form: flow compile ${label}`, async ($, on) => {
    const { ui, seen } = await open($, on, { compile: reply })
    expect(await ui.find({ type: 'Text', text: /Inputs unavailable \(/ })).toBeDefined()
    expect(await ui.find({ type: 'Input', key: 'in:service' })).toBeUndefined()
    expect(await ui.findAll({ type: 'Input' })).toHaveLength(1) // only the Runs filter
    expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
    expect(runs(seen)).toEqual([])
    await ui.unmount()
  })
}

test('a compile that cannot start (no flow binary) is no form and no run', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: 'deny' })
  expect(await ui.find({ type: 'Text', text: /Inputs unavailable \(/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('a workflow that declares no inputs says so and can still be run', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: schemaOf({}) })
  expect(await ui.find({ type: 'Text', text: /declares no inputs/ })).toBeDefined()
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([['flow', 'run', 'local', '--no-color', '--', 'deploy.flow.yaml']])
  await ui.unmount()
})

for (const [label, name, text, reason] of [
  ['an int that is not a whole number', 'replicas', '2.5', /replicas: must be a whole number/],
  ['an int that does not fit 64 bits', 'replicas', '9223372036854775808', /replicas: must be a whole number/],
  ['a number that is not one', 'ratio', 'NaN', /ratio: must be a number/],
  ['a string shorter than min_len', 'service', 'a', /service: shorter than 2 characters/],
  ['a list that is not JSON', 'tags', '[1,', /tags: must be valid JSON/],
  ['a list that is another JSON kind', 'tags', '{"a":1}', /tags: must be a JSON list/],
  ['a record that is not an object', 'order', '[1]', /order: must be a JSON object/],
  ['a value with a control character', 'region', 'eu\u001b[2J', /region: contains a control or invisible character/],
  ['a value with a zero-width character', 'region', 'eu​-west', /region: contains a control or invisible character/],
  ['a value over the bound', 'region', 'x'.repeat(1001), /region: longer than 1000 characters/],
] as const) {
  test(`invalid input blocks Run with the reason: ${label}`, async ($, on) => {
    const { ui, seen } = await filled($, on)
    expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeDefined()
    await type(ui, name, text)
    expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
    expect(await ui.find({ type: 'Text', text: /Run locally is unavailable/ })).toBeDefined()
    expect((await texts(ui)).match(/Run locally is unavailable: .*/)?.[0]).toMatch(reason)
    await expect(ui.press({ key: 'run-local' })).rejects.toThrow()
    expect(runs(seen)).toEqual([])
    await ui.unmount()
  })
}

test('a required input with nothing typed blocks Run and says it is required; typing it unblocks', async ($, on) => {
  const { ui, seen } = await open($, on)
  expect(await ui.find({ type: 'Text', text: /Run locally is unavailable: service: required/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  await type(ui, 'service', 'api')
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeDefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('the first press only asks: it sends NOTHING, and the question names the verb, the file and the values', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await pick(ui, 'tier', 'staging')
  await ui.press({ key: 'run-local' })

  expect(runs(seen)).toEqual([])
  const all = await texts(ui)
  expect(all).toMatch(/Run `flow run local` on deploy\.flow\.yaml\? It executes the workflow's tasks, which can have side effects\./)
  expect(all).toMatch(/with these inputs:/)
  for (const line of ['service = api', 'region = eu-west-1', 'replicas = 2', 'ratio = 0.5', 'dry_run = false', 'tier = staging', 'tags = []']) expect(all).toContain(`  ${line}`)
  expect(all).not.toMatch(/order =/) // an empty optional input is not sent
  expect(all).toMatch(/Nothing runs until you confirm\. A run is stopped after 30 seconds and then reported as outcome unknown\./)
  expect(await ui.find({ type: 'Button', text: /Confirm: run locally/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /Cancel/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /^Run locally$/ })).toBeUndefined()
  await ui.unmount()
})

test('Cancel runs nothing and takes the question away', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'cancel-run' })
  expect(runs(seen)).toEqual([])
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeDefined()
  await expect(ui.press({ key: 'confirm-run' })).rejects.toThrow()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('Confirm runs exactly one flow run local, argv only, one --input= element per input, `--` before the file', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await type(ui, 'region', '-x,y=z "q" $(id); a b')
  await pick(ui, 'dry_run', 'true')
  await type(ui, 'tags', '["a,b","--input=evil"]')
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })

  expect(runs(seen)).toEqual([
    [
      'flow', 'run', 'local', '--no-color',
      '--input=service=api',
      '--input=region=-x,y=z "q" $(id); a b',
      '--input=replicas=2',
      '--input=ratio=0.5',
      '--input=dry_run=true',
      '--input=tags=["a,b","--input=evil"]',
      '--', 'deploy.flow.yaml',
    ],
  ])
  expect(await ui.find({ type: 'Text', text: /✓ ran deploy\.flow\.yaml/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /COMPLETED workflow x/ })).toBeDefined()
  // The question is gone: a second Confirm has nothing to confirm.
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  await ui.unmount()
})

test('a Flowfile in workflows/ is run by its relative path', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: schemaOf({}), dirs: { '': [entry('workflows', 'dir')], workflows: [entry('deep.yaml')] } }, 'workflows/deep.yaml')
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([['flow', 'run', 'local', '--no-color', '--', 'workflows/deep.yaml']])
  await ui.unmount()
})

test('two Confirm presses at once run exactly one workflow', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  await Promise.all([ui.press({ key: 'confirm-run' }), ui.press({ key: 'confirm-run' })])
  expect(runs(seen)).toHaveLength(1)
  await ui.unmount()
})

test('an edit after the question takes the question away: Confirm can only run what was named', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  await type(ui, 'service', 'api2')
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  await expect(ui.press({ key: 'confirm-run' })).rejects.toThrow()
  expect(runs(seen)).toEqual([])
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toHaveLength(1)
  expect(runs(seen)[0]).toContain('--input=service=api2')
  await ui.unmount()
})

test('choosing another Flowfile drops a pending question and the typed values', async ($, on) => {
  const dirs = { '': [entry('deploy.flow.yaml'), entry('other.flow.yaml')] }
  const { ui, seen } = await filled($, on, { dirs })
  await ui.press({ key: 'run-local' })
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'other.flow.yaml' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect((await control(ui, 'Input', 'in:service')).value).toBe('')
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'deploy.flow.yaml' })
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('typing does not recompile the file; an edited file does', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await type(ui, 'region', 'us-east-1')
  await type(ui, 'region', 'us-east-2')
  expect(compiles(seen)).toHaveLength(1)
  world.dirs = { '': [entry('deploy.flow.yaml', 'file', 99)] }
  await type(ui, 'region', 'us-east-3')
  expect(compiles(seen)).toHaveLength(2)
  await ui.unmount()
})

test('a failing run shows the engine\'s message, cleaned and bounded, and is not retried', async ($, on) => {
  const refusal = `ERROR\ninput "replicas" must satisfy \`this >= 1\`; got 99\u001b[31m ${'x'.repeat(600)}\n  arguments are given with --input name=value\u001b]0;pwned\u0007`
  const { ui, seen } = await filled($, on, { run: fail(refusal) })
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })

  expect(runs(seen)).toHaveLength(1)
  const head = await ui.find({ type: 'Text', text: /✗ failed \(exit 1\) deploy\.flow\.yaml \(output cut\)/ })
  expect(head).toBeDefined()
  const shown = await ui.find({ type: 'Text', text: /input "replicas" must satisfy `this >= 1`; got 99/ })
  expect(shown).toBeDefined()
  expect(shown!.text.length).toBeLessThan(260)
  expect(await ui.find({ type: 'Text', text: /arguments are given with --input name=value/ })).toBeDefined()
  const all = await texts(ui)
  expect(all).not.toMatch(NO_CONTROL_CHARS)
  expect(all).not.toMatch(/pwned/)
  expect(all).not.toMatch(/^\s*ERROR\s*$/m)
  await ui.unmount()
})

test('a failing run with nothing on stderr still says it failed', async ($, on) => {
  const { ui } = await filled($, on, { run: fail('', 3) })
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(await ui.find({ type: 'Text', text: /✗ failed \(exit 3\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /flow printed no message/ })).toBeDefined()
  await ui.unmount()
})

test('a successful run shows its output cleaned and bounded', async ($, on) => {
  const out = Array.from({ length: 40 }, (_, i) => `INFO line ${i}\u001b[32m \u0007`).join('\n')
  const { ui } = await filled($, on, { run: ok(out) })
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(await ui.find({ type: 'Text', text: /✓ ran deploy\.flow\.yaml \(output cut\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /INFO line 11\s*$/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /INFO line 12/ })).toBeUndefined()
  expect(await texts(ui)).not.toMatch(NO_CONTROL_CHARS)
  await ui.unmount()
})

for (const label of ['times out or cannot be started']) {
  test(`a run that ${label} is "outcome unknown", never "failed"`, async ($, on) => {
    const { ui, seen } = await filled($, on, { run: 'deny' })
    await ui.press({ key: 'run-local' })
    await ui.press({ key: 'confirm-run' })
    expect(runs(seen)).toHaveLength(1)
    const head = await ui.find({ type: 'Text', text: /\? outcome unknown for deploy\.flow\.yaml/ })
    expect(head).toBeDefined()
    expect(head!.text).not.toMatch(/failed/)
    expect(await ui.find({ type: 'Text', text: /may have started, finished or stopped part way/ })).toBeDefined()
    expect(await ui.find({ type: 'Text', text: /✗/ })).toBeUndefined()
    await ui.unmount()
  })
}

test('the run is given the timeout $.process.run allows by default (30 s), and no more', async ($, on) => {
  const { ui } = await filled($, on)
  limits.length = 0
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(limits).toEqual([30000])
  await ui.unmount()
})

test('hostile schema text is cleaned before it is drawn', async ($, on) => {
  const compile = schemaOf({
    service: { type: 'string', description: 'ship\u001b[31m it‮​ now\n\nNEXT', 'x-flowstate-must': 'this != "\u001b]0;x\u0007"', examples: ['ex\u001b[2Jample'] },
  }, ['service'])
  const { ui } = await open($, on, { compile })
  const all = await texts(ui)
  expect(all).toMatch(/ship\[31m it now/)
  expect(all).not.toMatch(NO_CONTROL_CHARS)
  expect(all).not.toMatch(/[‮​]/)
  expect((await control(ui, 'Input', 'in:service')).placeholder).toBe('e.g. ex[2Jample')
  await ui.unmount()
})

for (const [label, props, reason] of [
    ['default', { region: { type: 'string', default: 'eu\u001b[2J' } }, /region: its declared default cannot be shown or sent as declared/],
    ['enum value', { tier: { type: 'string', enum: ['ok', 'pro​d'] } }, /tier: an allowed value cannot be shown or sent as declared/],
    ['name', { 'a=b': { type: 'string' } }, /a=b: its name is not a plain identifier/],
    ['name with a flag', { '--x': { type: 'string' } }, /--x: its name is not a plain identifier/],
  ] as const) {
  test(`a ${label} the form would have to alter is refused, not stripped: Run is blocked`, async ($, on) => {
    const { ui, seen } = await open($, on, { compile: schemaOf(props) })
    expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
    expect((await texts(ui)).match(/Run locally is unavailable: .*/)?.[0]).toMatch(reason)
    expect(runs(seen)).toEqual([])
    await ui.unmount()
  })
}

test('file names that are flags, have spaces or shell syntax are not offered', async ($, on) => {
  const names = ['-x.flow.yaml', 'a b.flow.yaml', 'a;id.flow.yaml', '$(id).flow.yaml', 'é.flow.yaml', '.hidden.flow.yaml', `${'x'.repeat(130)}.flow.yaml`, 'ok.flow.yaml']
  const { ui, seen } = await open($, on, { dirs: { '': names.map(n => entry(n)) } }, '')
  const select = await control(ui, 'Select', 'run-file')
  expect(select.options.map((o: { value: string }) => o.value)).toEqual(['', 'ok.flow.yaml'])
  expect(await ui.find({ type: 'Text', text: /and 7 more Flowfiles not offered/ })).toBeDefined()
  expect(formCalls(seen)).toEqual([])
  await ui.unmount()
})

test('a file outside the listing cannot be selected', async ($, on) => {
  const { ui, seen } = await open($, on, {}, '')
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: '../../etc/passwd' }).catch(() => undefined)
  expect(compiles(seen)).toEqual([])
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  await ui.unmount()
})

test('if the file leaves the listing before Confirm, nothing is run and the card says so', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  world.dirs = { '': [] }
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([])
  expect(await ui.find({ type: 'Text', text: /– not run: the file is no longer listed/ })).toBeDefined()
  await ui.unmount()
})

test('if the declaration no longer accepts the values at Confirm, nothing is run', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  // The file was edited after the question: `service` is now an int and `region` is gone.
  world.compile = schemaOf({ service: { type: 'integer' } }, ['service'])
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([])
  expect(await ui.find({ type: 'Text', text: /– not run: service: must be a whole number/ })).toBeDefined()
  await ui.unmount()
})

test('if the inputs cannot be read again at Confirm, nothing is run', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  world.compile = fail('boom')
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([])
  expect(await ui.find({ type: 'Text', text: /– not run: its inputs could not be read \(boom\)/ })).toBeDefined()
  await ui.unmount()
})

test('a required sensitive input is never collected, so it blocks Run', async ($, on) => {
  const blocked = await open($, on, { compile: schemaOf({ token: { type: 'string', 'x-flowstate-sensitive': true } }, ['token']) })
  expect(await blocked.ui.find({ type: 'Input', key: 'in:token' })).toBeUndefined()
  expect(await blocked.ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  expect(await texts(blocked.ui)).toMatch(/token: sensitive and required/)
  expect(runs(blocked.seen)).toEqual([])
  await blocked.ui.unmount()
})

test('an optional sensitive input is left out of the form and the run', async ($, on) => {
  const optional = await open($, on, { compile: schemaOf({ token: { type: 'string', 'x-flowstate-sensitive': true }, name: { type: 'string' } }) })
  expect(await optional.ui.find({ type: 'Input', key: 'in:token' })).toBeUndefined()
  await type(optional.ui, 'name', 'n')
  await optional.ui.press({ key: 'run-local' })
  await optional.ui.press({ key: 'confirm-run' })
  expect(runs(optional.seen)).toEqual([['flow', 'run', 'local', '--no-color', '--input=name=n', '--', 'deploy.flow.yaml']])
  await optional.ui.unmount()
})

test('a required enum with no default must be chosen; choosing it unblocks Run', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: schemaOf({ tier: { type: 'string', enum: ['canary', 'staging'] } }, ['tier']) })
  expect(await control(ui, 'Select', 'in:tier')).toMatchObject({ value: '', options: [{ value: '', label: '(choose)' }, { value: 'canary' }, { value: 'staging' }] })
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  await pick(ui, 'tier', 'canary')
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([['flow', 'run', 'local', '--no-color', '--input=tier=canary', '--', 'deploy.flow.yaml']])
  await ui.unmount()
})

test('clearing an input that has a default sends nothing for it, so the engine applies its default', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await type(ui, 'region', '')
  await ui.press({ key: 'run-local' })
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)[0].some(a => a.startsWith('--input=region'))).toBe(false)
  await ui.unmount()
})

// The pure model, directly.

const fieldsOf = (reply: Reply): Field[] => {
  const parsed = parseInputs(reply.stdout)
  if (!('fields' in parsed)) throw new Error(parsed.error)
  return parsed.fields
}

test('runArgv builds nothing outside the allowlist', () => {
  const fields = fieldsOf(DEPLOY)
  const good = [{ name: 'service', value: 'api' }]
  expect(runArgv('flow', 'a.flow.yaml', good, fields)).toEqual(['flow', 'run', 'local', '--no-color', '--input=service=api', '--', 'a.flow.yaml'])
  expect(runArgv('/opt/flow', 'workflows/a.yaml', [], fields)).toEqual(['/opt/flow', 'run', 'local', '--no-color', '--', 'workflows/a.yaml'])

  for (const file of ['', '-x.flow.yaml', '--help', 'a b.flow.yaml', '../a.flow.yaml', '/etc/a.flow.yaml', 'a.flow.yaml\n', 'sub/a.flow.yaml', 'notes.md', 'a.test.yaml', 'x'.repeat(200) + '.flow.yaml']) {
    expect(runArgv('flow', file, good, fields), JSON.stringify(file)).toBeUndefined()
  }
  for (const bad of [
    [{ name: 'undeclared', value: 'x' }],
    [{ name: 'service', value: 'a' }, { name: 'service', value: 'b' }],
    [{ name: 'service', value: '' }],
    [{ name: 'ser vice', value: 'x' }],
    [{ name: 'service=x', value: 'y' }],
    [{ name: '--service', value: 'y' }],
    [{ name: 'service', value: 'a\nb' }],
    [{ name: 'service', value: 'a‮b' }],
    [{ name: 'service', value: 'x'.repeat(1001) }],
    Array.from({ length: 9 }, () => ({ name: 'service', value: 'x'.repeat(1000) })),
  ]) {
    expect(runArgv('flow', 'a.flow.yaml', bad, fields), JSON.stringify(bad).slice(0, 60)).toBeUndefined()
  }
  // A sensitive or refused input is never sendable, even by name.
  const odd = fieldsOf(schemaOf({ token: { type: 'string', 'x-flowstate-sensitive': true }, bad: { type: 'string', default: 'a\u0007' } }))
  expect(runArgv('flow', 'a.flow.yaml', [{ name: 'token', value: 's' }], odd)).toBeUndefined()
  expect(runArgv('flow', 'a.flow.yaml', [{ name: 'bad', value: 's' }], odd)).toBeUndefined()
})

test('parseInputs reads the shapes flow compile writes and bounds its work', () => {
  expect(parseInputs('{"type":"object"}')).toEqual({ fields: [] })
  const f = fieldsOf(schemaOf({ when: { type: 'string', format: 'date-time' }, any: {}, nil: { type: 'null' }, big: { type: 'string', enum: Array.from({ length: 60 }, (_, i) => `v${i}`) } }))
  expect(f.map(x => [x.name, x.kind, x.shape])).toEqual([['when', 'string', 'any'], ['any', 'json', 'any'], ['nil', 'json', 'any'], ['big', 'enum', 'any']])
  expect(f[0].example).toBe('RFC 3339, e.g. 2026-10-09T10:00:00Z')
  expect(f[3].refused).toMatch(/more than 50 values/)
  // A sensitive declaration's default and example are never drawn, even if a schema carried them.
  const s = fieldsOf(schemaOf({ k: { type: 'string', default: 'hunter2', examples: ['hunter2'], 'x-flowstate-sensitive': true } }))[0]
  expect([s.initial, s.example]).toEqual(['', ''])
})

test('checkForm and submission leave the rest to the engine: must: rules are not evaluated', () => {
  const fields = fieldsOf(DEPLOY)
  expect(checkForm(fields, { service: 'api', replicas: '999' }).blocked).toBe('') // 999 breaks `this <= 50`; the engine says so
  expect(submission(fields, { service: 'api', region: '' }).map(p => p.name)).toEqual(['service', 'replicas', 'ratio', 'dry_run', 'tags'])
})

test('candidates bounds its scan and lists in name order', () => {
  const many = Array.from({ length: 600 }, (_, i) => entry(`f${String(i).padStart(3, '0')}.flow.yaml`))
  const c = candidates(many)
  expect(c.files).toHaveLength(12)
  expect(c.files[0]).toBe('f000.flow.yaml')
  expect(c.more).toBe(500 - 12)
})

test('cleanLines drops escapes and controls, keeps lines apart and bounds both counts and widths', () => {
  expect(cleanLines('a\u001b[31mred\u001b[0m\tb\r\n\r\nc\u0007').lines).toEqual(['ared b', 'c'])
  const long = cleanLines(`${'y'.repeat(500)}\n${Array.from({ length: 30 }, () => 'z').join('\n')}`)
  expect(long.lines).toHaveLength(12)
  expect(long.lines[0]).toHaveLength(200)
  expect(long.cut).toBe(true)
  expect(resultOf({ exitCode: 0, stdout: 'fine', stderr: '', isStdoutTruncated: true }, 'a.flow.yaml').text).toMatch(/\(output cut\)/)
})

// Review follow-ups.

test('an input named __proto__ is refused by name, and its refusal blocks Run', async ($, on) => {
  const compile = schemaOf(JSON.parse('{"__proto__":{"type":"integer"}}'), ['__proto__'])
  const f = fieldsOf(compile)
  expect(f.map(x => [x.name, x.refused])).toEqual([['__proto__', 'its name is not a plain identifier']])
  expect(checkForm(f, {}).blocked).toBe('__proto__: its name is not a plain identifier')
  expect(runArgv('flow', 'a.flow.yaml', [{ name: '__proto__', value: '1' }], f)).toBeUndefined()
  const { ui, seen } = await open($, on, { compile })
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /Run locally is unavailable: __proto__: its name is not a plain identifier/ })).toBeDefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('errors are keyed without a prototype, so no declared name can lose its error', () => {
  // `constructor` and `toString` are inherited names on a plain object; their errors must still be found.
  const f = fieldsOf(schemaOf({ constructor: { type: 'integer' }, toString: { type: 'integer' } }, ['constructor', 'toString']))
  const c = checkForm(f, { constructor: 'abc', toString: 'x' })
  expect(Object.keys(c.errors)).toEqual(['constructor', 'toString'])
  expect(c.blocked).toMatch(/^constructor: must be a whole number/)
})

test('a broken file is compiled once, not on every redraw', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: fail('deploy.flow.yaml:3:1: bad') })
  for (let i = 0; i < 10; i++) {
    await ui.input({ plugin: 'flowstate', key: 'filter', text: `status == "x${i}"` })
    expect(await ui.find({ type: 'Text', text: /Inputs unavailable/ })).toBeDefined()
  }
  // Each submit redraws the pane (and lists runs again), so the count proves the redraws happened.
  expect(seen.filter(a => a[1] === 'list').length).toBeGreaterThan(10)
  expect(compiles(seen)).toHaveLength(1)
  await ui.unmount()
})

test('an edit and a revert does not bring the old question back', async ($, on) => {
  const { ui, seen } = await filled($, on)
  await ui.press({ key: 'run-local' })
  await type(ui, 'service', 'api2')
  await type(ui, 'service', 'api')
  expect(await ui.find({ type: 'Button', text: /Confirm/ })).toBeUndefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('a value typed for an input that the schema now marks sensitive is neither echoed nor sent', async ($, on) => {
  const { ui, seen } = await open($, on, { compile: schemaOf({ token: { type: 'string' }, name: { type: 'string' } }) })
  await type(ui, 'token', 'hunter2-value')
  world.compile = schemaOf({ token: { type: 'string', 'x-flowstate-sensitive': true }, name: { type: 'string' } })
  world.dirs = { '': [entry('deploy.flow.yaml', 'file', 77)] }
  await type(ui, 'name', 'n')
  await ui.press({ key: 'run-local' })
  expect(await texts(ui)).not.toMatch(/hunter2/)
  await ui.press({ key: 'confirm-run' })
  expect(runs(seen)).toEqual([['flow', 'run', 'local', '--no-color', '--input=name=n', '--', 'deploy.flow.yaml']])
  await ui.unmount()
})

test('submission never includes a sensitive input, whatever was typed', () => {
  const f = fieldsOf(schemaOf({ token: { type: 'string', 'x-flowstate-sensitive': true } }))
  expect(submission(f, { token: 'x' })).toEqual([])
})

test('invisible and format characters are refused: word joiner, soft hyphen, separators, tags, lone surrogates', () => {
  const f = fieldsOf(schemaOf({ s: { type: 'string' } }))[0]
  for (const bad of ['a⁠b', 'a⁤b', 'a­b', 'a؜b', 'a b', 'a b', 'a᠎b', 'a͏b', 'a\u{e0041}b', 'a\ud800b', 'a\udc00b']) {
    expect(checkForm([f], { s: bad }).blocked, JSON.stringify(bad)).toMatch(/contains a control or invisible character/)
  }
  expect(checkForm([f], { s: 'ünï 😀 — ok' }).blocked).toBe('')
  expect(clean('a⁠­ \u{e0041}\ud800b')).toBe('ab')
  expect(clean('😀')).toBe('😀')
})

test('a number a double cannot hold exactly is refused as a default, scalar or nested, and never sent', async ($, on) => {
  const text = (dflt: string) => `{"type":"object","properties":{"n":{"type":"integer","default":${dflt}}}}`
  for (const dflt of ['9007199254740993', '-9007199254740993', '1.2345678901234567']) {
    const f = fieldsOf(ok(text(dflt)))[0]
    expect([f.initial, f.refused], dflt).toEqual(['', 'its declared default is a number the form cannot show or send exactly'])
  }
  const nested = fieldsOf(ok('{"type":"object","properties":{"l":{"type":"array","default":[1,{"a":[9007199254740993]}]},"safe":{"type":"integer","default":9007199254740991},"x":{"type":"number","default":0.1}}}'))
  expect(nested.map(f => [f.name, f.refused !== '', f.initial])).toEqual([['l', true, ''], ['safe', false, '9007199254740991'], ['x', false, '0.1']])
  // An example is just not shown.
  expect(fieldsOf(ok('{"type":"object","properties":{"n":{"type":"integer","examples":[9007199254740993]}}}'))[0].example).toBe('')
  const { ui, seen } = await open($, on, { compile: ok(text('9007199254740993')) })
  expect(await ui.find({ type: 'Button', text: /Run locally/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /Run locally is unavailable: n: its declared default is a number/ })).toBeDefined()
  expect(runs(seen)).toEqual([])
  await ui.unmount()
})

test('a 100k-entry listing is cut before any search: a late workflows/ directory and late Flowfiles are not seen', async ($, on) => {
  const junk = Array.from({ length: 100000 }, (_, i) => entry(`n${i}.md`))
  junk[600] = entry('workflows', 'dir')
  junk[700] = entry('late.flow.yaml')
  junk[10] = entry('early.flow.yaml')
  const { ui } = await open($, on, { dirs: { '': junk, workflows: [entry('deep.yaml')] } }, '')
  const select = await control(ui, 'Select', 'run-file')
  expect(select.options.map((o: { value: string }) => o.value)).toEqual(['', 'early.flow.yaml'])
  await ui.unmount()
})
