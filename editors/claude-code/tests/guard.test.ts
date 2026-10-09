import { DEFAULT_ADDRESS, analyzeCommand, askReason, findSecrets, secretsIn, tokenize } from '../hooks/guard'
import { expect, mock, test } from 'claude-code/testing'

const verbs = (command: string, binary?: string) => analyzeCommand(command, binary).actions.map(a => a.verb)

test('server verbs are found, local verbs and reads never are', () => {
  expect(verbs('flow run x.flow.yaml')).toEqual(['flow run'])
  expect(verbs('flow signal wf-1 approve')).toEqual(['flow signal'])
  expect(verbs('flow cancel wf-1')).toEqual(['flow cancel'])
  expect(verbs('flow terminate wf-1')).toEqual(['flow terminate'])
  expect(verbs('flow schedule delete nightly')).toEqual(['flow schedule delete'])
  expect(verbs('flow -v run --detach x.flow.yaml')).toEqual(['flow run'])

  for (const local of [
    'flow run local x.flow.yaml',
    'flow validate x.flow.yaml',
    'flow fmt .',
    'flow lint .',
    'flow test .',
    'flow tasks -o json',
    'flow compile x.flow.yaml',
    'flow timeline wf-1',
    'flow list',
    'flow graph .',
    'flow get wf-1',
    'flow schedule list',
    'flow schedule describe nightly',
    'flow run --help',
    'echo flow run is how you start one',
  ]) {
    expect(analyzeCommand(local)).toEqual({ actions: [], uncertain: false })
  }
})

test('separators, env prefixes, wrappers, and quoted arguments are followed', () => {
  expect(verbs('flow validate x.yaml && flow run x.yaml; flow list')).toEqual(['flow run'])
  expect(verbs('cd d\nflow cancel wf-1 | tee out')).toEqual(['flow cancel'])
  expect(verbs('env FOO=1 flow run x.yaml')).toEqual(['flow run'])
  expect(verbs('FOO=1 BAR=2 flow signal wf a')).toEqual(['flow signal'])
  expect(verbs('sudo -u ci env -u X flow terminate wf')).toEqual(['flow terminate'])
  expect(verbs('timeout 30 flow run x.yaml')).toEqual(['flow run'])
  expect(verbs('/usr/local/bin/flow run x.yaml')).toEqual(['flow run'])
  expect(verbs('bash -c "flow run x.yaml"')).toEqual(['flow run'])
  expect(verbs("sh -lc 'cd d && flow cancel wf'")).toEqual(['flow cancel'])
  expect(verbs('eval "flow run x.yaml"')).toEqual(['flow run'])
  expect(verbs('"flow" signal "wf 1" go')).toEqual(['flow signal'])
  // A quoted `;` is an argument, not a separator.
  expect(verbs('echo "a; flow run x"')).toEqual([])
  expect(verbs('echo ok # flow run x')).toEqual([])
})

test('a configured flowBinary is recognized, and so is the bare name', () => {
  expect(verbs('/opt/tools/flowstate-cli run x.yaml', '/opt/tools/flowstate-cli')).toEqual(['flow run'])
  expect(verbs('flowstate-cli run x.yaml', '/opt/tools/flowstate-cli')).toEqual(['flow run'])
  expect(verbs('flow run x.yaml', '/opt/tools/flowstate-cli')).toEqual(['flow run'])
  expect(verbs('workflow run x.yaml')).toEqual([])
  expect(verbs('flowstate-cli run x.yaml')).toEqual([])
})

test('the address comes from --address, then the command environment, else the caller decides', () => {
  const one = (c: string) => analyzeCommand(c).actions[0]
  expect(one('flow run --address prod.example:9233 x.yaml').address).toBe('prod.example:9233')
  expect(one('flow run --address=prod.example:9233 x.yaml').address).toBe('prod.example:9233')
  expect(one('FLOWSTATE_ADDRESS=stage:1 flow run x.yaml').address).toBe('stage:1')
  expect(one('export FLOWSTATE_ADDRESS=stage:2; flow run x.yaml').address).toBe('stage:2')
  expect(one('flow run x.yaml').address).toBeUndefined()
  expect(one('flow cancel wf-9').subject).toBe('wf-9')
})

test('what cannot be parsed asks when it names a server verb, and stays quiet otherwise', () => {
  for (const hidden of [
    'flow run "x.yaml',
    'flow run $(ls *.yaml)',
    'flow signal `cat id` go',
    'echo x | xargs flow cancel',
    'FLOW=flow; $FLOW run x.yaml',
    'bash -c "flow run $(cat f)"',
  ]) {
    expect(analyzeCommand(hidden).uncertain).toBe(true)
  }
  for (const quiet of ['echo $(date)', 'ls "unterminated', 'git commit -m "$(cat msg)"', 'flow validate $(ls *.yaml)']) {
    expect(analyzeCommand(quiet).uncertain).toBe(false)
  }
})

test('the question names the verb and the address, and cleans text from the command', () => {
  const text = (c: string, session?: string) => askReason(analyzeCommand(c), session)
  expect(text('flow run --address prod:9233 x.yaml', 'ignored:1')).toContain('flow run acts on server prod:9233')
  expect(text('flow cancel wf-1', 'env:9')).toContain('flow cancel wf-1 acts on server env:9')
  expect(text('flow run x.yaml')).toContain(`the default server ${DEFAULT_ADDRESS}`)
  expect(text('flow run --address "a\u001b[2Jb" x.yaml')).not.toContain('\u001b')
  expect(text('flow run $(x)')).toContain('could not be fully read')
  expect(text('flow run a; flow run b; flow run c; flow run d; flow run e; flow run f; flow run g')).toContain('and 2 more')
})

test('tokenize reports what it did not follow', () => {
  expect(tokenize("a 'b c' d\\ e").segments).toEqual([['a', 'b c', 'd e']])
  expect(tokenize('a && b || c').segments).toEqual([['a'], ['b'], ['c']])
  expect(tokenize('a $(b)').exact).toBe(false)
  expect(tokenize("a 'b").exact).toBe(false)
  expect(tokenize('a "b $(c)"').exact).toBe(false)
})

const flagged = (text: string) => findSecrets(text).map(f => f.what)

test('token shapes are refused', () => {
  const body = 'Abcdefghijklmnopqrstuvwxyz0123456789'
  expect(flagged(`run: curl -H "Authorization: ghp_${body}"`)).toEqual(['a GitHub token'])
  expect(flagged(`x: github_pat_${body}_ab`)).toEqual(['a GitHub fine-grained token'])
  expect(flagged('x: xoxb-123456789012-abcdefghij')).toEqual(['a Slack token'])
  expect(flagged('x: AKIAIOSFODNN7EXAMPLE')).toEqual(['an AWS access key id'])
  expect(flagged(`x: sk-${body}`)).toEqual(['an API key (sk-)'])
  expect(flagged(`x: sk-ant-${body}`)).toEqual(['an API key (sk-)'])
  expect(flagged('x: |\n  -----BEGIN RSA PRIVATE KEY-----\n  abc')).toEqual(['a private key'])
  expect(flagged('x: -----BEGIN PRIVATE KEY-----')).toEqual(['a private key'])
  expect(flagged(`token: ghp_${body}`)).toContain('a GitHub token')
})

test('look-alikes and placeholders are allowed', () => {
  expect(flagged('x: ghp_short')).toEqual([])
  expect(flagged('x: ghp_' + 'x'.repeat(40))).toEqual([])
  expect(flagged('x: sk-learn')).toEqual([])
  expect(flagged('x: disk-' + 'a'.repeat(30))).toEqual([])
  expect(flagged('x: AKIA_NOT_A_KEY')).toEqual([])
  expect(flagged('x: xoxo-hugs')).toEqual([])
  expect(flagged('x: -----BEGIN CERTIFICATE-----')).toEqual([])
  expect(flagged('x: task-skeleton-for-the-build-step')).toEqual([])
})

test('a credential-named key holding a plain string is refused; references and non-values are not', () => {
  expect(flagged('password: hunter2hunter2')).toEqual(['the key password holding a literal value'])
  expect(flagged('    db_password: "s3cr3t-value"')).toEqual(['the key db_password holding a literal value'])
  expect(flagged('  - api_key: abcdef123456')).toEqual(['the key api_key holding a literal value'])
  expect(flagged('apiKey: abcdef123456')).toEqual(['the key apiKey holding a literal value'])
  expect(flagged('client-secret: abcdef123456  # prod')).toEqual(['the key client-secret holding a literal value'])

  for (const ok of [
    "password: ${secret('env:DB_PASSWORD')}",
    'token: \'${secret("keychain:ci")}\'',
    'token: prefix-${secret("env:T")}',
    'password: ${inputs.password}',
    'secret: |',
    'token: ',
    'token: ""',
    'password: short',
    'token: 12345678',
    'token: GITHUB_TOKEN_NAME',
    'token: steps.login.outputs.token',
    'token: <your-token-here>',
    'password: changeme',
    'token: null',
    'max_tokens: 100000000',
    'token_url: https://idp.example.com/oauth2/token',
    'secrets: abcdefghijkl',
    'client_secret_file: /etc/flowstate/secrets/billing-client',
    'name: password reset flow',
    'description: the password is read from the vault',
  ]) {
    expect(flagged(ok)).toEqual([])
  }
})

test('findings carry the line and never the value', () => {
  const found = findSecrets('a: 1\nb: 2\npassword: hunter2hunter2')
  expect(found).toEqual([{ line: 3, what: 'the key password holding a literal value' }])
  expect(JSON.stringify(found)).not.toContain('hunter2')
})

test('every piece of an Edit, Write, or MultiEdit is read', () => {
  expect(secretsIn({ content: 'password: hunter2hunter2' })).toHaveLength(1)
  expect(secretsIn({ new_string: 'password: hunter2hunter2' })).toHaveLength(1)
  expect(secretsIn({ edits: [{ new_string: 'a: 1' }, { new_string: 'token: abcdefgh1234' }] })).toHaveLength(1)
  expect(secretsIn({ old_string: 'password: hunter2hunter2' })).toEqual([])
  expect(secretsIn(null)).toEqual([])
  expect(secretsIn({ edits: [null, 3] })).toEqual([])
})

// The hook: `tool.check` answers with what the rules decided, so each test
// stubs that decision and fires the check the way Claude Code would.
const decide = (on: any, decision: 'allow' | 'ask' | 'deny' = 'allow') => on('tool.check', () => ({ decision }))
const bash = (command: string) => ({ tool: 'Bash', input: { command } })

test('a server verb turns an allow into a question that names the address', async ($, on) => {
  decide(on)
  mock.env(on, { FLOWSTATE_ADDRESS: 'prod.example:9233' })

  const out = await $.tool.check(bash('flow signal wf-1 approve'))

  expect(out.decision).toBe('ask')
  expect(out.reason).toContain('flow signal wf-1 acts on server prod.example:9233')
})

test('an explicit --address wins over the environment; an absent one reads as the default', async ($, on) => {
  decide(on)
  mock.env(on, {})

  const flagged = await $.tool.check(bash('flow run --address stage:1 x.yaml'))
  const bare = await $.tool.check(bash('flow run x.yaml'))

  expect(flagged.reason).toContain('server stage:1')
  expect(bare.reason).toContain(`default server ${DEFAULT_ADDRESS}`)
})

test('local verbs and other commands pass with the decision they had', async ($, on) => {
  decide(on)
  mock.env(on, {})

  for (const command of ['flow run local x.yaml', 'flow validate x.yaml', 'ls -la', 'flow list']) {
    expect((await $.tool.check(bash(command))).decision).toBe('allow')
  }
})

test('an earlier deny stays a deny', async ($, on) => {
  decide(on, 'deny')
  mock.env(on, {})

  expect((await $.tool.check(bash('flow run x.yaml'))).decision).toBe('deny')
})

test('an unparseable command that names a server verb asks', async ($, on) => {
  decide(on)
  mock.env(on, {})

  expect((await $.tool.check(bash('flow run $(ls *.yaml)'))).decision).toBe('ask')
  expect((await $.tool.check(bash('echo $(date)'))).decision).toBe('allow')
})

test('a configured flowBinary is guarded', { options: { flowBinary: '/opt/flow' } }, async ($, on) => {
  decide(on)
  mock.env(on, {})

  expect((await $.tool.check(bash('/opt/flow cancel wf-1'))).decision).toBe('ask')
})

test('the question can be turned off', { options: { guardServerActions: false } }, async ($, on) => {
  decide(on)
  mock.env(on, {})

  expect((await $.tool.check(bash('flow terminate wf-1'))).decision).toBe('allow')
})

test('an environment that cannot be read still asks', async ($, on) => {
  decide(on)
  on('env.get', () => ({ deny: 'unavailable' }))

  const out = await $.tool.check(bash('flow run x.yaml'))

  expect(out.decision).toBe('ask')
})

const write = (file_path: string, content: string) => ({ tool: 'Write', input: { file_path, content } })

test('a secret written to a Flowfile is denied without echoing it', async ($, on) => {
  decide(on)

  const out = await $.tool.check(write('deploy/a.flow.yaml', 'steps:\n  - password: hunter2hunter2\n'))

  expect(out.decision).toBe('deny')
  expect(out.reason).toContain("${secret('scheme:name')}")
  expect(out.reason).toContain('line 2')
  expect(out.reason).not.toContain('hunter2')
})

test('Edit and MultiEdit are read too', async ($, on) => {
  decide(on)
  const token = `ghp_${'Abcdefghijklmnopqrstuvwxyz0123456789'}`

  const edit = await $.tool.check({ tool: 'Edit', input: { file_path: 'Flowfile', old_string: 'x', new_string: `t: ${token}` } })
  const multi = await $.tool.check({
    tool: 'MultiEdit',
    input: { file_path: 'a.flow.yaml', edits: [{ old_string: 'x', new_string: 'ok' }, { old_string: 'y', new_string: `t: ${token}` }] },
  })

  expect(edit.decision).toBe('deny')
  expect(multi.decision).toBe('deny')
  expect(edit.reason).not.toContain(token)
})

test('references, other files, and test fixtures are not refused', async ($, on) => {
  decide(on)

  const ref = write('a.flow.yaml', "token: ${secret('env:GITHUB_TOKEN')}\n")
  const notes = write('notes.md', 'password: hunter2hunter2\n')
  const fixture = write('a.test.yaml', 'password: hunter2hunter2\n')

  for (const call of [ref, notes, fixture]) expect((await $.tool.check(call)).decision).toBe('allow')
})

test('an earlier deny on a Flowfile keeps its own reason', async ($, on) => {
  on('tool.check', () => ({ decision: 'deny', reason: 'rule says no' }))

  const out = await $.tool.check(write('a.flow.yaml', 'password: hunter2hunter2'))

  expect(out.reason).toBe('rule says no')
})
