import {
  DEFAULT_ADDRESS,
  EDIT_TOO_LARGE,
  LONG_LINE,
  MAX_COMMAND,
  MAX_SCAN,
  UNCHECKED_BASH,
  UNCHECKED_EDIT,
  afterEdits,
  alreadyPresent,
  analyzeCommand,
  askReason,
  denyReason,
  findSecrets,
  namesFlow,
  secretsIn,
  tokenize,
} from '../hooks/guard'
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

// Bounds: input another party controls must cost time in proportion to its size, and a cap.
const took = (run: () => unknown): number => {
  const start = performance.now()
  run()
  return performance.now() - start
}

test('pathological secret text is bounded', () => {
  const spaces = 'password: a' + ' '.repeat(40_000) + 'b'
  expect(took(() => findSecrets(spaces))).toBeLessThan(500)
  expect(findSecrets(spaces).map(f => f.what)).toEqual([LONG_LINE])

  const manyLines = ('password: a' + ' '.repeat(4000) + 'b\n').repeat(60)
  expect(took(() => findSecrets(manyLines))).toBeLessThan(500)

  const hashes = 'password: a ' + '# '.repeat(2000) + '\n'
  expect(took(() => findSecrets(hashes.repeat(60)))).toBeLessThan(500)

  const huge = 'a: ' + ' '.repeat(1_000_000)
  expect(took(() => findSecrets(huge))).toBeLessThan(500)
  expect(findSecrets(huge)).toHaveLength(1)
  expect(took(() => secretsIn({ content: huge }))).toBeLessThan(500)
})

test('over the cap a Flowfile edit is denied as too large to scan; a long line is read whole', async ($, on) => {
  decide(on)
  const big = 'x: 1\n'.repeat(MAX_SCAN / 5 + 10)
  const out = await $.tool.check(write('a.flow.yaml', big))
  expect(out.decision).toBe('deny')
  expect(out.reason).toContain('too large to scan')

  const token = `ghp_${'Abcdefghijklmnopqrstuvwxyz0123456789'}`
  expect(findSecrets(`x: ${token}${' '.repeat(5000)}`)).toHaveLength(1)
  // A long line is read whole: a token after padding, or a credential key word anywhere, is not missed.
  expect(flagged(`${' '.repeat(5000)}x: ${token}`)).toEqual(['a GitHub token'])
  expect(findSecrets(`a: 1\npassword: hunter2hunter2${' '.repeat(5000)}`)).toEqual([{ line: 2, what: LONG_LINE }])
  expect(findSecrets(`${' '.repeat(5000)}password: hunter2hunter2`)).toEqual([{ line: 1, what: LONG_LINE }])
  expect(flagged(`${' '.repeat(5000)}x: -----BEGIN RSA PRIVATE KEY-----`)).toEqual(['a private key'])
  // Without a credential word or a token shape, a long line is ignored.
  expect(findSecrets(`note: ${'lorem ipsum '.repeat(500)}`)).toEqual([])
  expect(findSecrets(`${'sk-'.repeat(80_000)}`)).toEqual([])
  expect(took(() => findSecrets(`${'sk-ghp_'.repeat(35_000)}`))).toBeLessThan(500)
  expect(findSecrets('x: 1\n'.repeat(1000))).toEqual([])
})

test('pathological commands are bounded', () => {
  const rescan = 'echo $(x) ' + 'flow '.repeat(20_000)
  expect(took(() => analyzeCommand(rescan))).toBeLessThan(500)
  const long = 'echo $(x) ' + 'flow '.repeat(100_000)
  expect(long.length).toBeGreaterThan(MAX_COMMAND)
  expect(took(() => analyzeCommand(long))).toBeLessThan(500)
  expect(analyzeCommand(long)).toEqual({ actions: [], uncertain: true })
  expect(analyzeCommand('x'.repeat(MAX_COMMAND + 1))).toEqual({ actions: [], uncertain: false })
  expect(took(() => analyzeCommand('env '.repeat(15_000) + 'flow run x'))).toBeLessThan(500)
  expect(took(() => analyzeCommand('a=1 '.repeat(15_000) + 'flow run x'))).toBeLessThan(500)
  expect(took(() => analyzeCommand('$a' + ' '.repeat(60_000) + 'b $(x) flow'))).toBeLessThan(500)
  // Within the cap the real thing is still seen.
  expect(verbs('echo $(x) ' + 'flow validate '.repeat(1000) + '\nflow run x')).toEqual(['flow run'])
})

test('a long command that names flow is asked about, unread', async ($, on) => {
  decide(on)
  mock.env(on, {})
  const out = await $.tool.check(bash('echo ' + 'flow '.repeat(20_000)))
  expect(out.decision).toBe('ask')
  expect((await $.tool.check(bash('echo ' + 'y'.repeat(70_000)))).decision).toBe('allow')
})

test('the binary behind a wrapper is found', () => {
  for (const [command, verb] of [
    ['go run ./cmd/flow run x', 'flow run'],
    ['nice -n 5 flow run x', 'flow run'],
    ['sudo --user ci flow terminate wf', 'flow terminate'],
    ['timeout -s KILL 30 flow run x', 'flow run'],
    ['watch flow run x', 'flow run'],
    ['find . -exec flow cancel {} \;', 'flow cancel'],
    ['ssh h flow run x', 'flow run'],
    ['docker exec c flow run x', 'flow run'],
    ['npx flow run x', 'flow run'],
    ['command -p flow run x', 'flow run'],
    ['ssh h flow schedule delete nightly', 'flow schedule delete'],
    ['ssh h bash -c "flow signal wf go"', 'flow signal'],
    ['git flow run x', 'flow run'],
  ]) {
    expect(verbs(command)).toEqual([verb])
  }
  for (const quiet of [
    'cp flow run',
    'mv flow signal',
    'bun test flow run',
    'npm test flow run',
    'rm -f flow cancel',
    'git commit -m "flow run local"',
    'git commit -m "flow run x"',
    'grep "flow run" .',
    'go run ./cmd/flow validate x',
    'ssh h flow list',
    'docker exec c flow run local x',
    'git flow feature start',
    'echo flow run x',
    'grep -r "flow run" .',
    'cat flow run',
    'mkdir -p workflow run',
  ]) {
    expect(analyzeCommand(quiet).actions).toEqual([])
  }
  // The wrapper's environment reaches the verb.
  expect(analyzeCommand('FLOWSTATE_ADDRESS=p:1 nice flow run x').actions[0].address).toBe('p:1')
})

test('a shell without -c, and interpreter code, ask only when the text names flow and a verb', () => {
  for (const hidden of ['echo "flow run x" | sh', 'bash <<< "flow run x"', "python3 -c 'import os; os.system(\"flow run x\")'", `node -e "require('child_process').execSync('flow cancel wf')"`, `perl -e 'system("flow terminate wf")'`, `ruby -e 'system "flow run x"'`]) {
    expect(analyzeCommand(hidden).uncertain).toBe(true)
  }
  for (const quiet of ['echo hi | sh', 'bash script.sh', 'python3 -c "print(1)"', 'node -e "1"', 'echo "flow list" | sh', 'python3 build.py']) {
    expect(analyzeCommand(quiet).uncertain).toBe(false)
  }
})

test('redirections do not hide the verb or split the command', () => {
  expect(verbs('flow run x 2>&1 | tee out')).toEqual(['flow run'])
  expect(verbs('flow 2>/dev/null run x')).toEqual(['flow run'])
  expect(verbs('flow >out.txt run x')).toEqual(['flow run'])
  expect(verbs('flow > out.txt run x')).toEqual(['flow run'])
  expect(verbs('>out flow cancel wf')).toEqual(['flow cancel'])
  expect(verbs('flow run x &> all.log')).toEqual(['flow run'])
  expect(verbs('flow 2> err run x')).toEqual(['flow run'])
  expect(tokenize('a 2>&1 | b').segments).toEqual([['a', '2>&1'], ['b']])
  expect(tokenize('a & b').segments).toEqual([['a'], ['b']])
  expect(verbs('flow validate x 2>&1')).toEqual([])
})

test('the address is the one the command gives flow, no more', () => {
  const addr = (c: string) => analyzeCommand(c).actions.map(a => a.address)
  // A prefix applies to its own command only.
  expect(addr('FLOWSTATE_ADDRESS=x:1 flow run a; flow run b')).toEqual(['x:1', undefined])
  expect(addr('FLOWSTATE_ADDRESS=x:1 true && flow run b')).toEqual([undefined])
  expect(addr('env FLOWSTATE_ADDRESS=x:1 flow run a; flow run b')).toEqual(['x:1', undefined])
  // `export` persists; a bare assignment is a shell variable flow never sees.
  expect(addr('export FLOWSTATE_ADDRESS=x:2; flow run a; flow run b')).toEqual(['x:2', 'x:2'])
  expect(addr('FLOWSTATE_ADDRESS=prod:1; flow run x')).toEqual([undefined])
  expect(addr('FLOWSTATE_ADDRESS=prod:1\nflow run x')).toEqual([undefined])
  // The last --address wins and `--` ends the options.
  expect(addr('flow run --address a:1 --address b:2 x')).toEqual(['b:2'])
  expect(addr('flow run --address=a:1 --address b:2 x')).toEqual(['b:2'])
  expect(addr('flow run x -- --address evil:1')).toEqual([undefined])
  expect(addr('flow --address a:1 run x')).toEqual(['a:1'])
  expect(addr('bash -c "flow run x"')).toEqual([undefined])
  expect(addr('FLOWSTATE_ADDRESS=x:3 bash -c "flow run x"')).toEqual(['x:3'])
})

test('--help exempts only a help request right after the verb', () => {
  expect(verbs('flow run --help')).toEqual([])
  expect(verbs('flow cancel -h')).toEqual([])
  expect(verbs('flow run wf --help')).toEqual(['flow run'])
  expect(verbs('flow run x -- --help')).toEqual(['flow run'])
  expect(verbs('flow signal wf --name -h')).toEqual(['flow signal'])
})

test('credential keys cover the common spellings and keep the exemptions', () => {
  for (const key of ['passwd', 'pwd', 'secret_key', 'secretKey', 'access_key', 'accessKey', 'private-key', 'privateKey', 'auth_token', 'authToken', 'apikey', 'api.key', 'credential', 'credentials', 'client_secret', 'clientSecret', 'clientsecret', 'aws_secret_access_key', 'awsSecretAccessKey', 'db.password']) {
    expect(flagged(`${key}: abcdefgh1234`)).toEqual([`the key ${key} holding a literal value`])
  }
  for (const ok of ['secret_key: ${inputs.k}', 'credentials: GITHUB_CREDENTIALS', 'authToken: steps.a.outputs.t', 'pwd: 12345678', 'access_key: <your-key>', 'private_key_path: /etc/key.pem', 'secret_keys: abcdefghij']) {
    expect(flagged(ok)).toEqual([])
  }
})

test('a quoted value is a literal even with spaces; an unquoted one with spaces is prose', () => {
  expect(flagged('password: "correct horse battery staple"')).toEqual(['the key password holding a literal value'])
  expect(flagged("password: 'correct horse battery staple'")).toEqual(['the key password holding a literal value'])
  expect(flagged('password: correct horse battery staple')).toEqual([])
  expect(flagged('password: "short"')).toEqual([])
  expect(flagged('password: "prefix ${secret(\'env:P\')} suffix"')).toEqual([])
  expect(flagged('password: "A_CONSTANT_NAME"')).toEqual([])
})

test('block scalars and next-line values under a credential key are found', () => {
  expect(flagged('private_key: |\n  line one of the key\n  line two')).toEqual(['the key private_key holding a literal value'])
  expect(flagged('- token: >-\n    hunter2hunter2')).toEqual(['the key token holding a literal value'])
  expect(flagged('secret: |+\n\n  hunter2hunter2\nnext: 1')).toEqual(['the key secret holding a literal value'])
  expect(flagged('password:\n  hunter2hunter2')).toEqual(['the key password holding a literal value'])
  expect(flagged('password:\n  "correct horse battery"')).toEqual(['the key password holding a literal value'])
  expect(findSecrets('a: 1\nsecret: |\n  hunter2hunter2')[0].line).toBe(2)

  for (const ok of [
    'secret: |\nname: other',
    'secret: |\n  ${secret(\'env:S\')}',
    'secret: |\n# comment\nnext: hunter2hunter2',
    'password:\n  value: ${secret(\'env:P\')}',
    'password:\n  - one\n  - two',
    'token:\n\nnext: hunter2hunter2',
    'token:\n  nested:\n    other: 1',
    'password:\nname: hunter2hunter2',
  ]) {
    expect(flagged(ok)).toEqual([])
  }
})

test('inline maps are read', () => {
  expect(flagged('with: {user: a, password: hunter2hunter2}')).toEqual(['the key password holding a literal value'])
  expect(flagged('{"password": "hunter2hunter2"}')).toEqual(['the key password holding a literal value'])
  expect(flagged('  "api_key": "abcdef123456",')).toEqual(['the key api_key holding a literal value'])
  expect(flagged('with: {a: 1, token: "correct horse battery"}')).toEqual(['the key token holding a literal value'])
  expect(flagged("with: {token: ${secret('env:T')}}")).toEqual([])
  expect(flagged('with: {password: short, token: GITHUB_TOKEN, n: 12345678}')).toEqual([])
  expect(flagged('with: {token_url: https://x.example/oauth2/token}')).toEqual([])
  expect(flagged('with: {password: correct horse}')).toEqual([])
  expect(flagged('with: {user: a, password: hunter2hunter2}').length).toBe(1)
  expect(took(() => findSecrets('{,'.repeat(2000)))).toBeLessThan(500)
})

test('an edit is scanned as the file it leaves, not as the piece replaced', async ($, on) => {
  decide(on)
  const file = 'steps:\n  - with:\n      password: "x"\n'
  on('fs.read', () => ({ value: file }))

  // The new string alone is a bare value; in the file it is a password literal.
  const edit = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: '"x"', new_string: '"hunter2hunter2"' } })
  expect(edit.decision).toBe('deny')
  expect(edit.reason).toContain('line 3')
  expect(edit.reason).not.toContain('hunter2')

  const multi = await $.tool.check({
    tool: 'MultiEdit',
    input: { file_path: 'a.flow.yaml', edits: [{ old_string: '"x"', new_string: 'hunter2hunter2' }, { old_string: 'steps', new_string: 'jobs' }] },
  })
  expect(multi.decision).toBe('deny')

  const fine = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: '"x"', new_string: "${secret('env:P')}" } })
  expect(fine.decision).toBe('allow')
})

test('an edit to a file that cannot be read falls back to the new text', async ($, on) => {
  decide(on)
  on('fs.read', () => ({ deny: 'unreadable' }))
  const token = `ghp_${'Abcdefghijklmnopqrstuvwxyz0123456789'}`
  const out = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: 'x', new_string: `t: ${token}` } })
  expect(out.decision).toBe('deny')
  const partial = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: 'x', new_string: '"hunter2hunter2"' } })
  expect(partial.decision).toBe('allow')
})

test('afterEdits applies edits in order, by position, and declines what does not apply', () => {
  expect(afterEdits('a b a', { old_string: 'a', new_string: 'c' })).toBe('c b a')
  expect(afterEdits('a b a', { old_string: 'a', new_string: 'c', replace_all: true })).toBe('c b c')
  expect(afterEdits('abc', { old_string: 'b', new_string: '$&$1' })).toBe('a$&$1c')
  expect(afterEdits('abc', { edits: [{ old_string: 'a', new_string: 'x' }, { old_string: 'x', new_string: 'y' }] })).toBe('ybc')
  expect(afterEdits('abc', { old_string: 'z', new_string: 'y' })).toBeUndefined()
  expect(afterEdits('abc', { old_string: '', new_string: 'y' })).toBeUndefined()
  expect(afterEdits('abc', { edits: [{ old_string: 'a', new_string: 'x' }, null] })).toBeUndefined()
  expect(afterEdits('abc', null)).toBeUndefined()
})

// The `.catch` handlers answer with these when a guard throws; the engine
// cannot be made to run them without rethrowing from `next`, so the pure parts
// they use are proved here and the handlers hold nothing else.
test('a guard that failed asks for a command naming flow and denies a Flowfile edit', () => {
  expect(namesFlow('flow run x')).toBe(true)
  expect(namesFlow('FOO=1 flowstate-cli run x', '/opt/flowstate-cli')).toBe(true)
  expect(namesFlow('/opt/tool/flowstate-cli run x', '/opt/tool/flowstate-cli')).toBe(true)
  expect(namesFlow('ls -la')).toBe(false)
  expect(namesFlow(42)).toBe(false)
  expect(namesFlow(undefined)).toBe(false)
  expect(UNCHECKED_BASH).toContain('asks first')
  expect(UNCHECKED_EDIT).toContain('not made')
  expect(denyReason('a.flow.yaml', [{ line: 0, what: 'text too large to scan' }])).toContain('too large to scan')
})

test('a command line passed as one quoted word is read', () => {
  for (const [command, verb] of [
    ['ssh host "flow run x"', 'flow run'],
    ["ssh host 'flow run x'", 'flow run'],
    ['watch "flow run x"', 'flow run'],
    ['su -c "flow run x"', 'flow run'],
    ['env -S "flow run x"', 'flow run'],
    ['ssh host "cd d && flow cancel wf"', 'flow cancel'],
    ['git commit -m x && flow run y', 'flow run'],
  ]) {
    expect(verbs(command)).toEqual([verb])
  }
  for (const quiet of ['ssh host "flow run local x"', 'ssh host "ls -la"', 'git commit -m "flow run local"', 'grep "flow run" .', 'echo "flow run x"', 'ssh host "flow list"']) {
    expect(analyzeCommand(quiet)).toEqual({ actions: [], uncertain: false })
  }
})

test('a quoted word that starts with a redirection character is text, not a redirection', () => {
  expect(verbs('bash -c ">x; flow run y"')).toEqual(['flow run'])
  expect(verbs('bash -c "<f flow run y"')).toEqual(['flow run'])
  expect(verbs('eval "<f flow run y"')).toEqual(['flow run'])
  expect(verbs('eval ">x" "; flow run y"')).toEqual(['flow run'])
  expect(verbs("sh -c '&>x; flow run y'")).toEqual(['flow run'])
  expect(tokenize('a ">b" >c \\>d').quoted).toEqual([[false, true, false, true]])
  // The real thing is still stripped.
  expect(verbs('flow >out run x')).toEqual(['flow run'])
  expect(verbs('>out flow run x')).toEqual(['flow run'])
  // A dropped redirection target that holds a command's worth of text is uncertain.
  expect(analyzeCommand('>"a; b" flow validate x').uncertain).toBe(false)
  expect(analyzeCommand('>"a; flow run y"').uncertain).toBe(true)
})

test('a bare FLOWSTATE_ADDRESS assignment makes the address unknown, not the session address', () => {
  const text = (c: string, session?: string) => askReason(analyzeCommand(c), session)
  const bare = text('FLOWSTATE_ADDRESS=prod:1; flow run x', 'session:9')
  expect(bare).toContain('address may be overridden in this command')
  expect(bare).not.toContain('session:9')
  expect(text('FLOWSTATE_ADDRESS=prod:1\nflow cancel wf', 'session:9')).toContain('address may be overridden')
  // A known address, or no assignment, reads as before.
  expect(text('FLOWSTATE_ADDRESS=prod:1; flow run --address a:1 x', 'session:9')).toContain('server a:1')
  expect(text('FLOWSTATE_ADDRESS=prod:1 flow run x', 'session:9')).toContain('server prod:1')
  expect(text('flow run x; FLOWSTATE_ADDRESS=prod:1', 'session:9')).toContain('server session:9')
  expect(text('flow run x', 'session:9')).toContain('server session:9')
})

test('the fallback asks only when the binary is directly followed by a gated verb', () => {
  for (const hidden of ['echo $(x); flow run y', 'sh -c "$(printf \'flow run x\')"', 'flow -v run $(f)', 'flow --address h:1 run $(f)', 'x=`a`; flow schedule delete n']) {
    expect(analyzeCommand(hidden).uncertain).toBe(true)
  }
  for (const quiet of [
    "git commit -m \"$(cat <<'EOF'\nFix flow run local startup\nEOF\n)\"",
    'echo "$(x)" Teach flow to run workflows',
    'git commit -m "$(cat <<\'EOF\'\nTeach flow to run workflows\nEOF\n)"',
    'echo $(x) flow schedule list',
    'echo $(x) flow validate run',
  ]) {
    expect(analyzeCommand(quiet).uncertain).toBe(false)
  }
})

test('chained edits that grow the text are stopped before they are built', () => {
  const edits = Array.from({ length: 24 }, () => ({ old_string: 'a', new_string: 'aa', replace_all: true }))
  const original = 'a'.repeat(100)
  let after: unknown
  expect(took(() => (after = afterEdits(original, { edits })))).toBeLessThan(500)
  expect(after).toBe(EDIT_TOO_LARGE)
  expect(secretsIn({ edits }, original)).toEqual([{ line: 0, what: 'text too large to scan' }])
  // One replace_all can multiply the text on its own.
  const single = { old_string: 'a', new_string: 'b'.repeat(60_000), replace_all: true }
  expect(afterEdits('a'.repeat(1000), single)).toBe(EDIT_TOO_LARGE)
  // More than 64 edits, or a string over 64 KiB, is refused whatever it does.
  const many = Array.from({ length: 65 }, () => ({ old_string: 'a', new_string: 'a' }))
  expect(secretsIn({ edits: many }, original)).toEqual([{ line: 0, what: 'text too large to scan' }])
  expect(secretsIn({ edits: many })).toEqual([{ line: 0, what: 'text too large to scan' }])
  expect(afterEdits(original, { old_string: 'a', new_string: 'b'.repeat(64 * 1024 + 1) })).toBe(EDIT_TOO_LARGE)
  expect(afterEdits(original, { old_string: 'a'.repeat(64 * 1024 + 1), new_string: 'b' })).toBe(EDIT_TOO_LARGE)
  // Within the limits the same shape of edit still applies.
  const few = Array.from({ length: 4 }, () => ({ old_string: 'a', new_string: 'aa', replace_all: true }))
  expect(afterEdits('aaaa', { edits: few })).toBe('a'.repeat(64))
  expect(afterEdits('aaaa', { edits: Array.from({ length: 64 }, () => ({ old_string: 'a', new_string: 'a' })) })).toBe('aaaa')
})

test('an edit to a file that already holds a secret says so and names the fix, not the secret', async ($, on) => {
  decide(on)
  on('fs.read', () => ({ value: 'name: x\npassword: hunter2hunter2\n' }))

  const out = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: 'name: x', new_string: 'name: y' } })

  expect(out.decision).toBe('deny')
  expect(out.reason).toContain('already holds')
  expect(out.reason).toContain("${secret('scheme:name')}")
  expect(out.reason).toContain('line 2')
  expect(out.reason).not.toContain('hunter2')

  // Replacing the literal is the fix, and is allowed.
  const fixed = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: 'hunter2hunter2', new_string: "${secret('env:P')}" } })
  expect(fixed.decision).toBe('allow')
})

test('an edit that adds a secret to a clean file is not described as already there', async ($, on) => {
  decide(on)
  on('fs.read', () => ({ value: 'name: x\n' }))

  const out = await $.tool.check({ tool: 'Edit', input: { file_path: 'a.flow.yaml', old_string: 'name: x', new_string: 'password: hunter2hunter2' } })

  expect(out.decision).toBe('deny')
  expect(out.reason).not.toContain('already')
  expect(alreadyPresent('password: hunter2hunter2', [{ line: 1, what: 'the key password holding a literal value' }])).toBe(true)
  expect(alreadyPresent('a: 1', [{ line: 1, what: 'the key password holding a literal value' }])).toBe(false)
  expect(alreadyPresent(undefined, [{ line: 1, what: 'x' }])).toBe(false)
})

test('env -S, --split-string and su -c carry a command line that is followed, attached or not', () => {
  for (const c of [
    'env -S "FOO=1 flow run x"',
    'env -S"flow run x"',
    'env --split-string="flow run x"',
    'env -vS"flow run x"',
    'su -c"flow run x" bob',
  ]) {
    expect(verbs(c)).toEqual(['flow run'])
  }
  expect(analyzeCommand('env -S "echo hi"').uncertain).toBe(false)
  expect(analyzeCommand('env -S "FOO=1 flow run local x"').actions).toEqual([])
  expect(verbs('FOO=1 BAR=2 flow run x')).toEqual(['flow run'])
})

test('git runs its arguments for bisect, rebase --exec and aliases, never for text-only subcommands', () => {
  expect(verbs('git bisect run flow run x')).toEqual(['flow run'])
  expect(verbs("git -c alias.x='!flow run x' x")).toEqual(['flow run'])
  expect(verbs('git rebase --exec "flow run x"')).toEqual(['flow run'])
  expect(verbs('git commit -m "flow run local"')).toEqual([])
  expect(verbs('git commit -am "feat: flow run retries"')).toEqual([])
  expect(verbs('git log --grep "flow run x"')).toEqual([])
  expect(verbs('git config user.name "flow run x"')).toEqual([])
})

test('a long line of the binary name is answered fast, not scanned quadratically', () => {
  const command = '$(x) ' + 'flow -a '.repeat(8000) + 'z'
  expect(command.length).toBeLessThanOrEqual(MAX_COMMAND)
  const start = performance.now()
  const out = analyzeCommand(command)
  expect(performance.now() - start).toBeLessThan(500)
  expect(out.uncertain).toBe(true)
  expect(analyzeCommand('$(x) ' + 'a '.repeat(8000)).uncertain).toBe(false)
})

test('the registered Bash check asks for the flow run commands seen in a live session', async ($, on) => {
  decide(on)
  mock.env(on, {})

  for (const command of [
    'flow run workflow.yaml --address 127.0.0.1:9233 --token-file /tmp/tok',
    'cd /tmp/work && flow run workflow.yaml --address 127.0.0.1:9233 --token-file /tmp/tok',
  ]) {
    const out = await $.tool.check(bash(command))
    expect(out.decision).toBe('ask')
    expect(out.reason).toContain('flow run')
    expect(out.reason).toContain('127.0.0.1:9233')
  }
})

test('the registered Bash check tightens a settings allow rule to a question', async ($, on) => {
  on('tool.check', () => ({ decision: 'allow', rule: 'Bash(flow:*)', reason: 'allowed by rule' }))
  mock.env(on, {})

  const out = await $.tool.check(bash('cd /tmp/work && flow run workflow.yaml --address 127.0.0.1:9233'))

  expect(out.decision).toBe('ask')
  expect(out.reason).toContain('127.0.0.1:9233')
})
