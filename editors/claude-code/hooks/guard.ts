import { clean } from './runs'

/** The address `flow` falls back to when neither `--address` nor `FLOWSTATE_ADDRESS` names one. */
export const DEFAULT_ADDRESS = 'localhost:9233'

/** What a command may name before the guard stops listing it; the rest are counted. */
const MAX_LISTED = 5

/** Wrappers can nest (`sudo bash -c "env X=1 flow run ..."`), but not without bound. */
const MAX_DEPTH = 3

/**
 * The verbs that change the world on a server, from `flow --help`. `run` is
 * the server venue unless it is `run local`; `schedule` is gated by subcommand
 * so `schedule list` and `describe` stay free. Everything else (validate, fmt,
 * lint, test, tasks, compile, timeline, list, graph, get, watch) reads.
 */
const SERVER_VERBS = new Set(['run', 'signal', 'cancel', 'terminate'])
const SCHEDULE_CHANGES = new Set(['create', 'delete', 'pause', 'resume', 'trigger'])

/** One server-side verb found in a command. */
export interface ServerAction {
  /** `flow run`, `flow schedule delete`, ... */
  verb: string
  /** The `--address` or `FLOWSTATE_ADDRESS=` the command itself set; absent when the session's environment decides. */
  address?: string
  /** The run or schedule the verb names, when it is the first argument. */
  subject?: string
}

/** What a shell command does to a server. */
export interface Analysis {
  actions: ServerAction[]
  /** True when the command may run a server verb the parser could not see: a substitution, an `eval`, an unbalanced quote. */
  uncertain: boolean
}

interface Tokens {
  segments: string[][]
  /** False when the text used a construct this tokenizer does not follow. */
  exact: boolean
}

/**
 * Splits a shell command into simple commands and their words. It follows
 * quotes, backslashes, comments, and the separators `; & | ( )` and newline,
 * and nothing else: a `$(...)`, a backtick, or an unterminated quote makes the
 * result inexact, so the caller can ask instead of trusting what it saw.
 */
export const tokenize = (raw: string): Tokens => {
  const segments: string[][] = []
  let words: string[] = []
  let word = ''
  let inWord = false
  let exact = true

  const endWord = () => {
    if (inWord) words.push(word)
    word = ''
    inWord = false
  }
  const endSegment = () => {
    endWord()
    if (words.length > 0) segments.push(words)
    words = []
  }

  for (let i = 0; i < raw.length; i++) {
    const c = raw[i]
    if (c === '\\') {
      if (raw[i + 1] === '\n') i++
      else if (i + 1 < raw.length) (word += raw[++i]), (inWord = true)
      continue
    }
    if (c === "'" || c === '"') {
      const close = c
      inWord = true
      i++
      while (i < raw.length && raw[i] !== close) {
        if (close === '"' && raw[i] === '\\' && i + 1 < raw.length) {
          const next = raw[i + 1]
          // Inside double quotes a backslash only escapes these four.
          if ('"\\$`'.includes(next)) i++
          else if (next === '\n') {
            i += 2
            continue
          }
        } else if (close === '"' && (raw[i] === '`' || (raw[i] === '$' && raw[i + 1] === '('))) {
          exact = false
        }
        word += raw[i++]
      }
      if (i >= raw.length) exact = false
      continue
    }
    if (c === '`' || (c === '$' && raw[i + 1] === '(')) exact = false
    if (c === '#' && !inWord) {
      while (i < raw.length && raw[i] !== '\n') i++
      i--
      continue
    }
    // `2>&1`, `>&2`, and `&>file` redirect; the `&` there is not a separator.
    const redirecting = c === '&' && (raw[i - 1] === '>' || raw[i - 1] === '<' || raw[i + 1] === '>')
    if (c === ' ' || c === '\t') endWord()
    else if (!redirecting && ';&|()\n'.includes(c)) endSegment()
    else (word += c), (inWord = true)
  }
  endSegment()
  return { segments, exact }
}

const basename = (path: string): string => path.replace(/^.*[\\/]/, '')
const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/
/** A redirection operator with a word attached (`2>log`, `>>out`, `2>&1`) or standing alone (`>`, `<<<`, `&>`). */
const REDIRECT = /^(?:\d*|&)[<>]/
const REDIRECT_ALONE = /^(?:\d*|&)[<>]+&?$/
/** Words that precede a command without being one. */
const KEYWORDS = new Set(['{', '}', '!', 'if', 'then', 'else', 'elif', 'while', 'until', 'do', 'time', 'command', 'exec', 'nohup', 'builtin'])
const SHELLS = new Set(['sh', 'bash', 'zsh', 'dash', 'ksh'])
const INTERPRETER = /^(?:python[0-9.]*|node|perl|ruby)$/
/** Commands whose arguments are text to show or search, never a command to run. */
const DISPLAY = new Set(['echo', 'printf', 'cat', 'grep', 'egrep', 'fgrep', 'rg', 'man', 'which', 'whereis', 'type', 'ls', 'head', 'tail', 'less', 'more', 'wc'])

/** Drops redirections so they cannot hide a verb: `flow 2>/dev/null run x`, `>out flow run x`. */
const withoutRedirects = (words: string[]): string[] => {
  const kept: string[] = []
  for (let i = 0; i < words.length; i++) {
    if (!REDIRECT.test(words[i])) kept.push(words[i])
    else if (REDIRECT_ALONE.test(words[i])) i++
  }
  return kept
}

const setEnv = (env: Map<string, string>, assignment: string) => {
  const eq = assignment.indexOf('=')
  env.set(assignment.slice(0, eq), assignment.slice(eq + 1))
}

/**
 * Strips what runs a command without being it: leading `VAR=value`, shell
 * keywords, `env`, `sudo`, `timeout`. A prefix assignment lands in `env`, which
 * is this command's alone; `export` also lands in `exported`, which later
 * commands inherit. A bare `VAR=value` is a shell variable `flow` never sees,
 * so it changes neither past the command it prefixes.
 */
const unwrap = (words: string[], env: Map<string, string>, exported: Map<string, string>): string[] => {
  let i = 0
  while (i < words.length) {
    const w = words[i]
    if (ASSIGNMENT.test(w)) {
      setEnv(env, w)
      i++
    } else if (KEYWORDS.has(w)) {
      i++
    } else if (w === 'export') {
      for (let j = i + 1; j < words.length; j++) {
        if (ASSIGNMENT.test(words[j])) (setEnv(env, words[j]), setEnv(exported, words[j]))
      }
      return []
    } else if (w === 'env' || w === 'sudo' || w === 'timeout') {
      i++
      // Options, and for `timeout` its duration; `env -u NAME` and `sudo -u user` take a value.
      while (i < words.length && (words[i].startsWith('-') || (w === 'timeout' && /^\d/.test(words[i])))) {
        const takesValue = w !== 'timeout' && ['-u', '-C', '-S', '-g', '-h', '-p'].includes(words[i])
        i += takesValue ? 2 : 1
      }
    } else {
      break
    }
  }
  return i === 0 ? words : words.slice(i)
}

/** The last `--address` in an argument list; `--` ends the options. */
const addressFlag = (args: string[]): string | undefined => {
  let address: string | undefined
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--') break
    if (args[i] === '--address') address = args[i + 1] ?? address
    else if (args[i].startsWith('--address=')) address = args[i].slice('--address='.length)
  }
  return address
}

/** The server verb a `flow` argument list runs, if any. */
const classify = (args: string[], env: Map<string, string>): ServerAction | undefined => {
  // Global flags take no value except `--address`, so skipping dashes finds the verb.
  let i = 0
  while (args[i]?.startsWith('-')) i += args[i] === '--address' ? 2 : 1
  const verb = args[i]
  const rest = args.slice(i + 1)
  // Only a bare `--help`/`-h` right after the verb is a help request; later it may be a value.
  if (verb === undefined || rest[0] === '-h' || rest[0] === '--help') return undefined

  let name: string
  if (verb === 'schedule') {
    if (!SCHEDULE_CHANGES.has(rest[0] ?? '')) return undefined
    name = `schedule ${rest[0]}`
  } else if (SERVER_VERBS.has(verb)) {
    // Only the token right after `run` can be the `local` venue.
    if (verb === 'run' && rest[0] === 'local') return undefined
    name = verb
  } else {
    return undefined
  }

  const first = rest[verb === 'schedule' ? 1 : 0]
  return {
    verb: `flow ${name}`,
    address: addressFlag(args) ?? env.get('FLOWSTATE_ADDRESS'),
    subject: first !== undefined && !first.startsWith('-') ? first : undefined,
  }
}

const escapeRe = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/** The script a shell runs with `-c`: the word after the flag cluster that holds `c`. */
const shellScript = (words: string[], from: number): string | undefined => {
  for (let i = from + 1; i < words.length - 1; i++) if (/^-[A-Za-z]*c[A-Za-z]*$/.test(words[i])) return words[i + 1]
  return undefined
}

/** A command longer than this is asked about, unread: the parse would spend more than the question is worth. */
export const MAX_COMMAND = 64 * 1024

/**
 * Finds the server-side `flow` verbs a Bash command runs. `flowBinary` is the
 * plugin's option; the bare name `flow` is always recognized too. The parse is
 * conservative: whatever it could not follow, but that still names the binary
 * beside a gated verb, is reported as `uncertain` so the caller asks.
 */
export const analyzeCommand = (command: string, flowBinary = 'flow'): Analysis => {
  const names = new Set(['flow', basename(flowBinary)])
  if (command.length > MAX_COMMAND) {
    return { actions: [], uncertain: [...names].some(n => command.includes(n)) }
  }
  const actions: ServerAction[] = []
  let uncertain = false

  const walk = (text: string, depth: number, base: Map<string, string>) => {
    const { segments, exact } = tokenize(text)
    if (!exact) uncertain = true
    const exported = new Map(base)
    const recurse = (script: string, env: Map<string, string>) => {
      if (depth >= MAX_DEPTH) uncertain = true
      else walk(script, depth + 1, env)
    }
    for (const segment of segments) {
      const env = new Map(exported)
      const words = unwrap(withoutRedirects(segment), env, exported)
      if (words.length === 0) continue
      const head = basename(words[0])
      if (names.has(head)) {
        const action = classify(words.slice(1), env)
        if (action) actions.push(action)
        continue
      }
      if (SHELLS.has(head)) {
        const script = shellScript(words, 0)
        // Without `-c` the script comes from a file or stdin (`echo "flow run x" | sh`).
        if (script === undefined) uncertain = true
        else recurse(script, env)
        continue
      }
      if (head === 'eval') {
        recurse(words.slice(1).join(' '), env)
        continue
      }
      // The command comes from input or a variable, or from an interpreter's own code.
      if (head === 'xargs' || words[0].includes('$')) uncertain = true
      if (INTERPRETER.test(head) && words.some((w, i) => i > 0 && /^(?:-c|-e|-E|--eval)$/.test(w))) uncertain = true
      if (DISPLAY.has(head)) continue

      // The wrapper pass: the binary behind something this parser does not model
      // (`go run ./cmd/flow`, `nice -n 5 flow`, `ssh h flow`, `find -exec flow`).
      for (let i = 1; i < words.length; i++) {
        if (names.has(basename(words[i]))) {
          const action = classify(words.slice(i + 1), env)
          if (action) actions.push(action)
        } else if (SHELLS.has(basename(words[i]))) {
          const script = shellScript(words, i)
          if (script !== undefined) recurse(script, env)
        }
      }
    }
  }
  walk(command, 0, new Map())

  if (!uncertain) return { actions, uncertain }
  // The parse is incomplete. Only a command that also names the binary beside a
  // gated verb is worth a question; `echo $(date)` is not.
  return { actions, uncertain: namesVerb(command, names) }
}

/**
 * Whether a line of the text names the binary and, later on that same line, a
 * gated verb, or a variable standing in for the binary beside one. Each line
 * is scanned once with two forward searches, so the work is linear.
 */
const namesVerb = (text: string, names: Set<string>): boolean => {
  const binary = new RegExp(`(?:^|[^A-Za-z0-9_.-])(?:${[...names].map(escapeRe).join('|')})\\b`)
  const verb = new RegExp(`\\b(?:${[...SERVER_VERBS, 'schedule'].join('|')})\\b`, 'g')
  // A variable standing for the binary (`$FLOW run x`) names no binary at all.
  const variable = new RegExp(`\\$\\{?\\w+\\}?[ \\t]+(?:${[...SERVER_VERBS, 'schedule'].join('|')})\\b`)
  for (let start = 0; start <= text.length; ) {
    let end = text.indexOf('\n', start)
    if (end < 0) end = text.length
    const line = text.slice(start, end)
    const m = binary.exec(line)
    if (m) {
      verb.lastIndex = m.index + m[0].length
      if (verb.test(line)) return true
    }
    if (variable.test(line)) return true
    start = end + 1
  }
  return false
}

/**
 * The question put to the user. Names every verb and where it points; the
 * address is the command's own, then `FLOWSTATE_ADDRESS` from the session, then
 * the default. Text from the command is cleaned, since it reaches a terminal.
 */
export const askReason = (analysis: Analysis, sessionAddress: string | undefined): string => {
  const lines = analysis.actions.slice(0, MAX_LISTED).map(a => {
    const address = clean(a.address ?? sessionAddress ?? '', 120)
    const where = address !== '' ? `server ${address}` : `the default server ${DEFAULT_ADDRESS}`
    const what = a.subject !== undefined && a.subject !== '' ? ` ${clean(a.subject, 80)}` : ''
    return `${clean(a.verb, 40)}${what} acts on ${where}`
  })
  const more = analysis.actions.length - lines.length
  if (more > 0) lines.push(`and ${more} more`)
  if (analysis.uncertain) {
    lines.push('this command could not be fully read, so it may also run a flow verb that changes a server')
  }
  return `Flowstate: ${lines.join('; ')}. Confirm before it changes anything. Local verbs (validate, test, run local) never ask.`
}

/** One apparent secret: where, and what kind. The value itself is never kept. */
export interface SecretFinding {
  line: number
  what: string
}

/**
 * Token shapes that no ordinary Flowfile text looks like. Each is a prefix
 * the issuer documents plus enough characters to rule out a word.
 */
export const TOKEN_SHAPES: readonly { name: string; pattern: RegExp }[] = [
  { name: 'a GitHub token', pattern: /\bgh[pousr]_[A-Za-z0-9]{36,}/ },
  { name: 'a GitHub fine-grained token', pattern: /\bgithub_pat_[A-Za-z0-9_]{22,}/ },
  { name: 'a Slack token', pattern: /\bxox[baprs]-[A-Za-z0-9-]{10,}/ },
  { name: 'an AWS access key id', pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/ },
  { name: 'an API key (sk-)', pattern: /(?<![A-Za-z0-9])sk-(?:ant-|proj-)?[A-Za-z0-9_-]{20,}/ },
  { name: 'a private key', pattern: /-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----/ },
]

/** Most of a text the scan reads; a larger Flowfile edit is refused unread, since the work must be bounded where it is spent. */
export const MAX_SCAN = 256 * 1024
/** Most of one line the key scan reads; a longer line is searched for token shapes in its first part only. */
const MAX_LINE = 4096
/** Most body lines of a block scalar the scan reads. */
const MAX_BODY = 64

/** The finding for text over MAX_SCAN; line 0 marks it. */
export const TOO_LARGE: SecretFinding = { line: 0, what: 'text too large to scan' }

/** A key that names a credential: `password`, `db_password`, `client-secret`, `token`, `secret_key`, or camelCase `apiKey`. */
const CREDENTIAL_KEY_SEP =
  /(?:^|[_.-])(?:password|passwd|pwd|secret|secret[_.-]?key|access[_.-]?key|private[_.-]?key|auth[_.-]?token|token|api[_.-]?key|credentials?|client[_.-]?secret|aws[_.-]?secret[_.-]?access[_.-]?key)$/i
const CREDENTIAL_KEY_CAMEL = /(?:^|[a-z0-9])(?:[Ss]ecret|[Aa]ccess|[Pp]rivate|[Aa]pi|[Cc]lient)(?:Key|Secret)$|[a-z0-9](?:Password|Passwd|Pwd|Secret|Token|Credentials?)$/
const isCredentialKey = (key: string): boolean => CREDENTIAL_KEY_SEP.test(key) || CREDENTIAL_KEY_CAMEL.test(key)
// No lazy quantifier before the end anchor: the value is trimmed by hand, so a long run of spaces costs one pass.
const KEY_VALUE = /^(\s*)(?:-\s+)?["']?([A-Za-z0-9_.-]+)["']?\s*:\s+(\S.*)$/
/** `{user: a, password: b}` and `{"password": "b"}`: a key after `{` or `,`, then a quoted value or one up to the next `,` or `}`. */
const INLINE_PAIR = /[{,]\s*["']?([A-Za-z0-9_.-]+)["']?\s*:\s*("[^"]*"|'[^']*'|[^,}\s][^,}]*)/g
const BLOCK_INDICATOR = /^[|>][+\-0-9]*(?:\s+#.*)?$/
/** What a value that is not a credential looks like: a placeholder, a bare number, an env-style constant, a path into the run. */
const PLACEHOLDER = /^(?:<.*>|\*+|x+|changeme|change-me|example|placeholder|todo|redacted|your[-_ ].*|true|false|null)$/i
const NOT_A_VALUE = /^(?:\d+|[A-Z][A-Z0-9_]*|[A-Za-z_]\w*(?:\.\w+|\[\w+\])+)$/

/** A value after the colon without quotes or a trailing comment; `undefined` for a block scalar, a flow collection, or an alias. */
const scalar = (value: string): { text: string; quoted: boolean } | undefined => {
  const q = value[0]
  if (q === '"' || q === "'") {
    const end = value.indexOf(q, 1)
    return end > 0 ? { text: value.slice(1, end), quoted: true } : undefined
  }
  if ('|>{[&*!'.includes(q)) return undefined
  // A comment starts at a `#` after whitespace.
  for (let i = value.indexOf('#'); i > 0; i = value.indexOf('#', i + 1)) {
    if (value[i - 1] === ' ' || value[i - 1] === '\t') return { text: value.slice(0, i).trimEnd(), quoted: false }
  }
  return { text: value.trimEnd(), quoted: false }
}

/** Whether a value is a credential's literal: long enough, no reference, no placeholder; an unquoted one with whitespace is prose. */
const isLiteral = (value: string, quoted: boolean): boolean =>
  value.length >= 8 && (quoted || !/\s/.test(value)) && !value.includes('${') && !NOT_A_VALUE.test(value) && !PLACEHOLDER.test(value)

const indentOf = (line: string): number => line.length - line.trimStart().length

/** Fewer than four distinct characters is a placeholder (`ghp_xxxx...`), not a token. */
const looksReal = (match: string): boolean => new Set(match.slice(-20)).size > 3

/**
 * Looks for secrets in text about to be written to a Flowfile: the token
 * shapes in TOKEN_SHAPES, and a credential-named key holding a literal: a
 * plain or quoted string, a block scalar, a value on the next line, or a pair in
 * an inline map. A `${...}` expression is never a finding, so
 * `${secret('env:TOKEN')}` passes. Text over MAX_SCAN is not read and yields
 * TOO_LARGE; a line over 4 KiB is read for token shapes in its first 4 KiB.
 */
export const findSecrets = (text: string): SecretFinding[] => {
  if (text.length > MAX_SCAN) return [TOO_LARGE]
  const found: SecretFinding[] = []
  const seen = new Set<string>()
  const add = (line: number, what: string) => {
    if (!seen.has(`${line}:${what}`) && seen.add(`${line}:${what}`)) found.push({ line, what })
  }
  const whole = text.split('\n')
  const long = whole.map(l => l.length > MAX_LINE)
  const lines = whole.map((l, i) => (long[i] ? l.slice(0, MAX_LINE) : l))
  const keyFinding = (key: string) => `the key ${clean(key, 40)} holding a literal value`

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    for (const { name, pattern } of TOKEN_SHAPES) {
      const m = pattern.exec(line)
      if (m && looksReal(m[0])) add(i + 1, name)
    }
    if (long[i]) continue

    for (const pair of line.matchAll(INLINE_PAIR)) {
      const value = scalar(pair[2])
      if (isCredentialKey(pair[1]) && value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(pair[1]))
    }

    const kv = KEY_VALUE.exec(line)
    if (!kv || !isCredentialKey(kv[2])) continue
    const raw = kv[3].trimEnd()
    if (BLOCK_INDICATOR.test(raw)) {
      // The indented lines below are the value.
      const indent = kv[1].length
      for (let j = i + 1; j < lines.length && j <= i + MAX_BODY; j++) {
        const body = lines[j].trim()
        if (body === '') continue
        if (indentOf(lines[j]) <= indent) break
        if (isLiteral(body, true)) add(i + 1, keyFinding(kv[2]))
      }
      continue
    }
    const value = scalar(raw)
    if (value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(kv[2]))
  }

  // `token:` alone, the string on the next line.
  for (let i = 0; i + 1 < lines.length; i++) {
    const m = long[i] ? null : /^(\s*)(?:-\s+)?["']?([A-Za-z0-9_.-]+)["']?\s*:\s*$/.exec(lines[i])
    if (!m || !isCredentialKey(m[2])) continue
    for (let j = i + 1; j < lines.length && j <= i + 2; j++) {
      const next = lines[j].trim()
      if (next === '' || next.startsWith('#')) continue
      if (indentOf(lines[j]) > m[1].length && !next.startsWith('- ') && !KEY_VALUE.test(next) && !next.endsWith(':')) {
        const value = scalar(next)
        if (value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(m[2]))
      }
      break
    }
  }
  return found.sort((a, b) => a.line - b.line)
}

/** The text an Edit, Write, or MultiEdit puts into a file; anything else yields none. */
export const writtenText = (input: unknown): string[] => {
  if (typeof input !== 'object' || input === null) return []
  const { content, new_string, edits } = input as { content?: unknown; new_string?: unknown; edits?: unknown }
  const texts = [content, new_string]
  if (Array.isArray(edits)) for (const edit of edits) texts.push((edit as { new_string?: unknown } | null)?.new_string)
  return texts.filter((t): t is string => typeof t === 'string')
}

/**
 * The file as an Edit or MultiEdit leaves it, from the file as it is now; `undefined`
 * when an edit does not apply (its `old_string` is absent or empty), so the caller
 * falls back to the text written. Replacement is by position, never by pattern.
 */
export const afterEdits = (original: string, input: unknown): string | undefined => {
  if (typeof input !== 'object' || input === null) return undefined
  const { old_string, new_string, replace_all, edits } = input as Record<string, unknown>
  const list = Array.isArray(edits) ? edits : [{ old_string, new_string, replace_all }]
  let text = original
  for (const edit of list) {
    const e = edit as { old_string?: unknown; new_string?: unknown; replace_all?: unknown } | null
    if (typeof e?.old_string !== 'string' || typeof e.new_string !== 'string' || e.old_string === '') return undefined
    const at = text.indexOf(e.old_string)
    if (at < 0) return undefined
    text = e.replace_all === true ? text.split(e.old_string).join(e.new_string) : text.slice(0, at) + e.new_string + text.slice(at + e.old_string.length)
  }
  return text
}

/**
 * Every apparent secret in what a tool call writes, de-duplicated. With `current`,
 * the file's text before an Edit or MultiEdit, the file as the edit leaves it is
 * scanned and lines are the file's; else each piece written, with lines relative to it.
 */
export const secretsIn = (input: unknown, current?: string): SecretFinding[] => {
  const after = current === undefined ? undefined : afterEdits(current, input)
  const texts = after === undefined ? writtenText(input) : [after]
  if (texts.reduce((n, t) => n + t.length, 0) > MAX_SCAN) return [TOO_LARGE]
  const seen = new Set<string>()
  return texts.flatMap(findSecrets).filter(f => {
    const key = `${f.line}:${f.what}`
    return !seen.has(key) && seen.add(key)
  })
}

/** The refusal: where, what kind, the fix. It never repeats the matched text. */
export const denyReason = (file: string, findings: SecretFinding[]): string => {
  if (findings.some(f => f.line === 0)) {
    return `Flowstate refused this edit to ${clean(file, 120)}: it is too large to scan for secrets (over ${MAX_SCAN / 1024} KiB), so it was not made. Split the Flowfile or make a smaller edit.`
  }
  const listed = findings
    .slice(0, MAX_LISTED)
    .map(f => `line ${f.line}: ${f.what}`)
    .join('; ')
  const more = findings.length - MAX_LISTED
  return [
    `Flowstate refused this edit to ${clean(file, 120)}: it appears to write a secret into the Flowfile (${listed}${more > 0 ? `; and ${more} more` : ''}).`,
    "A Flowfile is committed and its values reach durable history, so reference the secret instead: use ${secret('scheme:name')}, such as ${secret('env:GITHUB_TOKEN')}, and have the operator supply the value where the run happens.",
    'If this is a harmless look-alike, change its spelling so it no longer matches a credential.',
  ].join(' ')
}

/** Whether a Bash command that could not be checked should still ask: only text that names the binary. */
export const namesFlow = (command: unknown, flowBinary = 'flow'): boolean =>
  typeof command === 'string' && (/\bflow/.test(command) || command.includes(basename(flowBinary)))

export const UNCHECKED_BASH = 'Flowstate could not check this flow command, so it asks first.'
export const UNCHECKED_EDIT = 'Flowstate could not check this Flowfile edit for secrets, so it was not made. Try again.'
