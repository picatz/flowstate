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
    if (c === ' ' || c === '\t') endWord()
    else if (';&|()\n'.includes(c)) endSegment()
    else (word += c), (inWord = true)
  }
  endSegment()
  return { segments, exact }
}

const basename = (path: string): string => path.replace(/^.*[\\/]/, '')
const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/
/** Words that precede a command without being one. */
const KEYWORDS = new Set(['{', '}', '!', 'if', 'then', 'else', 'elif', 'while', 'until', 'do', 'time', 'command', 'exec', 'nohup', 'builtin'])
const SHELLS = new Set(['sh', 'bash', 'zsh', 'dash', 'ksh'])

/**
 * Strips what runs a command without being it: leading `VAR=value`, shell
 * keywords, `env`, `sudo`, `timeout`. Assignments land in `env`.
 */
const unwrap = (words: string[], env: Map<string, string>): string[] => {
  let rest = words
  for (;;) {
    const w = rest[0]
    if (w === undefined) return rest
    if (ASSIGNMENT.test(w)) {
      env.set(w.slice(0, w.indexOf('=')), w.slice(w.indexOf('=') + 1))
      rest = rest.slice(1)
    } else if (KEYWORDS.has(w)) {
      rest = rest.slice(1)
    } else if (w === 'export') {
      // `export A=1` sets A for the commands after it.
      for (const a of rest.slice(1)) if (ASSIGNMENT.test(a)) env.set(a.slice(0, a.indexOf('=')), a.slice(a.indexOf('=') + 1))
      return []
    } else if (w === 'env' || w === 'sudo' || w === 'timeout') {
      rest = rest.slice(1)
      // Options, and for `timeout` its duration; `env -u NAME` and `sudo -u user` take a value.
      while (rest[0] !== undefined && (rest[0].startsWith('-') || (w === 'timeout' && /^\d/.test(rest[0])))) {
        const takesValue = ['-u', '-C', '-S', '-g', '-h', '-p'].includes(rest[0]) && w !== 'timeout'
        rest = rest.slice(takesValue ? 2 : 1)
      }
    } else {
      return rest
    }
  }
}

/** `--address` and `--address=` in an argument list. */
const addressFlag = (args: string[]): string | undefined => {
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--address') return args[i + 1]
    if (args[i].startsWith('--address=')) return args[i].slice('--address='.length)
  }
  return undefined
}

/** The server verb a `flow` argument list runs, if any. */
const classify = (args: string[], env: Map<string, string>): ServerAction | undefined => {
  // Global flags take no value, so skipping dashes finds the verb.
  let i = 0
  while (args[i]?.startsWith('-')) i++
  const verb = args[i]
  const rest = args.slice(i + 1)
  if (verb === undefined || rest.some(a => a === '-h' || a === '--help')) return undefined

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
    address: addressFlag(rest) ?? env.get('FLOWSTATE_ADDRESS'),
    subject: first !== undefined && !first.startsWith('-') ? first : undefined,
  }
}

const escapeRe = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/**
 * Finds the server-side `flow` verbs a Bash command runs. `flowBinary` is the
 * plugin's option; the bare name `flow` is always recognized too. The parse is
 * conservative: whatever it could not follow, but that still names the binary
 * beside a gated verb, is reported as `uncertain` so the caller asks.
 */
export const analyzeCommand = (command: string, flowBinary = 'flow'): Analysis => {
  const names = new Set(['flow', basename(flowBinary)])
  const actions: ServerAction[] = []
  let uncertain = false
  const env = new Map<string, string>()

  const walk = (text: string, depth: number) => {
    const { segments, exact } = tokenize(text)
    if (!exact) uncertain = true
    for (const segment of segments) {
      const words = unwrap(segment, env)
      if (words.length === 0) continue
      const head = basename(words[0])
      if (names.has(head)) {
        const action = classify(words.slice(1), env)
        if (action) actions.push(action)
      } else if (SHELLS.has(head)) {
        // `bash -c "flow run x"`: the script is the word after the flag cluster that holds `c`.
        const flag = words.findIndex((w, i) => i > 0 && /^-[A-Za-z]*c[A-Za-z]*$/.test(w))
        if (flag > 0 && words[flag + 1] !== undefined) {
          if (depth >= MAX_DEPTH) uncertain = true
          else walk(words[flag + 1], depth + 1)
        }
      } else if (head === 'eval') {
        if (depth >= MAX_DEPTH) uncertain = true
        else walk(words.slice(1).join(' '), depth + 1)
      } else if (head === 'xargs' || words[0].includes('$')) {
        // The command comes from input or a variable; nothing here says what it is.
        uncertain = true
      }
    }
  }
  walk(command, 0)

  if (!uncertain) return { actions, uncertain }
  // The parse is incomplete. Only a command that also names the binary beside a
  // gated verb is worth a question; `echo $(date)` is not.
  const binary = [...names].map(escapeRe).join('|')
  const verbs = [...SERVER_VERBS, 'schedule'].join('|')
  const mentions = new RegExp(`(^|[^A-Za-z0-9_.-])(${binary})\\b[^\\n]*\\b(${verbs})\\b`).test(command)
  // A variable standing for the binary (`$FLOW run x`) names no binary at all.
  const variable = new RegExp(`\\$\\{?\\w+\\}?\\s+(${verbs})\\b`).test(command)
  return { actions, uncertain: mentions || variable }
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

/** A key that names a credential: `password`, `db_password`, `client-secret`, `token`, or camelCase `apiKey`. */
const CREDENTIAL_KEY_SEP = /(?:^|[_.-])(?:password|passwd|secret|token|api[_-]?key)$/i
const CREDENTIAL_KEY_CAMEL = /[a-z0-9](?:Password|Secret|Token|ApiKey)$/
const KEY_VALUE = /^\s*(?:-\s+)?["']?([A-Za-z0-9_.-]+)["']?\s*:\s+(\S.*?)\s*$/
/** What a value that is not a credential looks like: a placeholder, a bare number, an env-style constant, a path into the run. */
const PLACEHOLDER = /^(?:<.*>|\*+|x+|changeme|change-me|example|placeholder|todo|redacted|your[-_ ].*|true|false|null)$/i
const NOT_A_VALUE = /^(?:\d+|[A-Z][A-Z0-9_]*|[A-Za-z_]\w*(?:\.\w+|\[\w+\])+)$/

/** A value after the colon, unquoted and without a trailing comment; `undefined` for a block scalar, a flow collection, or an alias. */
const scalar = (value: string): string | undefined => {
  const q = value[0]
  if (q === '"' || q === "'") {
    const end = value.indexOf(q, 1)
    return end > 0 ? value.slice(1, end) : undefined
  }
  if ('|>{[&*!'.includes(q)) return undefined
  return value.replace(/\s+#.*$/, '')
}

/** Fewer than four distinct characters is a placeholder (`ghp_xxxx...`), not a token. */
const looksReal = (match: string): boolean => new Set(match.slice(-20)).size > 3

/**
 * Looks for secrets in text about to be written to a Flowfile: the token
 * shapes in TOKEN_SHAPES, and a credential-named key holding a plain string.
 * A `${...}` expression is never a finding, so `${secret('env:TOKEN')}` passes.
 */
export const findSecrets = (text: string): SecretFinding[] => {
  const found: SecretFinding[] = []
  text.split('\n').forEach((line, index) => {
    const n = index + 1
    for (const { name, pattern } of TOKEN_SHAPES) {
      const m = pattern.exec(line)
      if (m && looksReal(m[0])) found.push({ line: n, what: name })
    }
    const kv = KEY_VALUE.exec(line)
    if (!kv || !(CREDENTIAL_KEY_SEP.test(kv[1]) || CREDENTIAL_KEY_CAMEL.test(kv[1]))) return
    const value = scalar(kv[2])
    if (value === undefined || value.length < 8 || /\s/.test(value) || value.includes('${') || NOT_A_VALUE.test(value) || PLACEHOLDER.test(value)) {
      return
    }
    found.push({ line: n, what: `the key ${clean(kv[1], 40)} holding a literal value` })
  })
  return found
}

/** The text an Edit, Write, or MultiEdit puts into a file; anything else yields none. */
export const writtenText = (input: unknown): string[] => {
  if (typeof input !== 'object' || input === null) return []
  const { content, new_string, edits } = input as { content?: unknown; new_string?: unknown; edits?: unknown }
  const texts = [content, new_string]
  if (Array.isArray(edits)) for (const edit of edits) texts.push((edit as { new_string?: unknown } | null)?.new_string)
  return texts.filter((t): t is string => typeof t === 'string')
}

/** Every apparent secret in what a tool call writes, de-duplicated, with lines relative to each piece of text. */
export const secretsIn = (input: unknown): SecretFinding[] => {
  const seen = new Set<string>()
  return writtenText(input)
    .flatMap(findSecrets)
    .filter(f => {
      const key = `${f.line}:${f.what}`
      return !seen.has(key) && seen.add(key)
    })
}

/** The refusal: where, what kind, the fix. It never repeats the matched text. */
export const denyReason = (file: string, findings: SecretFinding[]): string => {
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
