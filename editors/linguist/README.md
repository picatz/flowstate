# GitHub Linguist preparation

What a [Linguist](https://github.com/github-linguist/linguist) pull request
would need, prepared and tested here, **never submitted**. Linguist's
[usage requirements](https://github.com/github-linguist/linguist/blob/main/CONTRIBUTING.md)
ask for at least 2,000 indexed files in the last year per extension that can
occur more than once in a repository (`.flow.yaml`, `.flow.yml`), or 200 per
name that occurs once (`Flowfile`), spread across many `user/repo` pairs, with
forks excluded. Flowstate is nowhere near that. When it is, this directory is
the whole submission.

| Linguist wants | Here |
| --- | --- |
| a `languages.yml` entry | [`languages.yml`](languages.yml) |
| a grammar and its scope | [`grammars.yml`](grammars.yml); the TextMate grammar is `editors/vscode/syntaxes/flowfile.tmLanguage.json`. Linguist already vendors a `source.cel` and rejects a duplicate scope, so only `source.flowfile` is registered and its CEL injection uses that one |
| a sample for every extension, under `samples/<Language>/` | [`samples/Flowfile/`](samples/Flowfile), one `.flow.yml` and two `.flow.yaml` |
| a heuristic where an extension is shared | none needed: `.flow.yaml` and `.flow.yml` belong to no other language, and `Flowfile` is a whole filename |

`type` is `programming` because a Flowfile declares a workload that runs; the
rest follows the YAML entry, whose editor modes Linguist's other YAML dialects
reuse.

The samples are copies of files under `examples/`, which are original to this
repository. Linguist wants samples from real repositories with a stated
licence, so replace them with files from other users' repositories before
submitting.

`linguist_test.go` keeps the preparation honest: every sample parses with the
real Flowfile parser and every declared extension has a sample, the entry's
names are the ones the editor configs detect, and the registered scope is the
one the bundled TextMate grammar declares (and `source.cel` is not registered).
