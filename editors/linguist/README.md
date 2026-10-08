# GitHub Linguist preparation

What a [Linguist](https://github.com/github-linguist/linguist) pull request
would need, prepared and tested here, **never submitted**: Linguist asks for a
language to appear in about two hundred repositories first, and Flowstate does
not meet that bar. When it does, this directory is the whole submission.

| Linguist wants | Here |
| --- | --- |
| a `languages.yml` entry | [`languages.yml`](languages.yml) |
| a grammar and its scope | [`grammars.yml`](grammars.yml); the TextMate grammars are `editors/vscode/syntaxes/{flowfile,cel}.tmLanguage.json` |
| samples under `samples/<Language>/` | [`samples/Flowfile/`](samples/Flowfile) |
| a heuristic where an extension is shared | none needed: `.flow.yaml` and `.flow.yml` belong to no other language, and `Flowfile` is a whole filename |

`type` is `programming` because a Flowfile declares a workload that runs; the
rest follows the YAML entry, whose editor modes Linguist's other YAML dialects
reuse.

The samples are copies of files under `examples/`, which are original to this
repository. Linguist wants samples from real repositories with a stated
licence, so replace them with files from other users' repositories before
submitting.

`linguist_test.go` keeps the preparation honest: every sample parses with the
real Flowfile parser, the entry's names are the ones the editor configs
detect, and the grammar scopes exist in the bundled TextMate grammars.
