# Support

Flowstate is super-alpha (#1216): there is no release and no support contract. What exists is a maintainer who reads the tracker and a repository built to be verifiable.

- **A defect, a missing capability, or a design question:** open an issue from a template. Include the `flow` revision (`flow version`), the Flowfile or a minimal reproducer, and the exact command and output. An issue with a reproducer is the fastest path to a fix.
- **"Does it do X?"** Read [`docs/README.md`](docs/README.md) for the document map, [`examples/README.md`](examples/README.md) for the journeys, and `flow tasks`, `flow tasks --expressions`, and `flow <verb> --help` for the surface of the build you have. If the answer is not there, that is a documentation issue; open one.
- **A vulnerability:** never a public issue. Follow [`SECURITY.md`](SECURITY.md).
- **Contributing:** [`CONTRIBUTING.md`](CONTRIBUTING.md).

What is not supported during super-alpha: compatibility across revisions for the CLI, the Flowfile edition, the protobuf API, persisted run history, or the plugin SDK; running workers on Windows (authoring works there; see `docs/DEPLOYMENT.md` "Blockers"); any hosted or managed offering.
