# Cache scopes

Each file beside this one exists to be *different from the others*, and for no
other reason.

`actions/setup-go` derives its cache key from a hash of the files named in
`cache-dependency-path`, which defaults to `go.sum`. Six jobs in `ci.yml`
therefore asked for one key, and only one of them can have it: `actions/cache`
reserves a key for the first writer and warns the rest, so exactly one job's
build cache is saved per run and which one is a race between finishing times.
The measured effect on 2026-08-15's runs was `vulncheck` scanning in five
seconds against a warm `govulncheck` build while `staticcheck` spent
eighty-five seconds rebuilding staticcheck from source, run after run, with
nothing in the file explaining the difference.

A job that names one of these files alongside `go.sum` gets a key of its own,
so its `go run tool@version` build is cached against itself rather than against
whoever won.

The contents are arbitrary and are not a version pin. That is deliberate: the
tool versions live once, in `ci.yml`'s `env:` block, and copying them here
would be the same value written down twice — the defect this repository has
paid for more than any other. Nothing needs the copy, because Go's build and
module caches are content-addressed: a key that fails to change after a version
bump costs one cold build, and a key that changes needlessly costs one cold
build. Neither can produce a wrong answer, which is the property that makes a
cache safe to key loosely and a test suite not.
