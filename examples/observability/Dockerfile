# The lab's Flowstate image: one build, two commands.
#
# `flow` is a single binary that is a server, a worker, and a client depending on
# the verb, so the lab builds it once and runs it twice (see docker-compose.yaml,
# where both services share this build and this image tag). Building it twice
# would let the server and the worker be different binaries, which is exactly the
# disagreement Worker Deployment Versioning exists to prevent — a lab that models
# it wrongly teaches the wrong thing.
#
# Pinned to the toolchain go.mod selects. CLAUDE.md's CI-parity list pins
# GOTOOLCHAIN for the same reason: a build that silently picks a different
# toolchain than the one the tree was tested against is a difference nobody chose.
FROM golang:1.27.0-alpine AS build

WORKDIR /src

# Dependencies first, so editing a .go file does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off, so the result runs on any base image and the runtime stage can stay
# small. -trimpath keeps build paths out of the binary; the version stamp is what
# `flow --version` prints and what telemetry reports as service.version, so the
# lab's traces say "lab" rather than "dev" and nobody mistakes one for a release.
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=lab" -o /out/flow ./cmd/flow

FROM alpine:3.22

# ca-certificates because the http task speaks TLS to the wider internet, and a
# lab where `https://` fails for want of a root store sends people debugging the
# wrong thing.
RUN apk add --no-cache ca-certificates

# Unprivileged. There is no log directory to prepare any more: logs leave over
# OTLP rather than into a file on a shared volume, so stderr is stderr and
# `docker compose logs` shows it.
RUN adduser -D -u 10001 flowstate

COPY --from=build /out/flow /usr/local/bin/flow

USER flowstate
WORKDIR /home/flowstate

ENTRYPOINT ["flow"]
CMD ["--help"]
