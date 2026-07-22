# CLAUDE.md

Project-specific rules for `teamspeak-stream-live`. See `README.md` for what the service does.

## Commands

- Run `gofmt -l .` (fix with `gofmt -w .`), `go vet ./...`, and `go test ./...` before committing.
- `go build .` compiles the standalone `teamspeak-stream-live` binary.
- Requires Go 1.25+.

## Code style

- Formatting is owned by `gofmt`. Never hand-format — run `gofmt -w .`.
- Prefer the standard library; reach for a well-known library before hand-rolling (why HTTP is `go-resty`, config is `caarlos0/env`, TeamSpeak is `multiplay/go-ts3`, backoff is `cenkalti/backoff`).
- Keep application packages under `internal/`; `main.go` at the root is the only entrypoint.

## Conventions

- **Logging:** `import ".../internal/logger"` and use `logger.Log` — one shared `slog.Logger`. No logger factory; don't pass loggers through constructors.
- **HTTP:** a `go-resty` client with a typed struct decoded via `encoding/json`; don't hand-write `net/http` + parsing.
- **Config:** all env parsing lives in `internal/config`. Every setting must be env-configurable, and secrets are never logged.
- **Watcher:** keep it stateless — each poll re-reads actual state from TeamSpeak and diffs it against Broadcast Box/Twitch. Don't add in-memory tracking.
- **Tests:** co-located as `*_test.go`; call `logger.Discard()` to keep output quiet.

## Gotchas

- The Dockerfile cross-compiles with `CGO_ENABLED=0` and ships on `gcr.io/distroless/static`, which provides the CA certificates needed for Twitch's HTTPS API. Don't switch to a base image without CA certs.
- `docker-publish.yml` publishes on `v*` tags only, not on pushes to `main`.

## Git

- Work directly on `main`.
- Never commit secrets, real/private hostnames (use `example.com` placeholders), or the built binary (`/teamspeak-stream-live`).
- If sensitive data was already committed, scrub it from history and force-push — don't just add a delete commit.
