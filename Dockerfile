# syntax=docker/dockerfile:1

# ---- Build stage: compile a static binary ----
# Pinned to the build platform and cross-compiled to the target arch, so the Go
# toolchain never runs under QEMU emulation for arm64.
FROM --platform=$BUILDPLATFORM golang:1-alpine AS build
WORKDIR /app

# Download modules first for better layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
# CGO disabled -> a fully static binary with no libc dependency.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /teamspeak-stream-live .

# ---- Runtime stage: distroless static ----
# Unlike the previous Bun binary, a static Go binary bundles no CA roots, and
# Twitch is reached over public HTTPS. distroless/static ships ca-certificates
# and a nonroot user while staying ~2 MB.
FROM gcr.io/distroless/static:nonroot AS runtime
COPY --from=build /teamspeak-stream-live /usr/local/bin/teamspeak-stream-live
USER nonroot
ENTRYPOINT ["/usr/local/bin/teamspeak-stream-live"]
