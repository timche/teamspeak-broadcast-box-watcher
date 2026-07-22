# syntax=docker/dockerfile:1

# ---- Build stage: compile a single self-contained binary with Bun ----
FROM oven/bun:1 AS build
WORKDIR /app

# Install dependencies against the lockfile first for better layer caching.
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Compile the app into a standalone executable that embeds the Bun runtime,
# so the runtime image needs neither Bun nor node_modules.
COPY tsconfig.json ./
COPY src ./src
RUN bun build --compile --minify --sourcemap src/index.ts --outfile watcher

# ---- Runtime stage: minimal glibc base with just the binary ----
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Bun's compiled binary links against glibc; ca-certificates is needed for
# outbound HTTPS to Broadcast Box when it is served over TLS.
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/watcher /usr/local/bin/watcher

# Run as the non-root user provided by the base image.
USER nobody

ENTRYPOINT ["/usr/local/bin/watcher"]
