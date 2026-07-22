// Package logger exposes a single shared slog logger for the whole service,
// mirroring the project's one-shared-consola-instance convention.
package logger

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// levelVar lets the configured level be swapped at runtime (e.g. in tests).
var levelVar = new(slog.LevelVar)

// Log is the shared logger. Reference it at call time (do not cache it) so that
// Discard can silence output in tests.
var Log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: levelVar}))

func init() {
	levelVar.Set(parseLevel(os.Getenv("LOG_LEVEL")))
}

// parseLevel maps a LOG_LEVEL string to an slog.Level, defaulting to info.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Discard silences the shared logger. Intended for tests, replacing the
// `logger.level = 0` used in the TypeScript suite.
func Discard() {
	Log = slog.New(slog.NewTextHandler(io.Discard, nil))
}
