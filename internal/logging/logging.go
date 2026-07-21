// Package logging provides a small helper to construct a structured slog.Logger
// consistently across the worker and coordinator binaries.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON structured logger enriched with a stable "component" and
// "node_id" attribute so logs from the whole cluster can be correlated. The log
// level is read from the AGORA_LOG_LEVEL env var (debug|info|warn|error),
// defaulting to info.
func New(component, nodeID string) *slog.Logger {
	level := parseLevel(os.Getenv("AGORA_LOG_LEVEL"))
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With(
		slog.String("component", component),
		slog.String("node_id", nodeID),
	)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
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
