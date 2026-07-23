// Command teamspeak-stream-live reflects live-streaming status (Broadcast Box,
// Twitch) into TeamSpeak as server-group prefixes.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/timche/teamspeak-stream-live/internal/broadcastbox"
	"github.com/timche/teamspeak-stream-live/internal/config"
	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
	"github.com/timche/teamspeak-stream-live/internal/twitch"
	"github.com/timche/teamspeak-stream-live/internal/watcher"
)

// namedWatcher is a unit of work run each poll and torn down on shutdown.
type namedWatcher interface {
	Name() string
	Reconcile(ctx context.Context) error
	Cleanup(ctx context.Context) error
}

func main() {
	if err := run(); err != nil {
		logger.Log.Error("teamspeak-stream-live exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger.Log.Info("Starting teamspeak-stream-live")
	logger.Log.Debug("Features",
		"broadcastBox", cfg.BroadcastBox != nil,
		"twitch", cfg.Twitch != nil,
		"pollInterval", cfg.PollInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ts, err := teamspeak.Connect(teamspeak.ConnectOptions{
		Host:       cfg.TeamSpeak.Host,
		QueryPort:  cfg.TeamSpeak.QueryPort,
		ServerPort: cfg.TeamSpeak.ServerPort,
		Username:   cfg.TeamSpeak.Username,
		Password:   cfg.TeamSpeak.Password,
		Nickname:   cfg.TeamSpeak.Nickname,
	})
	if err != nil {
		return err
	}

	watchers, err := buildWatchers(ctx, cfg, ts)
	if err != nil {
		return err
	}

	pollLoop(ctx, cfg.PollInterval, watchers)

	// Best-effort cleanup under a fresh timeout, since the main context is already
	// cancelled by the time we shut down.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, w := range watchers {
		if err := w.Cleanup(cleanupCtx); err != nil {
			logger.Log.Error("cleanup during shutdown failed", "watcher", w.Name(), "error", err)
		}
	}
	if err := ts.Disconnect(); err != nil {
		logger.Log.Debug("disconnect error", "error", err)
	}
	logger.Log.Info("Shutdown complete")
	return nil
}

func buildWatchers(ctx context.Context, cfg config.Config, ts *teamspeak.Manager) ([]namedWatcher, error) {
	var watchers []namedWatcher

	if cfg.BroadcastBox != nil {
		liveGroupSgid, err := ts.EnsureLiveGroup(ctx, cfg.BroadcastBox.LiveGroupName)
		if err != nil {
			return nil, err
		}
		client := broadcastbox.New(broadcastbox.Options{
			APIURL:        cfg.BroadcastBox.APIURL,
			Authorization: cfg.BroadcastBox.Authorization,
		})
		watchers = append(watchers, watcher.NewBroadcastBoxWatcher(client, ts, liveGroupSgid, watcher.BroadcastBoxOptions{
			StreamGroupPrefix:   cfg.BroadcastBox.StreamGroupPrefix,
			PublicStreamHost:    cfg.BroadcastBox.PublicStreamHost,
			LiveMessageTemplate: cfg.BroadcastBox.LiveMessageTemplate,
		}))
	}

	if cfg.Twitch != nil {
		liveGroupSgid, err := ts.EnsureLiveGroup(ctx, cfg.Twitch.LiveGroupName)
		if err != nil {
			return nil, err
		}
		client, err := twitch.New(twitch.Options{
			ClientID:     cfg.Twitch.ClientID,
			ClientSecret: cfg.Twitch.ClientSecret,
		})
		if err != nil {
			return nil, err
		}
		watchers = append(watchers, watcher.NewTwitchWatcher(client, ts, liveGroupSgid, watcher.TwitchOptions{
			TwitchGroupPrefix:   cfg.Twitch.TwitchGroupPrefix,
			PublicTwitchHost:    cfg.Twitch.PublicTwitchHost,
			LiveMessageTemplate: cfg.Twitch.LiveMessageTemplate,
		}))
	}

	return watchers, nil
}

// pollLoop reconciles every watcher each cycle, isolating per-watcher failures,
// then sleeps until the next tick or shutdown.
func pollLoop(ctx context.Context, interval time.Duration, watchers []namedWatcher) {
	for {
		// Honour a shutdown that arrived before or during the last cycle before
		// starting another reconcile.
		if ctx.Err() != nil {
			return
		}
		for _, w := range watchers {
			reconcileSafely(ctx, w)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// reconcileSafely runs one watcher in isolation so one feature's failure doesn't
// skip the other; both self-heal on the next poll. Errors during shutdown
// (context cancelled) are suppressed.
func reconcileSafely(ctx context.Context, w namedWatcher) {
	if err := w.Reconcile(ctx); err != nil && ctx.Err() == nil {
		logger.Log.Error("reconcile cycle failed", "watcher", w.Name(), "error", err)
	}
}
