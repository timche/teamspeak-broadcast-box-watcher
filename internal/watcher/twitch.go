package watcher

import (
	"context"
	"strings"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
)

// LiveUsernameSource reports which of a set of usernames are live on Twitch.
type LiveUsernameSource interface {
	FetchLiveUsernames(ctx context.Context, usernames []string) (map[string]struct{}, error)
}

// TwitchTeamSpeak is the subset of the TeamSpeak manager this watcher uses.
type TwitchTeamSpeak interface {
	ListTwitchGroups(ctx context.Context, prefix string) ([]teamspeak.TwitchGroupRef, error)
	ListGroupMemberDbids(ctx context.Context, sgid string) (map[string]struct{}, error)
	ListClients(ctx context.Context) ([]teamspeak.ClientInfo, error)
	SendChannelMessage(ctx context.Context, channelID, text string) error
	AddClientToGroup(ctx context.Context, databaseID, sgid string) error
	RemoveClientFromGroup(ctx context.Context, databaseID, sgid string) error
}

// TwitchOptions configures a TwitchWatcher.
type TwitchOptions struct {
	TwitchGroupPrefix   string
	PublicTwitchHost    string
	LiveMessageTemplate string
}

// TwitchWatcher reconciles a shared "live" group against the Twitch channels
// currently live. The users to check are discovered from pre-assigned
// `twitch.tv/<username>` groups; the connected members of live channels get the
// shared live group. Only connected members are tagged.
type TwitchWatcher struct {
	source        LiveUsernameSource
	ts            TwitchTeamSpeak
	liveGroupSgid string
	opts          TwitchOptions
}

// NewTwitchWatcher builds a TwitchWatcher.
func NewTwitchWatcher(source LiveUsernameSource, ts TwitchTeamSpeak, liveGroupSgid string, opts TwitchOptions) *TwitchWatcher {
	return &TwitchWatcher{source: source, ts: ts, liveGroupSgid: liveGroupSgid, opts: opts}
}

// Name identifies this watcher in logs.
func (w *TwitchWatcher) Name() string { return "Twitch" }

type liveTwitchUser struct {
	username string
	client   teamspeak.ClientInfo
}

func (w *TwitchWatcher) streamLink(username string) string {
	return "https://" + w.opts.PublicTwitchHost + "/" + username
}

// Reconcile runs a single reconciliation cycle.
func (w *TwitchWatcher) Reconcile(ctx context.Context) error {
	groups, err := w.ts.ListTwitchGroups(ctx, w.opts.TwitchGroupPrefix)
	if err != nil {
		return err
	}
	currentMembers, err := w.ts.ListGroupMemberDbids(ctx, w.liveGroupSgid)
	if err != nil {
		return err
	}

	// No twitch.tv/ groups exist: clear the shared group and skip Twitch entirely.
	if len(groups) == 0 {
		w.removeMembers(ctx, currentMembers)
		return nil
	}

	usernames := uniqueUsernames(groups)
	liveUsernames, err := w.source.FetchLiveUsernames(ctx, usernames)
	if err != nil {
		return err
	}

	// Nothing is live: clear the shared group and skip the (larger) client list.
	if len(liveUsernames) == 0 {
		w.removeMembers(ctx, currentMembers)
		return nil
	}

	clients, err := w.ts.ListClients(ctx)
	if err != nil {
		return err
	}
	clientByDbid := make(map[string]teamspeak.ClientInfo, len(clients))
	for _, client := range clients {
		clientByDbid[client.DatabaseID] = client
	}

	desired := make(map[string]liveTwitchUser)
	for _, group := range groups {
		if _, ok := liveUsernames[group.Username]; !ok {
			continue
		}
		for databaseID := range group.Members {
			client, ok := clientByDbid[databaseID]
			if !ok {
				logger.Log.Debug("Twitch channel live but member not connected", "username", group.Username, "dbid", databaseID)
				continue
			}
			desired[databaseID] = liveTwitchUser{username: group.Username, client: client}
		}
	}

	w.reconcileMembership(ctx, currentMembers, desired)
	return nil
}

// Cleanup empties the shared group (best-effort).
func (w *TwitchWatcher) Cleanup(ctx context.Context) error {
	members, err := w.ts.ListGroupMemberDbids(ctx, w.liveGroupSgid)
	if err != nil {
		return err
	}
	w.removeMembers(ctx, members)
	return nil
}

func (w *TwitchWatcher) reconcileMembership(ctx context.Context, current map[string]struct{}, desired map[string]liveTwitchUser) {
	for databaseID, user := range desired {
		if _, ok := current[databaseID]; ok {
			continue
		}
		if err := w.ts.AddClientToGroup(ctx, databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Twitch failed to add to live group", "dbid", databaseID, "error", err)
			continue
		}
		logger.Log.Info("Twitch added to the live group", "dbid", databaseID)
		w.announce(ctx, user)
	}

	for databaseID := range current {
		if _, ok := desired[databaseID]; ok {
			continue
		}
		if err := w.ts.RemoveClientFromGroup(ctx, databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Twitch failed to remove from live group", "dbid", databaseID, "error", err)
			continue
		}
		logger.Log.Info("Twitch removed from the live group", "dbid", databaseID)
	}
}

func (w *TwitchWatcher) announce(ctx context.Context, user liveTwitchUser) {
	if w.opts.LiveMessageTemplate == "" {
		return
	}
	text := strings.ReplaceAll(w.opts.LiveMessageTemplate, "{nickname}", user.client.Nickname)
	text = strings.ReplaceAll(text, "{link}", w.streamLink(user.username))

	if err := w.ts.SendChannelMessage(ctx, user.client.ChannelID, text); err != nil {
		logger.Log.Error("Twitch failed to announce", "nickname", user.client.Nickname, "error", err)
		return
	}
	logger.Log.Info("Twitch announced live", "nickname", user.client.Nickname, "channel", user.client.ChannelID)
}

func (w *TwitchWatcher) removeMembers(ctx context.Context, databaseIDs map[string]struct{}) {
	for databaseID := range databaseIDs {
		if err := w.ts.RemoveClientFromGroup(ctx, databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Twitch failed to remove from live group", "dbid", databaseID, "error", err)
		}
	}
}

// uniqueUsernames returns the distinct group usernames, preserving first-seen
// order so a set of duplicated groups collapses into a single Twitch query.
func uniqueUsernames(groups []teamspeak.TwitchGroupRef) []string {
	seen := make(map[string]struct{}, len(groups))
	var usernames []string
	for _, group := range groups {
		if _, ok := seen[group.Username]; ok {
			continue
		}
		seen[group.Username] = struct{}{}
		usernames = append(usernames, group.Username)
	}
	return usernames
}
