// Package watcher reconciles TeamSpeak server groups against live-streaming
// state. Each watcher is stateless: every poll re-reads actual state and diffs
// it, so it self-heals across restarts.
package watcher

import (
	"context"
	"strings"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
)

// StreamKeySource yields the stream keys currently live on Broadcast Box.
type StreamKeySource interface {
	FetchLiveStreamKeys(ctx context.Context) (map[string]struct{}, error)
}

// BroadcastBoxTeamSpeak is the subset of the TeamSpeak manager this watcher uses.
type BroadcastBoxTeamSpeak interface {
	ListGroupMemberDbids(sgid string) (map[string]struct{}, error)
	ListGroupsByPrefix(prefix, excludeSgid string) ([]teamspeak.ServerGroupRef, error)
	ListClients() ([]teamspeak.ClientInfo, error)
	SendChannelMessage(channelID, text string) error
	AddClientToGroup(databaseID, sgid string) error
	RemoveClientFromGroup(databaseID, sgid string) error
	CreateGroupAndAssign(name, databaseID string) (string, error)
	DeleteGroup(group teamspeak.ServerGroupRef) error
}

// BroadcastBoxOptions configures a BroadcastBoxWatcher.
type BroadcastBoxOptions struct {
	StreamGroupPrefix   string
	PublicStreamHost    string
	LiveMessageTemplate string
}

// BroadcastBoxWatcher reconciles TeamSpeak groups against the users live on
// Broadcast Box. Every live user gets (1) membership in the shared "live" group
// and (2) an individual group named after their stream link.
type BroadcastBoxWatcher struct {
	source        StreamKeySource
	ts            BroadcastBoxTeamSpeak
	liveGroupSgid string
	opts          BroadcastBoxOptions
}

// NewBroadcastBoxWatcher builds a BroadcastBoxWatcher.
func NewBroadcastBoxWatcher(source StreamKeySource, ts BroadcastBoxTeamSpeak, liveGroupSgid string, opts BroadcastBoxOptions) *BroadcastBoxWatcher {
	return &BroadcastBoxWatcher{source: source, ts: ts, liveGroupSgid: liveGroupSgid, opts: opts}
}

// Name identifies this watcher in logs.
func (w *BroadcastBoxWatcher) Name() string { return "Broadcast Box" }

type liveUser struct {
	databaseID string
	channelID  string
	nickname   string
	streamKey  string
}

func (w *BroadcastBoxWatcher) streamGroupName(streamKey string) string {
	return w.opts.StreamGroupPrefix + " " + w.opts.PublicStreamHost + "/" + streamKey
}

func (w *BroadcastBoxWatcher) streamGroupNamePrefix() string {
	return w.opts.StreamGroupPrefix + " "
}

func (w *BroadcastBoxWatcher) streamLink(streamKey string) string {
	return "https://" + w.opts.PublicStreamHost + "/" + streamKey
}

// Reconcile runs a single reconciliation cycle.
func (w *BroadcastBoxWatcher) Reconcile(ctx context.Context) error {
	liveStreamKeys, err := w.source.FetchLiveStreamKeys(ctx)
	if err != nil {
		return err
	}
	currentMembers, err := w.ts.ListGroupMemberDbids(w.liveGroupSgid)
	if err != nil {
		return err
	}
	existingStreamGroups, err := w.ts.ListGroupsByPrefix(w.streamGroupNamePrefix(), w.liveGroupSgid)
	if err != nil {
		return err
	}

	// Nothing is live: clear the shared group and all per-user groups, and skip
	// the (larger) client list entirely.
	if len(liveStreamKeys) == 0 {
		w.removeMembers(currentMembers)
		w.deleteGroups(existingStreamGroups)
		return nil
	}

	clients, err := w.ts.ListClients()
	if err != nil {
		return err
	}
	clientByNickname := make(map[string]teamspeak.ClientInfo, len(clients))
	for _, client := range clients {
		clientByNickname[strings.ToLower(client.Nickname)] = client
	}

	desiredMembers := make(map[string]liveUser)
	desiredStreamGroups := make(map[string]liveUser)
	for streamKey := range liveStreamKeys {
		client, ok := clientByNickname[strings.ToLower(streamKey)]
		if !ok {
			logger.Log.Debug("Broadcast Box stream has no matching connected user", "streamKey", streamKey)
			continue
		}
		user := liveUser{
			databaseID: client.DatabaseID,
			channelID:  client.ChannelID,
			nickname:   client.Nickname,
			streamKey:  streamKey,
		}
		desiredMembers[client.DatabaseID] = user
		desiredStreamGroups[w.streamGroupName(streamKey)] = user
	}

	w.reconcileSharedMembership(currentMembers, desiredMembers)
	w.reconcileStreamGroups(existingStreamGroups, desiredStreamGroups)
	return nil
}

// Cleanup empties the shared group and deletes per-user groups (best-effort).
func (w *BroadcastBoxWatcher) Cleanup() error {
	members, err := w.ts.ListGroupMemberDbids(w.liveGroupSgid)
	if err != nil {
		return err
	}
	w.removeMembers(members)

	groups, err := w.ts.ListGroupsByPrefix(w.streamGroupNamePrefix(), w.liveGroupSgid)
	if err != nil {
		return err
	}
	w.deleteGroups(groups)
	return nil
}

func (w *BroadcastBoxWatcher) reconcileSharedMembership(current map[string]struct{}, desired map[string]liveUser) {
	for databaseID, user := range desired {
		if _, ok := current[databaseID]; ok {
			continue
		}
		if err := w.ts.AddClientToGroup(databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Broadcast Box failed to add to live group", "dbid", databaseID, "error", err)
			continue
		}
		logger.Log.Info("Broadcast Box added to the live group", "dbid", databaseID)
		w.announce(user)
	}

	for databaseID := range current {
		if _, ok := desired[databaseID]; ok {
			continue
		}
		if err := w.ts.RemoveClientFromGroup(databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Broadcast Box failed to remove from live group", "dbid", databaseID, "error", err)
			continue
		}
		logger.Log.Info("Broadcast Box removed from the live group", "dbid", databaseID)
	}
}

func (w *BroadcastBoxWatcher) reconcileStreamGroups(existing []teamspeak.ServerGroupRef, desired map[string]liveUser) {
	existingNames := make(map[string]struct{}, len(existing))
	var stale []teamspeak.ServerGroupRef
	for _, group := range existing {
		existingNames[group.Name] = struct{}{}
		if _, ok := desired[group.Name]; !ok {
			stale = append(stale, group)
		}
	}
	w.deleteGroups(stale)

	for name, user := range desired {
		if _, ok := existingNames[name]; ok {
			continue
		}
		if _, err := w.ts.CreateGroupAndAssign(name, user.databaseID); err != nil {
			logger.Log.Error("Broadcast Box failed to create/assign stream group", "streamKey", user.streamKey, "error", err)
		}
	}
}

func (w *BroadcastBoxWatcher) announce(user liveUser) {
	if w.opts.LiveMessageTemplate == "" {
		return
	}
	text := strings.ReplaceAll(w.opts.LiveMessageTemplate, "{nickname}", user.nickname)
	text = strings.ReplaceAll(text, "{link}", w.streamLink(user.streamKey))

	if err := w.ts.SendChannelMessage(user.channelID, text); err != nil {
		logger.Log.Error("Broadcast Box failed to announce", "nickname", user.nickname, "error", err)
		return
	}
	logger.Log.Info("Broadcast Box announced live", "nickname", user.nickname, "channel", user.channelID)
}

func (w *BroadcastBoxWatcher) removeMembers(databaseIDs map[string]struct{}) {
	for databaseID := range databaseIDs {
		if err := w.ts.RemoveClientFromGroup(databaseID, w.liveGroupSgid); err != nil {
			logger.Log.Error("Broadcast Box failed to remove from live group", "dbid", databaseID, "error", err)
		}
	}
}

func (w *BroadcastBoxWatcher) deleteGroups(groups []teamspeak.ServerGroupRef) {
	for _, group := range groups {
		if err := w.ts.DeleteGroup(group); err != nil {
			logger.Log.Error("Broadcast Box failed to delete group", "name", group.Name, "error", err)
		}
	}
}
