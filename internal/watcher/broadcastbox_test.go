package watcher_test

import (
	"context"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
	"github.com/timche/teamspeak-stream-live/internal/watcher"
)

func TestMain(m *testing.M) {
	logger.Discard()
	os.Exit(m.Run())
}

const bbLiveSgid = "100"

type recordedMessage struct {
	channelID string
	text      string
}

// fakeBBTeamSpeak implements watcher.BroadcastBoxTeamSpeak. ctx is accepted and
// ignored — the watcher only threads it to the real manager.
type fakeBBTeamSpeak struct {
	members       map[string]struct{}
	groups        []teamspeak.ServerGroupRef
	clients       []teamspeak.ClientInfo
	added         []string
	removed       []string
	created       []string
	deleted       []string
	messages      []recordedMessage
	clientFetches int
}

func (f *fakeBBTeamSpeak) ListGroupMemberDbids(_ context.Context, _ string) (map[string]struct{}, error) {
	snapshot := make(map[string]struct{}, len(f.members))
	for k := range f.members {
		snapshot[k] = struct{}{}
	}
	return snapshot, nil
}

func (f *fakeBBTeamSpeak) ListGroupsByPrefix(_ context.Context, prefix, excludeSgid string) ([]teamspeak.ServerGroupRef, error) {
	var out []teamspeak.ServerGroupRef
	for _, g := range f.groups {
		if g.SGID != excludeSgid && strings.HasPrefix(g.Name, prefix) {
			out = append(out, g)
		}
	}
	return out, nil
}

func (f *fakeBBTeamSpeak) ListClients(_ context.Context) ([]teamspeak.ClientInfo, error) {
	f.clientFetches++
	return f.clients, nil
}

func (f *fakeBBTeamSpeak) SendChannelMessage(_ context.Context, channelID, text string) error {
	f.messages = append(f.messages, recordedMessage{channelID, text})
	return nil
}

func (f *fakeBBTeamSpeak) AddClientToGroup(_ context.Context, databaseID, _ string) error {
	f.added = append(f.added, databaseID)
	f.members[databaseID] = struct{}{}
	return nil
}

func (f *fakeBBTeamSpeak) RemoveClientFromGroup(_ context.Context, databaseID, _ string) error {
	f.removed = append(f.removed, databaseID)
	delete(f.members, databaseID)
	return nil
}

func (f *fakeBBTeamSpeak) CreateGroupAndAssign(_ context.Context, name, _ string) (string, error) {
	f.created = append(f.created, name)
	f.groups = append(f.groups, teamspeak.ServerGroupRef{SGID: "new-" + name, Name: name})
	return "new-" + name, nil
}

func (f *fakeBBTeamSpeak) DeleteGroup(_ context.Context, group teamspeak.ServerGroupRef) error {
	f.deleted = append(f.deleted, group.Name)
	var kept []teamspeak.ServerGroupRef
	for _, g := range f.groups {
		if g.SGID != group.SGID {
			kept = append(kept, g)
		}
	}
	f.groups = kept
	return nil
}

type fakeStreamSource struct{ keys []string }

func (f fakeStreamSource) FetchLiveStreamKeys(context.Context) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(f.keys))
	for _, k := range f.keys {
		set[k] = struct{}{}
	}
	return set, nil
}

func bbOptions() watcher.BroadcastBoxOptions {
	return watcher.BroadcastBoxOptions{
		StreamGroupPrefix:   "📺",
		PublicStreamHost:    "stream.example.com",
		LiveMessageTemplate: "{nickname} is now live: {link}",
	}
}

func streamGroup(streamKey string) string {
	return "📺 stream.example.com/" + streamKey
}

func TestBroadcastBoxReconcile(t *testing.T) {
	tests := []struct {
		name          string
		members       []string
		groups        []teamspeak.ServerGroupRef
		clients       []teamspeak.ClientInfo
		keys          []string
		wantAdded     []string
		wantRemoved   []string
		wantCreated   []string
		wantDeleted   []string
		wantMessages  []recordedMessage
		wantNoClients bool
	}{
		{
			name:         "go-live adds to shared group and creates stream group",
			clients:      []teamspeak.ClientInfo{{Nickname: "Alice", DatabaseID: "42", ChannelID: "5"}},
			keys:         []string{"alice"},
			wantAdded:    []string{"42"},
			wantCreated:  []string{streamGroup("alice")},
			wantMessages: []recordedMessage{{"5", "Alice is now live: https://stream.example.com/alice"}},
		},
		{
			name:    "still-live leaves everything untouched",
			members: []string{"42"},
			groups:  []teamspeak.ServerGroupRef{{SGID: "1", Name: streamGroup("alice")}},
			clients: []teamspeak.ClientInfo{{Nickname: "alice", DatabaseID: "42", ChannelID: "1"}},
			keys:    []string{"alice"},
		},
		{
			name:    "stop removes the member and deletes their stream group",
			members: []string{"42", "7"},
			groups: []teamspeak.ServerGroupRef{
				{SGID: "1", Name: streamGroup("alice")},
				{SGID: "2", Name: streamGroup("bob")},
			},
			clients:     []teamspeak.ClientInfo{{Nickname: "alice", DatabaseID: "42", ChannelID: "1"}},
			keys:        []string{"alice"},
			wantRemoved: []string{"7"},
			wantDeleted: []string{streamGroup("bob")},
		},
		{
			name:          "no streams clears everything and skips the client fetch",
			members:       []string{"42"},
			groups:        []teamspeak.ServerGroupRef{{SGID: "1", Name: streamGroup("alice")}},
			keys:          nil,
			wantRemoved:   []string{"42"},
			wantDeleted:   []string{streamGroup("alice")},
			wantNoClients: true,
		},
		{
			name:    "live stream with no matching user changes nothing",
			clients: []teamspeak.ClientInfo{{Nickname: "someoneelse", DatabaseID: "9", ChannelID: "1"}},
			keys:    []string{"ghost"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			members := make(map[string]struct{}, len(tc.members))
			for _, m := range tc.members {
				members[m] = struct{}{}
			}
			ts := &fakeBBTeamSpeak{members: members, groups: tc.groups, clients: tc.clients}
			w := watcher.NewBroadcastBoxWatcher(fakeStreamSource{keys: tc.keys}, ts, bbLiveSgid, bbOptions())

			if err := w.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile error: %v", err)
			}

			// Sort before comparing: the reconciler iterates maps in random order,
			// so record order is not significant (each entry is independent).
			sort.Strings(ts.added)
			sort.Strings(ts.removed)
			sort.Strings(ts.created)
			sort.Strings(ts.deleted)
			sort.Strings(tc.wantAdded)
			sort.Strings(tc.wantRemoved)
			sort.Strings(tc.wantCreated)
			sort.Strings(tc.wantDeleted)

			assertEqual(t, "added", ts.added, tc.wantAdded)
			assertEqual(t, "removed", ts.removed, tc.wantRemoved)
			assertEqual(t, "created", ts.created, tc.wantCreated)
			assertEqual(t, "deleted", ts.deleted, tc.wantDeleted)
			assertEqual(t, "messages", ts.messages, tc.wantMessages)
			if tc.wantNoClients && ts.clientFetches != 0 {
				t.Errorf("clientFetches = %d, want 0", ts.clientFetches)
			}
		})
	}
}

// assertEqual compares two values, treating a nil slice and an empty slice as equal.
func assertEqual[T any](t *testing.T, name string, got, want T) {
	t.Helper()
	if isEmpty(got) && isEmpty(want) {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func isEmpty(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Slice && rv.Len() == 0
}
