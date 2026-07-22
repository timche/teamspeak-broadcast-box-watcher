package watcher

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
)

func init() { logger.Discard() }

const bbLiveSgid = "100"

type recordedMessage struct {
	channelID string
	text      string
}

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

func (f *fakeBBTeamSpeak) ListGroupMemberDbids(string) (map[string]struct{}, error) {
	snapshot := make(map[string]struct{}, len(f.members))
	for k := range f.members {
		snapshot[k] = struct{}{}
	}
	return snapshot, nil
}

func (f *fakeBBTeamSpeak) ListGroupsByPrefix(prefix, excludeSgid string) ([]teamspeak.ServerGroupRef, error) {
	var out []teamspeak.ServerGroupRef
	for _, g := range f.groups {
		if g.SGID != excludeSgid && strings.HasPrefix(g.Name, prefix) {
			out = append(out, g)
		}
	}
	return out, nil
}

func (f *fakeBBTeamSpeak) ListClients() ([]teamspeak.ClientInfo, error) {
	f.clientFetches++
	return f.clients, nil
}

func (f *fakeBBTeamSpeak) SendChannelMessage(channelID, text string) error {
	f.messages = append(f.messages, recordedMessage{channelID, text})
	return nil
}

func (f *fakeBBTeamSpeak) AddClientToGroup(databaseID, _ string) error {
	f.added = append(f.added, databaseID)
	f.members[databaseID] = struct{}{}
	return nil
}

func (f *fakeBBTeamSpeak) RemoveClientFromGroup(databaseID, _ string) error {
	f.removed = append(f.removed, databaseID)
	delete(f.members, databaseID)
	return nil
}

func (f *fakeBBTeamSpeak) CreateGroupAndAssign(name, _ string) (string, error) {
	f.created = append(f.created, name)
	f.groups = append(f.groups, teamspeak.ServerGroupRef{SGID: "new-" + name, Name: name})
	return "new-" + name, nil
}

func (f *fakeBBTeamSpeak) DeleteGroup(group teamspeak.ServerGroupRef) error {
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

func newBBTeamSpeak(members []string, groups []teamspeak.ServerGroupRef, clients []teamspeak.ClientInfo) *fakeBBTeamSpeak {
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m] = struct{}{}
	}
	return &fakeBBTeamSpeak{members: set, groups: groups, clients: clients}
}

func bbOptions() BroadcastBoxOptions {
	return BroadcastBoxOptions{
		StreamGroupPrefix:   "📺",
		PublicStreamHost:    "stream.example.com",
		LiveMessageTemplate: "{nickname} is now live: {link}",
	}
}

func streamGroup(streamKey string) string {
	return "📺 stream.example.com/" + streamKey
}

func runBB(t *testing.T, source StreamKeySource, ts BroadcastBoxTeamSpeak) {
	t.Helper()
	w := NewBroadcastBoxWatcher(source, ts, bbLiveSgid, bbOptions())
	if err := w.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
}

func TestBBGoLive(t *testing.T) {
	ts := newBBTeamSpeak(nil, nil, []teamspeak.ClientInfo{{Nickname: "Alice", DatabaseID: "42", ChannelID: "5"}})
	runBB(t, fakeStreamSource{keys: []string{"alice"}}, ts)

	assertEqual(t, "added", ts.added, []string{"42"})
	assertEqual(t, "created", ts.created, []string{streamGroup("alice")})
	assertEqual(t, "removed", ts.removed, nil)
	assertEqual(t, "deleted", ts.deleted, nil)
	assertEqual(t, "messages", ts.messages, []recordedMessage{{"5", "Alice is now live: https://stream.example.com/alice"}})
}

func TestBBStillLive(t *testing.T) {
	ts := newBBTeamSpeak(
		[]string{"42"},
		[]teamspeak.ServerGroupRef{{SGID: "1", Name: streamGroup("alice")}},
		[]teamspeak.ClientInfo{{Nickname: "alice", DatabaseID: "42", ChannelID: "1"}},
	)
	runBB(t, fakeStreamSource{keys: []string{"alice"}}, ts)

	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "created", ts.created, nil)
	assertEqual(t, "removed", ts.removed, nil)
	assertEqual(t, "deleted", ts.deleted, nil)
	assertEqual(t, "messages", ts.messages, nil)
}

func TestBBStop(t *testing.T) {
	ts := newBBTeamSpeak(
		[]string{"42", "7"},
		[]teamspeak.ServerGroupRef{
			{SGID: "1", Name: streamGroup("alice")},
			{SGID: "2", Name: streamGroup("bob")},
		},
		[]teamspeak.ClientInfo{{Nickname: "alice", DatabaseID: "42", ChannelID: "1"}},
	)
	runBB(t, fakeStreamSource{keys: []string{"alice"}}, ts)

	assertEqual(t, "removed", ts.removed, []string{"7"})
	assertEqual(t, "deleted", ts.deleted, []string{streamGroup("bob")})
	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "created", ts.created, nil)
}

func TestBBNoStreamsSkipsClientFetch(t *testing.T) {
	ts := newBBTeamSpeak(
		[]string{"42"},
		[]teamspeak.ServerGroupRef{{SGID: "1", Name: streamGroup("alice")}},
		nil,
	)
	runBB(t, fakeStreamSource{keys: nil}, ts)

	assertEqual(t, "removed", ts.removed, []string{"42"})
	assertEqual(t, "deleted", ts.deleted, []string{streamGroup("alice")})
	if ts.clientFetches != 0 {
		t.Errorf("clientFetches = %d, want 0", ts.clientFetches)
	}
}

func TestBBNoMatchingUserChangesNothing(t *testing.T) {
	ts := newBBTeamSpeak(nil, nil, []teamspeak.ClientInfo{{Nickname: "someoneelse", DatabaseID: "9", ChannelID: "1"}})
	runBB(t, fakeStreamSource{keys: []string{"ghost"}}, ts)

	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "created", ts.created, nil)
	assertEqual(t, "removed", ts.removed, nil)
	assertEqual(t, "deleted", ts.deleted, nil)
}

func assertEqual[T any](t *testing.T, name string, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}
