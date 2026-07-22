package watcher

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
)

const (
	twLiveSgid  = "200"
	twOtherSgid = "100" // e.g. the Broadcast Box 🔴 group — must never be touched.
)

type memberOp struct {
	databaseID string
	sgid       string
}

type fakeGroup struct {
	username string
	members  []string
}

type fakeTwitchTeamSpeak struct {
	liveMembers map[string]struct{}
	groups      []fakeGroup
	clients     []teamspeak.ClientInfo
	added       []memberOp
	removed     []memberOp
	messages    []recordedMessage
}

func (f *fakeTwitchTeamSpeak) ListTwitchGroups(prefix string) ([]teamspeak.TwitchGroupRef, error) {
	var out []teamspeak.TwitchGroupRef
	for i, g := range f.groups {
		set := make(map[string]struct{}, len(g.members))
		for _, m := range g.members {
			set[m] = struct{}{}
		}
		out = append(out, teamspeak.TwitchGroupRef{
			SGID:     fmt.Sprintf("tw-%d", i),
			Name:     prefix + g.username,
			Username: g.username,
			Members:  set,
		})
	}
	return out, nil
}

func (f *fakeTwitchTeamSpeak) ListGroupMemberDbids(string) (map[string]struct{}, error) {
	snapshot := make(map[string]struct{}, len(f.liveMembers))
	for k := range f.liveMembers {
		snapshot[k] = struct{}{}
	}
	return snapshot, nil
}

func (f *fakeTwitchTeamSpeak) ListClients() ([]teamspeak.ClientInfo, error) {
	return f.clients, nil
}

func (f *fakeTwitchTeamSpeak) SendChannelMessage(channelID, text string) error {
	f.messages = append(f.messages, recordedMessage{channelID, text})
	return nil
}

func (f *fakeTwitchTeamSpeak) AddClientToGroup(databaseID, sgid string) error {
	f.added = append(f.added, memberOp{databaseID, sgid})
	f.liveMembers[databaseID] = struct{}{}
	return nil
}

func (f *fakeTwitchTeamSpeak) RemoveClientFromGroup(databaseID, sgid string) error {
	f.removed = append(f.removed, memberOp{databaseID, sgid})
	delete(f.liveMembers, databaseID)
	return nil
}

type fakeTwitchSource struct {
	live  map[string]struct{}
	calls [][]string
}

func (f *fakeTwitchSource) FetchLiveUsernames(_ context.Context, usernames []string) (map[string]struct{}, error) {
	f.calls = append(f.calls, usernames)
	out := make(map[string]struct{})
	for _, u := range usernames {
		if _, ok := f.live[u]; ok {
			out[u] = struct{}{}
		}
	}
	return out, nil
}

func newTwitchTeamSpeak(liveMembers []string, groups []fakeGroup, clients []teamspeak.ClientInfo) *fakeTwitchTeamSpeak {
	set := make(map[string]struct{}, len(liveMembers))
	for _, m := range liveMembers {
		set[m] = struct{}{}
	}
	return &fakeTwitchTeamSpeak{liveMembers: set, groups: groups, clients: clients}
}

func newTwitchSource(live ...string) *fakeTwitchSource {
	set := make(map[string]struct{}, len(live))
	for _, u := range live {
		set[u] = struct{}{}
	}
	return &fakeTwitchSource{live: set}
}

func runTwitch(t *testing.T, source LiveUsernameSource, ts TwitchTeamSpeak) {
	t.Helper()
	w := NewTwitchWatcher(source, ts, twLiveSgid, TwitchOptions{
		TwitchGroupPrefix:   "twitch.tv/",
		PublicTwitchHost:    "twitch.tv",
		LiveMessageTemplate: "{nickname} is now live: {link}",
	})
	if err := w.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
}

func sortedDbids(ops []memberOp) []string {
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		out = append(out, op.databaseID)
	}
	sort.Strings(out)
	return out
}

func TestTwitchGoLive(t *testing.T) {
	ts := newTwitchTeamSpeak(nil, []fakeGroup{{"azn", []string{"42"}}},
		[]teamspeak.ClientInfo{{Nickname: "Azn", DatabaseID: "42", ChannelID: "5"}})
	runTwitch(t, newTwitchSource("azn"), ts)

	assertEqual(t, "added", ts.added, []memberOp{{"42", twLiveSgid}})
	assertEqual(t, "removed", ts.removed, nil)
	assertEqual(t, "messages", ts.messages, []recordedMessage{{"5", "Azn is now live: https://twitch.tv/azn"}})
}

func TestTwitchStillLive(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"42"}, []fakeGroup{{"azn", []string{"42"}}},
		[]teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}})
	runTwitch(t, newTwitchSource("azn"), ts)

	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "removed", ts.removed, nil)
	assertEqual(t, "messages", ts.messages, nil)
}

func TestTwitchStop(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"42"}, []fakeGroup{{"azn", []string{"42"}}},
		[]teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}})
	runTwitch(t, newTwitchSource(), ts) // azn no longer live

	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "removed", ts.removed, []memberOp{{"42", twLiveSgid}})
}

func TestTwitchOfflineMemberNotTagged(t *testing.T) {
	ts := newTwitchTeamSpeak(nil, []fakeGroup{{"azn", []string{"99"}}}, nil) // dbid 99 not connected
	runTwitch(t, newTwitchSource("azn"), ts)

	assertEqual(t, "added", ts.added, nil)
	assertEqual(t, "messages", ts.messages, nil)
}

func TestTwitchTagsConnectedSkipsOffline(t *testing.T) {
	ts := newTwitchTeamSpeak(nil, []fakeGroup{{"azn", []string{"42", "99"}}},
		[]teamspeak.ClientInfo{{Nickname: "Azn", DatabaseID: "42", ChannelID: "5"}}) // 99 offline
	runTwitch(t, newTwitchSource("azn"), ts)

	assertEqual(t, "added", ts.added, []memberOp{{"42", twLiveSgid}})
	assertEqual(t, "messages", ts.messages, []recordedMessage{{"5", "Azn is now live: https://twitch.tv/azn"}})
}

func TestTwitchDedupesUsernames(t *testing.T) {
	ts := newTwitchTeamSpeak(nil,
		[]fakeGroup{{"azn", []string{"1"}}, {"azn", []string{"2"}}},
		[]teamspeak.ClientInfo{{Nickname: "one", DatabaseID: "1", ChannelID: "1"}, {Nickname: "two", DatabaseID: "2", ChannelID: "1"}})
	source := newTwitchSource("azn")
	runTwitch(t, source, ts)

	assertEqual(t, "calls", source.calls, [][]string{{"azn"}})
	assertEqual(t, "added dbids", sortedDbids(ts.added), []string{"1", "2"})
}

func TestTwitchOnlyLiveGroupsTagged(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"7"}, // bob's member currently tagged
		[]fakeGroup{{"azn", []string{"42"}}, {"bob", []string{"7"}}},
		[]teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}})
	runTwitch(t, newTwitchSource("azn"), ts) // only azn is live

	assertEqual(t, "added", ts.added, []memberOp{{"42", twLiveSgid}})
	assertEqual(t, "removed", ts.removed, []memberOp{{"7", twLiveSgid}})
}

func TestTwitchNoGroupsClearsAndSkipsTwitch(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"5"}, nil, nil)
	source := newTwitchSource("azn")
	runTwitch(t, source, ts)

	assertEqual(t, "removed", ts.removed, []memberOp{{"5", twLiveSgid}})
	if len(source.calls) != 0 {
		t.Errorf("Twitch was queried %d times, want 0", len(source.calls))
	}
}

func TestTwitchOnlyTouchesLiveSgid(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"7"}, []fakeGroup{{"azn", []string{"42"}}},
		[]teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}})
	runTwitch(t, newTwitchSource("azn"), ts)

	if len(ts.added) == 0 || len(ts.removed) == 0 {
		t.Fatal("expected both an add and a remove")
	}
	for _, op := range append(append([]memberOp{}, ts.added...), ts.removed...) {
		if op.sgid != twLiveSgid {
			t.Errorf("op touched sgid %q, want %q", op.sgid, twLiveSgid)
		}
		if op.sgid == twOtherSgid {
			t.Errorf("op touched the other group sgid %q", twOtherSgid)
		}
	}
}
