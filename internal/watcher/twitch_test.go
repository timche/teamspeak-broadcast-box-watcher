package watcher_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
	"github.com/timche/teamspeak-stream-live/internal/watcher"
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

func (f *fakeTwitchTeamSpeak) ListTwitchGroups(_ context.Context, prefix string) ([]teamspeak.TwitchGroupRef, error) {
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

func (f *fakeTwitchTeamSpeak) ListGroupMemberDbids(_ context.Context, _ string) (map[string]struct{}, error) {
	snapshot := make(map[string]struct{}, len(f.liveMembers))
	for k := range f.liveMembers {
		snapshot[k] = struct{}{}
	}
	return snapshot, nil
}

func (f *fakeTwitchTeamSpeak) ListClients(_ context.Context) ([]teamspeak.ClientInfo, error) {
	return f.clients, nil
}

func (f *fakeTwitchTeamSpeak) SendChannelMessage(_ context.Context, channelID, text string) error {
	f.messages = append(f.messages, recordedMessage{channelID, text})
	return nil
}

func (f *fakeTwitchTeamSpeak) AddClientToGroup(_ context.Context, databaseID, sgid string) error {
	f.added = append(f.added, memberOp{databaseID, sgid})
	f.liveMembers[databaseID] = struct{}{}
	return nil
}

func (f *fakeTwitchTeamSpeak) RemoveClientFromGroup(_ context.Context, databaseID, sgid string) error {
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

func runTwitch(t *testing.T, source watcher.LiveUsernameSource, ts watcher.TwitchTeamSpeak) {
	t.Helper()
	w := watcher.NewTwitchWatcher(source, ts, twLiveSgid, watcher.TwitchOptions{
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

func TestTwitchReconcile(t *testing.T) {
	tests := []struct {
		name         string
		liveMembers  []string
		groups       []fakeGroup
		clients      []teamspeak.ClientInfo
		live         []string
		wantAdded    []string
		wantRemoved  []string
		wantMessages []recordedMessage
	}{
		{
			name:         "go-live adds and announces",
			groups:       []fakeGroup{{"azn", []string{"42"}}},
			clients:      []teamspeak.ClientInfo{{Nickname: "Azn", DatabaseID: "42", ChannelID: "5"}},
			live:         []string{"azn"},
			wantAdded:    []string{"42"},
			wantMessages: []recordedMessage{{"5", "Azn is now live: https://twitch.tv/azn"}},
		},
		{
			name:        "still-live untouched",
			liveMembers: []string{"42"},
			groups:      []fakeGroup{{"azn", []string{"42"}}},
			clients:     []teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}},
			live:        []string{"azn"},
		},
		{
			name:        "stop removes members no longer live",
			liveMembers: []string{"42"},
			groups:      []fakeGroup{{"azn", []string{"42"}}},
			clients:     []teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}},
			live:        nil,
			wantRemoved: []string{"42"},
		},
		{
			name:    "offline member not tagged",
			groups:  []fakeGroup{{"azn", []string{"99"}}},
			clients: nil,
			live:    []string{"azn"},
		},
		{
			name:         "tags connected members, skips offline",
			groups:       []fakeGroup{{"azn", []string{"42", "99"}}},
			clients:      []teamspeak.ClientInfo{{Nickname: "Azn", DatabaseID: "42", ChannelID: "5"}},
			live:         []string{"azn"},
			wantAdded:    []string{"42"},
			wantMessages: []recordedMessage{{"5", "Azn is now live: https://twitch.tv/azn"}},
		},
		{
			name:         "only live groups tagged, offline groups' members removed",
			liveMembers:  []string{"7"},
			groups:       []fakeGroup{{"azn", []string{"42"}}, {"bob", []string{"7"}}},
			clients:      []teamspeak.ClientInfo{{Nickname: "azn", DatabaseID: "42", ChannelID: "1"}},
			live:         []string{"azn"},
			wantAdded:    []string{"42"},
			wantRemoved:  []string{"7"},
			wantMessages: []recordedMessage{{"1", "azn is now live: https://twitch.tv/azn"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTwitchTeamSpeak(tc.liveMembers, tc.groups, tc.clients)
			runTwitch(t, newTwitchSource(tc.live...), ts)

			assertEqual(t, "added dbids", sortedDbids(ts.added), tc.wantAdded)
			assertEqual(t, "removed dbids", sortedDbids(ts.removed), tc.wantRemoved)
			assertEqual(t, "messages", ts.messages, tc.wantMessages)
			// Membership operations must only ever touch the Twitch live group.
			for _, op := range append(append([]memberOp{}, ts.added...), ts.removed...) {
				if op.sgid != twLiveSgid {
					t.Errorf("op touched sgid %q, want %q (never %q)", op.sgid, twLiveSgid, twOtherSgid)
				}
			}
		})
	}
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

func TestTwitchNoGroupsSkipsTwitch(t *testing.T) {
	ts := newTwitchTeamSpeak([]string{"5"}, nil, nil)
	source := newTwitchSource("azn")
	runTwitch(t, source, ts)

	assertEqual(t, "removed dbids", sortedDbids(ts.removed), []string{"5"})
	if len(source.calls) != 0 {
		t.Errorf("Twitch was queried %d times, want 0", len(source.calls))
	}
}
