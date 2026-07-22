package config_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/timche/teamspeak-stream-live/internal/config"
)

// validBroadcastBox is a minimal env map that enables the Broadcast Box feature.
func validBroadcastBox() map[string]string {
	return map[string]string{
		"BROADCAST_BOX_API_URL":     "http://broadcast-box:8080",
		"BROADCAST_BOX_ADMIN_TOKEN": "secret",
		"PUBLIC_STREAM_HOST":        "stream.example.com",
		"TEAMSPEAK_HOST":            "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD":  "pw",
	}
}

func TestParseDefaultsAndTransforms(t *testing.T) {
	t.Parallel()
	cfg, err := config.Parse(validBroadcastBox())
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}

	if cfg.BroadcastBox == nil {
		t.Fatal("expected Broadcast Box to be enabled")
	}
	bb := cfg.BroadcastBox
	wantAuth := "Bearer " + base64.StdEncoding.EncodeToString([]byte("secret"))
	checks := []struct {
		name string
		got  string
		want string
	}{
		{"LiveGroupName", bb.LiveGroupName, "🔴"},
		{"StreamGroupPrefix", bb.StreamGroupPrefix, "📺"},
		{"PublicStreamHost", bb.PublicStreamHost, "stream.example.com"},
		{"Authorization", bb.Authorization, wantAuth},
		{"LiveMessageTemplate", bb.LiveMessageTemplate, "{nickname} is now live: {link}"},
		{"Username", cfg.TeamSpeak.Username, "serveradmin"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
	if cfg.Twitch != nil {
		t.Error("expected Twitch to be disabled")
	}
	if cfg.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", cfg.PollInterval)
	}
	if cfg.TeamSpeak.QueryPort != 10011 {
		t.Errorf("QueryPort = %d, want 10011", cfg.TeamSpeak.QueryPort)
	}
}

func TestParsePublicStreamHostStripsSchemeAndSlashes(t *testing.T) {
	t.Parallel()
	env := validBroadcastBox()
	env["PUBLIC_STREAM_HOST"] = "https://stream.example.com/"
	env["BROADCAST_BOX_API_URL"] = "http://broadcast-box:8080/"

	cfg, err := config.Parse(env)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if got := cfg.BroadcastBox.PublicStreamHost; got != "stream.example.com" {
		t.Errorf("PublicStreamHost = %q, want stream.example.com", got)
	}
	if got := cfg.BroadcastBox.APIURL; got != "http://broadcast-box:8080" {
		t.Errorf("APIURL = %q, want http://broadcast-box:8080", got)
	}
}

func TestParseTemplateSemantics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		template *string // nil = key absent
		want     string
	}{
		{"absent uses default", nil, "{nickname} is now live: {link}"},
		{"blank disables", ptr(""), ""},
		{"custom kept verbatim", ptr("live! {link}"), "live! {link}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := validBroadcastBox()
			if tc.template != nil {
				env["LIVE_MESSAGE_TEMPLATE"] = *tc.template
			}
			cfg, err := config.Parse(env)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			if got := cfg.BroadcastBox.LiveMessageTemplate; got != tc.want {
				t.Errorf("LiveMessageTemplate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseValidationErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			"partial broadcast box",
			map[string]string{
				"BROADCAST_BOX_API_URL":    "http://broadcast-box:8080",
				"TEAMSPEAK_HOST":           "teamspeak",
				"TEAMSPEAK_QUERY_PASSWORD": "pw",
			},
		},
		{
			"twitch id without secret",
			map[string]string{
				"TWITCH_CLIENT_ID":         "id",
				"TEAMSPEAK_HOST":           "teamspeak",
				"TEAMSPEAK_QUERY_PASSWORD": "pw",
			},
		},
		{
			"no feature configured",
			map[string]string{
				"TEAMSPEAK_HOST":           "teamspeak",
				"TEAMSPEAK_QUERY_PASSWORD": "pw",
			},
		},
		{
			"missing required teamspeak password",
			map[string]string{
				"BROADCAST_BOX_API_URL":     "http://broadcast-box:8080",
				"BROADCAST_BOX_ADMIN_TOKEN": "secret",
				"PUBLIC_STREAM_HOST":        "stream.example.com",
				"TEAMSPEAK_HOST":            "teamspeak",
			},
		},
		{
			"non-positive poll interval",
			mergeEnv(validBroadcastBox(), map[string]string{"POLL_INTERVAL_MS": "0"}),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := config.Parse(tc.env); err == nil {
				t.Fatalf("expected an error for %q", tc.name)
			}
		})
	}
}

func TestParseTwitchEnabled(t *testing.T) {
	t.Parallel()
	cfg, err := config.Parse(map[string]string{
		"TWITCH_CLIENT_ID":         "client-id",
		"TWITCH_CLIENT_SECRET":     "client-secret",
		"TEAMSPEAK_HOST":           "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD": "pw",
	})
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if cfg.Twitch == nil {
		t.Fatal("expected Twitch to be enabled")
	}
	if cfg.Twitch.PublicTwitchHost != "twitch.tv" {
		t.Errorf("PublicTwitchHost = %q, want twitch.tv", cfg.Twitch.PublicTwitchHost)
	}
	if cfg.Twitch.LiveGroupName != "🟣" {
		t.Errorf("LiveGroupName = %q, want 🟣", cfg.Twitch.LiveGroupName)
	}
	if cfg.BroadcastBox != nil {
		t.Error("expected Broadcast Box to be disabled")
	}
}

func ptr(s string) *string { return &s }

func mergeEnv(base, over map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}
