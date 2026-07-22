package config

import (
	"encoding/base64"
	"os"
	"testing"
	"time"
)

// setEnv unsets every recognised variable (restoring it after the test), then
// applies the given overrides. Unsetting — rather than setting to "" — is what
// makes an absent template fall back to its default, matching the real env.
func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	keys := []string{
		"BROADCAST_BOX_API_URL", "BROADCAST_BOX_ADMIN_TOKEN", "PUBLIC_STREAM_HOST",
		"LIVE_GROUP_NAME", "STREAM_GROUP_PREFIX", "LIVE_MESSAGE_TEMPLATE",
		"TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET", "TWITCH_LIVE_GROUP_NAME",
		"TWITCH_GROUP_PREFIX", "TWITCH_LIVE_MESSAGE_TEMPLATE",
		"TEAMSPEAK_HOST", "TEAMSPEAK_QUERY_PORT", "TEAMSPEAK_SERVER_PORT",
		"TEAMSPEAK_QUERY_USERNAME", "TEAMSPEAK_QUERY_PASSWORD", "TEAMSPEAK_QUERY_NICKNAME",
		"POLL_INTERVAL_MS",
	}
	for _, key := range keys {
		if orig, ok := os.LookupEnv(key); ok {
			t.Cleanup(func() { os.Setenv(key, orig) })
		} else {
			t.Cleanup(func() { os.Unsetenv(key) })
		}
		os.Unsetenv(key)
	}
	for key, value := range overrides {
		os.Setenv(key, value)
	}
}

// validBroadcastBox is a minimal env that enables the Broadcast Box feature.
func validBroadcastBox() map[string]string {
	return map[string]string{
		"BROADCAST_BOX_API_URL":     "http://broadcast-box:8080",
		"BROADCAST_BOX_ADMIN_TOKEN": "secret",
		"PUBLIC_STREAM_HOST":        "stream.example.com",
		"TEAMSPEAK_HOST":            "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD":  "pw",
	}
}

func TestLoadDefaultsAndTransforms(t *testing.T) {
	setEnv(t, validBroadcastBox())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.BroadcastBox == nil {
		t.Fatal("expected Broadcast Box to be enabled")
	}
	if got := cfg.BroadcastBox.LiveGroupName; got != "🔴" {
		t.Errorf("LiveGroupName = %q, want 🔴", got)
	}
	if got := cfg.BroadcastBox.StreamGroupPrefix; got != "📺" {
		t.Errorf("StreamGroupPrefix = %q, want 📺", got)
	}
	if got := cfg.BroadcastBox.PublicStreamHost; got != "stream.example.com" {
		t.Errorf("PublicStreamHost = %q, want stream.example.com", got)
	}
	wantAuth := "Bearer " + base64.StdEncoding.EncodeToString([]byte("secret"))
	if got := cfg.BroadcastBox.Authorization; got != wantAuth {
		t.Errorf("Authorization = %q, want %q", got, wantAuth)
	}
	if got := cfg.BroadcastBox.LiveMessageTemplate; got != "{nickname} is now live: {link}" {
		t.Errorf("LiveMessageTemplate = %q, want default", got)
	}
	if cfg.Twitch != nil {
		t.Error("expected Twitch to be disabled")
	}
	if got := cfg.PollInterval; got != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", got)
	}
	if got := cfg.TeamSpeak.QueryPort; got != 10011 {
		t.Errorf("QueryPort = %d, want 10011", got)
	}
	if got := cfg.TeamSpeak.Username; got != "serveradmin" {
		t.Errorf("Username = %q, want serveradmin", got)
	}
}

func TestPublicStreamHostStripsSchemeAndSlashes(t *testing.T) {
	env := validBroadcastBox()
	env["PUBLIC_STREAM_HOST"] = "https://stream.example.com/"
	env["BROADCAST_BOX_API_URL"] = "http://broadcast-box:8080/"
	setEnv(t, env)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := cfg.BroadcastBox.PublicStreamHost; got != "stream.example.com" {
		t.Errorf("PublicStreamHost = %q, want stream.example.com", got)
	}
	if got := cfg.BroadcastBox.APIURL; got != "http://broadcast-box:8080" {
		t.Errorf("APIURL = %q, want http://broadcast-box:8080", got)
	}
}

func TestBlankTemplateDisablesMessage(t *testing.T) {
	env := validBroadcastBox()
	env["LIVE_MESSAGE_TEMPLATE"] = ""
	setEnv(t, env)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := cfg.BroadcastBox.LiveMessageTemplate; got != "" {
		t.Errorf("LiveMessageTemplate = %q, want empty (disabled)", got)
	}
}

func TestPartialBroadcastBoxRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"BROADCAST_BOX_API_URL":    "http://broadcast-box:8080",
		"TEAMSPEAK_HOST":           "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD": "pw",
	})
	if _, err := Load(); err == nil {
		t.Fatal("expected error for partially configured Broadcast Box")
	}
}

func TestTwitchRequiresBothCredentials(t *testing.T) {
	setEnv(t, map[string]string{
		"TWITCH_CLIENT_ID":         "id",
		"TEAMSPEAK_HOST":           "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD": "pw",
	})
	if _, err := Load(); err == nil {
		t.Fatal("expected error when only TWITCH_CLIENT_ID is set")
	}
}

func TestAtLeastOneFeatureRequired(t *testing.T) {
	setEnv(t, map[string]string{
		"TEAMSPEAK_HOST":           "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD": "pw",
	})
	if _, err := Load(); err == nil {
		t.Fatal("expected error when no feature is configured")
	}
}

func TestRequiredTeamSpeakVars(t *testing.T) {
	env := validBroadcastBox()
	delete(env, "TEAMSPEAK_QUERY_PASSWORD")
	setEnv(t, env)
	if _, err := Load(); err == nil {
		t.Fatal("expected error when TEAMSPEAK_QUERY_PASSWORD is missing")
	}
}

func TestTwitchEnabled(t *testing.T) {
	setEnv(t, map[string]string{
		"TWITCH_CLIENT_ID":         "client-id",
		"TWITCH_CLIENT_SECRET":     "client-secret",
		"TEAMSPEAK_HOST":           "teamspeak",
		"TEAMSPEAK_QUERY_PASSWORD": "pw",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
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
