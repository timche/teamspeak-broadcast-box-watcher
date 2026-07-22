// Package config validates the process environment and maps it into the runtime
// configuration. It mirrors the semantics of the original zod-based config: two
// independent features (Broadcast Box and Twitch), each enabled only when all of
// its variables are set, with at least one required.
package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// BroadcastBox holds the Broadcast Box feature config; nil when not configured.
type BroadcastBox struct {
	// APIURL is the Broadcast Box base URL with trailing slashes stripped.
	APIURL string
	// Authorization is the ready-to-send header value, "Bearer <base64(token)>".
	Authorization string
	// PublicStreamHost is the public host (scheme + trailing slashes stripped).
	PublicStreamHost string
	// LiveGroupName is the shared "live" group shown before the nickname.
	LiveGroupName string
	// StreamGroupPrefix prefixes the per-user stream-link groups.
	StreamGroupPrefix string
	// LiveMessageTemplate supports {nickname}/{link}; blank disables the message.
	LiveMessageTemplate string
}

// Twitch holds the Twitch feature config; nil when not configured.
type Twitch struct {
	ClientID     string
	ClientSecret string
	// LiveGroupName is the shared Twitch "live" group shown before the nickname.
	LiveGroupName string
	// TwitchGroupPrefix prefixes the pre-assigned per-user Twitch groups.
	TwitchGroupPrefix string
	// PublicTwitchHost is the host used to build the announcement link.
	PublicTwitchHost string
	// LiveMessageTemplate supports {nickname}/{link}; blank disables the message.
	LiveMessageTemplate string
}

// TeamSpeak holds the always-required ServerQuery connection config.
type TeamSpeak struct {
	Host       string
	QueryPort  int
	ServerPort int
	Username   string
	Password   string
	Nickname   string
}

// Config is the validated runtime configuration.
type Config struct {
	// BroadcastBox is nil when the feature is not configured (presence = enabled).
	BroadcastBox *BroadcastBox
	// Twitch is nil when the feature is not configured (presence = enabled).
	Twitch       *Twitch
	TeamSpeak    TeamSpeak
	PollInterval time.Duration
}

// The two message-template keys are handled specially: unlike every other
// variable, an explicitly blank value must survive as "" (disabled) rather than
// falling back to the default, so they are never blank-stripped or given to the
// env parser.
const (
	liveMessageTemplateKey   = "LIVE_MESSAGE_TEMPLATE"
	twitchMessageTemplateKey = "TWITCH_LIVE_MESSAGE_TEMPLATE"
	defaultMessageTemplate   = "{nickname} is now live: {link}"
)

// raw is the env-parsed struct. Defaults and required-ness come from the tags;
// the two message templates are intentionally excluded (see above).
type raw struct {
	BroadcastBoxAPIURL     string `env:"BROADCAST_BOX_API_URL"`
	BroadcastBoxAdminToken string `env:"BROADCAST_BOX_ADMIN_TOKEN"`
	PublicStreamHost       string `env:"PUBLIC_STREAM_HOST"`
	LiveGroupName          string `env:"LIVE_GROUP_NAME" envDefault:"🔴"`
	StreamGroupPrefix      string `env:"STREAM_GROUP_PREFIX" envDefault:"📺"`

	TwitchClientID      string `env:"TWITCH_CLIENT_ID"`
	TwitchClientSecret  string `env:"TWITCH_CLIENT_SECRET"`
	TwitchLiveGroupName string `env:"TWITCH_LIVE_GROUP_NAME" envDefault:"🟣"`
	TwitchGroupPrefix   string `env:"TWITCH_GROUP_PREFIX" envDefault:"twitch.tv/"`

	TeamSpeakHost          string `env:"TEAMSPEAK_HOST,required"`
	TeamSpeakQueryPort     int    `env:"TEAMSPEAK_QUERY_PORT" envDefault:"10011"`
	TeamSpeakServerPort    int    `env:"TEAMSPEAK_SERVER_PORT" envDefault:"9987"`
	TeamSpeakQueryUsername string `env:"TEAMSPEAK_QUERY_USERNAME" envDefault:"serveradmin"`
	TeamSpeakQueryPassword string `env:"TEAMSPEAK_QUERY_PASSWORD,required"`
	TeamSpeakQueryNickname string `env:"TEAMSPEAK_QUERY_NICKNAME" envDefault:"teamspeak-stream-live"`

	PollIntervalMs int `env:"POLL_INTERVAL_MS" envDefault:"10000"`
}

// Load validates the process environment and maps it into a Config.
func Load() (Config, error) {
	return Parse(env.ToMap(os.Environ()))
}

// Parse validates the given environment map and maps it into a Config, or
// returns an error describing what is missing or invalid. Taking the environment
// as an argument keeps it testable without mutating the process environment.
func Parse(environ map[string]string) (Config, error) {
	// blankToUndefined: whitespace-only values are treated as unset so that
	// defaults apply and required variables report as missing. The message
	// templates are exempt (a blank template means "disabled").
	parseEnv := make(map[string]string, len(environ))
	for key, value := range environ {
		if strings.TrimSpace(value) == "" {
			continue
		}
		parseEnv[key] = value
	}

	r, err := env.ParseAsWithOptions[raw](env.Options{Environment: parseEnv})
	if err != nil {
		return Config{}, err
	}

	if r.TeamSpeakQueryPort <= 0 {
		return Config{}, fmt.Errorf("environment variable TEAMSPEAK_QUERY_PORT must be a positive integer")
	}
	if r.TeamSpeakServerPort <= 0 {
		return Config{}, fmt.Errorf("environment variable TEAMSPEAK_SERVER_PORT must be a positive integer")
	}
	if r.PollIntervalMs <= 0 {
		return Config{}, fmt.Errorf("environment variable POLL_INTERVAL_MS must be a positive integer")
	}

	if err := validateFeatures(r); err != nil {
		return Config{}, err
	}

	cfg := Config{
		TeamSpeak: TeamSpeak{
			Host:       r.TeamSpeakHost,
			QueryPort:  r.TeamSpeakQueryPort,
			ServerPort: r.TeamSpeakServerPort,
			Username:   r.TeamSpeakQueryUsername,
			Password:   r.TeamSpeakQueryPassword,
			Nickname:   r.TeamSpeakQueryNickname,
		},
		PollInterval: time.Duration(r.PollIntervalMs) * time.Millisecond,
	}

	if broadcastBoxConfigured(r) {
		cfg.BroadcastBox = &BroadcastBox{
			APIURL:              strings.TrimRight(r.BroadcastBoxAPIURL, "/"),
			Authorization:       "Bearer " + base64.StdEncoding.EncodeToString([]byte(r.BroadcastBoxAdminToken)),
			PublicStreamHost:    stripHost(r.PublicStreamHost),
			LiveGroupName:       r.LiveGroupName,
			StreamGroupPrefix:   r.StreamGroupPrefix,
			LiveMessageTemplate: messageTemplate(environ, liveMessageTemplateKey),
		}
	}

	if twitchConfigured(r) {
		cfg.Twitch = &Twitch{
			ClientID:            r.TwitchClientID,
			ClientSecret:        r.TwitchClientSecret,
			LiveGroupName:       r.TwitchLiveGroupName,
			TwitchGroupPrefix:   r.TwitchGroupPrefix,
			PublicTwitchHost:    "twitch.tv",
			LiveMessageTemplate: messageTemplate(environ, twitchMessageTemplateKey),
		}
	}

	return cfg, nil
}

func broadcastBoxConfigured(r raw) bool {
	return r.BroadcastBoxAPIURL != "" && r.BroadcastBoxAdminToken != "" && r.PublicStreamHost != ""
}

func twitchConfigured(r raw) bool {
	return r.TwitchClientID != "" && r.TwitchClientSecret != ""
}

// validateFeatures enforces the cross-field rules: a partially configured
// Broadcast Box is rejected, Twitch id/secret must be set together, and at least
// one feature must be configured.
func validateFeatures(r raw) error {
	broadcastBoxVars := map[string]string{
		"BROADCAST_BOX_API_URL":     r.BroadcastBoxAPIURL,
		"BROADCAST_BOX_ADMIN_TOKEN": r.BroadcastBoxAdminToken,
		"PUBLIC_STREAM_HOST":        r.PublicStreamHost,
	}
	broadcastBoxSet := 0
	var broadcastBoxMissing []string
	// Iterate in a fixed order so the error message is deterministic.
	for _, key := range []string{"BROADCAST_BOX_API_URL", "BROADCAST_BOX_ADMIN_TOKEN", "PUBLIC_STREAM_HOST"} {
		if broadcastBoxVars[key] != "" {
			broadcastBoxSet++
		} else {
			broadcastBoxMissing = append(broadcastBoxMissing, key)
		}
	}
	if broadcastBoxSet > 0 && broadcastBoxSet < len(broadcastBoxVars) {
		return fmt.Errorf("broadcast box is partially configured; also set: %s", strings.Join(broadcastBoxMissing, ", "))
	}

	twitchSet := 0
	if r.TwitchClientID != "" {
		twitchSet++
	}
	if r.TwitchClientSecret != "" {
		twitchSet++
	}
	if twitchSet == 1 {
		return fmt.Errorf("set both TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET together")
	}

	if broadcastBoxSet == 0 && twitchSet == 0 {
		return fmt.Errorf("no feature configured: set the BROADCAST_BOX_* variables, the TWITCH_* variables, or both")
	}

	return nil
}

// messageTemplate returns the verbatim template when the key is present (an
// explicit blank disables the message), or the default when it is absent.
func messageTemplate(env map[string]string, key string) string {
	if value, ok := env[key]; ok {
		return value
	}
	return defaultMessageTemplate
}

// stripHost removes a leading http(s):// scheme and any trailing slashes.
func stripHost(host string) string {
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	return strings.TrimRight(host, "/")
}
