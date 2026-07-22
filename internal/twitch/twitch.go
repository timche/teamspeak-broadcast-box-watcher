// Package twitch reports which of a set of Twitch logins are currently live,
// using the Helix API with the client-credentials app-token flow.
package twitch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/timche/teamspeak-stream-live/internal/httpx"
	"github.com/timche/teamspeak-stream-live/internal/logger"
)

const (
	defaultHelixURL = "https://api.twitch.tv/helix"
	defaultTokenURL = "https://id.twitch.tv/oauth2/token"
	// maxLoginsPerRequest is the Helix cap on `user_login` values per request.
	maxLoginsPerRequest = 100
	requestTimeout      = 10 * time.Second
)

// errUnauthorized signals a 401 from the streams endpoint (token likely expired).
var errUnauthorized = errors.New("twitch: unauthorized")

type streamEntry struct {
	UserLogin string `json:"user_login"`
}

// Options configures a Client. HelixURL/TokenURL default to the public
// endpoints and are overridable in tests.
type Options struct {
	ClientID     string
	ClientSecret string
	HelixURL     string
	TokenURL     string
}

// Client is a Twitch Helix client. It obtains and caches an app access token
// and reports live logins; the token lifecycle is fully internal.
type Client struct {
	helix        *resty.Client
	auth         *resty.Client
	tokenURL     string
	clientID     string
	clientSecret string

	mu    sync.Mutex
	token string
}

// New builds a Client.
func New(opts Options) *Client {
	helixURL := opts.HelixURL
	if helixURL == "" {
		helixURL = defaultHelixURL
	}
	tokenURL := opts.TokenURL
	if tokenURL == "" {
		tokenURL = defaultTokenURL
	}

	helix := resty.New().
		SetBaseURL(helixURL).
		SetHeader("Client-Id", opts.ClientID).
		SetHeader("Accept", "application/json").
		SetTimeout(requestTimeout).
		SetRetryCount(2)
	httpx.RetryOnStatuses(helix)

	auth := resty.New().
		SetTimeout(requestTimeout).
		SetRetryCount(2)
	httpx.RetryOnStatuses(auth)

	return &Client{
		helix:        helix,
		auth:         auth,
		tokenURL:     tokenURL,
		clientID:     opts.ClientID,
		clientSecret: opts.ClientSecret,
	}
}

// FetchLiveUsernames returns the subset of usernames currently live on Twitch.
// It batches into Helix requests of up to 100 logins, performs no HTTP at all on
// empty input, fetches the token lazily, and refreshes it once on a 401.
func (c *Client) FetchLiveUsernames(ctx context.Context, usernames []string) (map[string]struct{}, error) {
	live := make(map[string]struct{})
	if len(usernames) == 0 {
		return live, nil
	}

	token, err := c.ensureToken(ctx)
	if err != nil {
		return nil, err
	}

	for _, logins := range chunk(usernames, maxLoginsPerRequest) {
		data, err := c.queryStreams(ctx, logins, token)
		if err != nil {
			if !errors.Is(err, errUnauthorized) {
				return nil, err
			}
			// Token likely expired: refresh once and retry this chunk.
			token, err = c.refreshToken(ctx)
			if err != nil {
				return nil, err
			}
			data, err = c.queryStreams(ctx, logins, token)
			if err != nil {
				return nil, err
			}
		}
		for _, stream := range data {
			live[strings.ToLower(stream.UserLogin)] = struct{}{}
		}
	}

	logger.Log.Debug("Twitch live channels", "count", len(live))
	return live, nil
}

func (c *Client) queryStreams(ctx context.Context, logins []string, token string) ([]streamEntry, error) {
	values := url.Values{}
	for _, login := range logins {
		values.Add("user_login", login)
	}

	resp, err := c.helix.R().
		SetContext(ctx).
		SetHeader("Authorization", "Bearer "+token).
		SetQueryParamsFromValues(values).
		Get("streams")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == 401 {
		return nil, errUnauthorized
	}
	if resp.IsError() {
		return nil, fmt.Errorf("twitch streams request failed: %d", resp.StatusCode())
	}

	var body struct {
		Data []streamEntry `json:"data"`
	}
	if err := json.Unmarshal(resp.Body(), &body); err != nil {
		return nil, err
	}
	return body.Data, nil
}

func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	token := c.token
	c.mu.Unlock()
	if token != "" {
		return token, nil
	}
	return c.refreshToken(ctx)
}

func (c *Client) refreshToken(ctx context.Context) (string, error) {
	resp, err := c.auth.R().
		SetContext(ctx).
		SetFormData(map[string]string{
			"client_id":     c.clientID,
			"client_secret": c.clientSecret,
			"grant_type":    "client_credentials",
		}).
		Post(c.tokenURL)
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("twitch token request failed: %d", resp.StatusCode())
	}

	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(resp.Body(), &body); err != nil {
		return "", err
	}

	c.mu.Lock()
	c.token = body.AccessToken
	c.mu.Unlock()
	logger.Log.Debug("Twitch obtained an app access token")
	return body.AccessToken, nil
}

func chunk[T any](items []T, size int) [][]T {
	var chunks [][]T
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[start:end])
	}
	return chunks
}
