// Package twitch reports which of a set of Twitch logins are currently live,
// using the Helix API (github.com/nicklaw5/helix) with the client-credentials
// app-token flow.
package twitch

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	helix "github.com/nicklaw5/helix/v2"

	"github.com/timche/teamspeak-stream-live/internal/logger"
)

const (
	// maxLoginsPerRequest is the Helix cap on `user_login` values per request.
	maxLoginsPerRequest = 100
	requestTimeout      = 10 * time.Second
)

// Options configures a Client. HTTPClient is optional and used by tests to route
// Helix and OAuth requests at a stub server.
type Options struct {
	ClientID     string
	ClientSecret string
	HTTPClient   *http.Client
}

// Client reports live Twitch logins. It wraps a helix.Client, which attaches the
// app access token to requests and transparently refreshes it on a 401 once an
// initial token is set — so this wrapper only fetches the first token and batches
// the logins.
type Client struct {
	helix *helix.Client

	mu        sync.Mutex
	haveToken bool
}

// New builds a Client.
func New(opts Options) (*Client, error) {
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: requestTimeout}
	}
	h, err := helix.NewClient(&helix.Options{
		ClientID:     opts.ClientID,
		ClientSecret: opts.ClientSecret,
		HTTPClient:   httpClient,
	})
	if err != nil {
		return nil, err
	}
	return &Client{helix: h}, nil
}

// FetchLiveUsernames returns the subset of usernames currently live on Twitch.
// It batches into Helix requests of up to 100 logins and performs no HTTP at all
// on empty input. The ctx is part of the interface for parity with the other
// source; helix binds its context at construction, so cancellation is bounded by
// the HTTP client timeout rather than this ctx.
func (c *Client) FetchLiveUsernames(_ context.Context, usernames []string) (map[string]struct{}, error) {
	live := make(map[string]struct{})
	if len(usernames) == 0 {
		return live, nil
	}

	if err := c.ensureToken(); err != nil {
		return nil, err
	}

	for _, logins := range chunk(usernames, maxLoginsPerRequest) {
		// First defaults to 20 server-side, so it is set to the batch size to
		// return every live channel in the batch on one page. (The original
		// TypeScript omitted it and silently reported only the first 20.)
		resp, err := c.helix.GetStreams(&helix.StreamsParams{
			UserLogins: logins,
			First:      maxLoginsPerRequest,
		})
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("twitch streams request failed: %d %s", resp.StatusCode, resp.ErrorMessage)
		}
		for _, stream := range resp.Data.Streams {
			live[strings.ToLower(stream.UserLogin)] = struct{}{}
		}
	}

	logger.Log.Debug("Twitch live channels", "count", len(live))
	return live, nil
}

// ensureToken fetches the initial app access token once (retrying on a later
// poll if it fails). Setting the token also enables helix's built-in refresh on
// a subsequent 401.
func (c *Client) ensureToken() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.haveToken {
		return nil
	}

	resp, err := c.helix.RequestAppAccessToken(nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("twitch token request failed: %d %s", resp.StatusCode, resp.ErrorMessage)
	}

	c.helix.SetAppAccessToken(resp.Data.AccessToken)
	c.haveToken = true
	logger.Log.Debug("Twitch obtained an app access token")
	return nil
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
