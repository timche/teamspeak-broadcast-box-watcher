// Package broadcastbox reads the live stream keys from a Broadcast Box server's
// admin status endpoint.
package broadcastbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/timche/teamspeak-stream-live/internal/httpx"
	"github.com/timche/teamspeak-stream-live/internal/logger"
)

const requestTimeout = 10 * time.Second

// streamSession is a lenient view of a Broadcast Box `StreamSessionState`. Only
// the fields we rely on are described; unknown fields are ignored by the decoder.
type streamSession struct {
	StreamKey   string `json:"streamKey"`
	VideoTracks []any  `json:"videoTracks"`
	AudioTracks []any  `json:"audioTracks"`
}

// isLive reports whether a session exposes a stream key and is actively
// publishing (signalled by any received media track). `streamStart` is set as
// soon as a key is provisioned, so it cannot be used to decide liveness.
func isLive(s streamSession) bool {
	return strings.TrimSpace(s.StreamKey) != "" &&
		(len(s.VideoTracks) > 0 || len(s.AudioTracks) > 0)
}

// Options configures a Client.
type Options struct {
	APIURL string
	// Authorization is the ready-to-send header value, "Bearer <base64(token)>".
	Authorization string
}

// Client fetches live stream keys from Broadcast Box.
type Client struct {
	http *resty.Client
}

// New builds a Client for the given Broadcast Box endpoint.
func New(opts Options) *Client {
	c := resty.New().
		SetBaseURL(opts.APIURL).
		SetHeader("Authorization", opts.Authorization).
		SetHeader("Accept", "application/json").
		SetTimeout(requestTimeout).
		SetRetryCount(2)
	httpx.RetryOnStatuses(c)
	return &Client{http: c}
}

// FetchLiveStreamKeys fetches the currently live stream keys from
// `/api/admin/status`. The admin endpoint is used because Broadcast Box runs
// with DISABLE_STATUS=true. A `null` body (no streams) is treated as empty.
func (c *Client) FetchLiveStreamKeys(ctx context.Context) (map[string]struct{}, error) {
	resp, err := c.http.R().SetContext(ctx).Get("api/admin/status")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("broadcast box status request failed: %d", resp.StatusCode())
	}

	var sessions []streamSession
	if len(resp.Body()) > 0 {
		if err := json.Unmarshal(resp.Body(), &sessions); err != nil {
			return nil, err
		}
	}

	live := make(map[string]struct{})
	for _, session := range sessions {
		if isLive(session) {
			live[session.StreamKey] = struct{}{}
		}
	}
	logger.Log.Debug("Broadcast Box live streams", "count", len(live))
	return live, nil
}
