// Package httpx holds small helpers shared by the resty-based HTTP clients.
package httpx

import "github.com/go-resty/resty/v2"

// kyRetryStatuses mirrors ky's default retry status set (notably excluding 401,
// which the Twitch client handles itself via a token refresh).
var kyRetryStatuses = map[int]bool{
	408: true, 413: true, 429: true, 500: true, 502: true, 503: true, 504: true,
}

// RetryOnStatuses adds a retry condition matching ky's default retryable
// statuses. Network/transport errors are already retried by resty's retry count.
func RetryOnStatuses(c *resty.Client) {
	c.AddRetryCondition(func(r *resty.Response, _ error) bool {
		return r != nil && kyRetryStatuses[r.StatusCode()]
	})
}
