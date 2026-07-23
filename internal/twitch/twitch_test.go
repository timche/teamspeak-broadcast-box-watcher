package twitch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/twitch"
)

func TestMain(m *testing.M) {
	logger.Discard()
	os.Exit(m.Run())
}

const (
	clientID     = "client-id"
	clientSecret = "client-secret"
)

// rewriteTransport redirects every request to the test server, preserving the
// path. helix hard-codes the OAuth host (id.twitch.tv) and the API host
// (api.twitch.tv), so routing by host isn't possible; this sends both to the
// stub, which dispatches on the path suffix (/token vs /streams).
type rewriteTransport struct{ host string }

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = t.host
	return http.DefaultTransport.RoundTrip(req)
}

// stub is a fake Twitch serving the token and streams endpoints.
type stub struct {
	server            *httptest.Server
	mu                sync.Mutex
	tokenCalls        int
	streamCalls       int
	streamLoginCounts []int
	totalRequests     int
}

func newStub(t *testing.T, liveLogins []string, unauthorizedUntilTokenNo int) (*twitch.Client, *stub) {
	t.Helper()
	live := make(map[string]struct{})
	for _, l := range liveLogins {
		live[strings.ToLower(l)] = struct{}{}
	}
	s := &stub{}

	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.totalRequests++
		s.mu.Unlock()

		switch {
		case strings.HasSuffix(r.URL.Path, "/token"):
			s.mu.Lock()
			s.tokenCalls++
			no := s.tokenCalls
			s.mu.Unlock()
			writeJSON(w, map[string]any{"access_token": fmt.Sprintf("token-%d", no), "expires_in": 3600, "token_type": "bearer"})

		case strings.HasSuffix(r.URL.Path, "/streams"):
			s.mu.Lock()
			s.streamCalls++
			s.mu.Unlock()
			auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			tokenNo, _ := strconv.Atoi(strings.TrimPrefix(auth, "token-"))
			if unauthorizedUntilTokenNo != 0 && tokenNo < unauthorizedUntilTokenNo {
				http.Error(w, `{"error":"Unauthorized","status":401,"message":"invalid token"}`, http.StatusUnauthorized)
				return
			}
			logins := r.URL.Query()["user_login"]
			s.mu.Lock()
			s.streamLoginCounts = append(s.streamLoginCounts, len(logins))
			s.mu.Unlock()
			var data []map[string]any
			for _, login := range logins {
				if _, ok := live[strings.ToLower(login)]; ok {
					data = append(data, map[string]any{"user_login": strings.ToLower(login)})
				}
			}
			writeJSON(w, map[string]any{"data": data, "pagination": map[string]any{}})

		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(s.server.Close)

	host := strings.TrimPrefix(s.server.URL, "http://")
	client, err := twitch.New(twitch.Options{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HTTPClient:   &http.Client{Transport: rewriteTransport{host: host}},
	})
	if err != nil {
		t.Fatalf("twitch.New: %v", err)
	}
	return client, s
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *stub) counts() (tokens, streams, total int, loginCounts []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenCalls, s.streamCalls, s.totalRequests, append([]int(nil), s.streamLoginCounts...)
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func TestFetchFiltersToLiveChannels(t *testing.T) {
	client, s := newStub(t, []string{"azn"}, 0)

	result, err := client.FetchLiveUsernames(context.Background(), []string{"azn", "offline"})
	if err != nil {
		t.Fatalf("FetchLiveUsernames error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
	if tokens, _, _, _ := s.counts(); tokens != 1 {
		t.Errorf("tokenCalls = %d, want 1", tokens)
	}
}

func TestEmptyInputPerformsNoHTTP(t *testing.T) {
	client, s := newStub(t, nil, 0)
	result, err := client.FetchLiveUsernames(context.Background(), nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("result = %v, want empty", result)
	}
	if _, _, total, _ := s.counts(); total != 0 {
		t.Errorf("totalRequests = %d, want 0", total)
	}
}

func TestFetchesTokenLazily(t *testing.T) {
	client, s := newStub(t, []string{"azn"}, 0)
	if tokens, _, _, _ := s.counts(); tokens != 0 {
		t.Fatalf("tokenCalls = %d before use, want 0", tokens)
	}
	if _, err := client.FetchLiveUsernames(context.Background(), []string{"azn"}); err != nil {
		t.Fatalf("error: %v", err)
	}
	if tokens, _, _, _ := s.counts(); tokens != 1 {
		t.Errorf("tokenCalls = %d, want 1", tokens)
	}
}

func TestRefreshesTokenOn401(t *testing.T) {
	// token-1 is rejected; helix auto-refreshes to token-2, which works.
	client, s := newStub(t, []string{"azn"}, 2)
	result, err := client.FetchLiveUsernames(context.Background(), []string{"azn"})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
	if tokens, _, _, _ := s.counts(); tokens != 2 {
		t.Errorf("tokenCalls = %d, want 2", tokens)
	}
}

func TestBatchesAt100Logins(t *testing.T) {
	logins := make([]string, 150)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	client, s := newStub(t, []string{"user0", "user149"}, 0)
	result, err := client.FetchLiveUsernames(context.Background(), logins)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 2 || got[0] != "user0" || got[1] != "user149" {
		t.Errorf("result = %v, want [user0 user149]", got)
	}
	_, streams, _, counts := s.counts()
	if streams != 2 {
		t.Errorf("streamCalls = %d, want 2", streams)
	}
	if len(counts) != 2 || counts[0] != 100 || counts[1] != 50 {
		t.Errorf("streamLoginCounts = %v, want [100 50]", counts)
	}
}

func TestStreamsServerErrorSurfaced(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			writeJSON(w, map[string]any{"access_token": "token-1", "expires_in": 3600})
			return
		}
		http.Error(w, `{"error":"Internal Server Error","status":500,"message":"boom"}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	client, err := twitch.New(twitch.Options{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HTTPClient:   &http.Client{Transport: rewriteTransport{host: host}},
	})
	if err != nil {
		t.Fatalf("twitch.New: %v", err)
	}

	if _, err := client.FetchLiveUsernames(context.Background(), []string{"azn"}); err == nil {
		t.Fatal("expected an error on a 500 streams response")
	} else if !strings.Contains(err.Error(), "500") {
		t.Errorf("error = %q, want it to mention 500", err.Error())
	}
}

func TestNormalizesReturnedLoginsToLowercase(t *testing.T) {
	client, _ := newStub(t, []string{"azn"}, 0)
	// Server lowercases in its response already; assert the wrapper keeps them lowercase.
	result, err := client.FetchLiveUsernames(context.Background(), []string{"AZN"})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
}
