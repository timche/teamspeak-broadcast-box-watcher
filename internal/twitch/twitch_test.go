package twitch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/logger"
)

func init() { logger.Discard() }

const (
	clientID     = "client-id"
	clientSecret = "client-secret"
)

// harness spins up a fake Twitch serving the token endpoint at /oauth2/token and
// the streams endpoint at /streams.
type harness struct {
	server            *httptest.Server
	client            *Client
	mu                sync.Mutex
	tokenCalls        int
	streamCalls       int
	streamLoginCounts []int
	totalRequests     int
}

func newHarness(t *testing.T, liveLogins []string, unauthorizedUntilTokenNo int) *harness {
	t.Helper()
	live := make(map[string]struct{})
	for _, l := range liveLogins {
		live[strings.ToLower(l)] = struct{}{}
	}
	h := &harness{}

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.totalRequests++
		h.mu.Unlock()

		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/token"):
			h.mu.Lock()
			h.tokenCalls++
			no := h.tokenCalls
			h.mu.Unlock()
			_ = r.ParseForm()
			if r.PostForm.Get("grant_type") != "client_credentials" {
				t.Errorf("token body grant_type = %q", r.PostForm.Get("grant_type"))
			}
			writeJSON(w, map[string]any{"access_token": fmt.Sprintf("token-%d", no), "expires_in": 3600})

		case strings.HasSuffix(r.URL.Path, "/streams"):
			h.mu.Lock()
			h.streamCalls++
			h.mu.Unlock()
			auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			tokenNo, _ := strconv.Atoi(strings.TrimPrefix(auth, "token-"))
			if unauthorizedUntilTokenNo != 0 && tokenNo < unauthorizedUntilTokenNo {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			logins := r.URL.Query()["user_login"]
			h.mu.Lock()
			h.streamLoginCounts = append(h.streamLoginCounts, len(logins))
			h.mu.Unlock()
			var data []map[string]string
			for _, login := range logins {
				if _, ok := live[strings.ToLower(login)]; ok {
					data = append(data, map[string]string{"user_login": strings.ToLower(login)})
				}
			}
			writeJSON(w, map[string]any{"data": data})

		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(h.server.Close)

	h.client = New(Options{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HelixURL:     h.server.URL,
		TokenURL:     h.server.URL + "/oauth2/token",
	})
	return h
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// small n; simple insertion sort keeps deps out
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func TestFetchesTokenSendsHeadersAndFilters(t *testing.T) {
	var seenClientID, seenAuth string
	live := map[string]struct{}{"azn": {}}
	tokenCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/oauth2/token") {
			tokenCalls++
			writeJSON(w, map[string]any{"access_token": "abc"})
			return
		}
		seenClientID = r.Header.Get("Client-Id")
		seenAuth = r.Header.Get("Authorization")
		var data []map[string]string
		for _, login := range r.URL.Query()["user_login"] {
			if _, ok := live[login]; ok {
				data = append(data, map[string]string{"user_login": login})
			}
		}
		writeJSON(w, map[string]any{"data": data})
	}))
	defer server.Close()

	client := New(Options{ClientID: clientID, ClientSecret: clientSecret, HelixURL: server.URL, TokenURL: server.URL + "/oauth2/token"})
	result, err := client.FetchLiveUsernames(context.Background(), []string{"azn", "offline"})
	if err != nil {
		t.Fatalf("FetchLiveUsernames error: %v", err)
	}

	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
	if seenClientID != clientID {
		t.Errorf("Client-Id = %q, want %q", seenClientID, clientID)
	}
	if seenAuth != "Bearer abc" {
		t.Errorf("Authorization = %q, want Bearer abc", seenAuth)
	}
	if tokenCalls != 1 {
		t.Errorf("tokenCalls = %d, want 1", tokenCalls)
	}
}

func TestEmptyInputPerformsNoHTTP(t *testing.T) {
	h := newHarness(t, nil, 0)
	result, err := h.client.FetchLiveUsernames(context.Background(), nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("result = %v, want empty", result)
	}
	if h.totalRequests != 0 {
		t.Errorf("totalRequests = %d, want 0", h.totalRequests)
	}
}

func TestFetchesTokenLazily(t *testing.T) {
	h := newHarness(t, []string{"azn"}, 0)
	if h.tokenCalls != 0 {
		t.Fatalf("tokenCalls = %d before use, want 0", h.tokenCalls)
	}
	if _, err := h.client.FetchLiveUsernames(context.Background(), []string{"azn"}); err != nil {
		t.Fatalf("error: %v", err)
	}
	if h.tokenCalls != 1 {
		t.Errorf("tokenCalls = %d, want 1", h.tokenCalls)
	}
}

func TestRefreshesTokenOnceOn401(t *testing.T) {
	h := newHarness(t, []string{"azn"}, 2) // token-1 rejected, token-2 works
	result, err := h.client.FetchLiveUsernames(context.Background(), []string{"azn"})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
	if h.tokenCalls != 2 {
		t.Errorf("tokenCalls = %d, want 2", h.tokenCalls)
	}
}

func TestBatchesAt100Logins(t *testing.T) {
	logins := make([]string, 150)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	h := newHarness(t, []string{"user0", "user149"}, 0)
	result, err := h.client.FetchLiveUsernames(context.Background(), logins)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 2 || got[0] != "user0" || got[1] != "user149" {
		t.Errorf("result = %v, want [user0 user149]", got)
	}
	if h.streamCalls != 2 {
		t.Errorf("streamCalls = %d, want 2", h.streamCalls)
	}
	if len(h.streamLoginCounts) != 2 || h.streamLoginCounts[0] != 100 || h.streamLoginCounts[1] != 50 {
		t.Errorf("streamLoginCounts = %v, want [100 50]", h.streamLoginCounts)
	}
}

func TestNormalizesReturnedLoginsToLowercase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/oauth2/token") {
			writeJSON(w, map[string]any{"access_token": "abc"})
			return
		}
		writeJSON(w, map[string]any{"data": []map[string]string{{"user_login": "AZN"}}})
	}))
	defer server.Close()

	client := New(Options{ClientID: clientID, ClientSecret: clientSecret, HelixURL: server.URL, TokenURL: server.URL + "/oauth2/token"})
	result, err := client.FetchLiveUsernames(context.Background(), []string{"AZN"})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got := sortedKeys(result); len(got) != 1 || got[0] != "azn" {
		t.Errorf("result = %v, want [azn]", got)
	}
}
