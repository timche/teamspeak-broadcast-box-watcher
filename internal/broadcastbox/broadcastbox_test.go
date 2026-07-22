package broadcastbox_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/timche/teamspeak-stream-live/internal/broadcastbox"
	"github.com/timche/teamspeak-stream-live/internal/logger"
)

func TestMain(m *testing.M) {
	logger.Discard()
	os.Exit(m.Run())
}

func clientFor(url string) *broadcastbox.Client {
	return broadcastbox.New(broadcastbox.Options{
		APIURL:        url,
		Authorization: "Bearer " + base64.StdEncoding.EncodeToString([]byte("s3cr3t")),
	})
}

func TestFetchLiveStreamKeysSendsBearerAndFilters(t *testing.T) {
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"streamKey":"azn","streamStart":"2026-07-22T13:29:24Z","audioTracks":[{"rid":"Audio"}],"videoTracks":[{"rid":"WebLow"}],"sessions":[]},
			{"streamKey":"offline","streamStart":"2026-07-22T13:28:42Z","audioTracks":[],"videoTracks":[],"sessions":[{"id":"c615e1c9"}]},
			{"streamKey":"audioonly","audioTracks":[{"rid":"a"}]},
			{"streamKey":"","audioTracks":[{"rid":"a"}]}
		]`))
	}))
	defer server.Close()

	live, err := clientFor(server.URL).FetchLiveStreamKeys(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveStreamKeys error: %v", err)
	}

	wantAuth := "Bearer " + base64.StdEncoding.EncodeToString([]byte("s3cr3t"))
	if seenAuth != wantAuth {
		t.Errorf("Authorization = %q, want %q", seenAuth, wantAuth)
	}
	want := map[string]struct{}{"azn": {}, "audioonly": {}}
	if !equalSets(live, want) {
		t.Errorf("live = %v, want %v", live, want)
	}
}

func TestNullStatusBodyIsEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`null`))
	}))
	defer server.Close()

	live, err := clientFor(server.URL).FetchLiveStreamKeys(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveStreamKeys error: %v", err)
	}
	if len(live) != 0 {
		t.Errorf("live = %v, want empty", live)
	}
}

func TestNon2xxReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := clientFor(server.URL).FetchLiveStreamKeys(context.Background())
	if err == nil {
		t.Fatal("expected an error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error = %q, want it to mention 401", err.Error())
	}
}

func equalSets(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
