package teamspeak

import (
	"errors"
	"testing"
	"time"

	ts3 "github.com/multiplay/go-ts3"
)

// TestFloodWait covers the parsing that mirrors the pre-migration
// ts3-nodejs-library: the server's "please wait N second(s)" is honoured, and
// anything unparseable falls back to 0 so the caller uses its default cooldown.
func TestFloodWait(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want time.Duration
	}{
		{
			"parses server-requested wait",
			&ts3.Error{ID: floodErrorID, Msg: "client is flooding", Details: map[string]interface{}{"extra_msg": "please wait 1 seconds"}},
			1*time.Second + 100*time.Millisecond,
		},
		{
			"multi-second wait",
			&ts3.Error{ID: floodErrorID, Msg: "client is flooding", Details: map[string]interface{}{"extra_msg": "please wait 3 second"}},
			3*time.Second + 100*time.Millisecond,
		},
		{
			"flood error without extra_msg falls back",
			&ts3.Error{ID: floodErrorID, Msg: "client is flooding"},
			0,
		},
		{
			"non-flood ts3 error falls back",
			&ts3.Error{ID: 1281, Msg: "empty result set"},
			0,
		},
		{
			"non-ts3 error falls back",
			errors.New("boom"),
			0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := floodWait(tc.err); got != tc.want {
				t.Errorf("floodWait = %v, want %v", got, tc.want)
			}
		})
	}
}
