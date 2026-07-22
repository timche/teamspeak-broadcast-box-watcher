package teamspeak_test

import (
	"bufio"
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timche/teamspeak-stream-live/internal/logger"
	"github.com/timche/teamspeak-stream-live/internal/teamspeak"
)

func TestMain(m *testing.M) {
	logger.Discard()
	os.Exit(m.Run())
}

// fakeServer is a minimal TeamSpeak 3 ServerQuery server, modelled on go-ts3's
// own mockserver_test.go: it writes the "TS3" header + banner on connect, reads
// commands terminated by "\n\r", and replies with an optional data line followed
// by the "error id=0 msg=ok" trailer (framing terminator "\n\r"). Responses are
// keyed by the command's first token; a value beginning with "error " is sent as
// the trailer instead of an ok.
type fakeServer struct {
	ln net.Listener

	mu        sync.Mutex
	responses map[string]string
	conns     map[net.Conn]struct{}
	closed    bool
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{
		ln:        ln,
		responses: defaultResponses(),
		conns:     make(map[net.Conn]struct{}),
	}
	go s.serve()
	t.Cleanup(s.Close)
	return s
}

func defaultResponses() map[string]string {
	return map[string]string{
		"login": "", "logout": "", "use": "", "clientupdate": "", "quit": "", "disconnect": "",
		"servergroupaddperm": "", "servergroupaddclient": "", "servergroupdelclient": "",
		"servergroupdel": "", "sendtextmessage": "", "clientmove": "",
		"servergrouplist":       "sgid=1 name=Guest type=2",
		"servergroupadd":        "sgid=99",
		"servergroupclientlist": "cldbid=7|cldbid=8",
		"clientlist":            "clid=1 cid=39 client_database_id=19 client_nickname=alice client_type=0",
		"whoami":                "client_id=94 client_channel_id=432",
	}
}

func (s *fakeServer) set(cmd, resp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses[cmd] = resp
}

func (s *fakeServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *fakeServer) handle(conn net.Conn) {
	write := func(msg string) bool {
		_, err := conn.Write([]byte(msg + "\n\r"))
		return err == nil
	}
	if !write("TS3") || !write(`Welcome to the TeamSpeak 3 ServerQuery interface.`) {
		return
	}

	sc := bufio.NewScanner(conn)
	sc.Split(bufio.ScanLines)
	for sc.Scan() {
		cmd := strings.TrimSpace(strings.SplitN(sc.Text(), " ", 2)[0])
		if cmd == "" {
			continue
		}
		s.mu.Lock()
		resp, ok := s.responses[cmd]
		s.mu.Unlock()

		switch {
		case !ok:
			write(`error id=256 msg=command\snot\sfound`)
		case strings.HasPrefix(resp, "error "):
			write(resp)
		default:
			if resp != "" && !write(resp) {
				return
			}
			write("error id=0 msg=ok")
		}
		if cmd == "quit" || cmd == "disconnect" {
			return
		}
	}
}

// dropConnections closes every active client connection, simulating a server-side
// drop so the next command triggers the manager's reconnect.
func (s *fakeServer) dropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
		delete(s.conns, c)
	}
}

func (s *fakeServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	_ = s.ln.Close()
}

func (s *fakeServer) options() teamspeak.ConnectOptions {
	host, portStr, _ := net.SplitHostPort(s.ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return teamspeak.ConnectOptions{
		Host:              host,
		QueryPort:         port,
		ServerPort:        9987,
		Username:          "serveradmin",
		Password:          "pw",
		Nickname:          "tsl",
		ReconnectInterval: 10 * time.Millisecond,
	}
}

func connect(t *testing.T, s *fakeServer) *teamspeak.Manager {
	t.Helper()
	m, err := teamspeak.Connect(s.options())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return m
}

func TestEnsureLiveGroupCreatesWhenMissing(t *testing.T) {
	s := newFakeServer(t)
	m := connect(t, s)

	sgid, err := m.EnsureLiveGroup(context.Background(), "live")
	if err != nil {
		t.Fatalf("EnsureLiveGroup: %v", err)
	}
	if sgid != "99" {
		t.Errorf("sgid = %q, want 99 (created)", sgid)
	}
}

func TestEnsureLiveGroupFindsExisting(t *testing.T) {
	s := newFakeServer(t)
	s.set("servergrouplist", "sgid=1 name=Guest type=2|sgid=5 name=live type=1")
	m := connect(t, s)

	sgid, err := m.EnsureLiveGroup(context.Background(), "live")
	if err != nil {
		t.Fatalf("EnsureLiveGroup: %v", err)
	}
	if sgid != "5" {
		t.Errorf("sgid = %q, want 5 (existing)", sgid)
	}
}

func TestListClientsFiltersQueryClients(t *testing.T) {
	s := newFakeServer(t)
	s.set("clientlist", "clid=1 cid=39 client_database_id=19 client_nickname=alice client_type=0"+
		"|clid=2 cid=40 client_database_id=20 client_nickname=admin client_type=1")
	m := connect(t, s)

	clients, err := m.ListClients(context.Background())
	if err != nil {
		t.Fatalf("ListClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("got %d clients, want 1 (query client filtered)", len(clients))
	}
	got := clients[0]
	if got.Nickname != "alice" || got.DatabaseID != "19" || got.ChannelID != "39" {
		t.Errorf("client = %+v, want {alice 19 39}", got)
	}
}

func TestListGroupMemberDbids(t *testing.T) {
	s := newFakeServer(t)
	m := connect(t, s)

	members, err := m.ListGroupMemberDbids(context.Background(), "100")
	if err != nil {
		t.Fatalf("ListGroupMemberDbids: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("members = %v, want 2 entries", members)
	}
	for _, want := range []string{"7", "8"} {
		if _, ok := members[want]; !ok {
			t.Errorf("missing member %q in %v", want, members)
		}
	}
}

func TestListGroupMemberDbidsEmptyResult(t *testing.T) {
	s := newFakeServer(t)
	// TeamSpeak returns error id 1281 for an empty group; it must normalise to {}.
	s.set("servergroupclientlist", `error id=1281 msg=empty\sresult\sset`)
	m := connect(t, s)

	members, err := m.ListGroupMemberDbids(context.Background(), "100")
	if err != nil {
		t.Fatalf("ListGroupMemberDbids: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("members = %v, want empty", members)
	}
}

func TestReconnectAfterDisconnect(t *testing.T) {
	s := newFakeServer(t)
	m := connect(t, s)

	// First call succeeds on the initial connection.
	if _, err := m.ListClients(context.Background()); err != nil {
		t.Fatalf("first ListClients: %v", err)
	}

	// Server drops the connection; the next call must transparently reconnect
	// (re-login, re-select the server, re-set the nickname) and succeed.
	s.dropConnections()

	clients, err := m.ListClients(context.Background())
	if err != nil {
		t.Fatalf("ListClients after disconnect: %v", err)
	}
	if len(clients) != 1 {
		t.Errorf("got %d clients after reconnect, want 1", len(clients))
	}
}
