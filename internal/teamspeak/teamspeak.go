// Package teamspeak wraps a TeamSpeak 3 ServerQuery connection (via go-ts3),
// exposing only the operations the watchers need plus transparent reconnection.
package teamspeak

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	ts3 "github.com/multiplay/go-ts3"
	"golang.org/x/time/rate"

	"github.com/timche/teamspeak-stream-live/internal/logger"
)

const (
	// serverGroupTypeRegular is a regular (non-template) server group.
	serverGroupTypeRegular = 1
	// showNameInTreeBefore renders the group name before the nickname.
	showNameInTreeBefore = 1
	// permNotNegated / permNoSkip are the required permnegated/permskip flags
	// for servergroupaddperm; both off for a plain granted permission.
	permNotNegated = 0
	permNoSkip     = 0
	// textMessageTargetModeChannel targets the query client's current channel.
	textMessageTargetModeChannel = 2
	// emptyResultErrorID is returned when a query yields an empty result set.
	emptyResultErrorID = 1281
	// emptyResponseReason is go-ts3's InvalidResponseError reason when a command
	// issued WithResponse comes back as a bare "error id=0 msg=ok" with no data
	// line — how some TeamSpeak servers report an empty result instead of id 1281.
	emptyResponseReason = "no lines"
	// floodErrorID is the ServerQuery anti-flood error ("client is flooding").
	floodErrorID = 524
	// commandTimeout bounds a single ServerQuery command.
	commandTimeout = 10 * time.Second
	// defaultReconnectInterval matches the original library's reconnect(-1, 2000).
	defaultReconnectInterval = 2 * time.Second
	// floodCooldown is how long to wait before retrying a command the server
	// rejected for flooding; it comfortably exceeds the default 3s flood window.
	floodCooldown = 3 * time.Second
	// floodMaxRetries bounds the anti-flood retries so a hard server-side limit
	// degrades to a logged failure instead of blocking the poll loop forever.
	floodMaxRetries = 3
)

// ClientInfo is a connected regular client.
type ClientInfo struct {
	Nickname   string
	DatabaseID string
	ChannelID  string
}

// ServerGroupRef is a server group resolved to its id and name.
type ServerGroupRef struct {
	SGID string
	Name string
}

// TwitchGroupRef is a `twitch.tv/<username>` group with its resolved username
// and current member database ids.
type TwitchGroupRef struct {
	SGID     string
	Name     string
	Username string
	Members  map[string]struct{}
}

// ConnectOptions holds the ServerQuery connection parameters.
type ConnectOptions struct {
	Host       string
	QueryPort  int
	ServerPort int
	Username   string
	Password   string
	Nickname   string
	// ReconnectInterval is the backoff between reconnect attempts; defaults to 2s.
	ReconnectInterval time.Duration
	// QueryRate caps ServerQuery commands per second to stay under TeamSpeak's
	// anti-flood limit; <= 0 disables client-side throttling.
	QueryRate float64
	// QueryBurst is the command burst allowance; clamped to at least 1 when
	// throttling is enabled.
	QueryBurst int
	// FloodCooldown is the wait before retrying a command rejected for flooding;
	// defaults to floodCooldown. Exposed for tests, like ReconnectInterval.
	FloodCooldown time.Duration
}

// Manager is a thin, reconnecting wrapper around a go-ts3 client. Its methods are
// safe for the serial poll loop; a single mutex serialises access and guards the
// client pointer across reconnects. Context is passed per call (not stored) so a
// reconnect's backoff is cancellable by the caller.
type Manager struct {
	opts              ConnectOptions
	reconnectInterval time.Duration
	floodCooldown     time.Duration
	// limiter paces outgoing ServerQuery commands to stay under TeamSpeak's
	// anti-flood limit; nil when throttling is disabled.
	limiter *rate.Limiter
	mu      sync.Mutex
	client  *ts3.Client
}

// Connect dials the ServerQuery interface, logs in, selects the virtual server,
// and sets the nickname.
func Connect(opts ConnectOptions) (*Manager, error) {
	interval := opts.ReconnectInterval
	if interval <= 0 {
		interval = defaultReconnectInterval
	}
	cooldown := opts.FloodCooldown
	if cooldown <= 0 {
		cooldown = floodCooldown
	}
	m := &Manager{
		opts:              opts,
		reconnectInterval: interval,
		floodCooldown:     cooldown,
		limiter:           newLimiter(opts),
	}
	client, err := m.dial()
	if err != nil {
		return nil, err
	}
	m.client = client
	logger.Log.Info("Connected to TeamSpeak ServerQuery",
		"host", opts.Host, "queryPort", opts.QueryPort)
	return m, nil
}

// newLimiter builds the command rate limiter, or nil when throttling is disabled
// (QueryRate <= 0). The burst is clamped to at least 1 so an enabled limiter can
// always make progress.
func newLimiter(opts ConnectOptions) *rate.Limiter {
	if opts.QueryRate <= 0 {
		return nil
	}
	burst := opts.QueryBurst
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(opts.QueryRate), burst)
}

// throttle blocks until the rate limiter allows another command, or ctx is done.
// It is a no-op when throttling is disabled.
func (m *Manager) throttle(ctx context.Context) error {
	if m.limiter == nil {
		return nil
	}
	return m.limiter.Wait(ctx)
}

func (m *Manager) dial() (*ts3.Client, error) {
	addr := net.JoinHostPort(m.opts.Host, strconv.Itoa(m.opts.QueryPort))
	client, err := ts3.NewClient(addr, ts3.Timeout(commandTimeout))
	if err != nil {
		return nil, err
	}
	if err := client.Login(m.opts.Username, m.opts.Password); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := client.UsePort(m.opts.ServerPort); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := client.SetNick(m.opts.Nickname); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// exec runs fn against the live client. A protocol-level error (a *ts3.Error,
// e.g. a rejected command) is returned as-is, except the anti-flood error which
// is retried after a cooldown. A connection-level failure triggers a reconnect
// (infinite backoff, cancellable via ctx) and one retry — mirroring the original
// transparent reconnection.
func (m *Manager) exec(ctx context.Context, fn func(*ts3.Client) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.attempt(ctx, fn)
	if err == nil || isProtocolError(err) {
		return err
	}

	logger.Log.Warn("TeamSpeak connection error, reconnecting", "error", err)
	if rerr := m.reconnect(ctx); rerr != nil {
		return rerr
	}
	return m.attempt(ctx, fn)
}

// attempt runs fn, retrying after a cooldown when the server rejects a command
// for flooding (error 524). Commands are already paced by the rate limiter, so
// this only backstops a server whose anti-flood limit is stricter than the
// configured rate; after floodMaxRetries it returns the flood error so the poll
// loop logs it and moves on rather than blocking indefinitely.
func (m *Manager) attempt(ctx context.Context, fn func(*ts3.Client) error) error {
	for retries := 0; ; retries++ {
		err := fn(m.client)
		if !isFloodError(err) || retries >= floodMaxRetries {
			return err
		}
		// Honour the server's requested wait when it gives one, else fall back to
		// the default cooldown.
		cooldown := floodWait(err)
		if cooldown <= 0 {
			cooldown = m.floodCooldown
		}
		logger.Log.Warn("TeamSpeak anti-flood throttle, backing off",
			"error", err, "cooldown", cooldown)
		select {
		case <-time.After(cooldown):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Manager) reconnect(ctx context.Context) error {
	if m.client != nil {
		_ = m.client.Close()
	}
	bo := backoff.WithContext(backoff.NewConstantBackOff(m.reconnectInterval), ctx)
	client, err := backoff.RetryWithData(func() (*ts3.Client, error) {
		return m.dial()
	}, bo)
	if err != nil {
		return err
	}
	m.client = client
	logger.Log.Info("Reconnected to TeamSpeak ServerQuery")
	return nil
}

func isProtocolError(err error) bool {
	var tsErr *ts3.Error
	return errors.As(err, &tsErr)
}

// isFloodError reports whether err is the ServerQuery anti-flood rejection.
func isFloodError(err error) bool {
	var tsErr *ts3.Error
	return errors.As(err, &tsErr) && tsErr.ID == floodErrorID
}

// floodWaitRe extracts the seconds from a flood error's "please wait N second(s)"
// extra message.
var floodWaitRe = regexp.MustCompile(`(\d+)\s*second`)

// floodWait returns the cooldown the server asked for in a flood error's
// extra_msg (e.g. "please wait 1 seconds"), plus a small buffer — mirroring the
// pre-migration ts3-nodejs-library, which parsed the same message and retried
// after waitSeconds*1000+100ms. Returns 0 when no wait can be parsed, so the
// caller falls back to its default cooldown.
func floodWait(err error) time.Duration {
	var tsErr *ts3.Error
	if !errors.As(err, &tsErr) {
		return 0
	}
	msg, _ := tsErr.Details["extra_msg"].(string)
	m := floodWaitRe.FindStringSubmatch(msg)
	if m == nil {
		return 0
	}
	secs, convErr := strconv.Atoi(m[1])
	if convErr != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs)*time.Second + 100*time.Millisecond
}

// isEmptyResult reports whether err means "the result set was empty" rather than
// a real failure. TeamSpeak servers signal this two ways: some return the
// protocol error id 1281 ("empty result set"); others return a plain
// "error id=0 msg=ok" with no data line, which go-ts3 surfaces as an
// InvalidResponseError with reason "no lines" when a response struct was expected.
func isEmptyResult(err error) bool {
	var tsErr *ts3.Error
	if errors.As(err, &tsErr) {
		return tsErr.ID == emptyResultErrorID
	}
	var invErr *ts3.InvalidResponseError
	return errors.As(err, &invErr) && invErr.Reason == emptyResponseReason
}

// EnsureLiveGroup finds or creates the shared "live" group and makes sure its
// name is shown before the nickname. Returns its server group id.
func (m *Manager) EnsureLiveGroup(ctx context.Context, name string) (string, error) {
	var sgid string
	err := m.exec(ctx, func(c *ts3.Client) error {
		if err := m.throttle(ctx); err != nil {
			return err
		}
		groups, err := c.Server.GroupList()
		if err != nil {
			return err
		}
		sgid = ""
		for _, g := range groups {
			if g.Name == name {
				sgid = strconv.Itoa(g.ID)
				break
			}
		}
		if sgid == "" {
			created, err := m.createServerGroup(ctx, c, name)
			if err != nil {
				return err
			}
			sgid = created
			logger.Log.Info("Created shared live group", "name", name, "sgid", sgid)
		}
		return m.addServerGroupPerm(ctx, c, sgid, "i_group_show_name_in_tree", showNameInTreeBefore)
	})
	return sgid, err
}

// ListClients returns the connected regular (non-query) clients.
func (m *Manager) ListClients(ctx context.Context) ([]ClientInfo, error) {
	var result []ClientInfo
	err := m.exec(ctx, func(c *ts3.Client) error {
		result = nil
		if err := m.throttle(ctx); err != nil {
			return err
		}
		clients, err := c.Server.ClientList()
		if err != nil {
			return err
		}
		for _, cl := range clients {
			if cl.Type != 0 {
				continue
			}
			result = append(result, ClientInfo{
				Nickname:   cl.Nickname,
				DatabaseID: strconv.Itoa(cl.DatabaseID),
				ChannelID:  strconv.Itoa(cl.ChannelID),
			})
		}
		return nil
	})
	return result, err
}

// ListGroupMemberDbids returns the database ids of the clients in a group.
func (m *Manager) ListGroupMemberDbids(ctx context.Context, sgid string) (map[string]struct{}, error) {
	var members map[string]struct{}
	err := m.exec(ctx, func(c *ts3.Client) error {
		got, err := m.groupMemberDbids(ctx, c, sgid)
		if err != nil {
			return err
		}
		members = got
		return nil
	})
	return members, err
}

// ListGroupsByPrefix returns server groups whose name starts with prefix,
// excluding excludeSgid.
func (m *Manager) ListGroupsByPrefix(ctx context.Context, prefix, excludeSgid string) ([]ServerGroupRef, error) {
	var result []ServerGroupRef
	err := m.exec(ctx, func(c *ts3.Client) error {
		result = nil
		if err := m.throttle(ctx); err != nil {
			return err
		}
		groups, err := c.Server.GroupList()
		if err != nil {
			return err
		}
		for _, g := range groups {
			sgid := strconv.Itoa(g.ID)
			if sgid == excludeSgid {
				continue
			}
			if strings.HasPrefix(g.Name, prefix) {
				result = append(result, ServerGroupRef{SGID: sgid, Name: g.Name})
			}
		}
		return nil
	})
	return result, err
}

// ListTwitchGroups returns the pre-assigned `twitch.tv/<username>` groups, each
// resolved to its (lowercased) username and current member database ids. Groups
// with an empty username (a bare prefix) are skipped.
func (m *Manager) ListTwitchGroups(ctx context.Context, prefix string) ([]TwitchGroupRef, error) {
	var result []TwitchGroupRef
	err := m.exec(ctx, func(c *ts3.Client) error {
		result = nil
		if err := m.throttle(ctx); err != nil {
			return err
		}
		groups, err := c.Server.GroupList()
		if err != nil {
			return err
		}
		for _, g := range groups {
			if !strings.HasPrefix(g.Name, prefix) {
				continue
			}
			username := strings.ToLower(strings.TrimSpace(g.Name[len(prefix):]))
			if username == "" {
				continue
			}
			sgid := strconv.Itoa(g.ID)
			members, err := m.groupMemberDbids(ctx, c, sgid)
			if err != nil {
				return err
			}
			result = append(result, TwitchGroupRef{
				SGID:     sgid,
				Name:     g.Name,
				Username: username,
				Members:  members,
			})
		}
		return nil
	})
	return result, err
}

// AddClientToGroup assigns a client (by database id) to a server group.
func (m *Manager) AddClientToGroup(ctx context.Context, databaseID, sgid string) error {
	return m.exec(ctx, func(c *ts3.Client) error {
		return m.addClient(ctx, c, databaseID, sgid)
	})
}

// RemoveClientFromGroup removes a client (by database id) from a server group.
func (m *Manager) RemoveClientFromGroup(ctx context.Context, databaseID, sgid string) error {
	return m.exec(ctx, func(c *ts3.Client) error {
		if err := m.throttle(ctx); err != nil {
			return err
		}
		_, err := c.ExecCmd(ts3.NewCmd("servergroupdelclient").WithArgs(
			ts3.NewArg("sgid", sgid),
			ts3.NewArg("cldbid", databaseID),
		))
		return err
	})
}

// CreateGroupAndAssign creates a regular server group and assigns the client.
func (m *Manager) CreateGroupAndAssign(ctx context.Context, name, databaseID string) (string, error) {
	var sgid string
	err := m.exec(ctx, func(c *ts3.Client) error {
		created, err := m.createServerGroup(ctx, c, name)
		if err != nil {
			return err
		}
		sgid = created
		if err := m.addClient(ctx, c, databaseID, sgid); err != nil {
			return err
		}
		logger.Log.Info("Created group and assigned client", "name", name, "dbid", databaseID)
		return nil
	})
	return sgid, err
}

// DeleteGroup deletes a server group, force-removing any members.
func (m *Manager) DeleteGroup(ctx context.Context, group ServerGroupRef) error {
	return m.exec(ctx, func(c *ts3.Client) error {
		if err := m.throttle(ctx); err != nil {
			return err
		}
		if _, err := c.ExecCmd(ts3.NewCmd("servergroupdel").WithArgs(
			ts3.NewArg("sgid", group.SGID),
			ts3.NewArg("force", 1),
		)); err != nil {
			return err
		}
		logger.Log.Info("Deleted group", "name", group.Name)
		return nil
	})
}

// SendChannelMessage sends a text message to a channel. Channel messages always
// go to the query client's current channel, so it is moved first if needed.
func (m *Manager) SendChannelMessage(ctx context.Context, channelID, text string) error {
	return m.exec(ctx, func(c *ts3.Client) error {
		if err := m.throttle(ctx); err != nil {
			return err
		}
		info, err := c.Whoami()
		if err != nil {
			return err
		}
		if strconv.Itoa(info.ClientChannelID) != channelID {
			if err := m.throttle(ctx); err != nil {
				return err
			}
			if _, err := c.ExecCmd(ts3.NewCmd("clientmove").WithArgs(
				ts3.NewArg("clid", info.ClientID),
				ts3.NewArg("cid", channelID),
			)); err != nil {
				return err
			}
		}
		if err := m.throttle(ctx); err != nil {
			return err
		}
		_, err = c.ExecCmd(ts3.NewCmd("sendtextmessage").WithArgs(
			ts3.NewArg("targetmode", textMessageTargetModeChannel),
			ts3.NewArg("target", channelID),
			ts3.NewArg("msg", text),
		))
		return err
	})
}

// Disconnect closes the ServerQuery connection.
func (m *Manager) Disconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client == nil {
		return nil
	}
	return m.client.Close()
}

// --- low-level command helpers (operate on a raw client) ---

func (m *Manager) createServerGroup(ctx context.Context, c *ts3.Client, name string) (string, error) {
	if err := m.throttle(ctx); err != nil {
		return "", err
	}
	var resp struct {
		SGID int `ms:"sgid"`
	}
	if _, err := c.ExecCmd(ts3.NewCmd("servergroupadd").WithArgs(
		ts3.NewArg("name", name),
		ts3.NewArg("type", serverGroupTypeRegular),
	).WithResponse(&resp)); err != nil {
		return "", err
	}
	return strconv.Itoa(resp.SGID), nil
}

func (m *Manager) addServerGroupPerm(ctx context.Context, c *ts3.Client, sgid, permsid string, value int) error {
	if err := m.throttle(ctx); err != nil {
		return err
	}
	_, err := c.ExecCmd(ts3.NewCmd("servergroupaddperm").WithArgs(
		ts3.NewArg("sgid", sgid),
		ts3.NewArg("permsid", permsid),
		ts3.NewArg("permvalue", value),
		ts3.NewArg("permnegated", permNotNegated),
		ts3.NewArg("permskip", permNoSkip),
	))
	return err
}

func (m *Manager) addClient(ctx context.Context, c *ts3.Client, databaseID, sgid string) error {
	if err := m.throttle(ctx); err != nil {
		return err
	}
	_, err := c.ExecCmd(ts3.NewCmd("servergroupaddclient").WithArgs(
		ts3.NewArg("sgid", sgid),
		ts3.NewArg("cldbid", databaseID),
	))
	return err
}

type groupClientRow struct {
	CLDBID int `ms:"cldbid"`
}

func (m *Manager) groupMemberDbids(ctx context.Context, c *ts3.Client, sgid string) (map[string]struct{}, error) {
	if err := m.throttle(ctx); err != nil {
		return nil, err
	}
	var rows []*groupClientRow
	if _, err := c.ExecCmd(ts3.NewCmd("servergroupclientlist").WithArgs(
		ts3.NewArg("sgid", sgid),
	).WithResponse(&rows)); err != nil {
		if isEmptyResult(err) {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}
	members := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		members[strconv.Itoa(r.CLDBID)] = struct{}{}
	}
	return members, nil
}
