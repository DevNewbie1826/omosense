package listen

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

// Logger is global and Op7 has no session identity. Install one immutable,
// silent dispatcher, and guard the single active source for this process.
var discordActive struct {
	sync.Mutex
	outage *outage
}

func init() {
	discordgo.Logger = func(_, _ int, format string, args ...any) {
		discordActive.Lock()
		defer discordActive.Unlock()
		if discordActive.outage != nil {
			discordActive.outage.capture(format, args...)
		}
	}
}

type gatewayTransport struct {
	ctx  context.Context
	base string
}

func (t gatewayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(t.ctx)
	if t.base != "" {
		target, err := url.Parse(t.base + "/gateway")
		if err != nil {
			return nil, err
		}
		req.URL = target
	}
	return http.DefaultTransport.RoundTrip(req)
}

type gatewaySocket struct {
	mu   sync.Mutex
	conn net.Conn
}

func (g *gatewaySocket) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		g.conn.Close()
	}
}

func (s src) discord(ctx context.Context, sink core.Sink) error {
	cl := s.clock
	if cl == nil {
		cl = wallClock{}
	}
	o := &outage{clock: cl, sink: sink}
	discordActive.Lock()
	if discordActive.outage != nil {
		discordActive.Unlock()
		return errors.New("discord source already active in this process")
	}
	discordActive.outage = o
	discordActive.Unlock()
	defer func() {
		o.stop()
		discordActive.Lock()
		discordActive.outage = nil
		discordActive.Unlock()
	}()
	if len(s.cfg.Profile.Discord.Bots) == 0 {
		return errors.New("discord source registered without bots")
	}
	token, err := core.Cred(os.Getenv("HOME"), "discordbot-credentials.json", s.cfg.Profile.Discord.Bots[0])
	if err != nil {
		return err
	}
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return err
	}
	session.StateEnabled = false
	session.SyncEvents = true
	session.LogLevel = discordgo.LogInformational
	// The source owns cancellable, injected backoff instead of discordgo's
	// unbounded reconnect loop. Reusing this session preserves resume state.
	session.ShouldReconnectOnError = false
	session.Identify.Intents = 1 | 512 | 4096 | 32768
	session.Identify.Properties.OS = "darwin"
	session.Identify.Properties.Browser = "omomeow"
	session.Identify.Properties.Device = "omomeow"
	session.Client = &http.Client{Timeout: 20 * time.Second, Transport: gatewayTransport{ctx, strings.TrimRight(os.Getenv("OMOSENSE_DISCORD_API"), "/")}}
	socket := &gatewaySocket{}
	defer socket.close()
	session.Dialer = &websocket.Dialer{HandshakeTimeout: 20 * time.Second, NetDialContext: func(_ context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			socket.mu.Lock()
			socket.conn = conn
			if ctx.Err() != nil {
				conn.Close()
			}
			socket.mu.Unlock()
		}
		return conn, err
	}}
	// Closing the transport interrupts Open/ReadMessage without racing Close()
	// against discordgo's same-connection Op9 identify. Its reader owns Close.
	shutdownDone := make(chan struct{})
	stopShutdown := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			socket.close()
		case <-stopShutdown:
		}
	}()
	defer func() { close(stopShutdown); <-shutdownDone }()
	disconnected := make(chan struct{}, 1)
	session.AddHandler(func(_ *discordgo.Session, _ *discordgo.Connect) { o.connect() })
	session.AddHandler(func(_ *discordgo.Session, _ *discordgo.Disconnect) {
		if ctx.Err() == nil {
			o.disconnect()
		}
		select {
		case disconnected <- struct{}{}:
		default:
		}
	})
	session.AddHandler(func(_ *discordgo.Session, r *discordgo.Ready) {
		username := ""
		if r.User != nil {
			username = r.User.Username
		}
		o.ready(username, false)
	})
	session.AddHandler(func(_ *discordgo.Session, _ *discordgo.Resumed) { o.ready("", true) })
	var handlers sync.WaitGroup
	defer handlers.Wait()
	session.AddHandler(func(_ *discordgo.Session, e *discordgo.Event) {
		if e.Type != "MESSAGE_CREATE" {
			return
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			if err := s.dcHandle(ctx, sink, e.RawData); err != nil && ctx.Err() == nil {
				sink.Log("dc handle " + redact(err, token))
			}
		}()
	})
	for ctx.Err() == nil {
		if err := session.Open(); err != nil {
			o.capture("", err)
			if ctx.Err() == nil {
				o.disconnect()
			}
		} else {
			// Cancellation closes the socket; join its Disconnect callback
			// before releasing the process guard or waiting on message handlers.
			<-disconnected
		}
		if ctx.Err() != nil {
			break
		}
		if err := s.sleep(ctx, 5*time.Second); err != nil {
			break
		}
	}
	return nil
}
