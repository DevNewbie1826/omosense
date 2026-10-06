package listen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

// Logger is global and the SDK carries no session identity. Install one
// immutable, silent dispatcher, and guard the bots active in this process
// by name so the same bot never opens two gateway sessions.
var discordActive struct {
	sync.Mutex
	bots map[string]*outage
}

func init() {
	discordActive.bots = map[string]*outage{}
	discordgo.Logger = func(_, _ int, format string, args ...any) {
		discordActive.Lock()
		defer discordActive.Unlock()
		// One process-global logger, many sessions: every active outage
		// sees the line and keeps its own close code.
		for _, o := range discordActive.bots {
			o.capture(format, args...)
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

// discord runs one gateway session per configured bot, concurrently; the
// first failure cancels the rest, exactly like the telegram fan-out.
func (s src) discord(ctx context.Context, sink core.Sink) error {
	if len(s.cfg.Profile.Discord.Bots) == 0 {
		return errors.New("discord source registered without bots")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(s.cfg.Profile.Discord.Bots))
	for _, bot := range s.cfg.Profile.Discord.Bots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.discordBot(ctx, sink, bot); err != nil {
				errs <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

// discordBot owns one bot's gateway session for the whole process: the
// registry entry makes a second session for the same bot name impossible.
func (s src) discordBot(ctx context.Context, sink core.Sink, bot string) error {
	cl := s.clock
	if cl == nil {
		cl = wallClock{}
	}
	o := &outage{clock: cl, sink: sink, bot: bot}
	discordActive.Lock()
	if _, active := discordActive.bots[bot]; active {
		discordActive.Unlock()
		return fmt.Errorf("discord bot %s already active in this process", bot)
	}
	discordActive.bots[bot] = o
	discordActive.Unlock()
	defer func() {
		o.stop()
		discordActive.Lock()
		delete(discordActive.bots, bot)
		discordActive.Unlock()
	}()
	token, err := core.Cred(os.Getenv("HOME"), "discordbot-credentials.json", bot)
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
	session.Identify.Properties.OS = runtime.GOOS
	session.Identify.Properties.Browser = "omosense"
	session.Identify.Properties.Device = "omosense"
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
			if err := s.dcHandle(ctx, sink, bot, e.RawData); err != nil && ctx.Err() == nil {
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
